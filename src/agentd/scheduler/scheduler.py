"""Scheduler — orchestration layer.

Makes decisions about turn formation, mailbox claiming, state transitions,
and close-subtree semantics. Does not execute backend processes directly.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from agentd.config import AgentDConfig
from agentd.protocol import (
    ROOT_SCOPE,
    ActorState,
    DeliverAs,
    EventType,
    PublicErrorCode,
    TurnOutcome,
    TurnState,
)
from agentd.store import Store

from .event_bus import EventBus

if TYPE_CHECKING:
    from agentd.runtime.runner import Runtime

logger = logging.getLogger(__name__)


def _public_error_code(turn: dict[str, Any] | None) -> str | None:
    if not turn:
        return None

    outcome = turn.get("outcome")
    error = str(turn.get("error") or "")

    if outcome in (TurnOutcome.CANCELED.value, TurnOutcome.INTERRUPTED.value):
        return PublicErrorCode.ACTOR_STOPPED.value
    if outcome != TurnOutcome.FAILED.value:
        return None
    if error == "no turn.end received":
        return PublicErrorCode.BACKEND_NO_TERMINAL_EVENT.value
    if error.startswith("exit code "):
        return PublicErrorCode.BACKEND_EXIT_NONZERO.value
    if "deadline exceeded" in error or "timed out" in error:
        return PublicErrorCode.BACKEND_TIMEOUT.value
    return PublicErrorCode.UNKNOWN_ERROR.value


class Scheduler:
    def __init__(self, store: Store, event_bus: EventBus, config: AgentDConfig):
        self.store = store
        self.event_bus = event_bus
        self.config = config
        self._runtime: Runtime | None = None
        # In-memory actor env (not persisted, lost on daemon restart)
        self._actor_env: dict[str, dict[str, str]] = {}
        # Track which message opened each turn (for acking on completion)
        self._turn_message: dict[str, str] = {}  # turn_id -> message_id
        # Env overlay for turns waiting dispatch (capacity-blocked)
        self._pending_env: dict[str, dict[str, str] | None] = {}
        # Turn-level env overlay (stored per message, applied when message opens a turn)
        self._message_env: dict[str, dict[str, str]] = {}
        # Concurrency tracking (counts actually dispatched turns, not pending)
        self._running_count = 0

    def set_runtime(self, runtime: Runtime) -> None:
        self._runtime = runtime

    # ------------------------------------------------------------------
    # Public operations
    # ------------------------------------------------------------------

    async def spawn(
        self,
        *,
        name: str | None,
        backend: str,
        parent_actor_id: str | None = None,
        backend_args: list[str] | None = None,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        checkpoint: bool | None = None,
        msg_type: str | None = None,
        msg_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        # Determine scope
        if parent_actor_id:
            parent = self.store.get_actor(parent_actor_id)
            if parent is None:
                raise SpawnError("not_found", f"parent actor not found: {parent_actor_id}")
            if parent["state"] == ActorState.CLOSED.value:
                raise SpawnError("actor_closed", f"parent actor is closed: {parent_actor_id}")
            scope_id = parent_actor_id
            # Depth check
            depth = self.store.actor_depth(parent_actor_id) + 1
            if depth > self.config.limits.max_depth:
                raise SpawnError("forbidden", f"max depth exceeded: {depth}")
            # Children count check
            count = self.store.count_children(parent_actor_id)
            if count >= self.config.limits.max_children_per_parent:
                raise SpawnError("forbidden", f"max children exceeded: {count}")
        else:
            scope_id = ROOT_SCOPE

        # Name uniqueness (null names skip check)
        if name is not None:
            existing = self.store.find_actor_by_name(name, scope_id)
            if existing:
                raise SpawnError(
                    "conflict",
                    f"actor name '{name}' already exists in scope {scope_id}",
                )

        # Checkpoint default: root=true, child=false
        if checkpoint is None:
            checkpoint = parent_actor_id is None
        checkpoint_val = {} if checkpoint else None

        # Determine initial state
        has_input = msg_type is not None
        initial_state = ActorState.IDLE

        actor = self.store.create_actor(
            name=name,
            scope_id=scope_id,
            backend=backend,
            parent_actor_id=parent_actor_id,
            backend_args=backend_args or [],
            cwd=cwd,
            state=initial_state,
            checkpoint=checkpoint_val,
        )
        actor_id = actor["actor_id"]

        # Store env in memory
        if env:
            self._actor_env[actor_id] = dict(env)

        # Emit actor.spawned event
        seq = self.store.append_event(
            actor_id,
            EventType.ACTOR_SPAWNED,
            {"actor_id": actor_id, "name": name, "backend": backend},
        )
        self._publish_event(
            actor_id,
            None,
            EventType.ACTOR_SPAWNED,
            {
                "actor_id": actor_id,
                "name": name,
                "backend": backend,
            },
            seq,
        )

        # If initial input provided, add to mailbox and schedule
        turn_info = None
        if has_input:
            self.store.add_message(actor_id, msg_type, msg_payload or {})
            turn_info = await self._try_open_turn(actor_id)

        result: dict[str, Any] = {
            "actor_id": actor_id,
            "state": turn_info["actor_state"] if turn_info else "idle",
            "current_turn": turn_info["turn_summary"] if turn_info else None,
            "event_seq": seq,
        }
        return result

    async def emit(
        self,
        *,
        actor_id: str,
        msg_type: str,
        msg_payload: dict[str, Any],
        env: dict[str, str] | None = None,
        deliver_as: DeliverAs = DeliverAs.AUTO,
    ) -> dict[str, Any]:
        actor = self.store.get_actor(actor_id)
        if actor is None:
            raise SchedulerError("not_found", f"actor not found: {actor_id}")
        if actor["state"] == ActorState.CLOSED.value:
            raise SchedulerError("actor_closed", f"actor is closed: {actor_id}")

        state = ActorState(actor["state"])
        resolved_mode = deliver_as

        if deliver_as == DeliverAs.AUTO:
            if state == ActorState.IDLE:
                resolved_mode = DeliverAs.FOLLOW_UP
            elif state == ActorState.ACTIVE:
                # v1: no backend supports steer, always follow_up
                resolved_mode = DeliverAs.FOLLOW_UP
        elif deliver_as == DeliverAs.STEER:
            # v1: no backend supports steer
            raise SchedulerError("conflict", "steer not supported by backend")

        # Store message in mailbox
        msg = self.store.add_message(actor_id, msg_type, msg_payload)

        # Store turn-level env overlay in message metadata
        # (will be captured in turn.opened input snapshot)
        if env:
            # We track env overlay per message for when it opens a turn
            self._message_env[msg["message_id"]] = dict(env)

        # Try to wake the actor
        woke = False
        if state == ActorState.IDLE:
            turn_info = await self._try_open_turn(actor_id)
            woke = turn_info is not None

        seq = self.store.get_max_seq()
        return {
            "actor_id": actor_id,
            "delivery_mode": resolved_mode.value,
            "woke": woke,
            "event_seq": seq,
        }

    async def stop(self, actor_id: str) -> dict[str, Any]:
        """Soft stop: interrupt current turn, actor → idle."""
        actor = self.store.get_actor(actor_id)
        if actor is None:
            raise SchedulerError("not_found", f"actor not found: {actor_id}")
        if actor["state"] == ActorState.CLOSED.value:
            raise SchedulerError("actor_closed", f"actor is closed: {actor_id}")

        changed = 0
        if actor["state"] == ActorState.ACTIVE.value:
            turn = self.store.get_active_turn(actor_id)
            if turn:
                if turn["state"] == TurnState.RUNNING.value and self._runtime:
                    # Running turn: runtime handles end_turn + event via on_turn_completed
                    await self._runtime.stop_turn(turn["turn_id"])
                else:
                    # Pending turn (or no runtime): end directly in store
                    self.store.end_turn_atomic(
                        turn["turn_id"],
                        actor_id,
                        self._turn_message.pop(turn["turn_id"], None),
                        outcome=TurnOutcome.INTERRUPTED,
                    )
                    self._pending_env.pop(turn["turn_id"], None)
                    seq = self.store.append_event(
                        actor_id,
                        EventType.TURN_END,
                        {
                            "turn_id": turn["turn_id"],
                            "outcome": TurnOutcome.INTERRUPTED.value,
                            "result": None,
                            "error": None,
                        },
                        turn_id=turn["turn_id"],
                    )
                    self._publish_event(
                        actor_id,
                        turn["turn_id"],
                        EventType.TURN_END,
                        {
                            "turn_id": turn["turn_id"],
                            "outcome": TurnOutcome.INTERRUPTED.value,
                            "result": None,
                            "error": None,
                        },
                        seq,
                    )
                changed = 1

        actor = self.store.get_actor(actor_id)
        return {
            "actor_id": actor_id,
            "state": actor["state"] if actor else "idle",
            "changed_count": changed,
        }

    async def close(self, actor_id: str) -> dict[str, Any]:
        """Hard close: cancel everything, close actor + subtree."""
        actor = self.store.get_actor(actor_id)
        if actor is None:
            raise SchedulerError("not_found", f"actor not found: {actor_id}")
        if actor["state"] == ActorState.CLOSED.value:
            return {"actor_id": actor_id, "state": "closed", "changed_count": 0}

        changed = await self._close_subtree(actor_id)
        return {
            "actor_id": actor_id,
            "state": "closed",
            "changed_count": changed,
        }

    async def wait(
        self,
        actor_id: str,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Wait for actor to reach idle or closed state."""
        actor = self.store.get_actor(actor_id)
        if actor is None:
            raise SchedulerError("not_found", f"actor not found: {actor_id}")

        if actor["state"] in (ActorState.IDLE.value, ActorState.CLOSED.value):
            return self._wait_result(actor_id)

        # Poll for state change
        deadline = asyncio.get_event_loop().time() + (timeout or 3600)
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                raise SchedulerError("timeout", "wait timed out")
            await asyncio.sleep(min(0.1, remaining))
            actor = self.store.get_actor(actor_id)
            if actor is None or actor["state"] in (ActorState.IDLE.value, ActorState.CLOSED.value):
                return self._wait_result(actor_id)

    def status(self, actor_id: str) -> dict[str, Any]:
        actor = self.store.get_actor(actor_id)
        if actor is None:
            raise SchedulerError("not_found", f"actor not found: {actor_id}")
        current_turn = self.store.get_active_turn(actor_id)
        last_turn = self.store.get_last_turn(actor_id)
        return {
            "actor": actor,
            "current_turn": current_turn,
            "last_turn": last_turn,
        }

    # ------------------------------------------------------------------
    # Turn lifecycle (called by runtime)
    # ------------------------------------------------------------------

    def on_turn_started(self, turn_id: str, pid: int) -> None:
        """Called by runtime when backend process starts."""
        turn = self.store.get_turn(turn_id)
        if turn is None:
            return
        actor_id = turn["actor_id"]
        self.store.transition_turn(turn_id, TurnState.RUNNING, exec_pid=pid)
        seq = self.store.append_event(
            actor_id,
            EventType.TURN_STARTED,
            {"turn_id": turn_id, "exec_pid": pid},
            turn_id=turn_id,
        )
        self._publish_event(
            actor_id,
            turn_id,
            EventType.TURN_STARTED,
            {
                "turn_id": turn_id,
                "exec_pid": pid,
            },
            seq,
        )

    async def on_turn_completed(
        self,
        turn_id: str,
        *,
        outcome: TurnOutcome,
        result: str | None = None,
        error: str | None = None,
    ) -> None:
        """Called by runtime when a turn completes."""
        turn = self.store.get_turn(turn_id)
        if turn is None or turn["state"] == TurnState.ENDED.value:
            return

        actor_id = turn["actor_id"]
        message_id = self._turn_message.pop(turn_id, None)

        # End turn atomically
        self.store.end_turn_atomic(
            turn_id,
            actor_id,
            message_id,
            outcome=outcome,
            result=result,
            error=error,
        )
        self._running_count = max(0, self._running_count - 1)

        # Emit turn.end event
        seq = self.store.append_event(
            actor_id,
            EventType.TURN_END,
            {"turn_id": turn_id, "outcome": outcome.value, "result": result, "error": error},
            turn_id=turn_id,
        )
        self._publish_event(
            actor_id,
            turn_id,
            EventType.TURN_END,
            {
                "turn_id": turn_id,
                "outcome": outcome.value,
                "result": result,
                "error": error,
            },
            seq,
        )

        # Notify parent BEFORE the child's own wakeup chain so the bus event
        # order is causal: child.turn.end → parent.mailbox → child.turn.opened
        # (next). Otherwise a chatty child can also starve its parent of a
        # dispatch slot under tight concurrency limits.
        #
        # In v1 single-loop asyncio nothing else mutates this child's row
        # while we await the parent emit, so the actor snapshot stays valid
        # for the wakeup-chain check below.
        actor = self.store.get_actor(actor_id)
        if actor and actor.get("parent_actor_id"):
            await self._notify_parent_turn_completed(
                parent_actor_id=actor["parent_actor_id"],
                child=actor,
                turn_id=turn_id,
                outcome=outcome,
                result=result,
                error=error,
            )

        # Check for next queued message (child's own wakeup chain)
        if (
            actor
            and actor["state"] == ActorState.IDLE.value
            and self.store.count_queued(actor_id) > 0
        ):
            await self._try_open_turn(actor_id)

        # Global wakeup: try to schedule other actors blocked by concurrency
        await self._try_schedule_waiting()

    def publish_event(
        self,
        actor_id: str,
        turn_id: str | None,
        event_type: str,
        payload: dict[str, Any],
    ) -> int:
        """Append event to store and publish to bus. Returns seq."""
        seq = self.store.append_event(
            actor_id,
            event_type,
            payload,
            turn_id=turn_id,
        )
        self._publish_event(actor_id, turn_id, event_type, payload, seq)
        return seq

    async def _notify_parent_turn_completed(
        self,
        *,
        parent_actor_id: str,
        child: dict[str, Any],
        turn_id: str,
        outcome: TurnOutcome,
        result: str | None,
        error: str | None,
    ) -> None:
        """Auto-emit env.turn_completed to the parent's mailbox on child turn.end.

        Convention path for the supervisor pattern: the daemon already knows
        the parent-child relationship and the moment a turn ends, so it can
        wake the parent without going through user-defined event triggers.
        Parents stay in control of how they react (skill prompt drives it);
        the daemon's job is just to deliver the signal.

        Heuristic: INTERRUPTED / CANCELED outcomes are suppressed because they
        usually originate from the parent (or a user via the parent) calling
        stop/close, so the parent already knows. This is imperfect for the
        grandparent-closes-subtree case (parent loses a signal); revisit if a
        real workflow needs that distinction.

        Delivery is best-effort: any exception is swallowed so the rest of
        on_turn_completed (notably _try_schedule_waiting) keeps running. The
        child's turn.end is already persisted; missing a wakeup is recoverable
        by the supervisor checking `agentd ps`, missing global scheduling is
        not.
        """
        if outcome in (TurnOutcome.INTERRUPTED, TurnOutcome.CANCELED):
            return

        payload: dict[str, Any] = {
            "actor_id": child["actor_id"],
            "actor_name": child.get("name"),
            "turn_id": turn_id,
            "outcome": outcome.value,
        }
        if result is not None:
            payload["result"] = result
        if error is not None:
            payload["error"] = error

        try:
            await self.emit(
                actor_id=parent_actor_id,
                msg_type="env.turn_completed",
                msg_payload=payload,
            )
        except SchedulerError as exc:
            if exc.error_type == "actor_closed":
                # Expected race: parent closed between turn.end and notify.
                logger.debug(
                    "parent %s closed; dropping turn_completed notification for %s",
                    parent_actor_id,
                    child["actor_id"],
                )
            else:
                logger.warning(
                    "unexpected SchedulerError notifying parent=%s child=%s: %s",
                    parent_actor_id,
                    child["actor_id"],
                    exc,
                )
        except Exception:
            logger.exception(
                "failed to notify parent=%s about child=%s",
                parent_actor_id,
                child["actor_id"],
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _try_open_turn(self, actor_id: str) -> dict[str, Any] | None:
        """Open a new turn for the actor.

        Always creates the turn + claims message (actor → active).
        Dispatches to runtime only if capacity is available; otherwise
        the turn stays ``pending`` and is picked up by
        ``_try_schedule_waiting`` when a slot opens.
        """
        result = self.store.open_turn_atomic(actor_id)
        if result is None:
            return None

        turn, msg = result
        turn_id = turn["turn_id"]
        message_id = msg["message_id"]

        # Track message for acking on completion
        self._turn_message[turn_id] = message_id

        # Build input snapshot (the claimed message is the turn input)
        input_messages = [msg]

        # Get env overlay if any
        env_overlay = self._message_env.pop(message_id, None)

        input_snapshot = {
            "messages": input_messages,
            "env": env_overlay,
        }

        # Emit turn.opened event
        seq = self.store.append_event(
            actor_id,
            EventType.TURN_OPENED,
            {"turn_id": turn_id, "input": input_snapshot},
            turn_id=turn_id,
        )
        self._publish_event(
            actor_id,
            turn_id,
            EventType.TURN_OPENED,
            {
                "turn_id": turn_id,
                "input": input_snapshot,
            },
            seq,
        )

        # Dispatch to runtime if capacity available
        if self._running_count < self.config.limits.max_total_workers:
            self._dispatch_to_runtime(turn_id, actor_id, input_messages, env_overlay)
        else:
            self._pending_env[turn_id] = env_overlay
            logger.info("capacity full, turn %s pending for actor %s", turn_id, actor_id)

        return {
            "actor_state": "active",
            "turn_summary": {"turn_id": turn_id, "state": "pending"},
        }

    def _dispatch_to_runtime(
        self,
        turn_id: str,
        actor_id: str,
        input_messages: list[dict[str, Any]],
        env_overlay: dict[str, str] | None = None,
    ) -> None:
        """Send a pending turn to the runtime for execution."""
        if not self._runtime:
            return
        actor = self.store.get_actor(actor_id)
        if actor is None:
            # Actors are never physically deleted (only marked CLOSED), so this
            # is unreachable under normal operation. Treat as a hard scheduler
            # invariant violation: fail the turn rather than leaving it pending.
            logger.error("actor %s missing at dispatch for turn %s", actor_id, turn_id)
            asyncio.create_task(
                self.on_turn_completed(
                    turn_id,
                    outcome=TurnOutcome.FAILED,
                    error=f"actor {actor_id} not found",
                )
            )
            return
        actor_env = self._actor_env.get(actor_id, {})
        merged_env = {**actor_env, **(env_overlay or {})}
        self._runtime.prepare_turn(turn_id)
        asyncio.create_task(
            self._runtime.execute_turn(
                turn_id=turn_id,
                actor=actor,
                input_messages=input_messages,
                env=merged_env,
            )
        )
        self._running_count += 1

    async def _close_subtree(self, actor_id: str) -> int:
        """Recursively close actor and all descendants."""
        actors = self.store.list_subtree(actor_id, include_root=True)
        changed = 0
        for a in actors:
            if a["state"] == ActorState.CLOSED.value:
                continue
            aid = a["actor_id"]
            # Cancel active turn
            if a["state"] == ActorState.ACTIVE.value:
                turn = self.store.get_active_turn(aid)
                if turn and self._runtime:
                    was_running = turn["state"] == TurnState.RUNNING.value
                    await self._runtime.cancel_turn(turn["turn_id"])
                    # Force end if runtime didn't complete it
                    turn = self.store.get_turn(turn["turn_id"])
                    if turn and turn["state"] != TurnState.ENDED.value:
                        self.store.end_turn_atomic(
                            turn["turn_id"],
                            aid,
                            self._turn_message.pop(turn["turn_id"], None),
                            outcome=TurnOutcome.CANCELED,
                        )
                        if was_running:
                            self._running_count = max(0, self._running_count - 1)
                    self._pending_env.pop(turn["turn_id"], None) if turn else None
                elif turn:
                    was_running = turn["state"] == TurnState.RUNNING.value
                    self.store.end_turn_atomic(
                        turn["turn_id"],
                        aid,
                        self._turn_message.pop(turn["turn_id"], None),
                        outcome=TurnOutcome.CANCELED,
                    )
                    if was_running:
                        self._running_count = max(0, self._running_count - 1)
                    self._pending_env.pop(turn["turn_id"], None)
            # Close actor
            if a["state"] != ActorState.CLOSED.value:
                self.store.transition_actor(aid, ActorState.CLOSED)
                # Delete triggers
                self.store.delete_triggers_for_actor(aid)
                # Emit event
                seq = self.store.append_event(
                    aid,
                    EventType.ACTOR_CLOSED,
                    {"reason": "closed"},
                )
                self._publish_event(
                    aid,
                    None,
                    EventType.ACTOR_CLOSED,
                    {
                        "reason": "closed",
                    },
                    seq,
                )
                changed += 1
            # Clean up env
            self._actor_env.pop(aid, None)
        return changed

    def _wait_result(self, actor_id: str) -> dict[str, Any]:
        actor = self.store.get_actor(actor_id)
        last_turn = self.store.get_last_turn(actor_id) if actor else None
        return {
            "actor": actor,
            "result": last_turn.get("result") if last_turn else None,
            "error": last_turn.get("error") if last_turn else None,
            "error_code": _public_error_code(last_turn),
            "turn_id": last_turn.get("turn_id") if last_turn else None,
        }

    def _recover_env_overlay(self, turn_id: str) -> dict[str, str] | None:
        """Recover env overlay from persisted turn.opened event (for reconcile)."""
        events = self.store.list_events_by_turn(turn_id, limit=1)
        for ev in events:
            if ev["event_type"] == EventType.TURN_OPENED:
                inp = ev.get("payload", {}).get("input", {})
                return inp.get("env")
        return None

    def _publish_event(
        self,
        actor_id: str,
        turn_id: str | None,
        event_type: str,
        payload: dict[str, Any],
        seq: int,
    ) -> None:
        self.event_bus.publish(
            {
                "seq": seq,
                "actor_id": actor_id,
                "turn_id": turn_id,
                "event_type": event_type,
                "payload": payload,
            }
        )

    # ------------------------------------------------------------------
    # Startup reconciliation
    # ------------------------------------------------------------------

    async def reconcile(self) -> None:
        """Reconcile stale state after daemon restart."""
        import os
        import signal

        # 1. Kill orphan processes from running turns
        for turn in self.store.list_running_turns():
            pid = turn.get("exec_pid")
            if pid:
                try:
                    os.kill(pid, 0)  # Check if alive
                    os.kill(pid, signal.SIGTERM)
                    logger.info("killed orphan process pid=%d for turn=%s", pid, turn["turn_id"])
                except OSError:
                    pass

        # 2. Fail running turns (recover claimed message for acking)
        for turn in self.store.list_running_turns():
            actor_id = turn["actor_id"]
            claimed = self.store.get_claimed_message(actor_id)
            message_id = claimed["message_id"] if claimed else None
            self.store.end_turn_atomic(
                turn["turn_id"],
                actor_id,
                message_id,
                outcome=TurnOutcome.FAILED,
                error="daemon restarted",
            )
            self.store.append_event(
                actor_id,
                EventType.TURN_END,
                {"turn_id": turn["turn_id"], "outcome": "failed", "error": "daemon restarted"},
                turn_id=turn["turn_id"],
            )
            logger.info("failed orphan turn=%s actor=%s", turn["turn_id"], actor_id)

        # 3. Reschedule pending turns (recover claimed message, track for acking)
        for turn in self.store.list_pending_turns():
            actor_id = turn["actor_id"]
            actor = self.store.get_actor(actor_id)
            if actor and actor["state"] == ActorState.ACTIVE.value:
                claimed = self.store.get_claimed_message(actor_id)
                if claimed:
                    self._turn_message[turn["turn_id"]] = claimed["message_id"]
                input_messages = [claimed] if claimed else []
                # Recover env overlay from turn.opened event
                env_overlay = self._recover_env_overlay(turn["turn_id"])
                # Dispatch if capacity available; otherwise track for later
                if self._running_count < self.config.limits.max_total_workers:
                    self._dispatch_to_runtime(
                        turn["turn_id"], actor_id, input_messages, env_overlay
                    )
                else:
                    self._pending_env[turn["turn_id"]] = env_overlay

        # 4. Wakeup idle actors with queued messages
        for actor in self.store.list_idle_actors_with_queued():
            await self._try_open_turn(actor["actor_id"])

    # ------------------------------------------------------------------
    # Pending turn scheduling (for concurrency limit)
    # ------------------------------------------------------------------

    async def _try_schedule_waiting(self) -> None:
        """Dispatch pending turns and wake idle actors when capacity available."""
        if self._running_count >= self.config.limits.max_total_workers:
            return

        # 1. Dispatch capacity-blocked pending turns (tracked in _pending_env)
        for turn_id in list(self._pending_env.keys()):
            if self._running_count >= self.config.limits.max_total_workers:
                return
            turn = self.store.get_turn(turn_id)
            if turn is None or turn["state"] != TurnState.PENDING.value:
                self._pending_env.pop(turn_id, None)
                continue
            actor_id = turn["actor_id"]
            claimed = self.store.get_claimed_message(actor_id)
            input_messages = [claimed] if claimed else []
            env_overlay = self._pending_env.pop(turn_id, None)
            self._dispatch_to_runtime(turn_id, actor_id, input_messages, env_overlay)

        # 2. Open new turns for idle actors with queued messages
        for actor in self.store.list_idle_actors_with_queued():
            if self._running_count >= self.config.limits.max_total_workers:
                return
            await self._try_open_turn(actor["actor_id"])


class SchedulerError(Exception):
    def __init__(self, error_type: str, message: str):
        super().__init__(message)
        self.error_type = error_type


class SpawnError(SchedulerError):
    pass
