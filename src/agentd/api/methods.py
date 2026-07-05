"""RPC method dispatch and handler implementations."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from agentd.config import AgentDConfig
from agentd.protocol import (
    BUSINESS_ERROR,
    INVALID_PARAMS,
    METHOD_NOT_FOUND,
    CloseParams,
    EmitParams,
    ErrorType,
    ListParams,
    LogsParams,
    SpawnParams,
    StatusParams,
    StopParams,
    TriggerAddParams,
    TriggerLsParams,
    TriggerRmParams,
    WaitParams,
    make_error,
    make_result,
    make_stream_end,
    make_stream_event,
)
from agentd.scheduler.cron import (
    CRON_CHECK_INTERVAL,
    compute_next_fire,
    parse_at,
    parse_duration,
    to_utc_iso,
)
from agentd.scheduler.event_bus import EventBus, SlowConsumerError
from agentd.scheduler.scheduler import Scheduler, SchedulerError
from agentd.store import Store

logger = logging.getLogger(__name__)


class MethodDispatcher:
    def __init__(
        self,
        scheduler: Scheduler,
        store: Store,
        event_bus: EventBus,
        config: AgentDConfig,
    ):
        self.scheduler = scheduler
        self.store = store
        self.event_bus = event_bus
        self.config = config

        self._methods: dict[str, Any] = {
            "actor.spawn": self._actor_spawn,
            "actor.emit": self._actor_emit,
            "actor.stop": self._actor_stop,
            "actor.close": self._actor_close,
            "actor.wait": self._actor_wait,
            "actor.list": self._actor_list,
            "actor.logs": self._actor_logs,
            "actor.status": self._actor_status,
            "trigger.add": self._trigger_add,
            "trigger.ls": self._trigger_ls,
            "trigger.rm": self._trigger_rm,
            "daemon.status": self._daemon_status,
            "daemon.doctor": self._daemon_doctor,
        }

    async def dispatch(
        self,
        req_id: str,
        method: str,
        params: dict[str, Any],
        writer: asyncio.StreamWriter,
    ) -> None:
        handler = self._methods.get(method)
        if handler is None:
            resp = make_error(req_id, METHOD_NOT_FOUND, f"unknown method: {method}")
            await _write(writer, resp)
            return
        await handler(req_id, params, writer)

    # ------------------------------------------------------------------
    # Actor methods
    # ------------------------------------------------------------------

    async def _actor_spawn(
        self,
        req_id: str,
        params: dict,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            p = SpawnParams.model_validate(params)
        except Exception as e:
            await _write(
                writer, make_error(req_id, INVALID_PARAMS, str(e), ErrorType.INVALID_PARAMS)
            )
            return

        # Resolve backend
        backend = p.backend or self.config.default_backend
        backend_args = list(p.backend_args)
        cwd = p.cwd

        # Fall back to config directory when no cwd is specified.
        if not cwd and self.config.path:
            from pathlib import Path

            cwd = str(Path(self.config.path).parent)

        msg_input = p.msg_input

        try:
            result = await self.scheduler.spawn(
                name=p.name,
                backend=backend,
                parent_actor_id=p.parent_actor_id,
                backend_args=backend_args,
                cwd=cwd,
                env=p.env or None,
                checkpoint=p.checkpoint,
                msg_type=msg_input.type if msg_input else None,
                msg_payload=msg_input.payload if msg_input else None,
            )
            await _write(writer, make_result(req_id, result))
        except SchedulerError as e:
            await _write(
                writer,
                make_error(
                    req_id,
                    BUSINESS_ERROR,
                    str(e),
                    e.error_type,
                ),
            )

    async def _actor_emit(
        self,
        req_id: str,
        params: dict,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            p = EmitParams.model_validate(params)
        except Exception as e:
            await _write(
                writer, make_error(req_id, INVALID_PARAMS, str(e), ErrorType.INVALID_PARAMS)
            )
            return

        # Resolve actor reference
        actor = self.store.resolve_actor(p.actor)
        if actor is None:
            await _write(
                writer,
                make_error(
                    req_id,
                    BUSINESS_ERROR,
                    f"actor not found: {p.actor}",
                    ErrorType.NOT_FOUND,
                ),
            )
            return

        msg = p.msg_input
        try:
            result = await self.scheduler.emit(
                actor_id=actor["actor_id"],
                msg_type=msg.type,
                msg_payload=msg.payload,
                env=p.env or None,
                deliver_as=p.deliver_as,
            )
            await _write(writer, make_result(req_id, result))
        except SchedulerError as e:
            await _write(
                writer,
                make_error(
                    req_id,
                    BUSINESS_ERROR,
                    str(e),
                    e.error_type,
                ),
            )

    async def _actor_stop(
        self,
        req_id: str,
        params: dict,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            p = StopParams.model_validate(params)
        except Exception as e:
            await _write(
                writer, make_error(req_id, INVALID_PARAMS, str(e), ErrorType.INVALID_PARAMS)
            )
            return

        actor = self.store.resolve_actor(p.actor)
        if actor is None:
            await _write(
                writer,
                make_error(
                    req_id,
                    BUSINESS_ERROR,
                    f"actor not found: {p.actor}",
                    ErrorType.NOT_FOUND,
                ),
            )
            return

        try:
            result = await self.scheduler.stop(actor["actor_id"])
            await _write(writer, make_result(req_id, result))
        except SchedulerError as e:
            await _write(
                writer,
                make_error(
                    req_id,
                    BUSINESS_ERROR,
                    str(e),
                    e.error_type,
                ),
            )

    async def _actor_close(
        self,
        req_id: str,
        params: dict,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            p = CloseParams.model_validate(params)
        except Exception as e:
            await _write(
                writer, make_error(req_id, INVALID_PARAMS, str(e), ErrorType.INVALID_PARAMS)
            )
            return

        actor = self.store.resolve_actor(p.actor)
        if actor is None:
            await _write(
                writer,
                make_error(
                    req_id,
                    BUSINESS_ERROR,
                    f"actor not found: {p.actor}",
                    ErrorType.NOT_FOUND,
                ),
            )
            return

        try:
            result = await self.scheduler.close(actor["actor_id"])
            await _write(writer, make_result(req_id, result))
        except SchedulerError as e:
            await _write(
                writer,
                make_error(
                    req_id,
                    BUSINESS_ERROR,
                    str(e),
                    e.error_type,
                ),
            )

    async def _actor_wait(
        self,
        req_id: str,
        params: dict,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            p = WaitParams.model_validate(params)
        except Exception as e:
            await _write(
                writer, make_error(req_id, INVALID_PARAMS, str(e), ErrorType.INVALID_PARAMS)
            )
            return

        actor = self.store.resolve_actor(p.actor)
        if actor is None:
            await _write(
                writer,
                make_error(
                    req_id,
                    BUSINESS_ERROR,
                    f"actor not found: {p.actor}",
                    ErrorType.NOT_FOUND,
                ),
            )
            return

        actor_id = actor["actor_id"]

        if p.progress:
            # Subscribe FIRST to buffer live events during replay
            sub = await self.event_bus.subscribe(
                actor_id=actor_id,
                since_seq=p.since_seq,
            )
            try:
                # Replay recent events (limited window; full history via logs)
                events = self.store.list_events(
                    actor_id,
                    since_seq=p.since_seq,
                    limit=20,
                )
                last_sent_seq = p.since_seq
                for ev in events:
                    await _write(writer, make_stream_event(req_id, ev))
                    last_sent_seq = ev["seq"]

                # Stream live events (deduplicate against replay)
                deadline = asyncio.get_event_loop().time() + p.timeout if p.timeout else None
                while True:
                    # Check if actor is done
                    current = self.store.get_actor(actor_id)
                    if current and current["state"] in ("idle", "closed"):
                        break

                    remaining = None
                    if deadline:
                        remaining = deadline - asyncio.get_event_loop().time()
                        if remaining <= 0:
                            await _write(
                                writer,
                                make_error(
                                    req_id,
                                    BUSINESS_ERROR,
                                    "wait timed out",
                                    ErrorType.TIMEOUT,
                                ),
                            )
                            return

                    try:
                        event = await asyncio.wait_for(
                            sub.__anext__(),
                            timeout=min(remaining or 1.0, 1.0),
                        )
                        if event.get("seq", 0) <= last_sent_seq:
                            continue
                        await _write(writer, make_stream_event(req_id, event))
                    except StopAsyncIteration:
                        break
                    except TimeoutError:
                        continue
                    except SlowConsumerError:
                        await _write(
                            writer,
                            make_error(
                                req_id,
                                BUSINESS_ERROR,
                                "slow consumer",
                                ErrorType.SLOW_CONSUMER,
                                data={"resume_seq": self.store.get_max_seq()},
                            ),
                        )
                        return
            finally:
                await self.event_bus.unsubscribe(sub)

            # Send final result
            result = self.scheduler._wait_result(actor_id)
            await _write(writer, make_stream_end(req_id, result))
        else:
            # Blocking mode
            try:
                result = await self.scheduler.wait(
                    actor_id,
                    timeout=p.timeout,
                )
                await _write(writer, make_result(req_id, result))
            except SchedulerError as e:
                await _write(
                    writer,
                    make_error(
                        req_id,
                        BUSINESS_ERROR,
                        str(e),
                        e.error_type,
                    ),
                )

    async def _actor_list(
        self,
        req_id: str,
        params: dict,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            p = ListParams.model_validate(params)
        except Exception as e:
            await _write(
                writer, make_error(req_id, INVALID_PARAMS, str(e), ErrorType.INVALID_PARAMS)
            )
            return

        if p.watch:
            # Streaming mode
            sub = await self.event_bus.subscribe()
            try:
                # Send initial snapshot
                actors = self.store.list_actors(
                    include_terminal=p.include_terminal,
                    limit=p.limit,
                )
                await _write(writer, make_stream_event(req_id, {"actors": actors}))

                # Stream updates
                while True:
                    try:
                        event = await asyncio.wait_for(sub.__anext__(), timeout=5.0)
                        etype = event.get("event_type", "")
                        if etype in (
                            "actor.spawned",
                            "actor.closed",
                            "turn.opened",
                            "turn.end",
                        ):
                            actors = self.store.list_actors(
                                include_terminal=p.include_terminal,
                                limit=p.limit,
                            )
                            await _write(
                                writer,
                                make_stream_event(
                                    req_id,
                                    {"actors": actors},
                                ),
                            )
                    except TimeoutError:
                        continue
                    except StopAsyncIteration:
                        break
                    except SlowConsumerError:
                        await _write(
                            writer,
                            make_error(
                                req_id,
                                BUSINESS_ERROR,
                                "slow consumer",
                                ErrorType.SLOW_CONSUMER,
                                data={"resume_seq": self.store.get_max_seq()},
                            ),
                        )
                        return
            except (ConnectionResetError, BrokenPipeError):
                pass
            finally:
                await self.event_bus.unsubscribe(sub)
        else:
            actors = self.store.list_actors(
                include_terminal=p.include_terminal,
                limit=p.limit,
            )
            await _write(writer, make_result(req_id, {"actors": actors}))

    async def _actor_logs(
        self,
        req_id: str,
        params: dict,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            p = LogsParams.model_validate(params)
        except Exception as e:
            await _write(
                writer, make_error(req_id, INVALID_PARAMS, str(e), ErrorType.INVALID_PARAMS)
            )
            return

        actor = self.store.resolve_actor(p.actor)
        if actor is None:
            await _write(
                writer,
                make_error(
                    req_id,
                    BUSINESS_ERROR,
                    f"actor not found: {p.actor}",
                    ErrorType.NOT_FOUND,
                ),
            )
            return

        actor_id = actor["actor_id"]

        if p.follow:
            # Subscribe FIRST to buffer live events during replay
            sub = await self.event_bus.subscribe(
                actor_id=actor_id,
                since_seq=p.since_seq,
            )
            try:
                # Replay historical events
                events = self.store.list_events(
                    actor_id,
                    since_seq=p.since_seq,
                    limit=p.limit,
                )
                last_sent_seq = p.since_seq
                for ev in events:
                    await _write(writer, make_stream_event(req_id, ev))
                    last_sent_seq = ev["seq"]

                # Stream live events (deduplicate against replay)
                while True:
                    try:
                        event = await asyncio.wait_for(sub.__anext__(), timeout=5.0)
                        if event.get("seq", 0) <= last_sent_seq:
                            continue
                        await _write(writer, make_stream_event(req_id, event))
                    except TimeoutError:
                        continue
                    except StopAsyncIteration:
                        break
                    except SlowConsumerError:
                        await _write(
                            writer,
                            make_error(
                                req_id,
                                BUSINESS_ERROR,
                                "slow consumer",
                                ErrorType.SLOW_CONSUMER,
                                data={"resume_seq": self.store.get_max_seq()},
                            ),
                        )
                        return
            except (ConnectionResetError, BrokenPipeError):
                pass
            finally:
                await self.event_bus.unsubscribe(sub)
        else:
            events = self.store.list_events(
                actor_id,
                since_seq=p.since_seq,
                limit=p.limit,
            )
            await _write(writer, make_result(req_id, {"events": events}))

    async def _actor_status(
        self,
        req_id: str,
        params: dict,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            p = StatusParams.model_validate(params)
        except Exception as e:
            await _write(
                writer, make_error(req_id, INVALID_PARAMS, str(e), ErrorType.INVALID_PARAMS)
            )
            return

        actor = self.store.resolve_actor(p.actor)
        if actor is None:
            await _write(
                writer,
                make_error(
                    req_id,
                    BUSINESS_ERROR,
                    f"actor not found: {p.actor}",
                    ErrorType.NOT_FOUND,
                ),
            )
            return

        actor_id = actor["actor_id"]
        current_turn = self.store.get_active_turn(actor_id)
        last_turn = self.store.get_last_turn(actor_id)

        if last_turn and not p.include_result:
            last_turn = {k: v for k, v in last_turn.items() if k != "result"}

        result: dict[str, Any] = {
            "actor": actor,
            "current_turn": current_turn,
            "last_turn": last_turn,
        }

        if p.include_events:
            events = self.store.list_events(
                actor_id,
                since_seq=p.since_seq,
                limit=p.limit,
            )
            result["events"] = events
            result["next_seq"] = events[-1]["seq"] + 1 if events else p.since_seq

        await _write(writer, make_result(req_id, result))

    # ------------------------------------------------------------------
    # Trigger methods
    # ------------------------------------------------------------------

    async def _trigger_add(
        self,
        req_id: str,
        params: dict,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            p = TriggerAddParams.model_validate(params)
        except Exception as e:
            await _write(
                writer, make_error(req_id, INVALID_PARAMS, str(e), ErrorType.INVALID_PARAMS)
            )
            return

        actor = self.store.resolve_actor(p.actor)
        if actor is None:
            await _write(
                writer,
                make_error(
                    req_id,
                    BUSINESS_ERROR,
                    f"actor not found: {p.actor}",
                    ErrorType.NOT_FOUND,
                ),
            )
            return

        modes = [
            name
            for name, value in (
                ("schedule", p.schedule),
                ("at", p.at),
                ("in", p.in_),
                ("every", p.every),
            )
            if value
        ]
        if len(modes) != 1:
            await _write(
                writer,
                make_error(
                    req_id,
                    INVALID_PARAMS,
                    "exactly one of schedule/at/in/every is required",
                    ErrorType.INVALID_PARAMS,
                ),
            )
            return

        try:
            kind: str
            spec: dict[str, Any]
            if p.schedule:
                kind = "cron"
                spec = {"cron": p.schedule}
                next_fire = compute_next_fire(p.schedule)
            elif p.at:
                at_dt = parse_at(p.at)
                if at_dt <= datetime.now(UTC):
                    raise ValueError(f"time is in the past: {p.at}")
                kind = "at"
                next_fire = to_utc_iso(at_dt)
                spec = {"at": next_fire}
            elif p.in_:
                delay = parse_duration(p.in_)
                kind = "at"
                next_fire = to_utc_iso(datetime.now(UTC) + timedelta(seconds=delay))
                spec = {"at": next_fire}
            else:
                interval = parse_duration(p.every or "")
                if interval < CRON_CHECK_INTERVAL:
                    raise ValueError(f"interval must be >= {CRON_CHECK_INTERVAL}s")
                kind = "every"
                spec = {"every_seconds": interval}
                next_fire = to_utc_iso(datetime.now(UTC) + timedelta(seconds=interval))
        except Exception as e:
            await _write(
                writer,
                make_error(
                    req_id,
                    INVALID_PARAMS,
                    f"invalid trigger schedule: {e}",
                    ErrorType.INVALID_PARAMS,
                ),
            )
            return

        trigger = self.store.add_trigger(
            target_actor_id=actor["actor_id"],
            kind=kind,
            spec=spec,
            message_type=p.type,
            payload=p.payload,
            next_fire_at=next_fire,
        )
        await _write(writer, make_result(req_id, trigger))

    async def _trigger_ls(
        self,
        req_id: str,
        params: dict,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            p = TriggerLsParams.model_validate(params)
        except Exception as e:
            await _write(
                writer, make_error(req_id, INVALID_PARAMS, str(e), ErrorType.INVALID_PARAMS)
            )
            return

        actor_id = None
        if p.actor:
            actor = self.store.resolve_actor(p.actor)
            if not actor:
                await _write(
                    writer,
                    make_error(
                        req_id, BUSINESS_ERROR, f"actor not found: {p.actor}", ErrorType.NOT_FOUND
                    ),
                )
                return
            actor_id = actor["actor_id"]

        triggers = self.store.list_triggers(actor_id)
        await _write(writer, make_result(req_id, {"triggers": triggers}))

    async def _trigger_rm(
        self,
        req_id: str,
        params: dict,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            p = TriggerRmParams.model_validate(params)
        except Exception as e:
            await _write(
                writer, make_error(req_id, INVALID_PARAMS, str(e), ErrorType.INVALID_PARAMS)
            )
            return

        deleted = self.store.delete_trigger(p.trigger_id)
        if not deleted:
            await _write(
                writer,
                make_error(
                    req_id,
                    BUSINESS_ERROR,
                    f"trigger not found: {p.trigger_id}",
                    ErrorType.NOT_FOUND,
                ),
            )
            return

        await _write(writer, make_result(req_id, {"deleted": True}))

    # ------------------------------------------------------------------
    # Daemon methods
    # ------------------------------------------------------------------

    async def _daemon_status(
        self,
        req_id: str,
        params: dict,
        writer: asyncio.StreamWriter,
    ) -> None:
        actors = self.store.list_actors()
        running_turns = self.store.list_running_turns()
        await _write(
            writer,
            make_result(
                req_id,
                {
                    "daemon": {
                        "pid": __import__("os").getpid(),
                        "socket": str(self.config.socket_path),
                        "workspace": str(self.config.resolve_workspace()),
                        "config_source": self.config.source,
                        "config_path": self.config.path,
                        "default_backend": self.config.default_backend,
                    },
                    "status": {
                        "active_actors": len([a for a in actors if a["state"] == "active"]),
                        "idle_actors": len([a for a in actors if a["state"] == "idle"]),
                        "running_turns": len(running_turns),
                        "max_workers": self.config.limits.max_total_workers,
                    },
                },
            ),
        )

    async def _daemon_doctor(
        self,
        req_id: str,
        params: dict,
        writer: asyncio.StreamWriter,
    ) -> None:
        fix = params.get("fix", False)
        issues: list[str] = []

        # Check for orphan running turns
        running = self.store.list_running_turns()
        for turn in running:
            pid = turn.get("exec_pid")
            if pid:
                try:
                    __import__("os").kill(pid, 0)
                except OSError:
                    issues.append(f"orphan turn {turn['turn_id']} (pid {pid} dead)")
                    if fix:
                        from agentd.protocol import TurnOutcome as _TO

                        await self.scheduler.on_turn_completed(
                            turn["turn_id"],
                            outcome=_TO.FAILED,
                            error="orphan process (doctor fix)",
                        )

        # Check for active actors without active turns
        for actor in self.store.list_active_actors():
            turn = self.store.get_active_turn(actor["actor_id"])
            if turn is None:
                issues.append(f"actor {actor['actor_id']} active but no turn")
                if fix:
                    from agentd.protocol import ActorState as _AS

                    self.store.transition_actor(actor["actor_id"], _AS.IDLE)

        await _write(
            writer,
            make_result(
                req_id,
                {
                    "issues": issues,
                    "fixed": fix and len(issues) > 0,
                },
            ),
        )


async def _write(writer: asyncio.StreamWriter, resp: Any) -> None:
    data = resp.model_dump(exclude_none=True)
    line = json.dumps(data, ensure_ascii=False) + "\n"
    writer.write(line.encode("utf-8"))
    await writer.drain()
