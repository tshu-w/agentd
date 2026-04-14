"""Runtime — process execution layer.

Executes already-formed turns by launching backend CLI processes,
parsing their output, and reporting results back to the scheduler.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from typing import TYPE_CHECKING, Any

from agentd.config import AgentDConfig
from agentd.protocol import (
    EventType,
    ParsedLine,
    TerminalIntent,
    TurnOutcome,
    render_prompt,
)
from agentd.store import Store

from .base import BackendAdapter

if TYPE_CHECKING:
    from agentd.scheduler.event_bus import EventBus
    from agentd.scheduler.scheduler import Scheduler

logger = logging.getLogger(__name__)

STOP_TIMEOUT = 10  # seconds before SIGKILL after SIGTERM
STREAM_READ_LIMIT = 16 * 1024 * 1024  # 16MiB per protocol line
STDERR_CAPTURE_LIMIT = 128 * 1024
STREAM_DRAIN_CHUNK_SIZE = 64 * 1024
TURN_DEADLINE_SECONDS = 30 * 60  # hard ceiling per turn; backend stuck => SIGTERM
STDOUT_DRAIN_TIMEOUT = 5  # cap post-turn.end drain so a chatty backend can't pin us


async def _discard_overlong_line(reader: asyncio.StreamReader, consumed: int) -> int:
    """Discard one overlong line (including trailing newline when present).

    All reads are capped at ``STREAM_DRAIN_CHUNK_SIZE`` so a 16 MiB overrun
    doesn't translate into a 16 MiB one-shot allocation.
    """
    dropped = 0
    remaining = consumed

    while remaining > 0:
        chunk = await reader.read(min(remaining, STREAM_DRAIN_CHUNK_SIZE))
        if not chunk:
            return dropped
        dropped += len(chunk)
        remaining -= len(chunk)

    while True:
        try:
            tail = await reader.readuntil(b"\n")
            dropped += len(tail)
            return dropped
        except asyncio.LimitOverrunError as exc:
            to_discard = max(exc.consumed, 1)
            chunk = await reader.read(min(to_discard, STREAM_DRAIN_CHUNK_SIZE))
            if not chunk:
                return dropped
            dropped += len(chunk)
        except asyncio.IncompleteReadError as exc:
            dropped += len(exc.partial)
            return dropped


async def _readline_resilient(
    reader: asyncio.StreamReader,
    *,
    turn_id: str,
) -> bytes | None:
    """Read one line, skipping oversized records without aborting the turn."""
    while True:
        try:
            return await reader.readuntil(b"\n")
        except asyncio.LimitOverrunError as exc:
            dropped = await _discard_overlong_line(reader, exc.consumed)
            logger.warning(
                "dropped oversized backend output line for turn=%s bytes=%d",
                turn_id,
                dropped,
            )
            if dropped == 0:
                return None
        except asyncio.IncompleteReadError as exc:
            if exc.partial:
                return exc.partial
            return None


async def _drain_stream(reader: asyncio.StreamReader) -> int:
    """Drain a stream to avoid subprocess pipe backpressure."""
    drained = 0
    while True:
        chunk = await reader.read(STREAM_DRAIN_CHUNK_SIZE)
        if not chunk:
            return drained
        drained += len(chunk)


async def _read_stderr_capped(
    reader: asyncio.StreamReader,
    *,
    turn_id: str,
    limit: int = STDERR_CAPTURE_LIMIT,
) -> str:
    """Read stderr concurrently, keeping bounded head and tail diagnostics.

    Maintains two sliding windows:
      - ``head``: locked to the first ``head_limit`` bytes once filled.
      - ``tail``: rolling window of the last ``tail_limit`` bytes.

    When ``total <= limit`` the two windows are disjoint and concatenate to the
    full stream; otherwise ``total > limit`` triggers the truncation marker.
    Peak memory stays at ``limit + chunk_size`` instead of the previous
    ``~1.5 × limit`` spike caused by the buffer-then-split approach.
    """
    head_limit = limit // 2
    tail_limit = limit - head_limit
    head = bytearray()
    tail = bytearray()
    total = 0

    while True:
        chunk = await reader.read(STREAM_DRAIN_CHUNK_SIZE)
        if not chunk:
            break

        total += len(chunk)

        # Fill head first, up to head_limit; spillover flows into tail.
        if len(head) < head_limit:
            take = min(head_limit - len(head), len(chunk))
            head.extend(chunk[:take])
            chunk = chunk[take:]
            if not chunk:
                continue

        tail.extend(chunk)
        if len(tail) > tail_limit:
            del tail[: len(tail) - tail_limit]

    if total <= limit:
        # head and tail are disjoint and together cover every byte read.
        return (bytes(head) + bytes(tail)).decode("utf-8", errors="replace").strip()

    logger.warning(
        "truncated backend stderr for turn=%s bytes=%d limit=%d",
        turn_id,
        total,
        limit,
    )
    marker = (
        f"[stderr truncated: showing first {head_limit} bytes and "
        f"last {tail_limit} bytes; total={total} bytes]"
    )
    parts = [
        head.decode("utf-8", errors="replace").rstrip(),
        marker,
        tail.decode("utf-8", errors="replace").lstrip(),
    ]
    return "\n".join(part for part in parts if part)


class Runtime:
    def __init__(
        self,
        store: Store,
        event_bus: EventBus,
        config: AgentDConfig,
        scheduler: Scheduler,
    ):
        self.store = store
        self.event_bus = event_bus
        self.config = config
        self.scheduler = scheduler
        self._backends: dict[str, BackendAdapter] = {}
        self._processes: dict[str, asyncio.subprocess.Process] = {}  # turn_id -> process
        self._intents: dict[str, TerminalIntent] = {}  # turn_id -> terminal intent
        self._turn_done: dict[str, asyncio.Event] = {}  # signal turn completion

    def register_backend(self, adapter: BackendAdapter) -> None:
        self._backends[adapter.name] = adapter

    def get_backend(self, name: str) -> BackendAdapter | None:
        return self._backends.get(name)

    def prepare_turn(self, turn_id: str) -> None:
        """Pre-register completion event so stop_turn can always find it."""
        if turn_id not in self._turn_done:
            self._turn_done[turn_id] = asyncio.Event()

    async def execute_turn(
        self,
        *,
        turn_id: str,
        actor: dict[str, Any],
        input_messages: list[dict[str, Any]],
        env: dict[str, str] | None = None,
    ) -> None:
        """Execute a pending turn: launch backend process, parse output, report completion."""
        done = self._turn_done.get(turn_id) or asyncio.Event()
        self._turn_done[turn_id] = done
        try:
            await self._do_execute_turn(
                turn_id=turn_id,
                actor=actor,
                input_messages=input_messages,
                env=env,
            )
        finally:
            done.set()
            self._turn_done.pop(turn_id, None)
            self._intents.pop(turn_id, None)

    async def _do_execute_turn(
        self,
        *,
        turn_id: str,
        actor: dict[str, Any],
        input_messages: list[dict[str, Any]],
        env: dict[str, str] | None = None,
    ) -> None:
        actor_id = actor["actor_id"]
        backend_name = actor["backend"]
        adapter = self._backends.get(backend_name)
        if adapter is None:
            logger.error("unknown backend: %s", backend_name)
            await self.scheduler.on_turn_completed(
                turn_id,
                outcome=TurnOutcome.FAILED,
                error=f"unknown backend: {backend_name}",
            )
            return

        self._intents.setdefault(turn_id, TerminalIntent.NONE)

        # Build prompt from input messages
        prompt = render_prompt(input_messages)

        # Handle checkpoint
        checkpoint = actor.get("checkpoint")
        checkpoint_enabled = checkpoint is not None

        if checkpoint_enabled and checkpoint:
            # Emit checkpoint loaded event
            self.scheduler.publish_event(
                actor_id,
                turn_id,
                EventType.CHECKPOINT_LOADED,
                {
                    "checkpoint": checkpoint,
                },
            )
        elif checkpoint_enabled:
            # First turn, no checkpoint data yet
            self.scheduler.publish_event(
                actor_id,
                turn_id,
                EventType.CHECKPOINT_MISSED,
                {},
            )

        # Build command
        try:
            cmd = adapter.build_command(
                prompt=prompt,
                backend_args=actor.get("backend_args", []),
                checkpoint=checkpoint if checkpoint_enabled else None,
                cwd=actor.get("cwd"),
            )
        except Exception as e:
            logger.error("failed to build command for turn=%s: %s", turn_id, e)
            await self.scheduler.on_turn_completed(
                turn_id,
                outcome=TurnOutcome.FAILED,
                error=f"command build failed: {e}",
            )
            return

        # Check if stop/cancel was requested before process launch
        pre_intent = self._intents.get(turn_id, TerminalIntent.NONE)
        if pre_intent == TerminalIntent.STOP:
            await self.scheduler.on_turn_completed(
                turn_id,
                outcome=TurnOutcome.INTERRUPTED,
                error=None,
            )
            return
        if pre_intent == TerminalIntent.CANCEL:
            await self.scheduler.on_turn_completed(
                turn_id,
                outcome=TurnOutcome.CANCELED,
                error=None,
            )
            return

        # Build process environment
        proc_env = self._build_env(actor, env)

        # Launch process
        cwd = actor.get("cwd") or os.getcwd()
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=cwd,
                env=proc_env,
                limit=STREAM_READ_LIMIT,
            )
        except Exception as e:
            logger.error("failed to start process for turn=%s: %s", turn_id, e)
            await self.scheduler.on_turn_completed(
                turn_id,
                outcome=TurnOutcome.FAILED,
                error=f"process start failed: {e}",
            )
            return

        self._processes[turn_id] = process
        pid = process.pid

        # Report turn started
        self.scheduler.on_turn_started(turn_id, pid)

        # If stop/cancel arrived during process creation, terminate immediately
        if self._intents.get(turn_id, TerminalIntent.NONE) != TerminalIntent.NONE:
            await self._terminate_process(turn_id)

        # Concurrently consume stdout (parse + drain) and stderr (capped capture).
        # Both pipes must be drained continuously to avoid backpressure deadlocks;
        # TaskGroup gives us automatic cancellation propagation and exception
        # aggregation so neither stream can leave an orphan task behind.
        got_turn_end = False
        last_result: str | None = None
        new_checkpoint: dict[str, Any] | None = None
        stderr_text = ""

        async def _consume_stdout() -> None:
            nonlocal got_turn_end, last_result, new_checkpoint
            try:
                assert process.stdout is not None
                while True:
                    line_bytes = await _readline_resilient(process.stdout, turn_id=turn_id)
                    if line_bytes is None:
                        break

                    line = line_bytes.decode("utf-8", errors="replace").rstrip("\n")
                    if not line:
                        continue

                    try:
                        parsed = adapter.parse_line(line)
                    except Exception:
                        logger.warning(
                            "failed to parse backend output line for turn=%s backend=%s",
                            turn_id,
                            backend_name,
                            exc_info=True,
                        )
                        continue

                    await self._handle_parsed(
                        turn_id,
                        actor_id,
                        parsed,
                    )

                    if parsed.event_type == EventType.TURN_END:
                        got_turn_end = True
                        if parsed.result is not None:
                            last_result = parsed.result
                        break
                    elif parsed.event_type == EventType.TURN_RESULT:
                        if parsed.result is not None:
                            last_result = parsed.result
                    elif parsed.checkpoint_update is not None:
                        new_checkpoint = parsed.checkpoint_update
            except Exception:
                logger.exception("error reading output for turn=%s", turn_id)

            # Drain any remaining stdout after turn.end so the process can exit.
            # Cap the drain so a chatty backend can't pin this task indefinitely.
            # On timeout we must terminate the process: otherwise the stderr task
            # keeps waiting for EOF while the process stays blocked on a full
            # stdout pipe, and the whole TaskGroup hangs until the 30 min turn
            # deadline fires.
            if process.stdout is not None:
                try:
                    await asyncio.wait_for(
                        _drain_stream(process.stdout), timeout=STDOUT_DRAIN_TIMEOUT
                    )
                except TimeoutError:
                    logger.warning(
                        "stdout drain exceeded %ds for turn=%s; terminating process",
                        STDOUT_DRAIN_TIMEOUT,
                        turn_id,
                    )
                    await self._terminate_process(turn_id)
                except Exception:
                    logger.debug("error draining stdout for turn=%s", turn_id, exc_info=True)

        async def _consume_stderr() -> None:
            nonlocal stderr_text
            try:
                assert process.stderr is not None
                stderr_text = await _read_stderr_capped(process.stderr, turn_id=turn_id)
            except Exception:
                logger.debug("error reading stderr for turn=%s", turn_id, exc_info=True)

        # Cap total turn IO time. A backend stuck mid-stream (no EOF, no new
        # line) would otherwise hang here forever; on deadline expiry we fall
        # through to the SIGTERM/SIGKILL path and report FAILED.
        deadline_exceeded = False
        try:
            async with asyncio.timeout(TURN_DEADLINE_SECONDS):
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(_consume_stdout())
                    tg.create_task(_consume_stderr())
        except TimeoutError:
            deadline_exceeded = True
            logger.warning(
                "turn=%s exceeded deadline of %ds; terminating process",
                turn_id,
                TURN_DEADLINE_SECONDS,
            )
            await self._terminate_process(turn_id)

        # Wait for process to exit
        try:
            await asyncio.wait_for(process.wait(), timeout=STOP_TIMEOUT)
        except TimeoutError:
            process.kill()
            await process.wait()
        except Exception:
            pass

        exit_code = process.returncode
        self._processes.pop(turn_id, None)

        # Save checkpoint if enabled and we got new data
        if checkpoint_enabled and new_checkpoint:
            self.store.update_checkpoint(actor_id, new_checkpoint)
            self.scheduler.publish_event(
                actor_id,
                turn_id,
                EventType.CHECKPOINT_SAVED,
                {
                    "checkpoint": new_checkpoint,
                },
            )

        # Determine outcome
        intent = self._intents.pop(turn_id, TerminalIntent.NONE)

        if got_turn_end and intent == TerminalIntent.NONE:
            outcome = TurnOutcome.SUCCEEDED
            error = None
        elif intent == TerminalIntent.STOP:
            outcome = TurnOutcome.INTERRUPTED
            error = None
        elif intent == TerminalIntent.CANCEL:
            outcome = TurnOutcome.CANCELED
            error = None
        elif deadline_exceeded:
            outcome = TurnOutcome.FAILED
            error = f"turn deadline exceeded ({TURN_DEADLINE_SECONDS}s)"
            if stderr_text:
                error += f": {stderr_text[:500]}"
        elif exit_code is not None and exit_code != 0:
            outcome = TurnOutcome.FAILED
            error = f"exit code {exit_code}"
            if stderr_text:
                error += f": {stderr_text[:500]}"
        else:
            outcome = TurnOutcome.SUCCEEDED if got_turn_end else TurnOutcome.FAILED
            error = None if got_turn_end else "no turn.end received"

        await self.scheduler.on_turn_completed(
            turn_id,
            outcome=outcome,
            result=last_result,
            error=error,
        )

    async def stop_turn(self, turn_id: str) -> None:
        """Soft stop: SIGTERM → timeout → SIGKILL, then wait for turn completion."""
        self._intents[turn_id] = TerminalIntent.STOP
        await self._terminate_process(turn_id)
        done = self._turn_done.get(turn_id)
        if done:
            await done.wait()

    async def cancel_turn(self, turn_id: str) -> None:
        """Hard cancel: SIGTERM → timeout → SIGKILL, then wait for turn completion."""
        self._intents[turn_id] = TerminalIntent.CANCEL
        await self._terminate_process(turn_id)
        done = self._turn_done.get(turn_id)
        if done:
            await done.wait()

    async def stop_all(self, timeout: float = STOP_TIMEOUT) -> None:
        """Stop all running turns (for graceful shutdown)."""
        for turn_id in list(self._turn_done.keys()):
            self._intents[turn_id] = TerminalIntent.STOP
        tasks = [self._terminate_process(tid) for tid in list(self._processes.keys())]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        # Wait for all turn completions to propagate
        done_events = list(self._turn_done.values())
        if done_events:
            await asyncio.gather(*[e.wait() for e in done_events], return_exceptions=True)

    async def _terminate_process(self, turn_id: str) -> None:
        process = self._processes.get(turn_id)
        if process is None or process.returncode is not None:
            return
        try:
            process.send_signal(signal.SIGTERM)
            try:
                await asyncio.wait_for(process.wait(), timeout=STOP_TIMEOUT)
            except TimeoutError:
                process.kill()
                await process.wait()
        except ProcessLookupError:
            pass
        except Exception:
            logger.exception("error terminating process for turn=%s", turn_id)

    async def _handle_parsed(
        self,
        turn_id: str,
        actor_id: str,
        parsed: ParsedLine,
    ) -> None:
        """Handle a parsed output line by emitting appropriate events."""
        etype = parsed.event_type
        # Only publish progress/result events here.
        # TURN_END is published once by scheduler.on_turn_completed (canonical).
        if etype in (EventType.TURN_PROGRESS, EventType.TURN_RESULT):
            self.scheduler.publish_event(
                actor_id,
                turn_id,
                etype,
                parsed.payload,
            )

    def _build_env(
        self,
        actor: dict[str, Any],
        extra_env: dict[str, str] | None,
    ) -> dict[str, str]:
        """Build process environment with proper layering."""
        # Start with daemon's inherited environment
        env = dict(os.environ)

        # Injected variables
        env["AGENTD_ACTOR_ID"] = actor["actor_id"]

        # Build inbox URL if gateway is configured
        gw = self.config.inbox_gateway
        if gw.enabled:
            base = gw.public_base_url or f"http://{gw.host}:{gw.port}"
            env["AGENTD_INBOX_URL"] = f"{base}/v1/actors/{actor['actor_id']}/inbox"

        # Actor-level env (from scheduler's in-memory store)
        actor_env = self.scheduler._actor_env.get(actor["actor_id"], {})
        env.update(actor_env)

        # Turn-level env overlay
        if extra_env:
            env.update(extra_env)

        return env
