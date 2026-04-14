"""Shared utilities for agentd channel adapters.

Provides agentd CLI wrappers and progress event parsing (channel-agnostic).
Channel adapters import this module and only implement channel-specific logic.
"""

import asyncio
import json
import logging
import os
import re
import subprocess
from dataclasses import dataclass
from typing import Any

from agentd.protocol import PublicErrorCode

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# agentd CLI wrappers
# ---------------------------------------------------------------------------

AGENTD_BIN = os.environ.get("AGENTD_BIN", "agentd")
AGENTD_WORKSPACE = os.environ.get("AGENTD_WORKSPACE")
AGENTD_TIMEOUT = 30


def _agentd_env() -> dict[str, str]:
    env = dict(os.environ)
    if AGENTD_WORKSPACE:
        env["AGENTD_WORKSPACE"] = AGENTD_WORKSPACE
    return env


def agentd_exec(args: list[str]) -> dict[str, Any]:
    """Run an agentd CLI command synchronously and return parsed JSON."""
    result = subprocess.run(
        [AGENTD_BIN, *args],
        capture_output=True,
        text=True,
        timeout=AGENTD_TIMEOUT,
        env=_agentd_env(),
    )
    if result.returncode != 0:
        msg = result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}"
        raise RuntimeError(f"agentd {args[0] if args else '?'}: {msg}")
    return json.loads(result.stdout)


def agentd_spawn(
    name: str,
    *,
    event_type: str | None = None,
    payload: dict[str, Any] | None = None,
    env_vars: dict[str, str] | None = None,
    backend: str | None = None,
    backend_args: list[str] | None = None,
) -> dict[str, Any]:
    args = ["spawn", "--name", name]
    if backend:
        args.extend(["--backend", backend])
    if event_type:
        args.extend(["--type", event_type])
        args.extend(["--payload", json.dumps(payload or {})])
    for k, v in (env_vars or {}).items():
        args.extend(["--env", f"{k}={v}"])
    if backend_args:
        args.append("--")
        args.extend(backend_args)
    return agentd_exec(args)


def agentd_emit(
    actor: str,
    *,
    event_type: str,
    payload: dict[str, Any] | None = None,
    env_vars: dict[str, str] | None = None,
) -> dict[str, Any]:
    args = [
        "emit",
        actor,
        "--type",
        event_type,
        "--payload",
        json.dumps(payload or {}),
    ]
    for k, v in (env_vars or {}).items():
        args.extend(["--env", f"{k}={v}"])
    return agentd_exec(args)


def agentd_stop(actor: str, *, close: bool = True) -> dict[str, Any]:
    args = ["stop", actor]
    if close:
        args.append("--close")
    return agentd_exec(args)


def agentd_status(actor: str | None = None) -> dict[str, Any]:
    args = ["status"]
    if actor:
        args.append(actor)
    return agentd_exec(args)


def notify(
    name: str,
    *,
    event_type: str,
    payload: dict[str, Any] | None = None,
    env_vars: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Send event to a named actor, spawning it if it doesn't exist."""
    try:
        return agentd_emit(
            actor=name,
            event_type=event_type,
            payload=payload,
            env_vars=env_vars,
        )
    except RuntimeError:
        return agentd_spawn(
            name,
            event_type=event_type,
            payload=payload,
            env_vars=env_vars,
        )


# ---------------------------------------------------------------------------
# Progress event parsing (channel-agnostic)
# ---------------------------------------------------------------------------


def normalize_tool_name(name: str) -> str:
    parts = name.strip().split(".")
    return parts[-1] if parts else name


def summarize_tool_call(payload: dict[str, Any]) -> str:
    """Build a short description from a turn.progress tool_call payload."""
    name = normalize_tool_name(payload.get("name", ""))
    args = payload.get("args", {})
    if not isinstance(args, dict):
        args = {}
    status = payload.get("status", "running")

    if name == "read" and args.get("path"):
        return f"Reading {_truncate(args['path'])}"
    if name == "edit" and args.get("path"):
        return f"Editing {_truncate(args['path'])}"
    if name == "write" and args.get("path"):
        return f"Writing {_truncate(args['path'])}"
    if name == "bash":
        cmd = args.get("command", "")
        if isinstance(cmd, list):
            cmd = " ".join(cmd)
        if cmd:
            return f"$ {_truncate(_redact(cmd))}"

    if name:
        return f"{name}" + (f" ({status})" if status != "running" else "")
    return ""


@dataclass
class ProgressState:
    """Mutable state for tracking progress across turn.progress events.

    This class is channel-agnostic — it parses the normalized turn.progress
    event format (§4 of the spec) and outputs human-readable progress text.
    """

    phase: str = "Thinking"
    tool_count: int = 0
    last_detail: str = ""

    def update(self, event: dict[str, Any]) -> str | None:
        """Process a streaming event. Returns updated text or None if no change."""
        etype = event.get("event_type", "")
        if etype != "turn.progress":
            return None

        payload = event.get("payload") or {}
        if not isinstance(payload, dict):
            return None

        ptype = payload.get("type", "")
        changed = False

        if ptype == "thinking":
            self.phase = "Thinking"
            self.last_detail = ""
            changed = True
        elif ptype == "tool_call":
            status = payload.get("status", "running")
            if status == "running":
                self.tool_count += 1
                self.phase = "Running tool"
                detail = summarize_tool_call(payload)
                if detail:
                    self.last_detail = detail
                changed = True
            elif status == "failed":
                self.phase = "Tool failed"
                changed = True
        elif ptype == "text":
            self.phase = "Generating reply"
            self.last_detail = ""
            changed = True

        if not changed:
            return None

        if self.tool_count > 0 and self.last_detail:
            return f"✨ {self.phase}…\nStep {self.tool_count}: {self.last_detail}"
        return f"✨ {self.phase}…"


# ---------------------------------------------------------------------------
# Wait + progress streaming
# ---------------------------------------------------------------------------


@dataclass
class WaitResult:
    ok: bool
    result_text: str = ""
    error: str = ""
    error_code: str = ""
    turn_id: str = ""
    stopped: bool = False


async def wait_for_actor(
    actor_id: str,
    *,
    on_progress: Any = None,
    since_seq: int = 0,
) -> WaitResult:
    """Wait for an actor turn, streaming progress events."""
    proc = await asyncio.create_subprocess_exec(
        AGENTD_BIN,
        "wait",
        actor_id,
        "--progress",
        "--since-seq",
        str(since_seq),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=_agentd_env(),
    )

    ps = ProgressState()
    last_payload = None

    assert proc.stdout is not None
    while True:
        line = await proc.stdout.readline()
        if not line:
            break
        text = line.decode().strip()
        if not text:
            continue
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            continue

        if "ok" in data:
            last_payload = data
        else:
            update = ps.update(data)
            if update and on_progress:
                try:
                    result = on_progress(update)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception:
                    logger.exception("progress callback failed")

    await proc.wait()

    if last_payload is None:
        return WaitResult(
            ok=False,
            error="agentd wait exited without payload",
            error_code=PublicErrorCode.UNKNOWN_ERROR.value,
        )

    state = (last_payload.get("actor") or {}).get("state", "")
    error_code = str(last_payload.get("error_code") or "")
    turn_id = str(last_payload.get("turn_id") or "")
    if state == "idle":
        return WaitResult(
            ok=True,
            result_text=str(last_payload.get("result") or ""),
            error_code=error_code,
            turn_id=turn_id,
        )
    if state == "closed":
        error_msg = last_payload.get("error")
        if error_msg:
            return WaitResult(
                ok=False,
                error=str(error_msg),
                error_code=error_code or PublicErrorCode.UNKNOWN_ERROR.value,
                turn_id=turn_id,
            )
        return WaitResult(
            ok=False,
            error_code=error_code or PublicErrorCode.ACTOR_STOPPED.value,
            turn_id=turn_id,
            stopped=True,
        )

    error = last_payload.get("error") or f"Actor {state or 'closed'}"
    return WaitResult(
        ok=False,
        error=str(error),
        error_code=error_code or PublicErrorCode.UNKNOWN_ERROR.value,
        turn_id=turn_id,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _truncate(text: str, max_len: int = 56) -> str:
    text = text.strip()
    return text[: max_len - 1] + "…" if len(text) > max_len else text


def _redact(command: str) -> str:
    result = re.sub(
        r"(token|api[_-]?key|secret|password)\s*[=:]\s*\S+",
        r"\1=***",
        command,
        flags=re.I,
    )
    result = re.sub(r"[A-Za-z0-9_-]{32,}", "***", result)
    return result
