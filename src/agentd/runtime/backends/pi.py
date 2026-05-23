"""Pi-mono backend adapter.

Command: pi --mode json -p "<prompt>" [--session <file>] <backend_args>
Output: NDJSON (--mode json)
Checkpoint: session file path
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from agentd.protocol import EventType, ParsedLine, ProgressType

from ..base import BackendAdapter, CheckpointLoadError


class PiAdapter(BackendAdapter):
    name = "pi"
    supports_steer = False

    def build_command(
        self,
        *,
        prompt: str,
        backend_args: list[str],
        checkpoint: dict[str, Any] | None,
        cwd: str | None,
    ) -> list[str]:
        args = list(backend_args)

        # Inject prompt
        args = _replace_or_inject_prompt(args, prompt)

        # Inject --mode json if not present
        if "--mode" not in args:
            args = ["--mode", "json"] + args

        # Inject session from checkpoint
        if checkpoint and not _has_arg(args, "--session", "--session-dir"):
            session_file = _resolve_session_from_checkpoint(checkpoint, cwd)
            if session_file:
                args = ["--session", session_file] + args
            elif checkpoint.get("session_id"):
                raise CheckpointLoadError(
                    f"checkpoint has session_id={checkpoint['session_id']!r} "
                    "but session file not found"
                )

        return ["pi"] + args

    def parse_line(self, line: str) -> ParsedLine:
        obj = json.loads(line)
        if not isinstance(obj, dict):
            raise ValueError("backend output JSON must be an object")

        etype = str(obj.get("type", "")).strip()

        # Session info → checkpoint update (store richer locator).
        # raw `obj` is not forwarded as public payload (spec §10); only the
        # canonical checkpoint locator escapes.
        if etype == "session":
            session_id = str(obj.get("id", "")).strip()
            if session_id:
                cp: dict[str, Any] = {"session_id": session_id}
                session_cwd = str(obj.get("cwd", "")).strip()
                session_ts = str(obj.get("timestamp", "")).strip()
                if session_cwd:
                    cp["session_cwd"] = session_cwd
                if session_ts:
                    cp["session_timestamp"] = session_ts
            else:
                cp = None  # type: ignore[assignment]
            return ParsedLine(event_type="log", checkpoint_update=cp)

        # turn_end → internal last_result update only.
        # pi emits turn_end after each assistant turn (tool-use turn, reply
        # turn, ...). The real end is process exit. The raw turn_end object
        # MUST NOT be forwarded as a public payload (spec §4 / §10): it can
        # carry full tool results (~100 KB+) and is backend-specific.
        if etype == "turn_end":
            result = _extract_assistant_text(obj)
            return ParsedLine(event_type="log", result=result)

        # text_delta → progress.text
        if etype == "text_delta":
            content = str(obj.get("text", ""))
            return ParsedLine(
                event_type=EventType.TURN_PROGRESS,
                payload={"type": ProgressType.TEXT, "content": content},
            )

        # thinking → progress.thinking
        if etype == "thinking":
            content = str(obj.get("text", obj.get("content", "")))
            return ParsedLine(
                event_type=EventType.TURN_PROGRESS,
                payload={"type": ProgressType.THINKING, "content": content},
            )

        # tool events → progress.tool_call
        if etype == "toolcall_start":
            return ParsedLine(
                event_type=EventType.TURN_PROGRESS,
                payload={
                    "type": ProgressType.TOOL_CALL,
                    "name": str(obj.get("name", "")),
                    "args": obj.get("input", {}),
                    "status": "running",
                },
            )

        if etype == "toolcall_end":
            return ParsedLine(
                event_type=EventType.TURN_PROGRESS,
                payload={
                    "type": ProgressType.TOOL_CALL,
                    "name": str(obj.get("name", "")),
                    "args": obj.get("input", {}),
                    "status": "completed",
                },
            )

        # result/final → internal last_result update only.
        # raw obj MUST NOT be forwarded as public payload (spec §10).
        if etype in ("result", "final", "done"):
            result = obj.get("result") or obj.get("text")
            result_text = str(result) if result is not None else None
            return ParsedLine(event_type="log", result=result_text)

        # message_update → extract standard progress from assistantMessageEvent
        if etype == "message_update":
            ame = obj.get("assistantMessageEvent") or {}
            ame_type = str(ame.get("type", ""))
            if ame_type in ("thinking_start", "thinking_delta"):
                return ParsedLine(
                    event_type=EventType.TURN_PROGRESS,
                    payload={"type": ProgressType.THINKING, "content": str(ame.get("delta", ""))},
                )
            if ame_type == "text_delta":
                return ParsedLine(
                    event_type=EventType.TURN_PROGRESS,
                    payload={"type": ProgressType.TEXT, "content": str(ame.get("delta", ""))},
                )
            if ame_type == "tool_use_start":
                return ParsedLine(
                    event_type=EventType.TURN_PROGRESS,
                    payload={
                        "type": ProgressType.TOOL_CALL,
                        "name": str(ame.get("name", "")),
                        "args": ame.get("input", {}),
                        "status": "running",
                    },
                )
            if ame_type == "tool_use_end":
                return ParsedLine(
                    event_type=EventType.TURN_PROGRESS,
                    payload={
                        "type": ProgressType.TOOL_CALL,
                        "name": str(ame.get("name", "")),
                        "args": ame.get("input", {}),
                        "status": "completed",
                    },
                )
            return ParsedLine(event_type="log")

        # tool_execution_start/end → progress.tool_call
        if etype == "tool_execution_start":
            return ParsedLine(
                event_type=EventType.TURN_PROGRESS,
                payload={
                    "type": ProgressType.TOOL_CALL,
                    "name": str(obj.get("toolName", "")),
                    "args": obj.get("args", {}),
                    "status": "running",
                },
            )
        if etype == "tool_execution_end":
            return ParsedLine(
                event_type=EventType.TURN_PROGRESS,
                payload={
                    "type": ProgressType.TOOL_CALL,
                    "name": str(obj.get("toolName", "")),
                    "args": obj.get("args", {}),
                    "status": "completed",
                },
            )

        return ParsedLine(event_type="log")


def _extract_assistant_text(obj: dict) -> str | None:
    message = obj.get("message")
    if not isinstance(message, dict):
        return None
    if str(message.get("role", "")).strip() != "assistant":
        return None
    content = message.get("content")
    if not isinstance(content, list):
        return None
    parts = []
    for item in content:
        if isinstance(item, dict) and str(item.get("type", "")) == "text":
            text = item.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text)
    return "".join(parts).strip() or None


def _replace_or_inject_prompt(args: list[str], prompt: str) -> list[str]:
    out = list(args)
    for idx, arg in enumerate(out):
        if arg in ("-p", "--prompt"):
            if idx + 1 < len(out):
                out[idx + 1] = prompt
            else:
                out.append(prompt)
            return out
        if arg.startswith("--prompt="):
            out[idx] = f"--prompt={prompt}"
            return out
    return ["-p", prompt] + out


def _has_arg(args: list[str], *names: str) -> bool:
    return any(a in names or any(a.startswith(f"{n}=") for n in names) for a in args)


def _resolve_session_from_checkpoint(
    checkpoint: dict[str, Any], actor_cwd: str | None
) -> str | None:
    """Resolve session file path from checkpoint data.

    Requires session_cwd + session_timestamp + session_id to construct
    the exact file path. Returns None if any field is missing or the
    file does not exist.
    """
    session_id = checkpoint.get("session_id")
    session_cwd = checkpoint.get("session_cwd")
    session_ts = checkpoint.get("session_timestamp")
    if not session_id or not session_cwd or not session_ts:
        return None

    session_dir = _session_dir_for_cwd(session_cwd)
    ts_slug = session_ts.replace(":", "-").replace(".", "-")
    exact = session_dir / f"{ts_slug}_{session_id}.jsonl"
    if exact.exists():
        return str(exact)

    return None


def _session_dir_for_cwd(cwd: str) -> Path:
    agent_dir = os.environ.get("PI_CODING_AGENT_DIR")
    if agent_dir:
        sessions_dir = Path(agent_dir).expanduser() / "sessions"
    else:
        sessions_dir = Path.home() / ".pi" / "agent" / "sessions"
    normalized = os.path.normpath(os.path.expanduser(cwd))
    slug = normalized.replace("/", "-").strip("-")
    return sessions_dir / f"--{slug}--"
