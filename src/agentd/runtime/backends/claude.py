"""Claude Code backend adapter.

Command: claude -p "<prompt>" --output-format stream-json --verbose
         --permission-mode auto [--resume <session_id>] <backend_args>
Output: stream-json NDJSON
Checkpoint: session_id → --resume <session_id>
"""

from __future__ import annotations

import json
from typing import Any

from agentd.protocol import EventType, ParsedLine, ProgressType

from ..base import BackendAdapter


class ClaudeAdapter(BackendAdapter):
    name = "claude"
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

        # Append prompt as positional (claude uses -p <prompt>)
        if not _has_arg(args, "-p", "--print"):
            args = ["-p"] + args
        args.append(prompt)

        # Structured JSONL output
        if not _has_arg(args, "--output-format"):
            args = ["--output-format", "stream-json"] + args
        if not _has_arg(args, "--verbose"):
            args = ["--verbose"] + args
        if not _has_arg(args, "--permission-mode"):
            args = ["--permission-mode", "auto"] + args

        # Inject checkpoint (resume session)
        if checkpoint and not _has_arg(args, "--resume", "--continue", "--session-id"):
            session_id = checkpoint.get("session_id")
            if session_id:
                args.extend(["--resume", session_id])

        return ["claude"] + args

    def parse_line(self, line: str) -> ParsedLine:
        obj = json.loads(line)
        if not isinstance(obj, dict):
            raise ValueError("backend output JSON must be an object")

        etype = str(obj.get("type", ""))

        # system → checkpoint (session_id)
        if etype == "system":
            session_id = str(obj.get("session_id", "")).strip()
            return ParsedLine(
                event_type="log",
                checkpoint_update={"session_id": session_id} if session_id else None,
            )

        # assistant → parse content blocks
        if etype == "assistant":
            if obj.get("error"):
                error_msg = _extract_text(obj) or str(obj["error"])
                # Carry error text as internal last_result; final canonical
                # turn.end will surface it. raw obj MUST NOT be public payload.
                return ParsedLine(event_type="log", result=f"Error: {error_msg}")
            return _parse_assistant(obj)

        # result → turn.end (final summary from Claude)
        if etype == "result":
            result_text = str(obj.get("result", "")).strip() or None
            if obj.get("is_error"):
                result_text = f"Error: {result_text}" if result_text else "Error: unknown"
            return ParsedLine(
                event_type=EventType.TURN_END,
                result=result_text,
            )

        # user (tool_result): backend-specific, no canonical mapping. Drop.
        if etype == "user":
            return ParsedLine(event_type="log")

        # stream_event → normalize to progress subtypes
        if etype == "stream_event":
            return _parse_stream_event(obj)

        return ParsedLine(event_type="log")


def _parse_assistant(obj: dict) -> ParsedLine:
    """Parse assistant event, extract text content for turn_end."""
    message = obj.get("message") or {}
    content = message.get("content")
    if not isinstance(content, list):
        return ParsedLine(event_type="log")

    text_parts: list[str] = []
    has_tool_use = False
    thinking_parts: list[str] = []

    for block in content:
        if not isinstance(block, dict):
            continue
        btype = str(block.get("type", ""))
        if btype == "text":
            text = block.get("text")
            if isinstance(text, str) and text.strip():
                text_parts.append(text.strip())
        elif btype == "tool_use":
            has_tool_use = True
        elif btype == "thinking":
            text = block.get("thinking", "")
            if isinstance(text, str) and text.strip():
                thinking_parts.append(text.strip())

    # Emit thinking as progress
    if thinking_parts and not text_parts and not has_tool_use:
        return ParsedLine(
            event_type=EventType.TURN_PROGRESS,
            payload={"type": ProgressType.THINKING, "content": "\n".join(thinking_parts)},
        )

    # Tool use blocks → progress.tool_call
    if has_tool_use:
        for block in content:
            if isinstance(block, dict) and block.get("type") == "tool_use":
                return ParsedLine(
                    event_type=EventType.TURN_PROGRESS,
                    payload={
                        "type": ProgressType.TOOL_CALL,
                        "name": str(block.get("name", "")),
                        "args": block.get("input", {}),
                        "status": "running",
                    },
                )

    # Has text → turn_end (fallback completion)
    if text_parts:
        result = "\n".join(text_parts)
        return ParsedLine(
            event_type=EventType.TURN_END,
            result=result,
        )

    return ParsedLine(event_type="log")


def _parse_stream_event(obj: dict) -> ParsedLine:
    """Parse stream_event into normalized progress."""
    # Extract inner event data
    event = obj.get("event") or obj
    content_type = str(event.get("content_type", ""))

    if content_type == "text":
        return ParsedLine(
            event_type=EventType.TURN_PROGRESS,
            payload={"type": ProgressType.TEXT, "content": str(event.get("text", ""))},
        )
    if content_type == "thinking":
        return ParsedLine(
            event_type=EventType.TURN_PROGRESS,
            payload={"type": ProgressType.THINKING, "content": str(event.get("text", ""))},
        )

    # Unmapped stream event: drop (spec §10 raw passthrough forbidden).
    return ParsedLine(event_type="log")


def _extract_text(obj: dict) -> str | None:
    message = obj.get("message") or {}
    content = message.get("content")
    if not isinstance(content, list):
        return None
    parts = []
    for block in content:
        if isinstance(block, dict) and block.get("type") == "text":
            t = block.get("text")
            if isinstance(t, str) and t.strip():
                parts.append(t.strip())
    return "\n".join(parts) if parts else None


def _has_arg(args: list[str], *names: str) -> bool:
    return any(a in names or any(a.startswith(f"{n}=") for n in names) for a in args)
