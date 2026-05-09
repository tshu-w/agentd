"""Codex CLI backend adapter.

Command: codex exec "<prompt>" --json --full-auto [resume <thread_id>] <backend_args>
Output: JSONL (--json)
Checkpoint: thread_id → codex exec resume <thread_id>
"""

from __future__ import annotations

import json
from typing import Any

from agentd.protocol import EventType, ParsedLine, ProgressType

from ..base import BackendAdapter

CODEX_SUBCOMMANDS = frozenset({"resume", "review", "help"})


class CodexAdapter(BackendAdapter):
    name = "codex"
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

        # Inject checkpoint (resume thread)
        if checkpoint and not _has_subcommand(args):
            thread_id = checkpoint.get("thread_id")
            if thread_id:
                # codex exec resume <thread_id> <args> <prompt>
                return ["codex", "exec", "resume", thread_id] + _ensure_flags(args) + [prompt]

        # Normal: codex exec <flags> <prompt>
        args = _ensure_flags(args)
        args.append(prompt)
        return ["codex", "exec"] + args

    def parse_line(self, line: str) -> ParsedLine:
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            return ParsedLine(event_type="log")

        if not isinstance(obj, dict):
            return ParsedLine(event_type="log")

        etype = str(obj.get("type", ""))

        # thread.started → checkpoint (thread_id)
        if etype == "thread.started":
            thread_id = str(obj.get("thread_id", "")).strip()
            return ParsedLine(
                event_type="log",
                checkpoint_update={"thread_id": thread_id} if thread_id else None,
            )

        # item.completed → agent_message (turn end) or command_execution (progress)
        if etype == "item.completed":
            return _parse_item_completed(obj)

        # item.started → progress
        if etype == "item.started":
            item = obj.get("item") or {}
            if item.get("type") == "command_execution":
                return ParsedLine(
                    event_type=EventType.TURN_PROGRESS,
                    payload={
                        "type": ProgressType.TOOL_CALL,
                        "name": "bash",
                        "args": {"command": str(item.get("command", ""))},
                        "status": "running",
                    },
                )
            # Unmapped item: drop (spec §10: raw passthrough forbidden).
            return ParsedLine(event_type="log")

        # turn.completed → turn end (fallback marker)
        if etype == "turn.completed":
            return ParsedLine(event_type=EventType.TURN_END)

        # turn.started: backend lifecycle marker; no public canonical mapping.
        if etype == "turn.started":
            return ParsedLine(event_type="log")

        return ParsedLine(event_type="log")


def _parse_item_completed(obj: dict) -> ParsedLine:
    item = obj.get("item") or {}
    item_type = str(item.get("type", ""))

    if item_type == "agent_message":
        text = str(item.get("text", ""))
        return ParsedLine(
            event_type=EventType.TURN_END,
            result=text or None,
        )

    if item_type == "command_execution":
        exit_code = item.get("exit_code", 0)
        status = "completed" if exit_code == 0 else "failed"
        return ParsedLine(
            event_type=EventType.TURN_PROGRESS,
            payload={
                "type": ProgressType.TOOL_CALL,
                "name": "bash",
                "args": {"command": str(item.get("command", ""))},
                "status": status,
            },
        )

    # Unmapped item type: drop.
    return ParsedLine(event_type="log")


def _has_subcommand(args: list[str]) -> bool:
    """Check if args already contain a subcommand like 'resume'."""
    return any(not arg.startswith("-") and arg in CODEX_SUBCOMMANDS for arg in args)


def _ensure_flags(args: list[str]) -> list[str]:
    """Ensure --json and --full-auto flags are present."""
    out = list(args)
    if "--json" not in out:
        out = ["--json"] + out
    if "--full-auto" not in out and "--dangerously-bypass-approvals-and-sandbox" not in out:
        out = ["--full-auto"] + out
    return out
