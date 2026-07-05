"""agentd shared protocol definitions.

IDs, state machines, RPC envelope (JSON-RPC 2.0), error codes,
event types, message normalization, and RPC parameter models.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ROOT_SCOPE = "__root__"
DEFAULT_MESSAGE_TYPE = "message"
AGENTD_FRAME_MAX = 4 * 1024 * 1024

_ID_PREFIXES: dict[str, str] = {
    "act": "act_",
    "turn": "turn_",
    "msg": "msg_",
    "trig": "trig_",
}


def gen_id(kind: str) -> str:
    """Generate a Stripe-style prefixed random ID (12 hex chars)."""
    prefix = _ID_PREFIXES.get(kind)
    if prefix is None:
        raise ValueError(f"unknown ID kind: {kind}")
    return f"{prefix}{uuid.uuid4().hex[:12]}"


def is_actor_ref_by_id(ref: str) -> bool:
    """True when *ref* looks like an actor_id (``act_`` prefix)."""
    return ref.startswith("act_")


# ---------------------------------------------------------------------------
# State machines
# ---------------------------------------------------------------------------


class ActorState(StrEnum):
    IDLE = "idle"
    ACTIVE = "active"
    CLOSED = "closed"


_ACTOR_TRANSITIONS: dict[ActorState, set[ActorState]] = {
    ActorState.IDLE: {ActorState.ACTIVE, ActorState.CLOSED},
    ActorState.ACTIVE: {ActorState.IDLE, ActorState.CLOSED},
    ActorState.CLOSED: set(),
}


def validate_actor_transition(current: ActorState, target: ActorState) -> None:
    if current == target:
        return
    if target not in _ACTOR_TRANSITIONS[current]:
        raise ValueError(f"invalid actor transition: {current} -> {target}")


class TurnState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    ENDED = "ended"


_TURN_TRANSITIONS: dict[TurnState, set[TurnState]] = {
    TurnState.PENDING: {TurnState.RUNNING, TurnState.ENDED},
    TurnState.RUNNING: {TurnState.ENDED},
    TurnState.ENDED: set(),
}


def validate_turn_transition(current: TurnState, target: TurnState) -> None:
    if current == target:
        return
    if target not in _TURN_TRANSITIONS[current]:
        raise ValueError(f"invalid turn transition: {current} -> {target}")


class TurnOutcome(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"
    INTERRUPTED = "interrupted"


class DeliverAs(StrEnum):
    AUTO = "auto"
    STEER = "steer"
    FOLLOW_UP = "follow_up"


class TerminalIntent(StrEnum):
    """Runtime-internal tracking for stop/close requests."""

    NONE = "none"
    STOP = "stop"
    CANCEL = "cancel"


# ---------------------------------------------------------------------------
# Event types
# ---------------------------------------------------------------------------


class EventType(StrEnum):
    ACTOR_SPAWNED = "actor.spawned"
    TURN_OPENED = "turn.opened"
    TURN_STARTED = "turn.started"
    TURN_PROGRESS = "turn.progress"
    TURN_END = "turn.end"
    ACTOR_CLOSED = "actor.closed"
    CHECKPOINT_LOADED = "actor.checkpoint.loaded"
    CHECKPOINT_SAVED = "actor.checkpoint.saved"
    CHECKPOINT_MISSED = "actor.checkpoint.missed"


class ProgressType(StrEnum):
    TEXT = "text"
    THINKING = "thinking"
    TOOL_CALL = "tool_call"


# ---------------------------------------------------------------------------
# RPC envelope — JSON-RPC 2.0 with streaming extension
# ---------------------------------------------------------------------------


class RpcRequest(BaseModel):
    jsonrpc: str = "2.0"
    id: str
    method: str
    params: dict[str, Any] = Field(default_factory=dict)


class RpcError(BaseModel):
    code: int
    message: str
    data: dict[str, Any] = Field(default_factory=dict)


class RpcResponse(BaseModel):
    jsonrpc: str = "2.0"
    id: str
    result: dict[str, Any] | None = None
    error: RpcError | None = None
    event: dict[str, Any] | None = None
    done: bool | None = None


# Standard JSON-RPC error codes
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603
BUSINESS_ERROR = -32000


class ErrorType(StrEnum):
    NOT_FOUND = "not_found"
    ACTOR_CLOSED = "actor_closed"
    CONFLICT = "conflict"
    FORBIDDEN = "forbidden"
    BACKEND_ERROR = "backend_error"
    DAEMON_UNAVAILABLE = "daemon_unavailable"
    TIMEOUT = "timeout"
    SLOW_CONSUMER = "slow_consumer"
    INVALID_PARAMS = "invalid_params"


class PublicErrorCode(StrEnum):
    ACTOR_STOPPED = "ACTOR_STOPPED"
    BACKEND_EXIT_NONZERO = "BACKEND_EXIT_NONZERO"
    BACKEND_NO_TERMINAL_EVENT = "BACKEND_NO_TERMINAL_EVENT"
    BACKEND_TIMEOUT = "BACKEND_TIMEOUT"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"


def make_result(req_id: str, result: dict[str, Any]) -> RpcResponse:
    return RpcResponse(id=req_id, result=result)


def make_error(
    req_id: str,
    code: int,
    message: str,
    error_type: str | None = None,
    data: dict[str, Any] | None = None,
) -> RpcResponse:
    d = dict(data or {})
    if error_type:
        d["type"] = error_type
    return RpcResponse(id=req_id, error=RpcError(code=code, message=message, data=d))


def make_stream_event(req_id: str, event: dict[str, Any]) -> RpcResponse:
    return RpcResponse(id=req_id, event=event, done=False)


def make_stream_end(req_id: str, result: dict[str, Any]) -> RpcResponse:
    return RpcResponse(id=req_id, result=result, done=True)


# ---------------------------------------------------------------------------
# Message input normalization
# ---------------------------------------------------------------------------


class MessageInput(BaseModel):
    """Normalized message input."""

    model_config = ConfigDict(populate_by_name=True)

    type: str
    payload: dict[str, Any] = Field(default_factory=dict)


def normalize_message_input(
    *,
    message: str | None = None,
    message_type: str | None = None,
    payload: dict[str, Any] | None = None,
    required: bool = True,
) -> MessageInput | None:
    if message is not None and (message_type is not None or payload is not None):
        raise ValueError("--message cannot be combined with --type/--payload")
    if message is not None:
        return MessageInput(type=DEFAULT_MESSAGE_TYPE, payload={"text": message})
    if message_type is not None:
        return MessageInput(type=message_type, payload=payload or {})
    if payload is not None:
        raise ValueError("--payload requires --type")
    if required:
        raise ValueError("message input is required")
    return None


# ---------------------------------------------------------------------------
# Prompt rendering (mailbox messages → text for backend CLI)
# ---------------------------------------------------------------------------


def render_prompt(messages: list[dict[str, Any]]) -> str:
    """Render mailbox messages into a single prompt string."""
    parts: list[str] = []
    for msg in messages:
        msg_type = msg.get("message_type", DEFAULT_MESSAGE_TYPE)
        payload = msg.get("payload", {})
        if isinstance(payload, str):
            payload = json.loads(payload)
        if msg_type == DEFAULT_MESSAGE_TYPE:
            parts.append(str(payload.get("text", "")))
        else:
            display_type = msg_type.removeprefix("env.")
            lines = [f"[{display_type}]"]
            for k, v in payload.items():
                if isinstance(v, (dict, list)):
                    v = json.dumps(v, ensure_ascii=False)
                lines.append(f"{k}: {v}")
            parts.append("\n".join(lines))
    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# Backend output parsing types
# ---------------------------------------------------------------------------


@dataclass
class ParsedLine:
    """A normalized backend output line.

    Fields are orthogonal signals to the runner:
      - ``event_type``: "turn.progress" / "turn.end" / "log".
        "log" means drop (no public event); use it for diagnostics, internal
        signals, or unmapped backend records.
      - ``payload``: canonical schema payload. Only used when
        ``event_type == 'turn.progress'``; ignored otherwise.
      - ``result``: when not None, runner updates internal ``last_result``.
        Independent from ``event_type``; works with any value.
      - ``checkpoint_update``: when not None, runner persists a new checkpoint.
        Independent from ``event_type``.
    """

    event_type: str  # "turn.progress" | "turn.end" | "log"
    payload: dict[str, Any] = field(default_factory=dict)
    result: str | None = None
    checkpoint_update: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# RPC parameter models
# ---------------------------------------------------------------------------


class SpawnParams(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    name: str | None = None
    backend: str | None = None
    parent_actor_id: str | None = None
    backend_args: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)
    cwd: str | None = None
    checkpoint: bool | None = None
    # Message input (mutually exclusive: message OR type+payload)
    message: str | None = None
    type: str | None = Field(default=None, alias="type")
    payload: dict[str, Any] | None = None
    _msg_input: MessageInput | None = None

    @model_validator(mode="after")
    def _normalize(self) -> SpawnParams:
        self._msg_input = normalize_message_input(
            message=self.message,
            message_type=self.type,
            payload=self.payload,
            required=False,
        )
        return self

    @property
    def msg_input(self) -> MessageInput | None:
        return self._msg_input


class EmitParams(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    actor: str
    message: str | None = None
    type: str | None = Field(default=None, alias="type")
    payload: dict[str, Any] | None = None
    env: dict[str, str] = Field(default_factory=dict)
    deliver_as: DeliverAs = DeliverAs.AUTO
    _msg_input: MessageInput | None = None

    @model_validator(mode="after")
    def _normalize(self) -> EmitParams:
        self._msg_input = normalize_message_input(
            message=self.message,
            message_type=self.type,
            payload=self.payload,
            required=True,
        )
        if self.deliver_as == DeliverAs.STEER and self.env:
            raise ValueError("deliver_as=steer cannot carry env")
        return self

    @property
    def msg_input(self) -> MessageInput:
        assert self._msg_input is not None
        return self._msg_input


class StopParams(BaseModel):
    actor: str


class CloseParams(BaseModel):
    actor: str


class WaitParams(BaseModel):
    actor: str
    timeout: float | None = None
    progress: bool = False
    since_seq: int = 0


class ListParams(BaseModel):
    include_terminal: bool = False
    watch: bool = False
    limit: int = 200


class LogsParams(BaseModel):
    actor: str
    since_seq: int = 0
    follow: bool = False
    limit: int = 200


class StatusParams(BaseModel):
    actor: str
    include_events: bool = False
    include_result: bool = False
    since_seq: int = 0
    limit: int = 200


class TriggerAddParams(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    actor: str
    schedule: str | None = None
    at: str | None = None
    in_: str | None = Field(default=None, alias="in")
    every: str | None = None
    type: str = "message"
    payload: dict[str, Any] = Field(default_factory=dict)


class TriggerLsParams(BaseModel):
    actor: str | None = None


class TriggerRmParams(BaseModel):
    trigger_id: str


class DaemonStatusParams(BaseModel):
    pass


class DaemonDoctorParams(BaseModel):
    fix: bool = False
