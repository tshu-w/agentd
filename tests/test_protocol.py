"""Tests for agentd.protocol — IDs, state machines, RPC envelope, prompt rendering."""

import pytest
from pydantic import ValidationError

from agentd.protocol import (
    BUSINESS_ERROR,
    ROOT_SCOPE,
    ActorState,
    ErrorType,
    EventType,
    ProgressType,
    RpcRequest,
    TurnOutcome,
    TurnState,
    gen_id,
    is_actor_ref_by_id,
    make_error,
    make_result,
    make_stream_end,
    make_stream_event,
    render_prompt,
    validate_actor_transition,
    validate_turn_transition,
)

# ---------------------------------------------------------------------------
# ID generation
# ---------------------------------------------------------------------------


class TestGenId:
    @pytest.mark.parametrize("prefix", ["act", "turn", "msg", "trig"])
    def test_format(self, prefix):
        i = gen_id(prefix)
        assert i.startswith(f"{prefix}_")
        suffix = i[len(prefix) + 1 :]
        assert len(suffix) == 12
        assert suffix.isalnum()

    def test_uniqueness(self):
        ids = {gen_id("act") for _ in range(100)}
        assert len(ids) == 100


# ---------------------------------------------------------------------------
# Actor reference resolution
# ---------------------------------------------------------------------------


class TestActorRef:
    def test_by_id(self):
        assert is_actor_ref_by_id("act_abc123def456") is True

    def test_by_name(self):
        assert is_actor_ref_by_id("my-actor") is False
        assert is_actor_ref_by_id("telegram:12345") is False


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------


class TestActorStateMachine:
    @pytest.mark.parametrize(
        "from_state,to_state",
        [
            (ActorState.IDLE, ActorState.ACTIVE),
            (ActorState.IDLE, ActorState.CLOSED),
            (ActorState.ACTIVE, ActorState.IDLE),
            (ActorState.ACTIVE, ActorState.CLOSED),
        ],
    )
    def test_valid_transitions(self, from_state, to_state):
        validate_actor_transition(from_state, to_state)

    @pytest.mark.parametrize(
        "from_state,to_state",
        [
            (ActorState.CLOSED, ActorState.IDLE),
            (ActorState.CLOSED, ActorState.ACTIVE),
        ],
    )
    def test_invalid_transitions(self, from_state, to_state):
        with pytest.raises(ValueError):
            validate_actor_transition(from_state, to_state)

    def test_self_transitions_are_idempotent(self):
        """Same-state transitions are allowed (idempotent stop/close)."""
        validate_actor_transition(ActorState.IDLE, ActorState.IDLE)
        validate_actor_transition(ActorState.CLOSED, ActorState.CLOSED)


class TestTurnStateMachine:
    @pytest.mark.parametrize(
        "from_state,to_state",
        [
            (TurnState.PENDING, TurnState.RUNNING),
            (TurnState.PENDING, TurnState.ENDED),
            (TurnState.RUNNING, TurnState.ENDED),
        ],
    )
    def test_valid_transitions(self, from_state, to_state):
        validate_turn_transition(from_state, to_state)

    @pytest.mark.parametrize(
        "from_state,to_state",
        [
            (TurnState.RUNNING, TurnState.PENDING),
            (TurnState.ENDED, TurnState.PENDING),
            (TurnState.ENDED, TurnState.RUNNING),
        ],
    )
    def test_invalid_transitions(self, from_state, to_state):
        with pytest.raises(ValueError):
            validate_turn_transition(from_state, to_state)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class TestEnumValues:
    def test_actor_states(self):
        assert [s.value for s in ActorState] == ["idle", "active", "closed"]

    def test_turn_states(self):
        assert [s.value for s in TurnState] == ["pending", "running", "ended"]

    def test_turn_outcomes(self):
        assert set(o.value for o in TurnOutcome) == {
            "succeeded",
            "failed",
            "canceled",
            "interrupted",
        }

    def test_event_types(self):
        assert EventType.TURN_END == "turn.end"
        assert EventType.TURN_PROGRESS == "turn.progress"
        assert EventType.ACTOR_SPAWNED == "actor.spawned"

    def test_progress_types(self):
        assert set(p.value for p in ProgressType) == {"text", "thinking", "tool_call"}

    def test_error_types(self):
        assert "not_found" in [e.value for e in ErrorType]
        assert "timeout" in [e.value for e in ErrorType]

    def test_root_scope(self):
        assert ROOT_SCOPE == "__root__"


# ---------------------------------------------------------------------------
# RPC envelope
# ---------------------------------------------------------------------------


class TestRpcEnvelope:
    def test_request_parse(self):
        req = RpcRequest(jsonrpc="2.0", id="r1", method="actor.spawn", params={"name": "test"})
        assert req.method == "actor.spawn"

    def test_request_missing_method(self):
        with pytest.raises(ValidationError):
            RpcRequest(jsonrpc="2.0", id="r1")  # ty: ignore[missing-argument]

    def test_make_result(self):
        resp = make_result("r1", {"actor_id": "act_abc"})
        assert resp.id == "r1"
        assert resp.result is not None
        assert resp.result["actor_id"] == "act_abc"
        assert resp.error is None

    def test_make_error(self):
        resp = make_error("r1", BUSINESS_ERROR, "not found", ErrorType.NOT_FOUND)
        assert resp.error is not None
        assert resp.error.code == -32000
        assert resp.error.data["type"] == "not_found"

    def test_stream_event(self):
        resp = make_stream_event("r1", {"event_type": "turn.progress"})
        assert resp.done is False
        assert resp.event is not None
        assert resp.event["event_type"] == "turn.progress"

    def test_stream_end(self):
        resp = make_stream_end("r1", {"actor": {}})
        assert resp.done is True
        assert resp.result is not None


# ---------------------------------------------------------------------------
# Prompt rendering
# ---------------------------------------------------------------------------


class TestRenderPrompt:
    def test_plain_message(self):
        msgs = [{"message_type": "message", "payload": {"text": "hello world"}}]
        assert render_prompt(msgs) == "hello world"

    def test_typed_event_strips_env_prefix(self):
        msgs = [
            {
                "message_type": "env.webhook.github.push",
                "payload": {"repo": "test", "ref": "main"},
            }
        ]
        text = render_prompt(msgs)
        assert "[webhook.github.push]" in text
        assert "env." not in text
        assert "repo: test" in text
        assert "ref: main" in text

    def test_multiple_messages(self):
        msgs = [
            {"message_type": "message", "payload": {"text": "first"}},
            {"message_type": "message", "payload": {"text": "second"}},
        ]
        text = render_prompt(msgs)
        assert "first" in text
        assert "second" in text

    def test_empty_messages(self):
        assert render_prompt([]) == ""

    def test_nested_payload_json_serialized(self):
        msgs = [
            {
                "message_type": "env.test",
                "payload": {"data": {"nested": [1, 2, 3]}},
            }
        ]
        text = render_prompt(msgs)
        assert "[test]" in text
        assert "[1, 2, 3]" in text or '"nested"' in text
