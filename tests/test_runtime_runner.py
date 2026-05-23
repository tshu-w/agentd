import asyncio

import pytest

from agentd.protocol import EventType, ParsedLine, ProgressType, TurnOutcome
from agentd.runtime.backends.codex import CodexAdapter
from agentd.runtime.backends.pi import PiAdapter
from agentd.runtime.runner import (
    STREAM_DRAIN_CHUNK_SIZE,
    _append_backend_record_too_large_error,
    _drain_stream,
    _read_stderr_capped,
    _readline_resilient,
    _resolve_process_outcome,
)


@pytest.mark.asyncio
async def test_readline_resilient_skips_overlong_line_and_continues():
    reader = asyncio.StreamReader(limit=64)
    reader.feed_data(b"a" * 70 + b"\n")
    reader.feed_data(b'{"type":"turn_end","ok":true}\n')
    reader.feed_eof()

    stats: dict[str, int] = {}
    line = await _readline_resilient(reader, turn_id="turn_test", stats=stats)
    assert line == b'{"type":"turn_end","ok":true}\n'
    assert stats.get("dropped_lines", 0) == 1
    assert stats.get("dropped_bytes", 0) >= 70

    line2 = await _readline_resilient(reader, turn_id="turn_test")
    assert line2 is None


@pytest.mark.asyncio
async def test_readline_resilient_handles_overlong_without_newline_at_eof():
    reader = asyncio.StreamReader(limit=64)
    reader.feed_data(b"a" * 70)
    reader.feed_eof()

    line = await _readline_resilient(reader, turn_id="turn_test")
    assert line is None


@pytest.mark.asyncio
async def test_readline_resilient_returns_partial_line_on_eof():
    reader = asyncio.StreamReader(limit=64)
    reader.feed_data(b'{"type":"turn_result","text":"ok"}')
    reader.feed_eof()

    line = await _readline_resilient(reader, turn_id="turn_test")
    assert line == b'{"type":"turn_result","text":"ok"}'


@pytest.mark.asyncio
async def test_read_stderr_capped_truncates_large_stream():
    reader = asyncio.StreamReader(limit=64)
    reader.feed_data(b"abcdef")
    reader.feed_eof()

    text = await _read_stderr_capped(reader, turn_id="turn_test", limit=4)

    assert text == (
        "ab\n[stderr truncated: showing first 2 bytes and last 2 bytes; total=6 bytes]\nef"
    )


@pytest.mark.asyncio
async def test_read_stderr_capped_keeps_small_stream_complete():
    reader = asyncio.StreamReader(limit=64)
    reader.feed_data(b"abcdef")
    reader.feed_eof()

    text = await _read_stderr_capped(reader, turn_id="turn_test", limit=6)

    assert text == "abcdef"


@pytest.mark.asyncio
async def test_drain_stream_consumes_all_data():
    reader = asyncio.StreamReader(limit=64)
    reader.feed_data(b"abc")
    reader.feed_data(b"def")
    reader.feed_eof()

    drained = await _drain_stream(reader)

    assert drained == 6


@pytest.mark.asyncio
async def test_readline_resilient_caps_per_read_during_overlong_discard():
    # Feed an overlong line larger than STREAM_DRAIN_CHUNK_SIZE to ensure
    # the discard loop chunks its reads instead of allocating in one shot.
    big = STREAM_DRAIN_CHUNK_SIZE * 3 + 17
    reader = asyncio.StreamReader(limit=128)
    reader.feed_data(b"x" * big + b"\n")
    reader.feed_data(b"after\n")
    reader.feed_eof()

    line = await _readline_resilient(reader, turn_id="turn_test")
    assert line == b"after\n"


def test_resolve_process_outcome_accepts_result_without_turn_end():
    outcome, error = _resolve_process_outcome(
        got_turn_end=False,
        last_result="done",
    )

    assert outcome == TurnOutcome.SUCCEEDED
    assert error is None


def test_resolve_process_outcome_fails_without_result_or_turn_end():
    outcome, error = _resolve_process_outcome(
        got_turn_end=False,
        last_result=None,
    )

    assert outcome == TurnOutcome.FAILED
    assert error == "no turn.end received"


def test_backend_record_too_large_prefix_is_preserved_when_appending_error():
    error = _append_backend_record_too_large_error(
        "exit code 1",
        {"dropped_lines": 2, "dropped_bytes": 123},
    )

    assert error.startswith("exit code 1; backend_record_too_large: ")
    assert "dropped 2 oversized stdout line(s)" in error


def test_pi_turn_end_does_not_leak_raw_payload():
    adapter = PiAdapter()
    raw = (
        '{"type":"turn_end",'
        '"message":{"role":"assistant","content":[{"type":"text","text":"hi"}]},'
        '"toolResults":[{"big":"' + "x" * 100000 + '"}]}'
    )
    parsed = adapter.parse_line(raw)
    # Public: dropped (event_type=="log", payload empty).
    assert parsed.event_type == "log"
    assert parsed.payload == {}
    # Internal: last_result is the extracted assistant text only.
    assert parsed.result == "hi"


def test_codex_unmapped_event_is_dropped_not_passed_through():
    adapter = CodexAdapter()
    parsed = adapter.parse_line('{"type":"some_unknown_event","foo":"bar"}')
    assert parsed.event_type == "log"
    assert parsed.payload == {}


def test_codex_command_execution_aggregated_output_does_not_leak():
    adapter = CodexAdapter()
    raw = (
        '{"type":"item.completed","item":{"type":"command_execution",'
        '"command":"ls","exit_code":0,'
        '"aggregated_output":"' + "y" * 100000 + '"}}'
    )
    parsed = adapter.parse_line(raw)
    assert parsed.event_type == EventType.TURN_PROGRESS
    # Canonical tool_call payload only; aggregated_output is not present.
    assert parsed.payload["type"] == ProgressType.TOOL_CALL
    assert "aggregated_output" not in parsed.payload
    assert parsed.payload["args"] == {"command": "ls"}


def test_pi_session_event_no_raw_payload_but_checkpoint_extracted():
    adapter = PiAdapter()
    parsed = adapter.parse_line(
        '{"type":"session","id":"sess1","cwd":"/tmp","timestamp":"2026-01-01T00:00:00Z"}'
    )
    assert parsed.event_type == "log"
    assert parsed.payload == {}
    assert parsed.checkpoint_update == {
        "session_id": "sess1",
        "session_cwd": "/tmp",
        "session_timestamp": "2026-01-01T00:00:00Z",
    }


def test_parsed_line_dataclass_omits_turn_result_enum():
    # EventType.TURN_RESULT was removed (spec §4): turn.result is not a
    # public event. Use ParsedLine.result field for last_result updates.
    assert not hasattr(EventType, "TURN_RESULT")
    # ParsedLine.result is independent from event_type.
    line = ParsedLine(event_type="log", result="text")
    assert line.event_type == "log"
    assert line.payload == {}
    assert line.result == "text"
