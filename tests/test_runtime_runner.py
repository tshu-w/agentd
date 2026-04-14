import asyncio

import pytest

from agentd.runtime.runner import (
    STREAM_DRAIN_CHUNK_SIZE,
    _drain_stream,
    _read_stderr_capped,
    _readline_resilient,
)


@pytest.mark.asyncio
async def test_readline_resilient_skips_overlong_line_and_continues():
    reader = asyncio.StreamReader(limit=64)
    reader.feed_data(b"a" * 70 + b"\n")
    reader.feed_data(b'{"type":"turn_end","ok":true}\n')
    reader.feed_eof()

    line = await _readline_resilient(reader, turn_id="turn_test")
    assert line == b'{"type":"turn_end","ok":true}\n'

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
