"""Tests for aioabrp._sse: incremental SSE byte-stream parsing.

Stream tests drive ``iter_sse_events`` with a real ``aiohttp.StreamReader``
fed manually (``feed_data`` / ``feed_eof``) so chunk boundaries are exercised
exactly as scripted; parser tests are ported in substance from the
originating integration's test suite.
"""

import asyncio
import contextlib
import json
import logging
from typing import Any
from unittest import mock

import pytest
from aiohttp import StreamReader

from aioabrp._sse import iter_sse_events, parse_sse_event
from aioabrp.exceptions import AbrpApiError

FRAME: dict[str, Any] = {"vehicleId": 1, "soc": {"frac": 0.5}}
FRAME_LINE = f"data: {json.dumps(FRAME)}"


def _make_reader() -> StreamReader:
    """Build a minimal real StreamReader that tests feed by hand."""
    protocol = mock.Mock(_reading_paused=False)
    return StreamReader(protocol, limit=2**16, loop=asyncio.get_running_loop())


async def _events_from_chunks(chunks: list[bytes]) -> list[str]:
    """Run iter_sse_events over chunks, each delivered as a separate read.

    Feeds one chunk, then yields to the event loop until the consumer task
    has drained it before feeding the next — so a scripted chunk boundary
    is a real ``iter_any()`` boundary, not coalesced into one read.
    """
    reader = _make_reader()

    async def _consume() -> list[str]:
        return [event async for event in iter_sse_events(reader)]

    task = asyncio.create_task(_consume())
    try:
        for chunk in chunks:
            reader.feed_data(chunk)
            # Deliberately peeks at an aiohttp-private attr: assert the
            # consumer drained this chunk before the next one is fed, so
            # if aiohttp's read path ever changes and chunks coalesce,
            # the boundary tests fail loudly instead of going vacuous.
            for _ in range(10):
                if not reader._buffer:
                    break
                await asyncio.sleep(0)
            assert not reader._buffer, "consumer did not drain the chunk"
        reader.feed_eof()
        return await task
    finally:
        if not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


# ---------- iter_sse_events: chunk-boundary battery --------------------------


async def test_one_event_in_one_chunk() -> None:
    events = await _events_from_chunks([f"{FRAME_LINE}\n\n".encode()])

    assert events == [FRAME_LINE]
    assert parse_sse_event(events[0]) == FRAME


async def test_event_split_mid_multibyte_character() -> None:
    """A multi-byte UTF-8 character split across chunks decodes intact.

    A naive per-chunk decode would mojibake the split character and the
    JSON parser would reject the otherwise-valid frame.
    """
    frame = {"vehicleId": 1, "name": "Škoda"}
    raw = f"data: {json.dumps(frame, ensure_ascii=False)}\n\n".encode()
    split_at = raw.index("Š".encode()) + 1  # between the two bytes of Š

    events = await _events_from_chunks([raw[:split_at], raw[split_at:]])

    assert len(events) == 1
    assert parse_sse_event(events[0]) == frame


async def test_crlf_pair_split_across_chunks() -> None:
    r"""A ``\r\n`` pair spanning chunk boundaries yields exactly one event.

    A lone trailing ``\r`` at the end of one chunk + the matching ``\n``
    at the start of the next must be held together through the normalize
    so the buffer doesn't see a spurious extra ``\n\n`` and split one SSE
    event into two.
    """
    events = await _events_from_chunks([f"{FRAME_LINE}\r".encode(), b"\n\r\n"])

    assert events == [FRAME_LINE]
    assert parse_sse_event(events[0]) == FRAME


async def test_intact_crlf_boundary() -> None:
    r"""Belt-and-suspenders: a single intact ``\r\n\r\n`` boundary parses.

    Pairs with the split-boundary test: a regression that flipped the
    replace-order would fail here but pass the split test.
    """
    events = await _events_from_chunks([f"{FRAME_LINE}\r\n\r\n".encode()])

    assert events == [FRAME_LINE]
    assert parse_sse_event(events[0]) == FRAME


async def test_two_events_in_one_chunk() -> None:
    frame2 = {"vehicleId": 2, "power": {"w": 100.0}}
    line2 = f"data: {json.dumps(frame2)}"

    events = await _events_from_chunks([f"{FRAME_LINE}\n\n{line2}\n\n".encode()])

    assert events == [FRAME_LINE, line2]
    assert [parse_sse_event(event) for event in events] == [FRAME, frame2]


async def test_final_event_without_trailing_blank_line_is_flushed() -> None:
    """A graceful close without a trailing blank line still yields the event."""
    events = await _events_from_chunks([FRAME_LINE.encode()])

    assert events == [FRAME_LINE]
    assert parse_sse_event(events[0]) == FRAME


async def test_lone_held_back_cr_is_flushed_and_normalized() -> None:
    r"""A stream ending in a lone held-back ``\r`` flushes one clean event.

    The held-back trailing ``\r`` is re-appended un-normalized, so the
    final-flush path must normalize again — this pins that re-normalization
    (the raw flushed event must not leak a ``\r``).
    """
    events = await _events_from_chunks([b'data: {"vehicleId": 1}\r'])

    assert len(events) == 1
    assert "\r" not in events[0]
    assert parse_sse_event(events[0]) == {"vehicleId": 1}


async def test_pure_comment_stream_yields_no_frames() -> None:
    events = await _events_from_chunks([b": keepalive\n\n: heartbeat\n\n"])

    assert [parse_sse_event(event) for event in events] == [None, None]


async def test_empty_stream_yields_nothing() -> None:
    events = await _events_from_chunks([])

    assert events == []


# ---------- parse_sse_event unit tests ----------------------------------------


def test_parse_sse_event_single_data_line() -> None:
    """A single ``data: <json>`` line parses to the JSON dict."""
    assert parse_sse_event(FRAME_LINE) == FRAME


def test_parse_sse_event_multi_line_data_concatenates() -> None:
    r"""Per SSE spec multiple ``data:`` lines accumulate joined by ``\n``."""
    event = 'data: {"vehicleId": 1,\ndata: "soc": {"frac": 0.5}}'

    assert parse_sse_event(event) == FRAME


@pytest.mark.parametrize(
    "event",
    [
        pytest.param(": keepalive", id="comment_only"),
        pytest.param(": heartbeat\n: still alive", id="multi_comment"),
        pytest.param("", id="empty"),
        pytest.param("\n\n", id="blank_lines"),
    ],
)
def test_parse_sse_event_comment_or_empty_returns_none(event: str) -> None:
    """Comment-only / empty events carry no ``data:`` payload — return None."""
    assert parse_sse_event(event) is None


def test_parse_sse_event_malformed_json_raises_api_error() -> None:
    """Malformed JSON in a ``data:`` payload surfaces as ``AbrpApiError``."""
    with pytest.raises(AbrpApiError):
        parse_sse_event("data: {not valid json")


@pytest.mark.parametrize(
    "event",
    [
        pytest.param("data: [1, 2, 3]", id="json_array"),
        pytest.param('data: "just a string"', id="json_string"),
        pytest.param("data: 42", id="json_number"),
        pytest.param("data: null", id="json_null"),
    ],
)
def test_parse_sse_event_non_dict_payload_raises_api_error(event: str) -> None:
    """Decoded payload that isn't a JSON object → ``AbrpApiError``."""
    with pytest.raises(AbrpApiError):
        parse_sse_event(event)


def test_parse_sse_event_missing_vehicle_id_returns_none(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A dict frame missing ``vehicleId`` is dropped with a keys-only log.

    Without ``vehicleId`` the consumer can't route the frame to a vehicle;
    dropping with a debug log is safer than killing the stream. PII
    contract: the log carries the frame's key names only — never values.
    """
    caplog.set_level(logging.DEBUG, logger="aioabrp._sse")
    event = (
        'data: {"power": {"w": 100.0, "provider": "secret-provider-value"},'
        ' "vin": "SECRET-VIN-123"}'
    )

    assert parse_sse_event(event) is None

    assert "vehicleId" in caplog.text
    assert "power" in caplog.text
    assert "vin" in caplog.text
    # Never the frame body: no values, nested or top-level.
    assert "secret-provider-value" not in caplog.text
    assert "SECRET-VIN-123" not in caplog.text
    assert "100.0" not in caplog.text
