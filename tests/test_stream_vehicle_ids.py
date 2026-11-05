"""Tests for TelemetryStream.set_vehicle_ids (live resubscription)."""

import asyncio
from typing import cast

import pytest
from conftest import (
    CallbackRecorder,
    SseScript,
    SseServerHarness,
    StreamFactory,
    build_frame,
)

from aioabrp.auth import AbstractAuth
from aioabrp.models import ConnectionState, Telemetry
from aioabrp.stream import VEHICLES_CHANGED_REASON


def _ids(server: SseServerHarness, index: int) -> str:
    return server.requests[index].query["vehicleIds"]


async def test_change_while_connected_reconnects_once_without_backoff(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []

    async def record(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("aioabrp.stream._sleep", record)
    sse_server.scripts = [
        SseScript([("frame", build_frame(1, soc=0.5)), ("sleep", 30)]),
        SseScript([("frame", build_frame(2, soc=0.6)), ("sleep", 30)]),
    ]
    stream = stream_factory(vehicle_ids=[1])
    await stream.start()
    await recorder.wait_for_updates(1)

    stream.set_vehicle_ids([1, 2])
    await recorder.wait_for_updates(2)

    assert len(sse_server.requests) == 2
    assert _ids(sse_server, 1) == "1,2"
    assert recorder.states == [
        ConnectionState.CONNECTED,
        ConnectionState.DISCONNECTED,
        ConnectionState.CONNECTED,
    ]
    assert recorder.events[1].reason == VEHICLES_CHANGED_REASON
    assert sleeps == []


async def test_unchanged_set_is_a_noop(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    sse_server.scripts = [
        SseScript([("frame", build_frame(1, soc=0.5)), ("sleep", 30)])
    ]
    stream = stream_factory(vehicle_ids=(1, 2, 1))  # constructor dedups too
    await stream.start()
    await recorder.wait_for_updates(1)
    assert _ids(sse_server, 0) == "1,2"

    stream.set_vehicle_ids([2, 1, 2])  # same set, reordered + duplicate
    await asyncio.sleep(0.1)

    assert len(sse_server.requests) == 1
    assert recorder.states == [ConnectionState.CONNECTED]


async def test_empty_to_nonempty_to_empty_cycle(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    class EmptyingOnceAuth(AbstractAuth):
        calls = 0

        async def async_get_access_token(self) -> str:
            self.calls += 1
            if self.calls == 1:
                stream.set_vehicle_ids([])  # emptied during the token fetch
            return "tok"

    sse_server.scripts = [
        SseScript([("frame", build_frame(5, soc=0.5)), ("sleep", 30)])
    ]
    stream = stream_factory(vehicle_ids=[], auth=EmptyingOnceAuth())
    await stream.start()
    stream.set_vehicle_ids([5])
    await asyncio.sleep(0.1)
    assert sse_server.requests == []
    assert recorder.events == []

    stream.set_vehicle_ids([5])
    await recorder.wait_for_updates(1)
    assert _ids(sse_server, 0) == "5"

    stream.set_vehicle_ids([])
    await recorder.wait_for_state(ConnectionState.DISCONNECTED)
    assert recorder.events[-1].reason == VEHICLES_CHANGED_REASON
    await asyncio.sleep(0.15)
    assert len(sse_server.requests) == 1  # idle again, no new request

    stream.set_vehicle_ids([5])
    await recorder.wait_for_state(ConnectionState.CONNECTED, 2)
    assert len(sse_server.requests) == 2


async def test_removed_vehicle_state_dropped_and_unsubscribed_frames_ignored(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    newer = "2026-06-11T10:00:00+00:00"
    older = "2026-06-11T09:00:00+00:00"
    newest = "2026-06-11T11:00:00+00:00"
    sse_server.scripts = [
        SseScript(
            [
                ("frame", build_frame(1, soc=0.5, time=newer)),
                ("frame", build_frame(2, soc=0.5, time=newer)),
                ("sleep", 30),
            ]
        ),
        SseScript(
            [
                # Not subscribed: must never reach on_update.
                ("frame", build_frame(9, soc=0.1, time=newer)),
                # Removed vehicle: ignored.
                ("frame", build_frame(1, soc=0.9, time=newest)),
                # Kept vehicle: its gate survives, so the older frame is dropped.
                ("frame", build_frame(2, soc=0.3, time=older)),
                # Sentinel proving the frames above were all processed.
                ("frame", build_frame(2, soc=0.2, time=newest)),
                ("sleep", 30),
            ]
        ),
        SseScript(
            [
                # Re-added vehicle: gate was cleared, an older time is accepted.
                ("frame", build_frame(1, soc=0.4, time=older)),
                ("sleep", 30),
            ]
        ),
    ]
    stream = stream_factory(vehicle_ids=[1, 2])
    await stream.start()
    await recorder.wait_for_updates(2)

    stream.set_vehicle_ids([2])
    await recorder.wait_for_updates(3)
    stream.set_vehicle_ids([1, 2])
    await recorder.wait_for_updates(4)

    delivered = [(vid, t.soc.value) for vid, t in recorder.updates[2:] if t.soc]
    assert delivered == [(2, 20.0), (1, 40.0)]


async def test_change_during_backoff_applies_after_the_sleep(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = asyncio.Event()
    sleeping = asyncio.Event()

    async def held_sleep(_delay: float) -> None:
        sleeping.set()
        await release.wait()

    monkeypatch.setattr("aioabrp.stream._sleep", held_sleep)
    sse_server.scripts = [
        SseScript([("status", 500)]),
        SseScript([("frame", build_frame(1, soc=0.5)), ("sleep", 30)]),
    ]
    stream = stream_factory(vehicle_ids=[1])
    await stream.start()
    async with asyncio.timeout(5):
        await sleeping.wait()

    stream.set_vehicle_ids([1, 2])
    await asyncio.sleep(0.1)
    assert len(sse_server.requests) == 1  # the sleep was not cut short

    release.set()
    await recorder.wait_for_updates(1)
    assert _ids(sse_server, 1) == "1,2"


async def test_setter_before_start_normalizes_any_iterable(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    sse_server.scripts = [
        SseScript([("frame", build_frame(7, soc=0.5)), ("sleep", 30)])
    ]
    stream = stream_factory(vehicle_ids=[1])
    stream.set_vehicle_ids(v for v in (7, 7, 3, cast(int, "7")))
    await stream.start()
    await recorder.wait_for_updates(1)
    assert _ids(sse_server, 0) == "7,3"


async def test_transient_token_failure_after_change_backs_off(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale change flag must not turn token failures into a tight loop."""
    sleeps: list[float] = []

    async def record(delay: float) -> None:
        sleeps.append(delay)
        await asyncio.sleep(0)

    class FlakyAuth(AbstractAuth):
        calls = 0

        async def async_get_access_token(self) -> str:
            self.calls += 1
            if self.calls <= 3:
                raise RuntimeError("token endpoint down")
            return "tok"

    monkeypatch.setattr("aioabrp.stream._sleep", record)
    sse_server.scripts = [
        SseScript([("frame", build_frame(2, soc=0.5)), ("sleep", 30)])
    ]
    auth = FlakyAuth()
    stream = stream_factory(vehicle_ids=[1], auth=auth)
    stream.set_vehicle_ids([2])  # flag set before the first token fetch
    await stream.start()
    await recorder.wait_for_updates(1)

    assert auth.calls == 4
    assert len(sleeps) == 3


async def test_set_vehicle_ids_after_watchdog_expiry_does_not_raise(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    """A second call before the stream task steps must not hit an expiring timeout."""
    sse_server.scripts = [
        SseScript([("frame", build_frame(1, soc=0.5)), ("sleep", 30)])
    ]
    stream = stream_factory(vehicle_ids=[1])
    await stream.start()
    await recorder.wait_for_updates(1)

    stream.set_vehicle_ids([1, 2])
    await asyncio.sleep(0)  # timeout fires, stream task not yet resumed
    stream.set_vehicle_ids([3])  # must not raise
    await recorder.wait_for_state(ConnectionState.CONNECTED, 2)
    assert sse_server.requests[-1].query["vehicleIds"] == "3"


async def test_change_from_inside_on_update_reconnects_at_once(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    sse_server.scripts = [
        SseScript([("frame", build_frame(1, soc=0.5)), ("sleep", 30)]),
        SseScript([("frame", build_frame(2, soc=0.6)), ("sleep", 30)]),
    ]

    def on_update(vehicle_id: int, telemetry: Telemetry) -> None:
        recorder.on_update(vehicle_id, telemetry)
        if vehicle_id == 1:
            stream.set_vehicle_ids([2])  # between frames, outside the timeout

    # A long watchdog: the change must not wait for it to fire.
    stream = stream_factory(vehicle_ids=[1], on_update=on_update, watchdog_seconds=30)
    await stream.start()
    await recorder.wait_for_updates(2)

    assert _ids(sse_server, 1) == "2"
    assert recorder.states == [
        ConnectionState.CONNECTED,
        ConnectionState.DISCONNECTED,
        ConnectionState.CONNECTED,
    ]
    assert recorder.events[1].reason == VEHICLES_CHANGED_REASON
