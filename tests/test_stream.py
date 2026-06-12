"""Lifecycle tests for aioabrp.stream.TelemetryStream.

Driven end-to-end against the real local SSE server harness from
``conftest.py`` (genuine ``text/event-stream`` bytes, one ``SseScript``
per connection attempt). All timings are tiny injected values — no clock
mocks; progress is awaited by polling the recorder/harness with bounded
timeouts, never by fixed sleeps standing in for "long enough".
"""

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime

from conftest import CallbackRecorder, SseScript, SseServerHarness, build_frame

import aioabrp._clock as _clock
from aioabrp.auth import AbstractAuth
from aioabrp.exceptions import AbrpAuthError
from aioabrp.models import ConnectionState, Metric, MetricValue, Telemetry
from aioabrp.stream import TelemetryStream

T1 = "2026-06-11T10:00:00+00:00"
T2 = "2026-06-11T11:00:00+00:00"
T1_DT = datetime(2026, 6, 11, 10, 0, 0, tzinfo=UTC)
T2_DT = datetime(2026, 6, 11, 11, 0, 0, tzinfo=UTC)
NOW_DT = datetime(2026, 6, 11, 12, 0, 0, tzinfo=UTC)
FUTURE = "2026-06-11T18:00:00+00:00"

StreamFactory = Callable[..., TelemetryStream]


class ScriptedAuth(AbstractAuth):
    """Auth stub returning or raising scripted results; the last repeats."""

    def __init__(self, *results: str | Exception) -> None:
        self._results = list(results)

    async def async_get_access_token(self) -> str:
        result = self._results.pop(0) if len(self._results) > 1 else self._results[0]
        if isinstance(result, Exception):
            raise result
        return result


async def test_happy_path_delivers_typed_updates(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    sse_server.scripts = [
        SseScript(
            [
                ("frame", build_frame(1, soc=0.5, time=T1, provider="RIVIAN_STREAM")),
                ("frame", build_frame(1, power=1500.0)),
                ("sleep", 30),
            ]
        )
    ]
    stream = stream_factory(vehicle_ids=[1, 2])
    await stream.start()
    await recorder.wait_for_updates(2)

    assert recorder.states == [ConnectionState.CONNECTED]
    vid0, tlm0 = recorder.updates[0]
    assert vid0 == 1
    assert tlm0.soc == MetricValue(value=50.0, time=T1_DT, provider="RIVIAN_STREAM")
    assert tlm0.power is None
    vid1, tlm1 = recorder.updates[1]
    assert vid1 == 1
    assert tlm1.power == MetricValue(value=1500.0, time=None, provider=None)
    assert tlm1.soc is None

    request = sse_server.requests[0]
    assert request.headers["X-API-KEY"] == "partner-key"
    assert request.headers["X-ABRP-SESSION"] == "stream-token"
    assert request.headers["Accept"] == "text/event-stream"
    assert request.query["vehicleIds"] == "1,2"

    await asyncio.wait_for(stream.stop(), timeout=1.0)


async def test_connected_fires_on_first_frame_not_on_http_connect(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    sse_server.scripts = [
        SseScript([("sleep", 0.8), ("frame", build_frame(1, soc=0.5)), ("sleep", 30)])
    ]
    stream = stream_factory()
    await stream.start()
    # The HTTP connection is established (server saw the request) but the
    # first frame has not arrived yet: a slow-loris 200 must not read as
    # CONNECTED.
    await sse_server.wait_for_requests(1)
    await asyncio.sleep(0.15)
    assert recorder.events == []
    await recorder.wait_for_updates(1)
    assert recorder.states == [ConnectionState.CONNECTED]


async def test_reconnects_after_server_close(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    sse_server.scripts = [
        SseScript([("frame", build_frame(1, soc=0.5))]),
        SseScript([("frame", build_frame(1, soc=0.6)), ("sleep", 30)]),
    ]
    stream = stream_factory()
    await stream.start()
    await recorder.wait_for_updates(2)

    assert recorder.states == [
        ConnectionState.CONNECTED,
        ConnectionState.DISCONNECTED,
        ConnectionState.CONNECTED,
    ]
    disconnected = recorder.events[1]
    assert disconnected.reason
    assert len(sse_server.requests) == 2


async def test_http_401_at_connect_is_terminal(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    sse_server.scripts = [SseScript([("status", 401)])]
    stream = stream_factory()
    await stream.start()
    await recorder.wait_for_state(ConnectionState.AUTH_FAILED)

    event = recorder.events[0]
    assert event.reason is not None
    assert "401" in event.reason
    # Longer than both backoff tiers (0.05/0.1): a retry would have
    # reached the server by now.
    await asyncio.sleep(0.3)
    assert len(sse_server.requests) == 1
    assert recorder.states == [ConnectionState.AUTH_FAILED]
    # stop() after the AUTH_FAILED self-stop is safe and prompt.
    await asyncio.wait_for(stream.stop(), timeout=1.0)


async def test_auth_getter_auth_error_is_terminal_without_any_request(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    stream = stream_factory(auth=ScriptedAuth(AbrpAuthError("refresh token revoked")))
    await stream.start()
    await recorder.wait_for_state(ConnectionState.AUTH_FAILED)

    assert sse_server.requests == []
    assert recorder.states == [ConnectionState.AUTH_FAILED]
    assert "revoked" in (recorder.events[0].reason or "")


async def test_auth_getter_transient_error_keeps_loop_alive(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    sse_server.scripts = [
        SseScript([("frame", build_frame(1, soc=0.5))]),
        SseScript([("frame", build_frame(1, soc=0.6)), ("sleep", 30)]),
    ]
    # Attempt 1 connects, attempt 2's token fetch raises ValueError (no
    # HTTP request is made), attempt 3 connects again: the loop survives.
    auth = ScriptedAuth(
        "stream-token", ValueError("transient refresh hiccup"), "stream-token"
    )
    stream = stream_factory(auth=auth)
    await stream.start()
    await recorder.wait_for_updates(2)

    assert len(sse_server.requests) == 2
    assert recorder.states.count(ConnectionState.CONNECTED) == 2


async def test_monotonicity_gate_sequence(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    sse_server.scripts = [
        SseScript(
            [
                # Adopt + store T2.
                ("frame", build_frame(1, soc=0.5, time=T2)),
                # Strictly older than stored T2: dropped, no update.
                ("frame", build_frame(1, soc=0.1, time=T1)),
                # Equal-time re-emit (reconnect snapshots re-deliver).
                ("frame", build_frame(1, soc=0.5, time=T2)),
                # Time-less block: adopted AND clears the gate entry.
                ("frame", build_frame(1, soc=0.7)),
                # T1 < T2, but the gate was cleared: adopted.
                ("frame", build_frame(1, soc=0.2, time=T1)),
                ("sleep", 30),
            ]
        )
    ]
    stream = stream_factory()
    await stream.start()
    await recorder.wait_for_updates(4)
    # The dropped frame must not surface late.
    await asyncio.sleep(0.1)
    assert len(recorder.updates) == 4

    values = [metrics.soc.value for _, metrics in recorder.updates if metrics.soc]
    assert values == [50.0, 50.0, 70.0, 20.0]
    times = [metrics.soc.time for _, metrics in recorder.updates if metrics.soc]
    assert times == [T2_DT, T2_DT, None, T1_DT]


async def test_stop_during_backoff_returns_promptly(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    sse_server.scripts = [SseScript([("status", 500)])]
    stream = stream_factory(backoff=(30.0,))
    await stream.start()
    await recorder.wait_for_state(ConnectionState.DISCONNECTED)
    assert "500" in (recorder.events[0].reason or "")
    # The task is parked in the 30 s backoff sleep; stop() must not wait
    # it out.
    await asyncio.wait_for(stream.stop(), timeout=1.0)


async def test_start_after_stop_restarts_and_keeps_monotonicity_gate(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    sse_server.scripts = [
        SseScript([("frame", build_frame(1, soc=0.5, time=T2)), ("sleep", 30)]),
        SseScript(
            [
                # Strictly older than the pre-restart T2: still dropped —
                # the gate spans the stream's LIFETIME, not one start().
                ("frame", build_frame(1, soc=0.1, time=T1)),
                # Equal-time re-emit proves the restarted stream delivers.
                ("frame", build_frame(1, soc=0.6, time=T2)),
                ("sleep", 30),
            ]
        ),
    ]
    stream = stream_factory()
    await stream.start()
    await recorder.wait_for_updates(1)
    await asyncio.wait_for(stream.stop(), timeout=1.0)

    # start() after stop() restarts the stream (new connection attempt).
    await stream.start()
    await recorder.wait_for_updates(2)

    assert len(sse_server.requests) == 2
    assert recorder.states.count(ConnectionState.CONNECTED) == 2
    # The T1 frame produced no update: only the T2 adopts surfaced.
    values = [metrics.soc.value for _, metrics in recorder.updates if metrics.soc]
    assert values == [50.0, 60.0]


async def test_stop_before_start_and_double_stop(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    stream = stream_factory()
    await stream.stop()  # before start: no-op

    sse_server.scripts = [
        SseScript([("frame", build_frame(1, soc=0.5)), ("sleep", 30)])
    ]
    await stream.start()
    await recorder.wait_for_updates(1)
    await asyncio.wait_for(stream.stop(), timeout=1.0)
    await asyncio.wait_for(stream.stop(), timeout=1.0)  # double stop: no-op

    # No callbacks after stop() returns.
    seen_events, seen_updates = len(recorder.events), len(recorder.updates)
    await asyncio.sleep(0.1)
    assert len(recorder.events) == seen_events
    assert len(recorder.updates) == seen_updates


async def test_on_update_delivers_telemetry_with_correct_fields(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    """on_update delivers a Telemetry; present fields are set, absent ones None.

    A single frame carrying soc and power for vehicle 1 must produce a
    Telemetry with .soc and .power populated and every other field None
    (e.g. .voltage). The arg type is verified at runtime to confirm the
    boundary packing, not just the annotation.
    """
    sse_server.scripts = [
        SseScript(
            [
                ("frame", build_frame(1, soc=0.75, power=3000.0)),
                ("sleep", 30),
            ]
        )
    ]
    stream = stream_factory()
    await stream.start()
    await recorder.wait_for_updates(1)
    await asyncio.wait_for(stream.stop(), timeout=1.0)

    assert len(recorder.updates) == 1
    vid, tlm = recorder.updates[0]
    assert vid == 1
    assert isinstance(tlm, Telemetry)
    # Present metrics.
    assert tlm.soc is not None
    assert tlm.soc.value == 75.0
    assert tlm.power is not None
    assert tlm.power.value == 3000.0
    # Absent metric (not in the frame) must be None.
    assert tlm.voltage is None


async def test_future_time_is_clamped_to_now_on_delivery(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
    monkeypatch,
) -> None:
    monkeypatch.setattr(_clock, "_now", lambda: NOW_DT)
    sse_server.scripts = [
        SseScript([("frame", build_frame(1, soc=0.5, time=FUTURE)), ("sleep", 30)])
    ]
    stream = stream_factory()
    await stream.start()
    await recorder.wait_for_updates(1)

    _vid, tlm = recorder.updates[0]
    # Consumer never sees the future stamp: it is rewritten to now.
    assert tlm.soc == MetricValue(value=50.0, time=NOW_DT, provider=None)


async def test_future_time_does_not_poison_the_gate(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
    monkeypatch,
) -> None:
    # Without the clamp, the 18:00 stamp becomes the high-water mark and a
    # later current-time frame (< 18:00) would be dropped. With the clamp it is
    # stored as `now` (12:00), so the 12:00 frame is adopted (equal-time).
    monkeypatch.setattr(_clock, "_now", lambda: NOW_DT)
    sse_server.scripts = [
        SseScript(
            [
                (
                    "frame",
                    build_frame(1, soc=0.5, time=FUTURE),
                ),  # 18:00 -> clamped to NOW
                (
                    "frame",
                    build_frame(1, soc=0.6, time="2026-06-11T12:00:00+00:00"),
                ),  # at NOW: adopted
                ("sleep", 30),
            ]
        )
    ]
    stream = stream_factory()
    await stream.start()
    await recorder.wait_for_updates(2)

    values = [m.soc.value for _, m in recorder.updates if m.soc]
    assert values == [50.0, 60.0]


async def test_seed_warms_gate_drops_older_than_seed(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
    monkeypatch,
) -> None:
    monkeypatch.setattr(_clock, "_now", lambda: NOW_DT)
    sse_server.scripts = [
        SseScript(
            [
                (
                    "frame",
                    build_frame(1, soc=0.1, time=T1),
                ),  # T1 (10:00) < seed T2: dropped
                (
                    "frame",
                    build_frame(1, soc=0.9, time=T2),
                ),  # == seed T2: adopted (re-emit)
                ("sleep", 30),
            ]
        )
    ]
    seed = {1: {Metric.SOC: T2_DT}}
    stream = stream_factory(seed=seed)
    await stream.start()
    await recorder.wait_for_updates(1)
    await asyncio.sleep(0.1)

    values = [m.soc.value for _, m in recorder.updates if m.soc]
    assert values == [90.0]  # the T1 frame was gated out by the seed


async def test_seed_future_time_is_validated_to_now(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
    monkeypatch,
) -> None:
    monkeypatch.setattr(_clock, "_now", lambda: NOW_DT)
    # A frame stamped exactly at NOW (12:00). If the seed's future 18:00 stamp
    # were stored verbatim, this frame (12:00 < 18:00) would be wrongly
    # dropped. It is adopted only because the seed time was clamped to NOW,
    # making the frame equal-time (adopted by design).
    seed_future = datetime(2026, 6, 11, 18, 0, 0, tzinfo=UTC)
    sse_server.scripts = [
        SseScript(
            [
                ("frame", build_frame(1, soc=0.6, time="2026-06-11T12:00:00+00:00")),
                ("sleep", 30),
            ]
        )
    ]
    seed = {1: {Metric.SOC: seed_future}}
    stream = stream_factory(seed=seed)
    await stream.start()
    await recorder.wait_for_updates(1)

    values = [m.soc.value for _, m in recorder.updates if m.soc]
    assert values == [60.0]


async def test_seed_is_per_vehicle_isolated(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
    monkeypatch,
) -> None:
    monkeypatch.setattr(_clock, "_now", lambda: NOW_DT)
    sse_server.scripts = [
        SseScript(
            [
                # Vehicle 2 has NO seed: an older-looking T1 frame still adopts.
                ("frame", build_frame(2, soc=0.3, time=T1)),
                ("sleep", 30),
            ]
        )
    ]
    seed = {1: {Metric.SOC: T2_DT}}
    stream = stream_factory(vehicle_ids=[1, 2], seed=seed)
    await stream.start()
    await recorder.wait_for_updates(1)

    vid, tlm = recorder.updates[0]
    assert vid == 2
    assert tlm.soc.value == 30.0  # vehicle 1's seed did not gate vehicle 2
