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

from aioabrp.auth import AbstractAuth
from aioabrp.exceptions import AbrpAuthError
from aioabrp.models import ConnectionState, Metric, MetricValue
from aioabrp.stream import TelemetryStream

T1 = "2026-06-11T10:00:00+00:00"
T2 = "2026-06-11T11:00:00+00:00"
T1_DT = datetime(2026, 6, 11, 10, 0, 0, tzinfo=UTC)
T2_DT = datetime(2026, 6, 11, 11, 0, 0, tzinfo=UTC)

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
    assert recorder.updates[0] == (
        1,
        {Metric.SOC: MetricValue(value=50.0, time=T1_DT, provider="RIVIAN_STREAM")},
    )
    assert recorder.updates[1] == (
        1,
        {Metric.POWER: MetricValue(value=1500.0, time=None, provider=None)},
    )

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

    values = [metrics[Metric.SOC].value for _, metrics in recorder.updates]
    assert values == [50.0, 50.0, 70.0, 20.0]
    times = [metrics[Metric.SOC].time for _, metrics in recorder.updates]
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
    values = [metrics[Metric.SOC].value for _, metrics in recorder.updates]
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
