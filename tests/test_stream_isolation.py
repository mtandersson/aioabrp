"""Multi-account isolation tests for aioabrp.stream.TelemetryStream.

Two streams with different session tokens share one ``ClientSession``
and one local SSE server (the harness routes scripts per
``X-ABRP-SESSION`` header). Nothing one stream does — auth death,
stop(), unknown-enum warnings, a slow callback — may bleed into the
other: the library holds zero module-global state.
"""

import logging
import time
from collections.abc import Callable

import pytest
from conftest import (
    CallbackRecorder,
    SseScript,
    SseServerHarness,
    StreamFactory,
    build_frame,
)

from aioabrp.auth import StaticAuth
from aioabrp.models import ConnectionState, Metric, MetricValue
from aioabrp.stream import TelemetryStream

TOKEN_A = "tok-account-a"
TOKEN_B = "tok-account-b"


def _make_pair(
    stream_factory: StreamFactory,
    recorder_a: CallbackRecorder,
    recorder_b: CallbackRecorder,
    *,
    on_update_a: Callable[[int, dict[Metric, MetricValue]], None] | None = None,
) -> tuple[TelemetryStream, TelemetryStream]:
    """Build the A/B stream pair: own token, own vehicle, own recorder."""
    stream_a = stream_factory(
        auth=StaticAuth(TOKEN_A),
        vehicle_ids=[1],
        name="A",
        on_update=on_update_a if on_update_a is not None else recorder_a.on_update,
        on_connection_change=recorder_a.on_connection_change,
    )
    stream_b = stream_factory(
        auth=StaticAuth(TOKEN_B),
        vehicle_ids=[2],
        name="B",
        on_update=recorder_b.on_update,
        on_connection_change=recorder_b.on_connection_change,
    )
    return stream_a, stream_b


async def test_two_streams_route_events_to_correct_callbacks(
    sse_server: SseServerHarness,
    stream_factory: StreamFactory,
) -> None:
    recorder_a, recorder_b = CallbackRecorder(), CallbackRecorder()
    sse_server.scripts_by_session = {
        TOKEN_A: [
            SseScript(
                [
                    ("frame", build_frame(1, soc=0.5)),
                    ("frame", build_frame(1, soc=0.6)),
                    ("sleep", 30),
                ]
            )
        ],
        TOKEN_B: [
            SseScript(
                [
                    ("frame", build_frame(2, soc=0.7)),
                    ("frame", build_frame(2, soc=0.8)),
                    ("sleep", 30),
                ]
            )
        ],
    }
    stream_a, stream_b = _make_pair(stream_factory, recorder_a, recorder_b)
    await stream_a.start()
    await stream_b.start()
    await recorder_a.wait_for_updates(2)
    await recorder_b.wait_for_updates(2)

    # Disjoint vehicle sets: no cross-talk in either direction.
    assert {vid for vid, _ in recorder_a.updates} == {1}
    assert {vid for vid, _ in recorder_b.updates} == {2}
    assert [m[Metric.SOC].value for _, m in recorder_a.updates] == [50.0, 60.0]
    assert [m[Metric.SOC].value for _, m in recorder_b.updates] == [70.0, 80.0]
    assert recorder_a.states == [ConnectionState.CONNECTED]
    assert recorder_b.states == [ConnectionState.CONNECTED]
    # Each connection went out under its own session token.
    assert len(sse_server.requests_for_session(TOKEN_A)) == 1
    assert len(sse_server.requests_for_session(TOKEN_B)) == 1


async def test_auth_failure_on_one_stream_leaves_other_streaming(
    sse_server: SseServerHarness,
    stream_factory: StreamFactory,
) -> None:
    recorder_a, recorder_b = CallbackRecorder(), CallbackRecorder()
    sse_server.scripts_by_session = {
        TOKEN_A: [SseScript([("status", 401)])],
        TOKEN_B: [
            SseScript(
                [
                    ("frame", build_frame(2, soc=0.5)),
                    ("sleep", 0.3),
                    ("frame", build_frame(2, soc=0.6)),
                    ("sleep", 30),
                ]
            )
        ],
    }
    stream_a, stream_b = _make_pair(stream_factory, recorder_a, recorder_b)
    await stream_a.start()
    await stream_b.start()

    await recorder_a.wait_for_state(ConnectionState.AUTH_FAILED)
    # B keeps streaming after A's terminal auth failure.
    await recorder_b.wait_for_updates(2)

    assert recorder_a.states == [ConnectionState.AUTH_FAILED]
    assert recorder_a.updates == []
    assert recorder_b.states == [ConnectionState.CONNECTED]
    assert {vid for vid, _ in recorder_b.updates} == {2}
    # A never retried; only its single rejected connect hit the server.
    assert len(sse_server.requests_for_session(TOKEN_A)) == 1


async def test_stop_on_one_stream_leaves_other_streaming(
    sse_server: SseServerHarness,
    stream_factory: StreamFactory,
) -> None:
    recorder_a, recorder_b = CallbackRecorder(), CallbackRecorder()
    sse_server.scripts_by_session = {
        TOKEN_A: [SseScript([("frame", build_frame(1, soc=0.5)), ("sleep", 30)])],
        TOKEN_B: [
            SseScript(
                [
                    ("frame", build_frame(2, soc=0.5)),
                    ("sleep", 0.3),
                    ("frame", build_frame(2, soc=0.6)),
                    ("sleep", 30),
                ]
            )
        ],
    }
    stream_a, stream_b = _make_pair(stream_factory, recorder_a, recorder_b)
    await stream_a.start()
    await stream_b.start()
    await recorder_a.wait_for_updates(1)
    await recorder_b.wait_for_updates(1)

    await stream_a.stop()
    # B still receives a frame sent after A was stopped.
    await recorder_b.wait_for_updates(2)

    assert len(recorder_a.updates) == 1
    assert recorder_a.states == [ConnectionState.CONNECTED]  # stop is silent
    assert [m[Metric.SOC].value for _, m in recorder_b.updates] == [50.0, 60.0]


async def test_unknown_charging_state_warning_dedup_is_per_instance(
    sse_server: SseServerHarness,
    stream_factory: StreamFactory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    recorder_a, recorder_b = CallbackRecorder(), CallbackRecorder()
    sse_server.scripts_by_session = {
        TOKEN_A: [
            SseScript(
                [
                    ("frame", build_frame(1, soc=0.5, charging_state="FOO")),
                    ("frame", build_frame(1, soc=0.6, charging_state="FOO")),
                    ("sleep", 30),
                ]
            )
        ],
        TOKEN_B: [
            SseScript(
                [
                    ("frame", build_frame(2, soc=0.5, charging_state="FOO")),
                    ("frame", build_frame(2, soc=0.6, charging_state="FOO")),
                    ("sleep", 30),
                ]
            )
        ],
    }
    stream_a, stream_b = _make_pair(stream_factory, recorder_a, recorder_b)
    await stream_a.start()
    await stream_b.start()
    await recorder_a.wait_for_updates(2)
    await recorder_b.wait_for_updates(2)

    warnings = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.WARNING and "'FOO'" in record.getMessage()
    ]
    # One warning per stream instance (per-instance dedup set) — neither
    # zero-after-the-first (shared global set) nor one per frame.
    assert len(warnings) == 2
    assert sorted(message[:3] for message in warnings) == ["A: ", "B: "]


async def test_slow_callback_on_one_stream_does_not_error_other(
    sse_server: SseServerHarness,
    stream_factory: StreamFactory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    recorder_a, recorder_b = CallbackRecorder(), CallbackRecorder()

    def slow_update(vehicle_id: int, metrics: dict[Metric, MetricValue]) -> None:
        recorder_a.on_update(vehicle_id, metrics)
        # Deliberately blocking: callbacks are sync on one loop, so this
        # delays B's delivery — but must never error it.
        time.sleep(0.1)

    sse_server.scripts_by_session = {
        TOKEN_A: [
            SseScript(
                [
                    ("frame", build_frame(1, soc=0.5)),
                    ("frame", build_frame(1, soc=0.6)),
                    ("sleep", 30),
                ]
            )
        ],
        TOKEN_B: [
            SseScript(
                [
                    ("frame", build_frame(2, soc=0.7)),
                    ("sleep", 0.05),
                    ("frame", build_frame(2, soc=0.8)),
                    ("sleep", 30),
                ]
            )
        ],
    }
    stream_a, stream_b = _make_pair(
        stream_factory, recorder_a, recorder_b, on_update_a=slow_update
    )
    await stream_a.start()
    await stream_b.start()
    await recorder_a.wait_for_updates(2)
    # Eventual delivery for B despite A's blocking callback.
    await recorder_b.wait_for_updates(2)

    assert [m[Metric.SOC].value for _, m in recorder_b.updates] == [70.0, 80.0]
    assert recorder_b.states == [ConnectionState.CONNECTED]
    # No containment logs and no errors anywhere: slow is not broken.
    assert "callback raised" not in caplog.text
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
