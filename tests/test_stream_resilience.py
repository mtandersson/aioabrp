"""Resilience battery for aioabrp.stream.TelemetryStream.

Adversarial edge-case pinning on top of the lifecycle tests in
``test_stream.py``: watchdog stalls, backoff ladder discipline, mid-stream
auth death, malformed/split/flush wire shapes, callback containment, both
cancellation paths of ``stop()``, teardown hardening, the internal-error
tripwire, and the PII logging contract.

All scenarios run end-to-end against the real local SSE server harness
from ``conftest.py`` except where a named monkeypatch seam is the point
(``aioabrp.stream._sleep`` for deterministic backoff capture,
``aioabrp.stream.iter_sse_events`` for masked-cancel iterators,
``aioabrp.stream.extract_metrics`` for the tripwire). Timings are tiny
injected values with bounded waits — no clock mocks, no unbounded waits.
"""

import asyncio
import json
import logging
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from typing import Any, cast

import aiohttp
import pytest
from conftest import (
    WAIT_TIMEOUT,
    CallbackRecorder,
    SseScript,
    SseServerHarness,
    StreamFactory,
    build_frame,
)

from aioabrp.auth import StaticAuth
from aioabrp.models import ConnectionEvent, ConnectionState, Metric, MetricValue
from aioabrp.stream import TelemetryStream

T1 = "2026-06-11T10:00:00+00:00"
T1_DT = datetime(2026, 6, 11, 10, 0, 0, tzinfo=UTC)


class _BackoffRecorder:
    """Recorded backoff delays with an awaitable progress hook.

    Stands in for the stream's ``_sleep`` seam: each recorded delay
    resolves immediately (keeping reconnect cycles fast) and wakes any
    bounded ``wait_for_sleeps`` waiter.
    """

    def __init__(self) -> None:
        self.sleeps: list[float] = []
        self._changed = asyncio.Event()

    async def record(self, delay: float) -> None:
        self.sleeps.append(delay)
        self._changed.set()
        await asyncio.sleep(0)

    async def wait_for_sleeps(self, count: int) -> None:
        """Wait (bounded) until ``count`` backoff sleeps were recorded."""
        async with asyncio.timeout(WAIT_TIMEOUT):
            while len(self.sleeps) < count:
                self._changed.clear()
                await self._changed.wait()


def _capture_backoff(monkeypatch: pytest.MonkeyPatch) -> _BackoffRecorder:
    """Patch the stream's backoff-sleep seam; return the delay recorder."""
    backoff_recorder = _BackoffRecorder()
    monkeypatch.setattr("aioabrp.stream._sleep", backoff_recorder.record)
    return backoff_recorder


# ---------------------------------------------------------------------------
# watchdog + backoff discipline
# ---------------------------------------------------------------------------


async def test_watchdog_fires_on_idle_stream(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    """An idle connection trips the watchdog and the stream reconnects."""
    sse_server.scripts = [SseScript([("sleep", 30)])]
    stream = stream_factory(watchdog_seconds=0.2)
    await stream.start()
    await recorder.wait_for_state(ConnectionState.DISCONNECTED)

    reason = recorder.events[0].reason or ""
    assert "watchdog_stall" in reason
    assert "0.2" in reason
    # No frame ever arrived, so CONNECTED must never have fired.
    assert ConnectionState.CONNECTED not in recorder.states
    # The loop reconnects after the stall (server sees attempt 2).
    await sse_server.wait_for_requests(2)


async def test_backoff_resets_after_successful_frame_then_watchdog_stall(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A frame resets the ladder even when the next disconnect is a stall."""
    backoff_recorder = _capture_backoff(monkeypatch)
    sse_server.scripts = [
        SseScript([("frame", build_frame(1, soc=0.5)), ("sleep", 30)])
    ]
    stream = stream_factory(watchdog_seconds=0.2, backoff=(0.05, 1.0, 5.0))
    await stream.start()
    # Two frame-then-stall cycles: both backoff gaps must be first-tier.
    await backoff_recorder.wait_for_sleeps(2)
    assert backoff_recorder.sleeps[:2] == [0.05, 0.05]
    await stream.stop()


async def test_backoff_ladder_escalates_while_connects_fail(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Consecutive failed connects climb the ladder and cap at the last tier."""
    backoff_recorder = _capture_backoff(monkeypatch)
    sse_server.scripts = [SseScript([("status", 500)])]
    stream = stream_factory(backoff=(0.01, 0.02, 0.03))
    await stream.start()
    await backoff_recorder.wait_for_sleeps(4)
    assert backoff_recorder.sleeps[:4] == [0.01, 0.02, 0.03, 0.03]
    await stream.stop()


async def test_reconnect_after_clean_close_stays_first_tier(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The natural frame-then-server-close cycle never escalates the ladder.

    Lands the reconnect-backoff-is-first-tier assertion deferred from the
    Task 6 lifecycle reconnect test, deterministically via the sleep seam.
    """
    backoff_recorder = _capture_backoff(monkeypatch)
    sse_server.scripts = [SseScript([("frame", build_frame(1, soc=0.5))])]
    stream = stream_factory(backoff=(0.05, 1.0))
    await stream.start()
    await backoff_recorder.wait_for_sleeps(3)
    assert backoff_recorder.sleeps[:3] == [0.05, 0.05, 0.05]
    await stream.stop()


# ---------------------------------------------------------------------------
# mid-stream auth death + wire-shape edge cases
# ---------------------------------------------------------------------------


async def test_mid_stream_401_is_terminal(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    """A 401 on reconnect after healthy streaming is terminal — no retry."""
    sse_server.scripts = [
        SseScript([("frame", build_frame(1, soc=0.5))]),
        SseScript([("status", 401)]),
    ]
    stream = stream_factory()
    await stream.start()
    await recorder.wait_for_state(ConnectionState.AUTH_FAILED)

    assert recorder.states == [
        ConnectionState.CONNECTED,
        ConnectionState.DISCONNECTED,
        ConnectionState.AUTH_FAILED,
    ]
    assert len(recorder.updates) == 1
    # Longer than both backoff tiers: a third attempt would have landed.
    await asyncio.sleep(0.3)
    assert len(sse_server.requests) == 2


async def test_malformed_frame_disconnects_then_reconnects(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    """Malformed JSON aborts the connection; the next attempt recovers."""
    sse_server.scripts = [
        SseScript([("raw", b"data: {not json}\n\n"), ("sleep", 30)]),
        SseScript([("frame", build_frame(1, soc=0.6)), ("sleep", 30)]),
    ]
    stream = stream_factory()
    await stream.start()
    await recorder.wait_for_updates(1)

    assert recorder.updates[0][1][Metric.SOC].value == 60.0
    # The malformed frame arrived before any good frame, so attempt 1
    # never reached CONNECTED.
    assert recorder.states == [
        ConnectionState.DISCONNECTED,
        ConnectionState.CONNECTED,
    ]
    assert "malformed" in (recorder.events[0].reason or "")


async def test_chunk_boundary_utf8_and_crlf_end_to_end(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    r"""A frame split mid-UTF-8-char and mid-``\r\n\r\n`` still parses."""
    frame = {"vehicleId": 1, "vehicleName": "Škoda Enyaq", "soc": {"frac": 0.5}}
    body = json.dumps(frame, ensure_ascii=False).encode()
    split_at = body.index("Š".encode()) + 1  # between the two UTF-8 bytes
    sse_server.scripts = [
        SseScript(
            [
                ("raw", b"data: " + body[:split_at]),
                ("sleep", 0.05),
                ("raw", body[split_at:] + b"\r\n"),
                ("sleep", 0.05),
                ("raw", b"\r"),  # first half of the terminating \r\n
                ("sleep", 0.05),
                ("raw", b"\n"),
                ("sleep", 30),
            ]
        )
    ]
    stream = stream_factory()
    await stream.start()
    await recorder.wait_for_updates(1)

    assert recorder.updates[0] == (
        1,
        {Metric.SOC: MetricValue(value=50.0, time=None, provider=None)},
    )
    assert recorder.states == [ConnectionState.CONNECTED]


async def test_flush_path_delivers_frame_without_trailing_blank_line(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    """A close right after an unterminated event still delivers the frame."""
    sse_server.scripts = [
        SseScript([("raw", b"data: " + json.dumps(build_frame(1, soc=0.5)).encode())]),
        SseScript([("frame", build_frame(1, soc=0.6)), ("sleep", 30)]),
    ]
    stream = stream_factory()
    await stream.start()
    await recorder.wait_for_updates(2)

    values = [metrics[Metric.SOC].value for _, metrics in recorder.updates]
    assert values == [50.0, 60.0]
    assert recorder.states == [
        ConnectionState.CONNECTED,
        ConnectionState.DISCONNECTED,
        ConnectionState.CONNECTED,
    ]


# ---------------------------------------------------------------------------
# callback containment
# ---------------------------------------------------------------------------


async def test_on_update_raising_keeps_stream_alive(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A raising on_update is logged and the next frame still arrives."""

    def exploding_update(vehicle_id: int, metrics: dict[Metric, MetricValue]) -> None:
        recorder.on_update(vehicle_id, metrics)
        raise RuntimeError("consumer bug in on_update")

    sse_server.scripts = [
        SseScript(
            [
                ("frame", build_frame(1, soc=0.5)),
                ("frame", build_frame(1, soc=0.6)),
                ("sleep", 30),
            ]
        )
    ]
    stream = stream_factory(on_update=exploding_update)
    await stream.start()
    await recorder.wait_for_updates(2)

    values = [metrics[Metric.SOC].value for _, metrics in recorder.updates]
    assert values == [50.0, 60.0]
    # Still healthy: no disconnect caused by the raising callback.
    assert recorder.states == [ConnectionState.CONNECTED]
    assert "on_update callback raised" in caplog.text


async def test_on_connection_change_raising_keeps_stream_alive(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A raising on_connection_change never kills the reconnect loop."""

    def exploding_connection_change(event: ConnectionEvent) -> None:
        recorder.on_connection_change(event)
        raise RuntimeError("consumer bug in on_connection_change")

    sse_server.scripts = [
        SseScript([("frame", build_frame(1, soc=0.5))]),
        SseScript([("frame", build_frame(1, soc=0.6)), ("sleep", 30)]),
    ]
    stream = stream_factory(on_connection_change=exploding_connection_change)
    await stream.start()
    await recorder.wait_for_updates(2)

    # Every transition (each of which raised) was still delivered, and the
    # stream survived the full disconnect/reconnect cycle in between.
    assert recorder.states == [
        ConnectionState.CONNECTED,
        ConnectionState.DISCONNECTED,
        ConnectionState.CONNECTED,
    ]
    assert "on_connection_change callback raised" in caplog.text


async def test_stop_after_auth_failed_no_callbacks_after_stop(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    """stop() after the AUTH_FAILED self-stop is clean and silent."""
    sse_server.scripts = [SseScript([("status", 403)])]
    stream = stream_factory()
    await stream.start()
    await recorder.wait_for_state(ConnectionState.AUTH_FAILED)

    await asyncio.wait_for(stream.stop(), timeout=1.0)
    seen_events, seen_updates = len(recorder.events), len(recorder.updates)
    await asyncio.sleep(0.2)
    assert recorder.states == [ConnectionState.AUTH_FAILED]
    assert (len(recorder.events), len(recorder.updates)) == (
        seen_events,
        seen_updates,
    )


# ---------------------------------------------------------------------------
# stop() cancellation paths + teardown hardening
# ---------------------------------------------------------------------------


class _MaskedCancelIterator:
    """SSE-event iterator that masks cancellation as ``StopAsyncIteration``.

    Mirrors a fixture/iterator that catches ``CancelledError`` inside
    ``__anext__`` and re-raises it as end-of-stream — the masked path the
    stream's ``task.cancelling()`` guard exists for.
    """

    def __aiter__(self) -> _MaskedCancelIterator:
        return self

    async def __anext__(self) -> str:
        try:
            await asyncio.get_running_loop().create_future()
        except asyncio.CancelledError:
            raise StopAsyncIteration from None
        raise StopAsyncIteration  # pragma: no cover - unreachable


def _masked_iter_sse_events(_content: object) -> _MaskedCancelIterator:
    return _MaskedCancelIterator()


async def test_masked_cancel_stop_returns_promptly(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An iterator masking CancelledError must not strand stop() in backoff.

    With ``backoff=(30,)``, a dropped ``task.cancelling()`` guard would
    park the loop in the backoff sleep with the cancel signal consumed.
    Detection is belt-and-braces: a stranded stop() surfaces as the
    ``wait_for`` below raising ``TimeoutError`` (its timeout cancels
    stop(), which re-raises the caller's own cancellation), while the
    elapsed-time assertion additionally guards against a regression of
    that re-raise (an indiscriminate swallow would let ``wait_for``
    return *normally* after the full timeout).
    """
    sse_server.scripts = [SseScript([("sleep", 30)])]
    monkeypatch.setattr("aioabrp.stream.iter_sse_events", _masked_iter_sse_events)
    stream = stream_factory(backoff=(30.0,), watchdog_seconds=30.0)
    await stream.start()
    await sse_server.wait_for_requests(1)
    await asyncio.sleep(0.05)  # ensure the task is parked in wait_for

    loop = asyncio.get_running_loop()
    started = loop.time()
    await asyncio.wait_for(stream.stop(), timeout=5.0)
    elapsed = loop.time() - started
    assert elapsed < 1.0, f"stop() took {elapsed:.2f}s — stranded in backoff sleep"
    # No frame ever flowed and stop() suppressed the final DISCONNECTED.
    assert recorder.events == []
    assert recorder.updates == []


async def test_stop_during_watchdog_wait_releases_cleanly(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """stop() while parked in the watchdog ``wait_for`` completes promptly."""
    sse_server.scripts = [SseScript([("sleep", 30)])]
    stream = stream_factory(watchdog_seconds=30.0, backoff=(30.0,))
    await stream.start()
    await sse_server.wait_for_requests(1)
    await asyncio.sleep(0.05)  # parked in wait_for on a silent connection

    loop = asyncio.get_running_loop()
    started = loop.time()
    with caplog.at_level(logging.WARNING):
        # A non-prompt release fails twice over: wait_for raises
        # TimeoutError (stop() re-raises the timeout's cancellation) and
        # the elapsed-time bound below trips.
        await asyncio.wait_for(stream.stop(), timeout=5.0)
    elapsed = loop.time() - started
    assert elapsed < 2.0, f"stop() took {elapsed:.2f}s — release was not prompt"

    leak_signatures = (
        "Task was destroyed",
        "was never retrieved",
        "coroutine was never awaited",
    )
    leaked = [
        record.getMessage()
        for record in caplog.records
        if any(signature in record.getMessage() for signature in leak_signatures)
    ]
    assert not leaked, f"unexpected leak warnings: {leaked}"
    assert recorder.events == []


async def test_caller_cancellation_propagates_out_of_stop(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancelling a task parked in stop() raises CancelledError out of it.

    stop()'s CancelledError swallow is for the awaited internal task's
    cancellation result only: a caller that is itself cancelled while
    stop() awaits the internal task (e.g. a timed-out
    ``wait_for(stop(), ...)``) must see its own CancelledError
    propagate, not an absorbed normal return claiming the stream
    stopped.

    Deterministic choreography via the ``_sleep`` seam: the internal
    task parks in the backoff sleep; the first cancel (from stop())
    enters a teardown that blocks until a second cancel arrives, so
    stop() is provably parked on ``await task`` when the test cancels
    the stop() task externally.
    """
    entered_teardown = asyncio.Event()

    async def stubborn_sleep(delay: float) -> None:
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            entered_teardown.set()
            # Parked until the second cancel (from stop_task.cancel())
            # lands here and propagates.
            await asyncio.get_running_loop().create_future()
            raise  # pragma: no cover - the future never resolves

    monkeypatch.setattr("aioabrp.stream._sleep", stubborn_sleep)
    sse_server.scripts = [SseScript([("status", 500)])]
    stream = stream_factory(backoff=(30.0,))
    await stream.start()
    await recorder.wait_for_state(ConnectionState.DISCONNECTED)
    await asyncio.sleep(0.05)  # internal task parked in the backoff sleep

    stop_task = asyncio.create_task(stream.stop())
    async with asyncio.timeout(WAIT_TIMEOUT):
        await entered_teardown.wait()
    # The internal task is wedged in teardown, so stop() is parked on
    # ``await task`` — exactly the window the re-raise protects.
    assert not stop_task.done()

    stop_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        async with asyncio.timeout(WAIT_TIMEOUT):
            await stop_task


class _AcloseRaisesClientError:
    """Wrap the real SSE generator; ``aclose()`` raises ``ClientError``.

    Simulates response teardown blowing up on a half-broken socket while
    the ``finally`` block releases the generator.
    """

    def __init__(self, inner: AsyncGenerator[dict[str, Any]]) -> None:
        self._inner = inner

    def __aiter__(self) -> _AcloseRaisesClientError:
        return self

    async def __anext__(self) -> dict[str, Any]:
        return await self._inner.__anext__()

    async def aclose(self) -> None:
        await self._inner.aclose()
        raise aiohttp.ClientError("teardown failed on half-broken socket")


async def test_aclose_raising_client_error_keeps_loop_alive(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """``aclose()`` raising ``ClientError`` is swallowed; the loop reconnects."""
    sse_server.scripts = [
        SseScript([("frame", build_frame(1, soc=0.5)), ("sleep", 30)]),
        SseScript([("frame", build_frame(1, soc=0.6)), ("sleep", 30)]),
    ]
    stream = stream_factory(watchdog_seconds=0.2)
    real_open = stream._open_stream

    def wrapped_open(token: str) -> _AcloseRaisesClientError:
        return _AcloseRaisesClientError(real_open(token))

    monkeypatch.setattr(stream, "_open_stream", wrapped_open)
    await stream.start()
    await recorder.wait_for_updates(2)

    values = [metrics[Metric.SOC].value for _, metrics in recorder.updates[:2]]
    assert values == [50.0, 60.0]
    assert recorder.states[:3] == [
        ConnectionState.CONNECTED,
        ConnectionState.DISCONNECTED,
        ConnectionState.CONNECTED,
    ]
    assert "watchdog_stall" in (recorder.events[1].reason or "")
    assert "aclose() raised" in caplog.text
    await stream.stop()


async def test_open_stream_raising_before_generator_assignment(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An eagerly-raising stream factory backs off; no UnboundLocalError."""
    sse_server.scripts = [
        SseScript([("frame", build_frame(1, soc=0.5)), ("sleep", 30)])
    ]
    stream = stream_factory()
    real_open = stream._open_stream
    calls = 0

    def flaky_open(token: str) -> AsyncGenerator[dict[str, Any]]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise aiohttp.ClientError("connector exploded before assignment")
        return real_open(token)

    monkeypatch.setattr(stream, "_open_stream", flaky_open)
    await stream.start()
    await recorder.wait_for_updates(1)

    assert calls >= 2
    assert recorder.states[:2] == [
        ConnectionState.DISCONNECTED,
        ConnectionState.CONNECTED,
    ]
    assert "connector exploded" in (recorder.events[0].reason or "")
    # Backoff path, not the task-death tripwire.
    assert all(event.reason != "internal error" for event in recorder.events)


# ---------------------------------------------------------------------------
# reconnect backfill, timeout params, tripwire, PII
# ---------------------------------------------------------------------------


async def test_reconnect_backfill_with_identical_times_re_emits(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
) -> None:
    """A reconnect snapshot with unchanged timestamps is delivered again."""
    sse_server.scripts = [
        SseScript([("frame", build_frame(1, soc=0.5, time=T1))]),
        SseScript([("frame", build_frame(1, soc=0.5, time=T1)), ("sleep", 30)]),
    ]
    stream = stream_factory()
    await stream.start()
    await recorder.wait_for_updates(2)

    expected = (
        1,
        {Metric.SOC: MetricValue(value=50.0, time=T1_DT, provider=None)},
    )
    assert recorder.updates[0] == expected
    assert recorder.updates[1] == expected


class _SpyingSession:
    """Delegating session wrapper capturing the kwargs of every ``get()``."""

    def __init__(self, inner: aiohttp.ClientSession) -> None:
        self._inner = inner
        self.get_kwargs: list[dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> Any:
        self.get_kwargs.append(kwargs)
        return self._inner.get(url, **kwargs)


async def test_connect_timeout_params_passed_to_session_get(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    websession: aiohttp.ClientSession,
) -> None:
    """The SSE GET carries ClientTimeout(total=None, connect=30, sock_connect=15)."""
    sse_server.scripts = [
        SseScript([("frame", build_frame(1, soc=0.5)), ("sleep", 30)])
    ]
    spy = _SpyingSession(websession)
    stream = TelemetryStream(
        cast(aiohttp.ClientSession, spy),
        "partner-key",
        StaticAuth("stream-token"),
        [1],
        recorder.on_update,
        recorder.on_connection_change,
        backoff=(0.05,),
        watchdog_seconds=5.0,
    )
    try:
        await stream.start()
        await recorder.wait_for_updates(1)
    finally:
        await stream.stop()

    assert len(spy.get_kwargs) == 1
    timeout = spy.get_kwargs[0]["timeout"]
    assert isinstance(timeout, aiohttp.ClientTimeout)
    # total stays None: a total budget would tear down the healthy stream.
    assert timeout.total is None
    assert timeout.connect == 30
    assert timeout.sock_connect == 15
    # sock_read stays None: stall detection is the watchdog's job.
    assert timeout.sock_read is None


async def test_internal_error_tripwire_fires_disconnected_once(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A bug escaping the exception bands kills the task through the tripwire."""

    def exploding_extract(*args: Any, **kwargs: Any) -> dict[Any, Any]:
        raise RuntimeError("bug escaped the exception bands")

    monkeypatch.setattr("aioabrp.stream.extract_metrics", exploding_extract)
    sse_server.scripts = [
        SseScript([("frame", build_frame(1, soc=0.5)), ("sleep", 30)])
    ]
    stream = stream_factory()
    await stream.start()
    await recorder.wait_for_state(ConnectionState.DISCONNECTED)

    assert recorder.states == [
        ConnectionState.CONNECTED,
        ConnectionState.DISCONNECTED,
    ]
    assert recorder.events[1].reason == "internal error"
    assert "died unexpectedly" in caplog.text
    # The task is dead: no reconnect attempts, no second tripwire event.
    await asyncio.sleep(0.3)
    assert len(sse_server.requests) == 1
    assert [e.reason for e in recorder.events].count("internal error") == 1
    # stop() after the unexpected death is safe.
    await asyncio.wait_for(stream.stop(), timeout=1.0)


async def test_no_pii_in_logs_across_full_cycle(
    sse_server: SseServerHarness,
    recorder: CallbackRecorder,
    stream_factory: StreamFactory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Token, API key, and metric value/provider never reach the logs."""
    caplog.set_level(logging.DEBUG)
    token = "tok-hush-9000"
    api_key = "key-hush-9000"
    provider = "PROVIDER_SECRET"
    frame = build_frame(1, soc=0.4242, time=T1, provider=provider)
    sse_server.scripts = [
        SseScript([("frame", frame)]),
        SseScript([("frame", frame), ("sleep", 30)]),
    ]
    stream = stream_factory(api_key=api_key, auth=StaticAuth(token), name="pii")
    await stream.start()
    # Full cycle: connect, frame, disconnect, reconnect, frame, stop.
    await recorder.wait_for_updates(2)
    await asyncio.wait_for(stream.stop(), timeout=1.0)

    # The planted values genuinely flowed through the stream...
    delivered = recorder.updates[0][1][Metric.SOC]
    assert delivered.value == 42.42
    assert delivered.provider == provider
    # ...the DEBUG capture is live (per-frame logs were recorded, so the
    # no-leak assertion below is meaningful, not vacuous)...
    assert any("Frame for vehicle" in record.getMessage() for record in caplog.records)
    # ...and none of them surfaced in any log record.
    secrets = (token, api_key, provider, "0.4242", "42.42")
    for record in caplog.records:
        message = record.getMessage()
        for secret in secrets:
            assert secret not in message, (
                f"PII {secret!r} leaked into log record: {message!r}"
            )
