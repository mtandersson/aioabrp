"""Resilient SSE telemetry stream for the ABRP v2 ``/2/tlm`` endpoint.

:class:`TelemetryStream` owns a long-lived background task that connects
to the v2 server-sent-events telemetry endpoint, converts each wire
frame into the library's typed event shape, and delivers it to the
consumer through two synchronous callbacks:

* ``on_update(vehicle_id, telemetry: Telemetry)`` — one call per frame
  that survives extraction and the monotonicity gate;
* ``on_connection_change(event)`` — connection lifecycle transitions.

Contracts (consumer-facing):

* Callbacks are synchronous and delivered on the event loop that ran
  :meth:`TelemetryStream.start`; they must not block.
* ``CONNECTED`` fires on the first *frame* of a connection, not on the
  HTTP 200 — a slow-loris connect must not read as healthy.
* ``DISCONNECTED`` is steady-state, not exceptional: the ABRP server
  unilaterally closes idle streams at ~200 s and the stream reconnects
  with backoff. Every failed/ended connection attempt emits one
  ``DISCONNECTED`` event with a short reason string.
* ``AUTH_FAILED`` is terminal: the stream stops itself and the consumer
  decides whether/when to retry with fresh credentials.
* A transient (non-``AbrpAuthError``) token-getter failure emits NO
  ``ConnectionEvent`` (debug log only) — no connection attempt was
  made, so consumers keep seeing the last known state during a
  token-endpoint outage.

PII contract: log lines carry frame keys, counts, and short exception
summaries only — never frame bodies, header values, or tokens. Every
line is prefixed with the stream's ``name`` when set.

The only mutable state is instance-scoped (the per-``(vehicle_id,
Metric)`` monotonicity map, the unknown-chargingState dedup set, and
lifecycle flags) so multiple streams — including multiple accounts on
one session — never share state.
"""

import asyncio
import logging
from collections.abc import AsyncGenerator, Callable, Mapping, Sequence
from datetime import datetime
from http import HTTPStatus
from typing import Any

from aiohttp import ClientError, ClientSession, ClientTimeout

from . import _clock
from ._clock import _clamp_time, clamp_future_times
from ._extract import extract_metrics
from ._sse import iter_sse_events, parse_sse_event
from .auth import AbstractAuth
from .const import (
    API_BASE_V2,
    DEFAULT_BACKOFF_SECONDS,
    DEFAULT_WATCHDOG_SECONDS,
    ENDPOINT_TLM,
    HEADER_ABRP_SESSION,
    HEADER_API_KEY,
    SSE_CONNECT_TIMEOUT_SECONDS,
    SSE_SOCK_CONNECT_TIMEOUT_SECONDS,
)
from .exceptions import AbrpApiError, AbrpAuthError
from .models import ConnectionEvent, ConnectionState, Metric, MetricValue, Telemetry

_LOGGER = logging.getLogger(__name__)

# Backoff-sleep seam: the reconnect loop awaits this module-level alias
# (instead of ``asyncio.sleep`` directly) so tests can monkeypatch
# ``aioabrp.stream._sleep`` to capture backoff delays deterministically.
_sleep = asyncio.sleep


def _summarize_exc(err: BaseException) -> str:
    """Short triage-friendly summary of an exception for connection events.

    Keeps the format predictable for diagnostics consumers — exception
    class name plus a truncated message — without surfacing
    potentially-long transport tracebacks. The 80-char cap is chosen to
    fit on a single line of typical issue-tracker rendering.
    """
    return f"{type(err).__name__}: {str(err)[:80]}"


class TelemetryStream:
    """Long-lived consumer of the v2 ``/2/tlm`` SSE telemetry stream.

    Reconnects with exponential backoff (``backoff``, capped at the last
    value) on transient failures; the ladder resets to the first delay
    after any successful frame is received from a connection.

    Before each connection attempt a fresh access token is fetched from
    the injected :class:`~aioabrp.auth.AbstractAuth`; a transparent
    consumer-side refresh prevents a spurious 401 once a short-lived
    access token lapses. Only :class:`~aioabrp.exceptions.AbrpAuthError`
    — from the token getter or as an HTTP 401/403 on the stream — is
    terminal (``AUTH_FAILED`` event, task exits). Any other token-getter
    or transport failure falls through to the backoff path.

    Stall detection lives at this layer, not in the aiohttp request:
    each ``anext()`` on the SSE iterator is wrapped in
    :func:`asyncio.wait_for` with a ``watchdog_seconds`` bound. A
    half-open TCP / NAT-rebind / modem-suspend that the kernel never
    sees a FIN for would otherwise park the consumer indefinitely
    (production stall shape — observed as a 16 h flatline). On timeout
    the inner await is cancelled, the generator is released in the
    ``finally`` block, and the loop reconnects after the standard
    backoff with reason ``watchdog_stall_{watchdog_seconds}s``.

    The server's natural ~200 s idle close surfaces as a payload error
    → ``DISCONNECTED`` → reconnect with the first-tier backoff;
    successful frames reset the ladder so the cycle stays at the first
    tier and does not escalate.
    """

    def __init__(
        self,
        websession: ClientSession,
        api_key: str,
        auth: AbstractAuth,
        vehicle_ids: list[int],
        on_update: Callable[[int, Telemetry], None],
        on_connection_change: Callable[[ConnectionEvent], None],
        *,
        name: str | None = None,
        backoff: Sequence[float] = DEFAULT_BACKOFF_SECONDS,
        watchdog_seconds: float = DEFAULT_WATCHDOG_SECONDS,
        seed: Mapping[int, Mapping[Metric, datetime]] | None = None,
    ) -> None:
        """Initialize the stream; no I/O happens until :meth:`start`.

        ``seed`` is an optional per-vehicle mapping of
        :class:`~aioabrp.models.Metric` to the last wire-block time
        (tz-aware) seen for it, used to warm the monotonicity gate so a
        reconnect does not re-deliver values already seen before the
        process restarted. Each time is clamped not-future against a
        single clock read; only times are needed (no typed values).
        """
        if not backoff:
            raise ValueError("backoff must contain at least one delay")
        self._websession = websession
        self._api_key = api_key
        self._auth = auth
        self._vehicle_ids = list(vehicle_ids)
        self._on_update = on_update
        self._on_connection_change = on_connection_change
        self._name = name
        self._log_prefix = f"{name}: " if name else ""
        self._backoff = tuple(backoff)
        self._watchdog_seconds = watchdog_seconds
        self._task: asyncio.Task[None] | None = None
        self._stopped = False
        # Per-(vehicle_id, Metric) tz-aware block time of the most recent
        # adopted value — the monotonicity gate (see _gate_metrics).
        # Optionally warmed from a consumer-supplied seed (timestamps only;
        # clamped so a future seed time cannot wrongly gate later frames).
        self._last_times: dict[tuple[int, Metric], datetime] = {}
        if seed:
            now = _clock._now()
            for vehicle_id, times in seed.items():
                for metric, wire_time in times.items():
                    self._last_times[(vehicle_id, metric)] = _clamp_time(wire_time, now)
        # Instance-scoped dedup set for the unrecognized-chargingState
        # warning (never module-global — multi-account safety).
        self._unknown_charging_states_seen: set[str] = set()

    async def start(self) -> None:
        """Start the background SSE consumer task.

        Idempotent: calling while already running is a debug-logged
        no-op. The attached done-callback is a tripwire for unexpected
        (non-cancel) task death — a bug escaping the loop's exception
        bands logs and surfaces one final ``DISCONNECTED`` event with
        reason ``"internal error"`` instead of dying silently.
        """
        if self._task is not None and not self._task.done():
            _LOGGER.debug(
                "%sstart() called while already running; ignoring", self._log_prefix
            )
            return
        self._stopped = False
        self._task = asyncio.create_task(
            self._run(), name=f"aioabrp-telemetry-stream-{self._name or 'sse'}"
        )
        self._task.add_done_callback(self._on_task_done)

    async def stop(self) -> None:
        """Stop the stream: cancel the task and wait for it to finish.

        Cancel-based, never a graceful join — the task is typically
        parked in a network read, the watchdog ``wait_for``, or the
        backoff sleep. Idempotent and safe before :meth:`start`, after
        the ``AUTH_FAILED`` self-stop, and during the backoff sleep.
        ``_stopped`` is set BEFORE cancelling so the done-callback
        tripwire and any in-flight dispatch suppress callbacks: no
        callback is invoked after ``stop()`` returns.
        """
        self._stopped = True
        task = self._task
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            # This swallow is for the awaited task's cancellation result
            # only. A caller that is itself cancelled while parked on
            # ``await task`` (e.g. a timed-out ``wait_for(stop(), ...)``)
            # must see its own CancelledError propagate — absorbing it
            # would report "stopped" for a stream that may still be
            # running.
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                raise
        except Exception:
            # Already logged by the done-callback tripwire; stop() never
            # raises on a task that died before it was stopped.
            _LOGGER.debug(
                "%sstream task had already failed before stop()", self._log_prefix
            )

    def _on_task_done(self, task: asyncio.Task[None]) -> None:
        """Tripwire for unexpected task death (non-cancel, non-return)."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return
        _LOGGER.error(
            "%sTelemetry stream task died unexpectedly (%s); stream is down",
            self._log_prefix,
            _summarize_exc(exc),
            exc_info=exc,
        )
        self._dispatch(
            lambda: self._on_connection_change(
                ConnectionEvent(
                    state=ConnectionState.DISCONNECTED, reason="internal error"
                )
            ),
            "on_connection_change",
        )

    def _dispatch(self, invoke: Callable[[], None], what: str) -> None:
        """Invoke one consumer callback with containment.

        Frame loss beats stream death: a raising callback is logged with
        its traceback and swallowed so the consumer task survives. All
        callbacks are suppressed once ``stop()`` has begun.
        """
        if self._stopped:
            return
        try:
            invoke()
        except Exception:
            _LOGGER.exception(
                "%s%s callback raised; continuing", self._log_prefix, what
            )

    def _connection_event(
        self, state: ConnectionState, reason: str | None = None
    ) -> None:
        """Build and dispatch one contained ConnectionEvent."""
        event = ConnectionEvent(state=state, reason=reason)
        self._dispatch(
            lambda: self._on_connection_change(event), "on_connection_change"
        )

    async def _run(self) -> None:
        """Reconnect loop: token fetch, one connection attempt, backoff.

        Two cancellation paths are supported on ``stop()``: (a) naked
        propagation — ``wait_for`` delivers ``CancelledError`` into the
        generator's frame, the ``finally`` cleanup releases it, and the
        exception propagates out of this coroutine; (b) masked — an
        iterator that catches ``CancelledError`` inside ``__anext__``
        and re-raises it as ``StopAsyncIteration`` would let the loop
        fall through to the backoff sleep with the cancel signal
        "consumed". The :meth:`asyncio.Task.cancelling` check before the
        sleep catches that case.
        """
        delay_idx = 0
        while True:
            try:
                token = await self._auth.async_get_access_token()
            except AbrpAuthError as err:
                reason = _summarize_exc(err)
                _LOGGER.warning(
                    "%sToken getter reported terminal auth failure (%s); "
                    "stopping stream",
                    self._log_prefix,
                    reason,
                )
                self._connection_event(ConnectionState.AUTH_FAILED, reason)
                return
            except Exception as err:
                # Any non-terminal getter failure (consumer-side refresh
                # hiccup, transient transport error, even a bug raising
                # ValueError) is treated as transient: the loop survives
                # and retries after the standard backoff.
                _LOGGER.debug(
                    "%sTransient token fetch failure (%s); backing off",
                    self._log_prefix,
                    _summarize_exc(err),
                )
            else:
                try:
                    reason, saw_frame = await self._connect_once(token)
                except AbrpAuthError as err:
                    reason = _summarize_exc(err)
                    _LOGGER.warning(
                        "%sSSE stream rejected (%s); stopping stream",
                        self._log_prefix,
                        reason,
                    )
                    self._connection_event(ConnectionState.AUTH_FAILED, reason)
                    return
                if saw_frame:
                    delay_idx = 0
                _LOGGER.info(
                    "%sSSE stream disconnected (%s); reconnecting",
                    self._log_prefix,
                    reason,
                )
                self._connection_event(ConnectionState.DISCONNECTED, reason)

            # The ``finally: aclose()`` in _connect_once handles the naked
            # cancellation path. This check covers the masked path: an
            # iterator that converts ``CancelledError`` into
            # ``StopAsyncIteration`` exits the frame loop cleanly and would
            # otherwise strand stop() in the backoff sleep with the pending
            # cancellation "consumed". Bailing here turns it back into a
            # clean return so stop() finishes promptly.
            task = asyncio.current_task()
            if task is not None and task.cancelling():
                return

            delay = self._backoff[min(delay_idx, len(self._backoff) - 1)]
            await _sleep(delay)
            delay_idx = min(delay_idx + 1, len(self._backoff) - 1)

    async def _connect_once(self, token: str) -> tuple[str, bool]:
        """Run one connection attempt to completion.

        Returns ``(disconnect_reason, saw_frame)`` for every transient
        outcome (clean server close, watchdog stall, transport/API
        failure). Raises :class:`AbrpAuthError` untouched — the terminal
        path is the caller's to handle. The ``finally`` block releases
        the SSE generator no matter how the attempt ends.
        """
        disconnect_reason = "stream_closed"
        saw_frame = False
        # ``_open_stream(...)`` is an async-generator factory — the call
        # returns an AsyncGenerator that carries ``aclose()`` for the
        # ``finally`` cleanup below. ``agen`` stays ``None`` if the
        # factory itself raised before assignment (e.g. a monkeypatched
        # replacement with an eager side effect, or a future refactor
        # that opens the response eagerly).
        agen: AsyncGenerator[dict[str, Any]] | None = None
        try:
            agen = self._open_stream(token)
            while True:
                try:
                    frame = await asyncio.wait_for(
                        anext(agen), timeout=self._watchdog_seconds
                    )
                except StopAsyncIteration:
                    break
                if not saw_frame:
                    # CONNECTED on the first FRAME, not on the HTTP
                    # 200: a connection that opens but never produces
                    # a frame (slow-loris shape) must keep reading as
                    # down until proven healthy.
                    saw_frame = True
                    _LOGGER.info(
                        "%sSSE stream connected (first frame received)",
                        self._log_prefix,
                    )
                    self._connection_event(ConnectionState.CONNECTED)
                self._handle_frame(frame)
        except TimeoutError:
            disconnect_reason = f"watchdog_stall_{self._watchdog_seconds}s"
            _LOGGER.warning(
                "%sSSE stream stalled (no frames for %ss); forcing reconnect",
                self._log_prefix,
                self._watchdog_seconds,
            )
        except (AbrpApiError, ClientError) as err:
            disconnect_reason = _summarize_exc(err)
        finally:
            # Release the underlying aiohttp response even if the watchdog
            # fires mid-iteration or an exception aborts the loop.
            # ``aclose()`` is idempotent and tolerates an already-finished
            # generator.
            #
            # The ``except Exception`` guard around ``aclose()`` is a belt
            # for the case where the response teardown raises a
            # ``ClientError`` while closing a half-broken socket: without
            # it, the exception escapes the reconnect loop, the background
            # task dies, and the stream never reconnects. Log + swallow so
            # the outer ``while True:`` continues into the standard
            # backoff path.
            if agen is not None:
                try:
                    await agen.aclose()
                except Exception:
                    _LOGGER.warning(
                        "%sSSE generator aclose() raised; continuing reconnect loop",
                        self._log_prefix,
                        exc_info=True,
                    )
        return disconnect_reason, saw_frame

    async def _open_stream(self, token: str) -> AsyncGenerator[dict[str, Any]]:
        """Open the SSE response and yield parsed frame dicts.

        ``API_BASE_V2`` is read from this module's namespace at connect
        time (not bound at import) so tests can monkeypatch
        ``aioabrp.stream.API_BASE_V2`` at a local server.

        The total timeout budget stays ``None`` — the stream runs for
        the lifetime of the connection and a total-elapsed budget would
        tear it down; ``connect``/``sock_connect`` bound the pre-headers
        handshake (see :mod:`aioabrp.const`). In-stream stall detection
        is the caller's watchdog, not ``sock_read``.

        Raises:
            AbrpAuthError: HTTP 401/403 (session rejected by ABRP).
            AbrpApiError: any other HTTP/transport/parse failure; the
                caller treats it as a transient disconnect.

        """
        url = f"{API_BASE_V2}/{ENDPOINT_TLM}"
        params = {"vehicleIds": ",".join(str(vid) for vid in self._vehicle_ids)}
        headers = {
            "Accept": "text/event-stream",
            HEADER_API_KEY: self._api_key,
            HEADER_ABRP_SESSION: token,
        }
        try:
            async with self._websession.get(
                url,
                params=params,
                headers=headers,
                timeout=ClientTimeout(
                    total=None,
                    connect=SSE_CONNECT_TIMEOUT_SECONDS,
                    sock_connect=SSE_SOCK_CONNECT_TIMEOUT_SECONDS,
                ),
            ) as response:
                if response.status in (
                    HTTPStatus.UNAUTHORIZED,
                    HTTPStatus.FORBIDDEN,
                ):
                    raise AbrpAuthError(f"HTTP {response.status}")
                if response.status >= HTTPStatus.BAD_REQUEST:
                    raise AbrpApiError(f"HTTP {response.status}")
                async for event in iter_sse_events(response.content):
                    frame = parse_sse_event(event)
                    if frame is not None:
                        yield frame
        except ClientError as err:
            raise AbrpApiError(str(err)) from err

    def _handle_frame(self, frame: dict[str, Any]) -> None:
        """Extract, gate, and dispatch one parsed wire frame."""
        vehicle_id: int = frame["vehicleId"]
        extracted = extract_metrics(
            frame,
            unknown_charging_states_seen=self._unknown_charging_states_seen,
            log_name=self._name,
        )
        # One clock read per frame; future block times are rewritten to now
        # BEFORE the gate so a bad stamp can neither be delivered nor become
        # an unreachable high-water mark (see _clock.clamp_future_times).
        extracted = clamp_future_times(extracted, _clock._now())
        metrics = self._gate_metrics(vehicle_id, extracted)
        # PII contract: key names and counts only — never the frame body.
        _LOGGER.debug(
            "%sFrame for vehicle %s: keys=%s, %d extracted, %d adopted",
            self._log_prefix,
            vehicle_id,
            sorted(frame.keys()),
            len(extracted),
            len(metrics),
        )
        if metrics:
            telemetry = Telemetry(
                **{metric.value: value for metric, value in metrics.items()}
            )
            self._dispatch(lambda: self._on_update(vehicle_id, telemetry), "on_update")

    def _gate_metrics(
        self, vehicle_id: int, extracted: dict[Metric, MetricValue[Any]]
    ) -> dict[Metric, MetricValue[Any]]:
        """Apply the per-``(vehicle_id, Metric)`` monotonicity gate.

        Four rules, keyed on the wire block's tz-aware ``time``:

        * ``time is None`` — adopt AND clear the stored entry. A
          time-less block carries no ordering claim, so it also stops
          gating subsequent values (the stored-block-time contract).
        * no stored entry — adopt and store.
        * strictly older than stored — drop. A backdated rollup must
          not overwrite a fresher value already delivered.
        * equal or newer — adopt and store. Equal-time re-emits are BY
          DESIGN: reconnect snapshots re-deliver the current state with
          unchanged timestamps and consumers rely on that backfill.
        """
        adopted: dict[Metric, MetricValue[Any]] = {}
        for metric, value in extracted.items():
            key = (vehicle_id, metric)
            if value.time is None:
                self._last_times.pop(key, None)
                adopted[metric] = value
                continue
            last = self._last_times.get(key)
            if last is not None and value.time < last:
                continue
            self._last_times[key] = value.time
            adopted[metric] = value
        return adopted
