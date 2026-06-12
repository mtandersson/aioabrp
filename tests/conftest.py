"""Shared fixtures: a real local SSE server harness for stream tests.

The HA-source test suite mocks at the client layer; the library tests
deliberately exercise genuine ``text/event-stream`` bytes end-to-end
through a real ``aiohttp.web`` server instead. Each connection attempt
against ``GET /2/tlm`` pops the next :class:`SseScript` from the
harness's script list (the last script is reused once the list is
exhausted) and executes its actions in order, so multi-connection
reconnect choreography is expressed as one script per attempt.

Base-URL override: ``aioabrp.stream`` imports ``API_BASE_V2`` into its
own namespace and interpolates it at connect time (not at import time),
so monkeypatching ``aioabrp.stream.API_BASE_V2`` to the test server's
``/2`` prefix redirects every connection attempt here without touching
the stream's public constructor surface. The ``sse_server`` fixture
performs that monkeypatch itself.
"""

import asyncio
import json
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from aioabrp.auth import AbstractAuth, StaticAuth
from aioabrp.const import HEADER_ABRP_SESSION
from aioabrp.models import ConnectionEvent, ConnectionState, Metric, Telemetry
from aioabrp.stream import TelemetryStream

# Upper bound for every wait helper below. Generous next to the tiny
# injected stream timings, so a healthy run never gets near it while a
# hung test still fails fast.
WAIT_TIMEOUT = 5.0

# One scripted server action: ("frame", dict) | ("raw", bytes)
# | ("sleep", seconds) | ("close", None) | ("status", int).
SseAction = tuple[str, Any]

# The shape of the ``stream_factory`` fixture's return value, shared by
# every stream test module.
StreamFactory = Callable[..., TelemetryStream]


@dataclass
class SseScript:
    r"""One scripted SSE connection: actions executed in order.

    Actions:
    * ``("frame", dict)`` — write ``data: <json>\\n\\n``.
    * ``("raw", bytes)`` — write bytes verbatim (CR/LF + split-chunk cases).
    * ``("sleep", seconds)`` — hold the connection open silently.
    * ``("close", None)`` — end the chunked response (server-side close).
    * ``("status", int)`` — respond with this HTTP status instead of a
      stream (must be the first action; the connection never streams).

    Falling off the end of the action list closes the response, same as
    an explicit ``("close", None)``.
    """

    actions: list[SseAction] = field(default_factory=list)


@dataclass
class SseRequest:
    """Header + query capture of one connection attempt, for assertions."""

    headers: dict[str, str]
    query: dict[str, str]


class SseServerHarness:
    """Scripted local SSE server: per-connection scripts + request capture.

    Script selection: when the connection's ``X-ABRP-SESSION`` header has
    an entry in ``scripts_by_session``, that per-session list serves the
    attempt (multi-account isolation tests script each account
    independently); otherwise the shared ``scripts`` list is used. Both
    lists share the same pop semantics: one script per connection
    attempt, the last script reused once the list is exhausted.
    """

    def __init__(self) -> None:
        self.scripts: list[SseScript] = []
        self.scripts_by_session: dict[str, list[SseScript]] = {}
        self.requests: list[SseRequest] = []
        self._changed = asyncio.Event()

    def _next_script(self, request: web.Request) -> SseScript | None:
        """Pop the next script for this connection (per-session aware)."""
        session_token = request.headers.get(HEADER_ABRP_SESSION, "")
        scripts = self.scripts_by_session.get(session_token, self.scripts)
        if not scripts:
            return None
        return scripts.pop(0) if len(scripts) > 1 else scripts[0]

    def requests_for_session(self, session_token: str) -> list[SseRequest]:
        """Return the captured requests carrying this session token."""
        return [
            request
            for request in self.requests
            if request.headers.get(HEADER_ABRP_SESSION) == session_token
        ]

    async def handle(self, request: web.Request) -> web.StreamResponse:
        """Serve one connection attempt from the next script."""
        self.requests.append(
            SseRequest(headers=dict(request.headers), query=dict(request.query))
        )
        self._changed.set()
        script = self._next_script(request)
        if script is None:
            return web.Response(status=500)
        response: web.StreamResponse | None = None
        for action, payload in script.actions:
            if action == "status":
                assert response is None, "status must be the first action"
                return web.Response(status=payload)
            if response is None:
                response = web.StreamResponse()
                response.headers["Content-Type"] = "text/event-stream"
                await response.prepare(request)
            if action == "frame":
                body = json.dumps(payload).encode()
                await response.write(b"data: " + body + b"\n\n")
            elif action == "raw":
                await response.write(payload)
            elif action == "sleep":
                await asyncio.sleep(payload)
            elif action == "close":
                break
            else:  # pragma: no cover - script authoring error
                raise AssertionError(f"unknown SSE script action: {action!r}")
        if response is None:
            # A script with no streaming actions still answers 200 with an
            # immediately-closed (empty) event stream.
            response = web.StreamResponse()
            response.headers["Content-Type"] = "text/event-stream"
            await response.prepare(request)
        return response

    async def wait_for_requests(self, count: int) -> None:
        """Wait (bounded) until the server saw ``count`` connection attempts."""
        async with asyncio.timeout(WAIT_TIMEOUT):
            while len(self.requests) < count:
                self._changed.clear()
                await self._changed.wait()


def build_frame(
    vehicle_id: int,
    *,
    soc: float | None = None,
    power: float | None = None,
    voltage: float | None = None,
    range_m: float | None = None,
    battery_temp_c: float | None = None,
    charging_state: str | None = None,
    time: str | None = None,
    provider: str | None = None,
) -> dict[str, Any]:
    """Build a partial ``OutputPointWithVehicleId`` wire frame.

    Mirrors the v2 SSE wire shape: ``vehicleId`` plus zero or more nested
    per-metric records (``soc.frac``, ``power.w``, ``voltage.v``,
    ``estimatedBatteryRange.m``, ``batteryTemperature.c``,
    ``chargingState.state``). Values default to ``None`` (omitted from
    the frame, the wire's way of saying "no update"), not ``0``. When
    given, ``time`` (ISO string) and ``provider`` are stamped into every
    included metric block.
    """
    frame: dict[str, Any] = {"vehicleId": vehicle_id}
    if soc is not None:
        frame["soc"] = {"frac": soc}
    if power is not None:
        frame["power"] = {"w": power}
    if voltage is not None:
        frame["voltage"] = {"v": voltage}
    if range_m is not None:
        frame["estimatedBatteryRange"] = {"m": range_m}
    if battery_temp_c is not None:
        frame["batteryTemperature"] = {"c": battery_temp_c}
    if charging_state is not None:
        frame["chargingState"] = {"state": charging_state}
    for key, block in frame.items():
        if key == "vehicleId":
            continue
        if time is not None:
            block["time"] = time
        if provider is not None:
            block["provider"] = provider
    return frame


class CallbackRecorder:
    """Capture stream callbacks with awaitable progress hooks.

    Every recorded callback sets the internal :class:`asyncio.Event`, so
    the ``wait_for_*`` helpers wake exactly when progress happens (no
    polling sleeps) and time out via ``WAIT_TIMEOUT`` when it doesn't.
    """

    def __init__(self) -> None:
        self.updates: list[tuple[int, Telemetry]] = []
        self.events: list[ConnectionEvent] = []
        self._changed = asyncio.Event()

    def on_update(self, vehicle_id: int, metrics: Telemetry) -> None:
        self.updates.append((vehicle_id, metrics))
        self._changed.set()

    def on_connection_change(self, event: ConnectionEvent) -> None:
        self.events.append(event)
        self._changed.set()

    @property
    def states(self) -> list[ConnectionState]:
        return [event.state for event in self.events]

    async def wait_for_updates(self, count: int) -> None:
        """Wait (bounded) until ``count`` on_update calls were recorded."""
        async with asyncio.timeout(WAIT_TIMEOUT):
            while len(self.updates) < count:
                self._changed.clear()
                await self._changed.wait()

    async def wait_for_state(self, state: ConnectionState, count: int = 1) -> None:
        """Wait (bounded) until ``state`` has been observed ``count`` times."""
        async with asyncio.timeout(WAIT_TIMEOUT):
            while self.states.count(state) < count:
                self._changed.clear()
                await self._changed.wait()


@pytest.fixture
async def websession() -> AsyncIterator[aiohttp.ClientSession]:
    async with aiohttp.ClientSession() as session:
        yield session


@pytest.fixture
async def sse_server(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[SseServerHarness]:
    """Start the scripted SSE server and point the stream module at it."""
    harness = SseServerHarness()
    app = web.Application()
    app.router.add_get("/2/tlm", harness.handle)
    server = TestServer(app)
    await server.start_server()
    monkeypatch.setattr("aioabrp.stream.API_BASE_V2", str(server.make_url("/2")))
    yield harness
    await server.close()


@pytest.fixture
def recorder() -> CallbackRecorder:
    return CallbackRecorder()


@pytest.fixture
async def stream_factory(
    sse_server: SseServerHarness,
    websession: aiohttp.ClientSession,
    recorder: CallbackRecorder,
) -> AsyncIterator[StreamFactory]:
    """Build streams wired to the recorder; auto-stop them on teardown.

    Depends on ``sse_server`` purely for the fixture graph: it pins the
    teardown order (every created stream is stopped before the local
    server closes) and guarantees the factory can never run without the
    ``API_BASE_V2`` monkeypatch in place — a stream built without it
    would dial the real ABRP API.
    """
    created: list[TelemetryStream] = []

    def factory(
        *,
        api_key: str = "partner-key",
        auth: AbstractAuth | None = None,
        vehicle_ids: list[int] | None = None,
        name: str | None = None,
        backoff: Sequence[float] = (0.05, 0.1),
        watchdog_seconds: float = 5.0,
        on_update: Callable[[int, Telemetry], None] | None = None,
        on_connection_change: Callable[[ConnectionEvent], None] | None = None,
        seed: Mapping[int, Mapping[Metric, datetime]] | None = None,
    ) -> TelemetryStream:
        stream = TelemetryStream(
            websession,
            api_key,
            auth if auth is not None else StaticAuth("stream-token"),
            vehicle_ids if vehicle_ids is not None else [1],
            on_update if on_update is not None else recorder.on_update,
            on_connection_change
            if on_connection_change is not None
            else recorder.on_connection_change,
            name=name,
            backoff=backoff,
            watchdog_seconds=watchdog_seconds,
            seed=seed,
        )
        created.append(stream)
        return stream

    yield factory

    for stream in created:
        await stream.stop()
