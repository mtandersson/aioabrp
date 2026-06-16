# aioabrp

Async Python client for the [A Better Routeplanner](https://abetterrouteplanner.com)
(ABRP) / Iternio telemetry API. The library mirrors ABRP's API points 1:1 and
has no Home Assistant dependency: a stateless request/response `AbrpClient`
(garage, vehicle catalog, vehicle-model display metadata, one-shot telemetry
snapshot) and a resilient
`TelemetryStream` (server-sent events with reconnect/backoff/watchdog) that
delivers extracted, typed metric values — never raw wire dicts — to consumer
callbacks. Authentication is injected: the consumer owns the token lifecycle
and hands the library a fresh access token via `AbstractAuth` (or the
fixed-token `StaticAuth` convenience).

## Installation

```sh
pip install aioabrp
```

Requires Python ≥ 3.14. The only runtime dependency is `aiohttp`.

## Usage

A runnable standalone example (fill in a real partner API key and access
token before running — the calls below hit the live API):

```python
import asyncio

import aiohttp

from aioabrp import (
    AbrpClient,
    ConnectionEvent,
    StaticAuth,
    Telemetry,
    TelemetryStream,
)

API_KEY = "your-iternio-partner-api-key"
ACCESS_TOKEN = "your-abrp-access-token"


def on_update(vehicle_id: int, telemetry: Telemetry) -> None:
    for metric, mv in telemetry.items():
        print(f"vehicle {vehicle_id}: {metric} = {mv.value!r} (time={mv.time})")


def on_connection_change(event: ConnectionEvent) -> None:
    print(f"connection: {event.state.name} (reason={event.reason})")


async def main() -> None:
    async with aiohttp.ClientSession() as session:
        auth = StaticAuth(ACCESS_TOKEN)
        client = AbrpClient(session, API_KEY, auth)

        vehicles = await client.async_get_vehicles()
        for vehicle in vehicles:
            print(f"{vehicle.vehicle_id}: {vehicle.name or vehicle.vehicle_model}")

        stream = TelemetryStream(
            session,
            API_KEY,
            auth,
            vehicle_ids=[v.vehicle_id for v in vehicles],
            on_update=on_update,
            on_connection_change=on_connection_change,
        )
        await stream.start()
        try:
            await asyncio.sleep(600)  # stream telemetry for ten minutes
        finally:
            await stream.stop()


if __name__ == "__main__":
    asyncio.run(main())
```

### Get your own API key

The `api_key` constructor argument is an Iternio **partner API key**, not a
per-user credential. If you are building your own consumer, obtain your own
key from Iternio — see the
[Iternio Telemetry API documentation](https://documenter.getpostman.com/view/7396339/SWTK5a8w)
or contact Iternio for a partner API key. The per-user access token comes
from your own auth flow and is handed to the library through `AbstractAuth`.

## Consumer contracts

These behaviors are pinned by the test suite; consumers may rely on them.

### Metrics and units

The library surfaces every metric ABRP's v2 telemetry `OutputPoint`
exposes — a 1:1 mirror of the 26 wire fields — as members of the `Metric`
enum, each carrying a `MetricValue[T]`. **Values keep the raw ABRP wire
scale; the library performs no unit conversion** (rendering is consumer
policy):

| Metric(s) | `value` type | Unit |
| --- | --- | --- |
| `soc`, `soh` | `float` | percent (wire `frac` surfaced ×100; `soh` not clamped) |
| `power`, `hvac_power` | `float` | W |
| `voltage` | `float` | V |
| `current` | `float` | A |
| `soe`, `battery_capacity`, `charging_energy_added` | `float` | Wh |
| `odometer`, `range`, `elevation` | `float` | m |
| `speed`, `calibrated_max_speed` | `float` | m/s |
| `heading` | `float` | degrees |
| `battery_temperature`, `cabin_set_point`, `cabin_temperature`, `external_temperature` | `float` | °C |
| `calibrated_ref_cons` | `float` | Wh/km |
| `speed_factor` | `float` | dimensionless multiplier |
| `charging_state` | `ChargingState` | closed enum |
| `driving_state` | `DrivingState` | closed enum (gear) |
| `location` | `Location` | lat/lon |
| `calibrated_confidence` | `tuple[float, ...]` | 1- or 4-element confidence vector (opaque; not interpreted) |
| `map_info` | `MapInfo` | struct: `region` (enum), `country_3`, `address`, `speed_limit_ms` (m/s), `is_free_speed_zone` |

Notes:

- A frame is a **sparse delta**: `Telemetry.items()` yields only the
  metrics present in that frame, and may yield **any of the 26**.
  Consumers that map `Metric` onto something else (e.g. an entity table)
  must tolerate members they don't handle rather than assume a fixed set.
- A categorical wire member the library doesn't recognize (an unknown
  `charging_state` / `driving_state`) **omits** that metric rather than
  leaking a raw string, and logs one warning per stream instance.
- `map_info` subfields are each independently optional; an unrecognized
  `region` degrades to `None` while the rest of the struct survives. A
  `map_info` block with no usable subfield is omitted entirely.

### Callbacks

- `on_update` and `on_connection_change` are **synchronous** callbacks,
  delivered on the event loop that ran `start()`. They must be
  non-blocking — a slow callback stalls the stream (and every other stream
  on the same loop).
- A raising callback is logged with its traceback and swallowed; the stream
  continues (frame loss beats stream death).

### Connection events

- `CONNECTED` fires on the **first frame** of a connection, not on the HTTP
  connect — a connection that opens but never produces a frame keeps
  reading as down until proven healthy.
- `DISCONNECTED` is **steady-state, not exceptional**: the ABRP server
  unilaterally closes idle streams at roughly 200 s and the stream
  reconnects with backoff. Do not treat a `DISCONNECTED` event as an
  outage. Events are status reports, not strict state transitions — a
  `DISCONNECTED` MAY arrive before the first `CONNECTED` (for example when
  the very first connection attempt fails).
- `AUTH_FAILED` is **terminal**: the stream stops itself and will not
  retry. The consumer decides whether/when to restart with fresh
  credentials.
- A transient (non-`AbrpAuthError`) failure from the token getter emits
  **no** `ConnectionEvent` at all (debug log only) — no connection attempt
  was made, so consumers keep seeing the last known state during a
  token-endpoint outage.

### Lifecycle

- `stop()` is idempotent and cancel-based (never a graceful join). No
  callbacks fire after `stop()` returns.
- `stop()` propagates the **caller's own** cancellation: if you wrap it in
  `asyncio.wait_for(stream.stop(), timeout)` and that times out, the
  resulting `TimeoutError` means the stream **may still be running** — the
  stop was interrupted, not completed.
- `start()` after `stop()` restarts the stream.

### Monotonicity gate

Each stream keeps one piece of state: a per-`(vehicle_id, Metric)` map of
the last adopted block timestamp.

- A block whose `time` is **strictly older** than the last adopted time for
  that `(vehicle, metric)` is dropped.
- An **equal-time** block re-emits **by design**: every reconnect
  re-delivers a full-state snapshot with unchanged block times, and
  consumers rely on that backfill.
- A **time-less** block (missing/malformed/naive `time`) is adopted and
  **clears** the gate for that metric — it carries no ordering claim, so it
  also stops gating subsequent values.
- A block whose `time` is in the **future** is rewritten to "now" before
  delivery (and before gating), so a clock-skewed upstream stamp can neither
  be delivered to the consumer nor become an unreachable high-water mark that
  silently stalls the metric. `AbrpClient.async_get_current_telemetry` applies
  the same clamp (it does not gate).
- **Known limitation:** a legitimately backdated server correction (an
  older timestamp that really is a newer truth) is suppressed for the
  stream's lifetime.

The gate can be **pre-warmed** across process restarts: pass
`TelemetryStream(..., seed=Mapping[int, Mapping[Metric, datetime]])` — a
per-vehicle map of metric to its last wire-block time (e.g. derived from the
consumer's last persisted snapshot) — and the stream seeds its high-water marks
from those times, each clamped not-future. Only times are needed; no typed
values. The clock is the `aioabrp._clock._now` seam (the monkeypatch target in
tests).

### Logging

Enable the `aioabrp` logger at `DEBUG` for triage. Connect/disconnect and
reasons log at `INFO`, watchdog stalls at `WARNING`, per-frame activity at
`DEBUG`. Frames are logged as keys and sizes only — frame bodies, header
values, and tokens (including GPS coordinates, the `map_info` street
address, and other PII) are never logged. The unknown-enum warnings
carry only the unrecognized member token, never any payload content.
Pass `name=` to `TelemetryStream` to prefix its log lines when running
multiple streams.

## Development

This project uses [uv](https://docs.astral.sh/uv/):

```sh
uv sync
uv run ruff format . && uv run ruff check . && uv run mypy && uv run pytest
```

Note: the locked dev environment constrains `aiohttp<3.14` because the
latest `aioresponses` release is incompatible with aiohttp ≥ 3.14
([aioresponses#289](https://github.com/pnuckowski/aioresponses/issues/289));
the published runtime dependency stays unpinned.

### Releases

Commits follow [Conventional Commits](https://www.conventionalcommits.org/);
there is no CHANGELOG file and no version literal in the source — the version
is derived from the git tag at build time ([hatch-vcs](https://github.com/ofek/hatch-vcs)),
so feature PRs never carry a version bump.

To cut a release, a maintainer runs the **Release** workflow manually
(`workflow_dispatch`, from `main`). It runs the quality gate, then derives the
next version from the Conventional Commits since the last tag with
[git-cliff](https://git-cliff.org/) (`feat` → minor, `fix` → patch,
breaking → major) — or uses the optional `version` input (`X.Y.Z`) to force a
specific version; leave it blank to auto-derive. The workflow tags `vX.Y.Z`,
builds the sdist + wheel (hatch-vcs reads the version from the tag), publishes
to PyPI via [Trusted Publishing](https://docs.pypi.org/trusted-publishers/)
(OIDC), then pushes the tag and creates the GitHub Release with git-cliff
notes. The tag is pushed only after a successful publish, so a failed run
leaves no dangling tag.

## License

MIT — see [LICENSE](LICENSE).
