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

### Identity from the OIDC `id_token`

If your auth flow yields an OIDC `id_token`, `parse_unverified_identity` turns it
into a stable identity — the `sub` claim plus a display name (the `name` → `email`
preference chain):

```python
from aioabrp import parse_unverified_identity

identity = parse_unverified_identity(id_token)
identity.subject       # OIDC `sub` — stable, non-empty; use as your unique id
identity.display_name  # `name` then `email`, stripped; None if neither present
identity.email         # the `email` claim alone, stripped, or None (e.g. a login_hint)
```

The decode is **unverified** — it reads claims without checking the signature.
That is safe only because the token arrives over TLS directly from the issuer
during the OAuth exchange; never treat the result as authenticated. A malformed
token or a missing/empty `sub` raises `AbrpAuthError`.

Distinct, similarly named properties live on the vehicle-model display metadata
returned by `async_get_vehicle_model_display` and carried on each garage
vehicle as `AbrpVehicle.display`. Its six fields mirror the wire
payload verbatim; two properties pre-assemble them into labels:

```python
display.manufacturer  # "Rivian" — wire field, as are model/years/title/start_year/end_year
display.model_name    # "R1S 2021-2024 Large Pack Performance" — make omitted
display.display_name  # "Rivian R1S 2021-2024 Large Pack Performance"
```

Both labels are always `str` (never `None`) and compose `model` + optional year
span + optional trim; `display_name` is `manufacturer` joined to `model_name` by
a single space. Prefer `model_name` where the make already has a field of its
own, pairing it with `manufacturer`.

## Consumer contracts

These behaviors are pinned by the test suite; consumers may rely on them.

### Garage

`async_get_vehicles()` returns the caller's garage, shared vehicles included,
in one request. The order is ABRP's (currently owned vehicles first, then
shared ones, each by id).
Each `AbrpVehicle` carries:

* `vehicle_id` and `vehicle_model` (the typecode: a catalogue typecode or a
  custom model id).
* `name`: the owner's nickname, `None` when unset.
* `display`: the `VehicleModelDisplay` ABRP shows for the vehicle's model, so
  no `async_get_vehicle_model_display` call per vehicle is needed. `None` when
  the typecode does not resolve.
* `relation`: `VehicleRelation.OWNED` or `VehicleRelation.SHARED`; `None` for a
  relation this version does not know.
* `paint`: always `None`. The garage endpoint does not return it, and the field
  is kept only for compatibility.

### Metrics and units

The library surfaces every metric ABRP's v2 telemetry `OutputPoint`
exposes — a 1:1 mirror of the 28 wire fields — as members of the `Metric`
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
| `battery_temperature`, `battery_temperature_min`, `battery_temperature_max`, `cabin_set_point`, `cabin_temperature`, `external_temperature` | `float` | °C |
| `calibrated_ref_cons` | `float` | Wh/km |
| `speed_factor` | `float` | dimensionless multiplier |
| `charging_state` | `ChargingState` | closed enum |
| `driving_state` | `DrivingState` | closed enum (gear) |
| `location` | `Location` | lat/lon |
| `calibrated_confidence` | `tuple[float, ...]` | 1- or 4-element confidence vector (opaque; not interpreted) |
| `map_info` | `MapInfo` | struct: `region` (enum), `country_3`, `address`, `speed_limit_ms` (m/s), `is_free_speed_zone` |

Notes:

- A frame is a **sparse delta**: `Telemetry.items()` yields only the
  metrics present in that frame, and may yield **any of the 28**.
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
  credentials. It fires on an HTTP 401 or a 403 `insufficient_scope`. Any
  other 403 (none of the requested vehicles is accessible) is a
  `DISCONNECTED` with backoff, since re-authenticating would not fix it.
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

### Changing vehicles at runtime

`stream.set_vehicle_ids(ids)` is synchronous and safe to call at any time
(before `start()`, connected, backing off, stopped). Duplicates are dropped
(first occurrence kept). If the *set* is unchanged nothing happens; otherwise
a live connection is closed promptly and re-opened with the new `vehicleIds`
— no backoff step, no error. That close emits one `DISCONNECTED` event with
reason `"vehicle set changed"`. A change during a backoff sleep takes effect
when it ends. An empty list is allowed (also at
construction): the stream idles without any HTTP request until ids are set.
Gate state for removed vehicles is dropped, and frames still in flight for a
removed vehicle are not delivered.

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

### Sending telemetry

`AbrpClient.async_send_telemetry(vehicle_id, point)` sends one
`TelemetryPoint` (`POST /2/app/tlm/{vehicle_id}`). The access token needs the
`tlm:write` scope. A token without it raises `AbrpScopeError` (a subclass of
`AbrpAuthError`, HTTP 403 `insufficient_scope`) with `.scope` naming the
missing scope: the token is still valid and needs re-authorizing with that
scope. Any call raises it when ABRP reports a missing scope. Any other 403
(say, a vehicle that is neither owned nor shared) raises `AbrpForbiddenError`,
an `AbrpApiError`: the session is fine, so re-authenticating won't help.

```python
from datetime import UTC, datetime

from aioabrp import ChargingState, Location, Provider, TelemetryPoint

point = TelemetryPoint(
    provider=Provider.APP_OBD,
    time=datetime.now(UTC),
    soc_frac=0.55,
    power_w=-11_000.0,
    location=Location(59.33, 18.07),
    charging_state=ChargingState.CHARGING_AC,
)
await client.async_send_telemetry(vehicle_id, point)
```

- **One time for all metrics.** `time` must be timezone-aware and is sent as
  UTC.
- **Units are in the field names** and are ABRP's wire units, sent
  unconverted: `soc_frac` is a fraction (0.55 = 55 %; the read side's
  `Telemetry.soc` is percent), then W, V, A, Wh, m, m/s (`speed_ms`), degrees
  and °C. Unset fields are not sent. Metrics ABRP computes itself (map info,
  calibrated values, speed factor) cannot be sent.
- **Newer spec fields** go in `extra`, which is added to the request body
  as-is. A mapping value without a `"time"` gets the point's time. A key
  that names a typed field (or `provider`) raises `ValueError`. `provider`
  also accepts a plain string for a provider newer than the `Provider` enum.
- **Types are checked when the point is built:** a `bool`, a non-finite
  number or a string where a number or enum belongs raises, and so does a
  point with no metric and no `extra`. Ranges are left to ABRP: its HTTP 400
  raises `AbrpApiError` carrying ABRP's error text.

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

CI also runs [gitleaks](https://github.com/gitleaks/gitleaks) on every PR and
push and fails if a commit contains a credential. Keep real keys and tokens in
the gitignored `.env`, never in source or tests.

### Live tests

`tests/test_live.py` holds read-only tests against the real ABRP API: garage,
catalog, display metadata, snapshots, the stream and `set_vehicle_ids`. They
are deselected by default (plain `uv run pytest` and CI never call the API)
and run only when selected explicitly:

```sh
uv run pytest -m live
```

They read `ABRP_API_KEY` and `ABRP_ACCESS_TOKEN` from the environment or from
a repo-root `.env` (see `.env.example`); a missing value fails the run. The
access token is short-lived, so refresh it right before a run. The account
needs at least one vehicle, and at least two for the `set_vehicle_ids` test
(skipped otherwise). The suite takes about 30 seconds.

One more test writes to a real vehicle, so `-m live` never selects it:

```sh
ABRP_WRITE_VEHICLE_ID=<vehicle id> uv run pytest -m live_write
```

It reads that vehicle's current state of charge and sends the same value
back, so the vehicle's data does not change (it is skipped when there is no
current SoC). `ABRP_WRITE_VEHICLE_ID` can also live in `.env`; the token must
be allowed to write telemetry.

### Releases

Commits follow [Conventional Commits](https://www.conventionalcommits.org/);
there is no CHANGELOG file and no version literal in the source — the version
is derived from the git tag at build time ([hatch-vcs](https://github.com/ofek/hatch-vcs)),
so feature PRs never carry a version bump.

To cut a release, a maintainer runs the **Release** workflow manually
(`workflow_dispatch`, from `main`). It runs the quality gate, then derives the
next version from the Conventional Commits since the last tag with
[git-cliff](https://git-cliff.org/) (`feat` → minor, `fix` → patch,
breaking → major from 1.0 on, minor before it) — or uses the optional `version` input (`X.Y.Z`) to force a
specific version; leave it blank to auto-derive. The workflow tags `vX.Y.Z`,
builds the sdist + wheel (hatch-vcs reads the version from the tag), publishes
to PyPI via [Trusted Publishing](https://docs.pypi.org/trusted-publishers/)
(OIDC), then pushes the tag and creates the GitHub Release with git-cliff
notes. The tag is pushed only after a successful publish, so a failed run
leaves no dangling tag.

## License

MIT — see [LICENSE](LICENSE).
