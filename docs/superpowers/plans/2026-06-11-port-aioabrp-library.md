# aioabrp Library Port Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port the ABRP service layer out of the Home Assistant integration into this standalone `aioabrp` library, per `~/code/ha/aioabrp/PLAN.md` Workstream A, with the full layered test suite green.

**Architecture:** Pure-asyncio library mirroring ABRP's API points 1:1: a stateless request/response `AbrpClient` (garage, catalog, one-shot telemetry), a resilient `TelemetryStream` (SSE with backoff/watchdog/auth-terminal semantics ported from the HA coordinator), and an extraction layer that converts wire frames into typed `dict[Metric, MetricValue]` events. Auth is injected via `AbstractAuth`. The only state is a per-`(vehicle_id, Metric)` monotonicity timestamp map inside each stream instance. Zero module-global state (multi-account safety).

**Tech Stack:** Python ≥3.14, aiohttp (only runtime dep), pytest + pytest-asyncio (auto mode), aioresponses (endpoint mocks; dev lock pins aiohttp<3.14 — see pyproject comment), local `aiohttp.web` SSE server for stream tests, ruff + mypy strict.

**Source material** (read these files; they are the port source — branch `add-abetterrouteplanner` already checked out):
- `~/code/ha/home-assistant/homeassistant/components/abetterrouteplanner/api.py` (647 lines)
- `~/code/ha/home-assistant/homeassistant/components/abetterrouteplanner/coordinator.py` (752 lines)
- `~/code/ha/home-assistant/homeassistant/components/abetterrouteplanner/_sensor_value_fns.py` (318 lines)
- `~/code/ha/home-assistant/homeassistant/components/abetterrouteplanner/_telemetry_models.py` (246 lines)
- `~/code/ha/home-assistant/homeassistant/components/abetterrouteplanner/const.py` (63 lines)
- Tests: `~/code/ha/home-assistant/tests/components/abetterrouteplanner/{test_api.py,test_telemetry_models.py,test_sse_reconnect.py,conftest.py,test_sensor.py}`

**Authoritative design:** `~/code/ha/aioabrp/PLAN.md` (Workstream A). Where this plan and PLAN.md disagree, PLAN.md wins; stop and report instead of guessing.

**Working conventions for every task:**
- TDD: write the failing test first, see it fail, implement, see it pass.
- After each task: `uv run ruff format . && uv run ruff check . && uv run mypy && uv run pytest` must all pass.
- One conventional commit per task (message given in the task). Do NOT push or tag.
- Logger namespace: every module logs via `logging.getLogger(__name__)` (package `aioabrp`).
- PII hygiene (PLAN.md "Logging contract"): NEVER log frame bodies, header values, or tokens. Log frame keys / sizes / reasons only. Exception summaries use an 80-char truncation helper. The HA `api.py:448`-style whole-frame debug log must NOT be ported.
- Port docstrings: the hard-won edge-case docstrings in the source ARE the spec — port them with the code, rewriting HA-specific references (coordinator, sensors, fixtures, HA util paths) to lib-appropriate wording. Do not silently drop a docstring's contract.

---

## File Structure (final state)

```
src/aioabrp/
  __init__.py      # public re-exports (grows per task; final surface in Task 8)
  exceptions.py    # AbrpError, AbrpAuthError, AbrpApiError
  const.py         # API bases, header names, endpoint paths, default timings
  models.py        # Metric, MetricValue, ChargingState, Location, ConnectionState,
                   # ConnectionEvent, AbrpVehicle, CatalogEntry
  auth.py          # AbstractAuth, StaticAuth
  _wire_types.py   # pruned TypedDicts + regen recipe (internal)
  _extract.py      # frame -> dict[Metric, MetricValue] (internal)
  _sse.py          # SSE byte-stream parsing internals
  client.py        # AbrpClient
  stream.py        # TelemetryStream
tests/
  test_smoke.py            # exists
  test_models.py           # Task 1
  test_wire_types.py       # Task 2
  test_extract.py          # Task 3
  test_sse_parser.py       # Task 4
  test_client.py           # Task 5
  conftest.py              # Task 6 (local SSE server fixtures)
  test_stream.py           # Task 6 (lifecycle)
  test_stream_resilience.py# Task 7 (edge-case battery)
  test_stream_isolation.py # Task 7 (multi-account)
```

---

## Task 1: Core types — exceptions, const, models, auth

**Files:**
- Create: `src/aioabrp/exceptions.py`, `src/aioabrp/const.py`, `src/aioabrp/models.py`, `src/aioabrp/auth.py`
- Modify: `src/aioabrp/__init__.py` (re-export everything public from this task)
- Test: `tests/test_models.py`

These are pure declarations. Write tests first for behavior that matters: enum values, frozenness, StaticAuth round-trip, exception hierarchy.

- [ ] **Step 1: Write failing tests** in `tests/test_models.py`:

```python
"""Tests for aioabrp's public models, exceptions, and auth helpers."""

import dataclasses

import pytest

from aioabrp import (
    AbrpApiError,
    AbrpAuthError,
    AbrpError,
    AbrpVehicle,
    CatalogEntry,
    ChargingState,
    ConnectionEvent,
    ConnectionState,
    Location,
    Metric,
    MetricValue,
    StaticAuth,
)


def test_exception_hierarchy() -> None:
    assert issubclass(AbrpAuthError, AbrpError)
    assert issubclass(AbrpApiError, AbrpError)
    assert issubclass(AbrpError, Exception)


def test_metric_values_match_registry_keys() -> None:
    assert {m.value for m in Metric} == {
        "soc", "power", "voltage", "soe", "odometer", "calibrated_ref_cons",
        "battery_capacity", "soh", "range", "battery_temperature",
        "charging_state", "location",
    }


def test_charging_state_members() -> None:
    assert {c.value for c in ChargingState} == {
        "charging_ac", "charging_dc", "charging_unknown",
        "not_charging", "plugged_in",
    }


@pytest.mark.parametrize(
    "instance",
    [
        MetricValue(value=1.0, time=None, provider=None),
        Location(lat=1.0, lon=2.0),
        ConnectionEvent(state=ConnectionState.CONNECTED, reason=None),
        AbrpVehicle(vehicle_id=1, name=None, vehicle_model="x", paint=None),
        CatalogEntry(
            typecode="t", manufacturer=None, model=None, title=None,
            start_year=None, end_year=None, battery_capacity_wh=None,
        ),
    ],
)
def test_models_are_frozen(instance: object) -> None:
    field = dataclasses.fields(instance)[0].name  # type: ignore[arg-type]
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(instance, field, "nope")


async def test_static_auth_returns_token() -> None:
    auth = StaticAuth("tok-123")
    assert await auth.async_get_access_token() == "tok-123"
```

- [ ] **Step 2:** `uv run pytest tests/test_models.py -q` → FAIL (ImportError).

- [ ] **Step 3: Implement.**

`src/aioabrp/exceptions.py`:
```python
"""Exceptions raised by aioabrp."""


class AbrpError(Exception):
    """Base for all aioabrp errors."""


class AbrpAuthError(AbrpError):
    """Authentication or session failure (terminal — do not retry)."""


class AbrpApiError(AbrpError):
    """Non-auth API failure (transport, malformed response, business error)."""
```

`src/aioabrp/const.py` — port from HA `const.py:13-26` and `api.py:49-56` (API-intrinsic only; the HA app key, OAuth endpoints, and HA conf keys do NOT move — PLAN.md "What deliberately does NOT move"):
```python
"""API constants for the ABRP / Iternio telemetry API."""

API_BASE_V1 = "https://api.iternio.com/1"
API_BASE_V2 = "https://api.iternio.com/2"
ENDPOINT_GET_TLM = "session/get_tlm"
ENDPOINT_TLM = "tlm"
ENDPOINT_VEHICLE_LIST = "vehicle/_list"

HEADER_API_KEY = "X-API-KEY"
HEADER_ABRP_SESSION = "X-ABRP-SESSION"

ONE_SHOT_TIMEOUT_SECONDS = 30
SSE_CONNECT_TIMEOUT_SECONDS = 30
SSE_SOCK_CONNECT_TIMEOUT_SECONDS = 15
DEFAULT_BACKOFF_SECONDS: tuple[float, ...] = (5, 10, 30, 60)
DEFAULT_WATCHDOG_SECONDS = 300.0
```
Port the rationale comments/docstrings for the timeout constants from `api.py:40-56` and `coordinator.py:50-63` (the sock_connect rationale and the 300s watchdog "~100s above the natural ~200s server close" comment) — rewritten to stand alone.

`src/aioabrp/models.py`:
```python
"""Public typed models for aioabrp."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum, StrEnum


class Metric(StrEnum):
    """A telemetry metric surfaced by the ABRP v2 API."""

    SOC = "soc"
    POWER = "power"
    VOLTAGE = "voltage"
    SOE = "soe"
    ODOMETER = "odometer"
    CALIBRATED_REF_CONS = "calibrated_ref_cons"
    BATTERY_CAPACITY = "battery_capacity"
    SOH = "soh"
    RANGE = "range"
    BATTERY_TEMPERATURE = "battery_temperature"
    CHARGING_STATE = "charging_state"
    LOCATION = "location"


class ChargingState(StrEnum):
    """Categorical charging state (closed enum mirroring the wire members)."""

    CHARGING_AC = "charging_ac"
    CHARGING_DC = "charging_dc"
    CHARGING_UNKNOWN = "charging_unknown"
    NOT_CHARGING = "not_charging"
    PLUGGED_IN = "plugged_in"


@dataclass(frozen=True, slots=True)
class Location:
    """A GPS coordinate pair."""

    lat: float
    lon: float


@dataclass(frozen=True, slots=True)
class MetricValue:
    """One extracted metric value.

    Units keep the HA extractor semantics: percent (soc/soh), W, V, Wh,
    m, °C, Wh/km. ``time`` is the wire block's tz-aware timestamp or
    ``None``; ``provider`` is the clean upstream provider string or ``None``.
    """

    value: float | ChargingState | Location
    time: datetime | None
    provider: str | None


class ConnectionState(Enum):
    """Stream connection state."""

    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    AUTH_FAILED = "auth_failed"


@dataclass(frozen=True, slots=True)
class ConnectionEvent:
    """A stream connection state change."""

    state: ConnectionState
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class AbrpVehicle:
    """One vehicle from the v1 garage enumeration (raw wire fields only)."""

    vehicle_id: int
    name: str | None
    vehicle_model: str
    paint: str | None


@dataclass(frozen=True, slots=True)
class CatalogEntry:
    """One catalog vehicle template from ``GET /2/vehicle/_list``."""

    typecode: str
    manufacturer: str | None
    model: str | None
    title: str | None
    start_year: int | None
    end_year: int | None
    battery_capacity_wh: int | None
```
Note: `AbrpVehicle` deliberately DROPS the HA dataclass's `device_model` / `device_manufacturer` enrichment fields (`api.py:82-108`) — enrichment is HA-side policy.

`src/aioabrp/auth.py`:
```python
"""Injected authentication for aioabrp.

The library never sees refresh tokens or OAuth endpoints; the consumer
owns the token lifecycle and hands the library a fresh access token via
:class:`AbstractAuth`.
"""

from abc import ABC, abstractmethod


class AbstractAuth(ABC):
    """Provides a fresh access token on demand.

    Implementations MUST raise :class:`aioabrp.AbrpAuthError` for terminal
    auth failure (e.g. a revoked refresh token). Any other exception is
    treated as transient by the library.
    """

    @abstractmethod
    async def async_get_access_token(self) -> str:
        """Return a valid access token."""


class StaticAuth(AbstractAuth):
    """A fixed-token :class:`AbstractAuth` for scripts and pre-OAuth flows."""

    def __init__(self, token: str) -> None:
        self._token = token

    async def async_get_access_token(self) -> str:
        return self._token
```

`src/aioabrp/__init__.py`: keep docstring + `__version__`, add re-exports of all names above plus `__all__`.

- [ ] **Step 4:** `uv run pytest -q` → all pass. `uv run ruff format . && uv run ruff check . && uv run mypy` → clean.
- [ ] **Step 5: Commit:** `feat: add core types, exceptions, and injected auth`

---

## Task 2: Wire types — `_wire_types.py` + keep-set canary

**Files:**
- Create: `src/aioabrp/_wire_types.py`
- Test: `tests/test_wire_types.py`

Port `_telemetry_models.py` (246 lines, 29 classes) essentially verbatim — same TypedDicts, same keep-set. Changes required:

1. Module docstring regen recipe: replace the `~/abrp/abrp` local-path spec source with the public URL `https://api.iternio.com/swagger-ui/spec/prod/IternioPlanning.out.yaml` (verified live 2026-06-11; tracks production). Keep the full 4-step codegen→prune→test-pin→discard procedure (`_telemetry_models.py:1-74`) otherwise intact, retargeting test references to `tests/test_wire_types.py`.
2. The keep-set justification for `location` changes: it is now consumed by the lib's `Metric.LOCATION` extractor (not HA diagnostics) — reword that bullet.
3. References to `STAMPED_VALUE_FNS` / `_sensor_value_fns` become references to `aioabrp._extract`.

- [ ] **Step 1: Write failing tests** in `tests/test_wire_types.py` — port `test_telemetry_models.py` (124 lines) verbatim in substance: the `_KEEP_SET_LEAVES` tuple (12 leaves with exact key frozensets, including `(Location, frozenset({"lat", "long"}))`), `_EXPECTED_OUTPUT_POINT_KEYS` (12 camelCase keys), and all 5 tests (`is_typeddict` checks, parametrized per-leaf key pin, OutputPoint cardinality canary, vehicleId extension). Import from `aioabrp._wire_types`.
- [ ] **Step 2:** `uv run pytest tests/test_wire_types.py -q` → FAIL (ImportError).
- [ ] **Step 3:** Create `src/aioabrp/_wire_types.py` by porting `_telemetry_models.py:76-246` (all type aliases incl. the 16-member `Provider` Literal and 5-member `ChargingStateValue` Literal, mixins `WithTime`/`WithTimeAndProvider`, the 12 leaf pairs, `OutputPoint`, `OutputPointWithVehicleId`) plus the updated module docstring per the three changes above.
- [ ] **Step 4:** `uv run pytest -q` → pass; ruff/mypy clean.
- [ ] **Step 5: Commit:** `feat: add pruned wire TypedDicts with regen recipe`

---

## Task 3: Extraction — `_extract.py` + tolerance matrix

**Files:**
- Create: `src/aioabrp/_extract.py`
- Test: `tests/test_extract.py`

This ports `_sensor_value_fns.py` and `coordinator.py:178-216` (`_parse_block_time`) and adds the NEW location extractor, restructured so the output is the lib's typed event shape.

**Public (package-internal) surface:**

```python
"""Frame -> typed metric extraction. Internal module."""

import logging
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from .models import ChargingState, Location, Metric, MetricValue

_LOGGER = logging.getLogger(__name__)

# Wire key for each metric (camelCase on the wire for 5 of them).
WIRE_KEYS: dict[Metric, str] = {
    Metric.SOC: "soc",
    Metric.POWER: "power",
    Metric.VOLTAGE: "voltage",
    Metric.SOE: "soe",
    Metric.ODOMETER: "odometer",
    Metric.CALIBRATED_REF_CONS: "calibratedRefCons",
    Metric.BATTERY_CAPACITY: "batteryCapacity",
    Metric.SOH: "soh",
    Metric.RANGE: "estimatedBatteryRange",
    Metric.BATTERY_TEMPERATURE: "batteryTemperature",
    Metric.CHARGING_STATE: "chargingState",
    Metric.LOCATION: "location",
}


def parse_block_time(block: Mapping[str, Any]) -> datetime | None: ...
def is_clean_provider_str(value: object) -> bool: ...


def extract_metrics(
    frame: Mapping[str, Any],
    *,
    unknown_charging_states_seen: set[str],
    log_name: str | None = None,
) -> dict[Metric, MetricValue]:
    """Extract every present, well-formed metric from one wire frame."""
```

**Port rules:**

- Each numeric extractor ports its tolerance contract verbatim from `_sensor_value_fns.py:27-156`: missing key / `null` block / empty dict / null leaf / non-numeric leaf / **bool-as-number** → metric omitted. Values: soc & soh are `frac * 100` (soh intentionally NOT clamped at 100 — port that docstring), power `w`, voltage `v`, soe & battery_capacity `wh`, odometer & range `m`, battery_temperature `c` (no lower-bound filter — port docstring), calibrated_ref_cons `wh_per_km`. Implement DRY: one `_float_leaf(block, leaf_key) -> float | None` helper handling the shared tolerance matrix, with soc/soh applying `* 100`; keep the per-metric docstrings' load-bearing notes in the registry or a module-level table comment.
- Charging state (`_sensor_value_fns.py:210-236`): map wire member → `ChargingState` enum (`"CHARGING_AC"` → `ChargingState.CHARGING_AC` etc.). All degenerate shapes → omitted. Unrecognized non-empty member → omitted + ONE warning per `unknown_charging_states_seen` set (the set is caller-owned/instance-scoped — NO module-global; PLAN.md multi-account rule). Warning text adapted from `_sensor_value_fns.py:231-235`, prefixed with `log_name` when given; must not include anything but the state string.
- **NEW location extractor**: wire shape `{"location": {"lat": <float>, "long": <float>, "time": ..., "provider": ...}}` (spec `Location` block uses `lat`/`long`; the model field is `lon`). Tolerance matrix mirrors the numeric ones: block missing/null/empty/non-dict → omitted; either leaf missing/null/non-numeric/bool → omitted; both numeric → `Location(lat=float(lat), lon=float(long))`.
- `parse_block_time` ports `coordinator.py:178-216` with `datetime.fromisoformat` replacing HA's `parse_datetime`: non-string → None; `ValueError` from fromisoformat (malformed OR structurally-valid-but-impossible dates) → None; naive result (`tzinfo is None`) → None; else the parsed aware datetime. Port the docstring contract (rewrite the HA-fixture justification to "callers rely on None to bypass the monotonicity gate — today's contract").
- `is_clean_provider_str` ports `_sensor_value_fns.py:287-318` verbatim including the full ASCII-whitespace-contract docstring (it is load-bearing; retarget the spec reference to the public spec URL).
- Provider per metric: from the same block, `provider` leaf, gated by `is_clean_provider_str` → else `None` (port `_extract_provider`, `_sensor_value_fns.py:263-284`).
- `extract_metrics` loops `WIRE_KEYS`; for each metric whose value extractor returns non-None, emits `MetricValue(value=..., time=parse_block_time(block), provider=<clean provider or None>)`. For non-dict blocks there is no time/provider — but note: a non-dict block never yields a value anyway, so MetricValue construction only happens with a dict block in hand.

- [ ] **Step 1: Write failing tests** in `tests/test_extract.py`. Port the 21-row null-safety matrix from HA `test_sensor.py:491-569` (same ids: `soc_ok`, `soc_absent`, `soc_null`, `soc_empty`, `soc_inner_null`, `soc_inner_bool`, `power_inner_str`, the 8 charging-state degenerate rows, etc.), re-targeted at `extract_metrics` (assert metric present/absent in the returned dict and the value when present — e.g. `{"soc": {"frac": 0.5}}` → `result[Metric.SOC].value == 50.0`). Add:
  - a full happy-path frame test (all 12 metrics in one frame → 12 entries with correct values, times, providers);
  - a **location tolerance matrix** (parametrized: ok / absent / null / empty / lat-only / long-only / lat-null / lat-bool / lat-string → omitted except ok);
  - time handling: block with `"time": "2026-05-25T12:00:00Z"` → aware datetime; `"time": 12345` → `time is None` (value still adopted); naive `"2026-05-25T12:00:00"` → `time is None`; `"2026-13-01T00:00:00Z"` → `time is None`;
  - provider handling: `"provider": "RIVIAN_STREAM"` → kept; `""`, `"  padded "`, `123`, missing → `None`;
  - charging-state warning dedup: with one shared `seen` set, two frames with `"FOO"` log exactly one warning (use `caplog`); two DIFFERENT sets (two instances) each log once;
  - soh overshoot: `{"soh": {"frac": 1.02}}` → `102.0` (not clamped).
- [ ] **Step 2:** `uv run pytest tests/test_extract.py -q` → FAIL.
- [ ] **Step 3:** Implement `src/aioabrp/_extract.py` per the port rules above.
- [ ] **Step 4:** `uv run pytest -q` → pass; ruff/mypy clean.
- [ ] **Step 5: Commit:** `feat: add typed frame extraction with tolerance matrix`

---

## Task 4: SSE parsing internals — `_sse.py`

**Files:**
- Create: `src/aioabrp/_sse.py`
- Test: `tests/test_sse_parser.py`

Port the SSE byte-stream machinery out of `api.py:349-452` into two standalone pieces:

```python
"""SSE byte-stream parsing internals."""

import logging
from codecs import getincrementaldecoder
from collections.abc import AsyncIterator
from typing import Any

from aiohttp import StreamReader

from .exceptions import AbrpApiError

_LOGGER = logging.getLogger(__name__)


async def iter_sse_events(content: StreamReader) -> AsyncIterator[str]:
    """Yield raw ``\\n\\n``-terminated SSE event blocks from a byte stream."""


def parse_sse_event(event: str) -> dict[str, Any] | None:
    """Parse one SSE event block into a JSON frame dict."""
```

**Port rules:**
- `iter_sse_events` ports `api.py:385-420`: incremental UTF-8 decoder (`getincrementaldecoder("utf-8")(errors="replace")` — port the mojibake-rationale docstring), chunk loop over `content.iter_any()` (match the source's chunk-read primitive — check `api.py:391-393` for the exact call), CR/LF normalization holding back a trailing `\r` to detect pairs spanning chunks, split on `\n\n`, and the final-flush path on stream close (decoder `final=True`, normalize, yield remaining non-blank buffer — port the cold-reconnect rationale docstring at `api.py:412-414`).
- `parse_sse_event` ports `api.py:425-452` with one change: the frame-body debug logs must follow the PII contract — on the missing-`vehicleId` drop, log `sorted(decoded.keys())` instead of the HA version's `%r` whole-frame dump. Behavior: skip blank/`:`-comment lines; join `data:` payloads with `\n` (strip one leading space); empty data → `None`; `json.JSONDecodeError` → raise `AbrpApiError`; decoded non-dict → raise `AbrpApiError`; missing `vehicleId` → debug log keys, return `None`; else return the dict.

- [ ] **Step 1: Write failing tests** in `tests/test_sse_parser.py`. Port the parser test surface from HA `test_api.py:542-671` (read those tests; they cover: single data line, multi-line data join, comment/keepalive → None, missing vehicleId → None, malformed JSON → AbrpApiError, CRLF intact vs split across chunks). Drive `iter_sse_events` with a real `aiohttp.StreamReader` fed manually (`feed_data` + `feed_eof`) — cover: one event in one chunk; one event split mid-multibyte-UTF-8-character; `\r\n\r\n` boundary split between chunks (the held-back `\r` path); two events in one chunk; final event without trailing blank line (flush path); pure-comment stream yields nothing. Also assert the missing-vehicleId debug log does NOT contain a frame value (caplog: log line contains the key name but not e.g. `"secret-value"` planted in the frame).
- [ ] **Step 2:** FAIL. **Step 3:** implement. **Step 4:** suite green + lint/type clean.
- [ ] **Step 5: Commit:** `feat: add incremental SSE event parser`

---

## Task 5: Request/response client — `client.py`

**Files:**
- Create: `src/aioabrp/client.py`
- Test: `tests/test_client.py`

```python
"""Request/response client for the ABRP v1/v2 endpoints."""


class AbrpClient:
    def __init__(
        self,
        websession: aiohttp.ClientSession,
        api_key: str,
        auth: AbstractAuth,
    ) -> None: ...

    async def async_get_vehicles(self) -> list[AbrpVehicle]: ...
    async def async_get_catalog(self) -> dict[str, CatalogEntry]: ...
    async def async_get_current_telemetry(
        self, vehicle_id: int
    ) -> dict[Metric, MetricValue]: ...
```

**Port rules (source `api.py`):**
- The auth-error regex `_AUTH_ERROR_RE` ports verbatim from `api.py:58-71`.
- Every method FIRST awaits `self._auth.async_get_access_token()` **outside** the `except ClientError` band (PLAN.md failure-containment: `AbrpAuthError` from the getter propagates untouched; a refresh `ClientResponseError` must not be laundered into `AbrpApiError`). Then the HTTP call inside `try: ... except (ClientError, TimeoutError) as err: raise AbrpApiError(...)`.
- `async_get_vehicles` ports `api.py:154-202`: `POST {API_BASE_V1}/{ENDPOINT_GET_TLM}`, headers `{"Authorization": f"APIKEY {api_key}"}`, body `{"session_id": <token>}`. 401/403 → `AbrpAuthError(f"HTTP {status}")`; other non-2xx → `AbrpApiError`. Envelope: non-dict payload → `AbrpApiError`; `status != "ok"` → error text through `_AUTH_ERROR_RE` → `AbrpAuthError` or `AbrpApiError`; missing/malformed `result` list → `AbrpApiError`; map records through ported `_parse_vehicle` (`api.py:455-472`, minus enrichment fields).
- `async_get_catalog` ports `api.py:204-272`: `GET {API_BASE_V2}/{ENDPOINT_VEHICLE_LIST}`, headers `Accept: application/json`, `X-API-KEY: <api_key>`, `X-ABRP-SESSION: <token>`, `ClientTimeout(total=ONE_SHOT_TIMEOUT_SECONDS)`. Bare JSON (no envelope). Port `_str_or_none` / `_int_or_none` / `_parse_catalog_entry` verbatim (`api.py:475-539`) INCLUDING their docstrings (retarget the `_compute_device_model` consumer note: that consumer is now HA-side; the trimming contract still holds for any display consumer). Skip non-dict records; skip entries with missing/empty typecode; key dict by typecode.
- **Do NOT port** `_typecode_prefix_match` / `_match_catalog_entry` / `_compose_device_model` / `_compute_device_model` / `_enrich_with_catalog` (`api.py:542-647`) — HA-side policy (PLAN.md, reaffirmed).
- `async_get_current_telemetry` ports `api.py:302-347` then extends: `GET {API_BASE_V2}/{ENDPOINT_TLM}/{vehicle_id}`, same v2 headers + timeout, bare JSON, `{}` is valid "no data yet". Non-dict payload → `AbrpApiError`. Then return `extract_metrics(payload, unknown_charging_states_seen=self._unknown_charging_states_seen)` — the client owns ONE instance-scoped set for charging-state warning dedup. Stateless otherwise: NO monotonicity gating here (PLAN.md: same extracted shape as stream events; seeding policy is the consumer's).

- [ ] **Step 1: Write failing tests** in `tests/test_client.py` using **aioresponses**. Port the endpoint test surface from HA `test_api.py` (sections: vehicles 76-232, one-shot 345-452, catalog 480-518, catalog-entry parsing 756-911 — read them and re-land every behavioral case at the lib boundary). Required cases:
  - vehicles: happy path (2 vehicles, fields mapped, name-null tolerated); envelope `status:"error"` with auth-flavored text (`"session expired"`) → `AbrpAuthError`; non-auth error text → `AbrpApiError`; HTTP 401 and 403 → `AbrpAuthError`; HTTP 500 → `AbrpApiError`; non-dict payload → `AbrpApiError`; missing `result` → `AbrpApiError`; malformed record → `AbrpApiError`; request assertion: URL, `Authorization: APIKEY <key>` header, JSON body `{"session_id": "tok"}`.
  - catalog: happy path keyed by typecode; whitespace normalization rows (`"  Rivian  "` → `"Rivian"`, `"   "` → None); strict-int rows (`True` → None, `"2022"` → None, `2022` → 2022); missing/null field rows; record without typecode skipped; non-dict record skipped; header assertions (X-API-KEY, X-ABRP-SESSION, Accept).
  - current telemetry: happy path returns `dict[Metric, MetricValue]` (assert soc 50.0 with aware time + provider); `{}` → `{}`; malformed JSON body → `AbrpApiError`; 401 → `AbrpAuthError`.
  - auth containment: an `AbstractAuth` stub whose getter raises `AbrpAuthError` → propagates from all three methods untouched; getter raising `aiohttp.ClientResponseError` → propagates (NOT converted to `AbrpApiError`).
- [ ] **Step 2:** FAIL. **Step 3:** implement. **Step 4:** suite green + lint/type clean.
- [ ] **Step 5: Commit:** `feat: add AbrpClient for garage, catalog, and one-shot telemetry`

---

## Task 6: Telemetry stream core — `stream.py` + local SSE server harness

**Files:**
- Create: `src/aioabrp/stream.py`, `tests/conftest.py`
- Test: `tests/test_stream.py`

**Public surface (PLAN.md, verbatim):**

```python
class TelemetryStream:
    def __init__(
        self,
        websession: aiohttp.ClientSession,
        api_key: str,
        auth: AbstractAuth,
        vehicle_ids: list[int],
        on_update: Callable[[int, dict[Metric, MetricValue]], None],
        on_connection_change: Callable[[ConnectionEvent], None],
        *,
        name: str | None = None,
        backoff: Sequence[float] = DEFAULT_BACKOFF_SECONDS,
        watchdog_seconds: float = DEFAULT_WATCHDOG_SECONDS,
    ) -> None: ...

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
```

**Implementation spec — port `coordinator.py:567-753` (`_run_sse_loop`) restructured around injected auth/callbacks. Read the source loop and its docstrings first; they are the spec. Control flow:**

1. `start()`: create the internal task (`asyncio.create_task`) running the loop; attach a done-callback that, for an unexpected (non-cancel) termination, logs the exception summary and fires `ConnectionEvent(DISCONNECTED, reason="internal error")` through the contained dispatcher (tripwire — PLAN.md failure-containment). Idempotent: a second `start()` while running is a no-op (log debug).
2. Loop per connection attempt:
   - token: `await self._auth.async_get_access_token()` — `AbrpAuthError` → terminal AUTH_FAILED path (below); any OTHER exception → log debug, normal backoff path (PLAN.md: "token getter raises ValueError mid-reconnect → loop survives").
   - open `GET {API_BASE_V2}/{ENDPOINT_TLM}?vehicleIds=<comma-joined>` with `Accept: text/event-stream`, `X-API-KEY`, `X-ABRP-SESSION` headers, `ClientTimeout(total=None, connect=SSE_CONNECT_TIMEOUT_SECONDS, sock_connect=SSE_SOCK_CONNECT_TIMEOUT_SECONDS)` (port the rationale from `api.py:40-48`). HTTP 401/403 → `AbrpAuthError`. Other non-2xx → `AbrpApiError` → backoff.
   - iterate `iter_sse_events(response.content)` with each `__anext__` wrapped in `asyncio.wait_for(..., timeout=self._watchdog_seconds)` (port the watchdog docstring from `coordinator.py:588-597` incl. the 16h-flatline rationale).
   - per event: `parse_sse_event` → None → continue; frame → `vehicle_id = frame["vehicleId"]`; `extract_metrics(frame, unknown_charging_states_seen=self._unknown_charging_states_seen, log_name=self._name)`; apply the **monotonicity gate** (below); if any metrics survive → contained `on_update(vehicle_id, metrics)`.
   - first frame of a connection: fire `ConnectionEvent(CONNECTED)` (CONNECTED on first FRAME, not HTTP 200 — port the slow-loris rationale from `coordinator.py:669-675`), reset backoff index to 0.
   - exception bands (port `coordinator.py:658-753` structure): `AbrpAuthError` → fire `ConnectionEvent(AUTH_FAILED, reason=<80-char summary>)`, log warning, **return** (stream stops itself; consumer decides — no retry). `TimeoutError` (watchdog) → reason `f"watchdog_stall_{watchdog_seconds}s"`, DISCONNECTED event, backoff. `AbrpApiError` / `ClientError` → DISCONNECTED with summary reason, backoff. `finally:` close the response/generator with the full `aclose()` hardening from `coordinator.py:707-730`: guard `except Exception` → log + swallow so teardown `ClientError` on a half-broken socket cannot kill the loop; tolerate the stream factory having raised before assignment.
   - after the masked-cancel-or-exhausted inner loop: the `task.cancelling()` guard from `coordinator.py:745-747` ports verbatim (an iterator that converts `CancelledError` → `StopAsyncIteration` must not strand `stop()` in the backoff sleep).
   - backoff sleep: `self._backoff[min(idx, len-1)]`; idx+1 capped; reset to 0 on any frame (`coordinator.py:677`).
3. **Monotonicity gate** (replaces the coordinator's wire-keyed stale pre-scan, `coordinator.py:434-468`, with a typed per-`(vehicle_id, Metric)` map):
   - For each extracted `MetricValue`: `last = self._last_times.get((vid, metric))`. If `mv.time is None` → adopt AND `self._last_times.pop((vid, metric), None)` (time-less block CLEARS the entry — today's stored-block-time contract). If `last is None` → adopt + store. If `mv.time < last` → DROP (strictly older). If `mv.time >= last` → adopt + store (**equal-time re-emits by design** — reconnect snapshots re-deliver; PLAN.md pins this).
4. **Callback containment:** every `on_update` / `on_connection_change` invocation goes through one helper: `try: cb(...) except Exception: _LOGGER.exception(...)` — frame loss beats stream death. No callbacks after `stop()` returns.
5. `stop()`: cancel-based — `task.cancel()`, `await task` swallowing `CancelledError` (never a graceful join). Idempotent; safe before `start()`, after AUTH_FAILED self-stop, during backoff sleep. Set a `self._stopped` flag BEFORE cancelling so the done-callback tripwire and any in-flight dispatch suppress post-stop callbacks.
6. **Logging contract:** connect/disconnect + reason at INFO, watchdog at WARNING, per-frame at DEBUG (keys/sizes only — never bodies/headers/tokens). All lines prefixed with `self._name` when set. Port `_summarize_exc` (`coordinator.py:66-74`): class name + message truncated to 80 chars.

**Test harness (`tests/conftest.py`):** a real local `aiohttp.web` SSE server, built fresh (the HA conftest mocks at the client layer; the lib tests must exercise genuine event-stream bytes — PLAN.md test layer 3). Provide:

```python
class SseScript:
    """One scripted connection: actions executed in order."""
    # actions: ("frame", dict) | ("raw", bytes) | ("sleep", seconds)
    # | ("close", None)  — server closes the response
    # | ("status", int)  — respond with this HTTP status instead of a stream

@pytest.fixture
async def sse_server(aiohttp_server):  # uses pytest-aiohttp? NO — build with aiohttp.web + test util below
    ...
```

Implementation guidance: use `aiohttp.web.Application` with a `GET /2/tlm` handler that pops the next `SseScript` from a list (one per connection attempt, last script reused), checks/captures headers + `vehicleIds` query for assertions, and executes actions: `frame` → write `data: <json>\n\n` bytes; `raw` → write bytes verbatim (for CR/LF and split-chunk cases); `sleep` → `asyncio.sleep`; `close` → return (ends chunked response); `status` → return `web.Response(status=...)` immediately. Start it with `aiohttp.test_utils.TestServer` (no extra dependency; `aiohttp.test_utils` is part of aiohttp) and hand tests a `base_url`. The stream under test gets `websession` pointing at the test server: build the stream with the real URL by monkeypatching `aioabrp.stream.API_BASE_V2` (or, cleaner: give `TelemetryStream` a private `_base_url` constructor-default reading `const.API_BASE_V2`, overridable in tests — choose the monkeypatch unless mypy/ruff push otherwise; do NOT add a public parameter for it).
Also provide a `build_frame(vehicle_id, **metrics)` helper mirroring HA conftest `build_telemetry_frame` (`conftest.py:327-372`, same wire shapes) plus `time=`/`provider=` kwargs, and a `CallbackRecorder` fixture capturing `(vid, metrics)` update calls and connection events with `asyncio.Event` hooks for "wait until N updates".

- [ ] **Step 1: Write failing lifecycle tests** in `tests/test_stream.py` (each with tiny injected `backoff=(0.05, 0.1)` and `watchdog_seconds` as needed — no clock mocks):
  - happy path: server scripts 2 frames for vid 1 → `start()` → recorder sees CONNECTED then 2 `on_update` calls with correctly typed `MetricValue`s; `vehicleIds` query + both auth headers asserted server-side; `stop()` returns promptly.
  - CONNECTED fires on first frame, not on connect: script = sleep(0.5) then frame; assert no CONNECTED before the frame arrives (poll recorder while sleeping).
  - reconnect: script1 = frame + close; script2 = frame → recorder sees CONNECTED, update, DISCONNECTED(reason mentions disconnect/payload), CONNECTED, update; backoff observed ≈ first tier.
  - 401 at connect: script = status 401 → AUTH_FAILED event; stream task finishes itself; `stop()` afterwards is safe; NO further connection attempts (server records exactly 1 request).
  - auth getter raises `AbrpAuthError` → AUTH_FAILED without any HTTP request.
  - auth getter raises `ValueError` on attempt 2: script1 = frame + close; flaky auth (ok, ValueError, ok) → stream survives to a third attempt (server sees ≥2 connections) — loop-survives contract.
  - monotonicity: script1 = frame(soc, time=T2) then frame(soc, time=T1<T2) → second frame produces NO on_update (strictly older dropped); then frame(soc, time=T2) again → RE-EMITTED (equal-time); then frame(soc, no time) → adopted; then frame(soc, time=T1) → adopted (time-less cleared the gate).
  - stop() during backoff sleep returns promptly (< 1s with backoff=(30,)).
  - double stop / stop before start: no errors.
- [ ] **Step 2:** FAIL. **Step 3:** implement `stream.py` + conftest harness. **Step 4:** suite green + lint/type clean (mypy strict on the callback types).
- [ ] **Step 5: Commit:** `feat: add resilient TelemetryStream with SSE reconnect loop`

---

## Task 7: Stream resilience battery + multi-account isolation

**Files:**
- Test: `tests/test_stream_resilience.py`, `tests/test_stream_isolation.py`
- Modify: `src/aioabrp/stream.py` only as needed to make these pass (expect small fixes, not redesign)

Port every remaining named scenario from HA `test_sse_reconnect.py` (read it: lines 229-771) and PLAN.md's must-not-shed list ("Tests (layered)" item 3). In `test_stream_resilience.py`:

- watchdog fires on idle stream (script: connect then sleep forever; `watchdog_seconds=0.2`) → DISCONNECTED with `watchdog_stall` in reason → reconnects (server sees attempt 2). [port of test_watchdog_fires_on_idle_stream]
- backoff resets after successful frame: frame → stall → reconnect → frame → stall; both gaps are first-tier (capture sleep durations by wrapping `asyncio.sleep` via monkeypatch on the stream module, or assert wall-clock with generous bounds; prefer the monkeypatch for determinism). [port of test_backoff_resets_after_successful_frame_then_watchdog_stall]
- backoff ladder escalates while connection attempts fail (server scripts: status 500 × 3) → recorded sleeps are backoff[0], backoff[1], backoff[2].
- mid-stream 401: script1 = frame, then `("status", 401)` on connection 2 → CONNECTED/update, then AUTH_FAILED terminal; no third attempt. [PLAN.md "401 mid-session"]
- malformed frame: script = raw `b"data: {not json}\n\n"` then a good frame → `AbrpApiError` path → DISCONNECTED → reconnect; good frame on attempt 2 delivered.
- chunk-boundary UTF-8 + CRLF variants end-to-end: script uses `raw` writes splitting a multi-byte character and a `\r\n\r\n` across writes → frame still parses (asserts the `_sse.py` integration, not the unit parser).
- no trailing blank line before close (flush path): raw `b"data: {...}"` then close → frame delivered, then clean reconnect.
- `on_update` raises → logged, stream continues (next frame still delivered). [PLAN.md callback containment]
- `on_connection_change` raises → stream continues.
- stop after AUTH_FAILED → no error, no callbacks after stop returns.
- masked cancel: monkeypatch the stream's internal event iteration the way HA's `_FrameStream` does (an iterator converting CancelledError → StopAsyncIteration — simplest: monkeypatch `aioabrp.stream.iter_sse_events` with such an iterator) → `stop()` still returns promptly. [port of test_unload_through_stopiteration_masked_cancel; the `task.cancelling()` guard is the code under test]
- stop during watchdog wait (parked in `wait_for`) releases cleanly within 2s. [port of test_unload_during_watchdog_wait_releases_cleanly]
- aclose/teardown hardening: monkeypatch `iter_sse_events` to raise `aiohttp.ClientError` from `aclose()` during teardown → loop survives to reconnect. [PLAN.md "aclose() raising ClientError keeps the loop alive"]
- factory-raises-before-assignment: auth ok but session.get itself raises `ClientError` immediately → backoff path, no UnboundLocalError. [PLAN.md "stream factory raising before generator assignment"]
- reconnect full-state backfill with identical times re-emits (two connections, same frame+time both delivered as updates). [PLAN.md pinned]
- connection timeout params: assert the `ClientTimeout` passed to `session.get` (`total=None, connect=30, sock_connect=15`) by monkeypatching/wrapping the session's get. [port of test_connect_timeout_passed_to_session_get]
- internal-error tripwire: monkeypatch `extract_metrics` to raise `RuntimeError` → task dies via done-callback → exactly one `ConnectionEvent(DISCONNECTED, reason="internal error")`, exception logged.
- PII: caplog over a full connect/frame/disconnect cycle never contains the token string, the api key, or a planted metric value/provider string.

In `tests/test_stream_isolation.py` (PLAN.md multi-account):
- two streams, two tokens, one shared `ClientSession`, same server (server routes scripts per `X-ABRP-SESSION` header): events route to the correct callbacks (vid sets disjoint).
- 401 for stream A only → A gets AUTH_FAILED and stops; B keeps streaming frames afterwards.
- `stop()` on A → B unaffected (still receives a subsequent frame).
- unknown-charging-state warning dedup is per-instance: both streams receive `"FOO"` → two warnings total (one per stream's `log_name`), not one.
- slow callback on A (0.2s sleep in `on_update`) delays but never errors B (B still gets its frames; no exceptions logged). Note: callbacks are sync on one loop — "delays" is acceptable; assert no errors and eventual delivery.

- [ ] **Step 1:** write the battery (failing where behavior is missing). **Step 2:** run; fix `stream.py` for any genuinely missing behavior. **Step 3:** full suite green + lint/type clean. Watch total runtime: keep injected timings tiny; the whole suite must stay under ~30s.
- [ ] **Step 4: Commit:** `test: pin stream resilience edge cases and multi-account isolation`

---

## Task 8: Public surface polish + README

**Files:**
- Modify: `src/aioabrp/__init__.py`, `README.md`
- Test: `tests/test_public_api.py`

- [ ] **Step 1: Write failing test** `tests/test_public_api.py`: `aioabrp.__all__` equals exactly the PLAN.md public surface — `AbstractAuth, StaticAuth, AbrpClient, TelemetryStream, AbrpVehicle, CatalogEntry, Metric, MetricValue, ChargingState, Location, ConnectionState, ConnectionEvent, AbrpError, AbrpAuthError, AbrpApiError, __version__` — and every name imports from the top level. Assert `aioabrp._wire_types` / `aioabrp._extract` / `aioabrp._sse` are NOT re-exported.
- [ ] **Step 2:** FAIL → finalize `__init__.py` re-exports (client.py / stream.py imports added) → PASS.
- [ ] **Step 3: README** — replace the placeholder body with (PLAN.md "Docs" bullet):
  - what the library is (1 paragraph; mirrors API points 1:1, no HA dependency);
  - a runnable standalone example: `StaticAuth` + `AbrpClient.async_get_vehicles` + `TelemetryStream` with both callbacks printing, `asyncio.run` main;
  - "Get your own API key" note: the `api_key` constructor arg is an Iternio partner key; non-HA consumers obtain their own from Iternio (link https://documenter.getpostman.com/view/7396339/SWTK5a8w or the Iternio contact note — verify which the spec/docs suggest; if unsure use "contact Iternio for a partner API key");
  - contracts section: callbacks are sync, delivered on the loop that ran `start()`, must be non-blocking; CONNECTED fires on first frame; DISCONNECTED is steady-state (~200s server idle close) not exceptional; AUTH_FAILED is terminal (stream stops itself);
  - monotonicity-gate contract + known limitation (strictly-older drops; equal-time re-emits by design; a legitimately backdated server correction is suppressed for the stream's lifetime);
  - dev section: uv, the aiohttp<3.14 dev-pin note (link aioresponses#289), release process (conventional commits → git-cliff GitHub Release notes; tag `v*` publishes via trusted publishing).
- [ ] **Step 4:** Verify the README example actually runs: extract it to `/tmp/readme_example.py`, point it at a bogus token, confirm it imports and constructs (it will fail on the network call — wrap the doc example's main in a comment noting a real token is needed; the verification is import + construction, e.g. `python -c` exercising the example's imports and constructors).
- [ ] **Step 5:** full suite + lint/type clean.
- [ ] **Step 6: Commit:** `docs: document usage, contracts, and finalize public API`

---

## Final verification (after Task 8)

- [ ] `uv run ruff format --check . && uv run ruff check . && uv run mypy && uv run pytest` — all green.
- [ ] `uv build` succeeds; wheel contains all modules + `py.typed`.
- [ ] `git log --oneline` shows exactly the 8 conventional commits from this plan on top of the scaffold commit; working tree clean; nothing pushed/tagged.
- [ ] Report: any deviations from PLAN.md taken along the way, listed explicitly.

## Self-review notes (already folded in)

- Spec coverage checked against PLAN.md Workstream A: public API ✓, event semantics ✓ (per-frame batch in stream; tolerance matrix in _extract), resilience ✓ (Task 6/7), failure containment ✓ (auth taxonomy Task 5/6, callback containment Task 6/7, stop semantics Task 6, monotonicity contract Task 6, logging/PII Task 4/6/7), multi-account ✓ (instance-scoped state Tasks 3/5/6, isolation tests Task 7), wire types no-codegen ✓ (Task 2), packaging already done in Phase 1, README/docs ✓ (Task 8).
- Known consistency points: `Metric`/`MetricValue`/`ChargingState`/`Location` defined once in Task 1 and imported everywhere; `WIRE_KEYS` lives in `_extract.py`; `iter_sse_events`/`parse_sse_event` names fixed in Task 4 and reused in Task 6; `DEFAULT_BACKOFF_SECONDS`/`DEFAULT_WATCHDOG_SECONDS` in const.py (Task 1) referenced by stream defaults (Task 6).
- The location extractor is NEW (no HA source) — its tolerance matrix is defined in Task 3 and is the spec.
