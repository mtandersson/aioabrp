"""Tests for aioabrp.client: the request/response AbrpClient.

Endpoint mocks use aioresponses against a real ``aiohttp.ClientSession``.
The wire surface under test:

* ``GET /2/abrp/me/vehicles`` — garage, with display metadata (bare JSON)
* ``GET /2/vehicle/_list`` — vehicle catalog (bare JSON)
* ``GET /2/tlm/{vehicle_id}`` — one-shot telemetry (bare JSON -> typed)
* ``GET /2/vehicle-model/by-typecode/{typecode}/display`` — display metadata
* ``POST /2/app/tlm/{vehicle_id}`` — send one telemetry point
"""

import copy
import json
import logging
import math
import pickle
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import UTC, datetime, timedelta, timezone
from http import HTTPStatus
from typing import Any
from unittest.mock import Mock
from urllib.parse import quote

import aiohttp
import pytest
from aioresponses import aioresponses
from yarl import URL

import aioabrp._clock as _clock
from aioabrp.auth import AbstractAuth, StaticAuth
from aioabrp.client import AbrpClient
from aioabrp.const import (
    API_BASE_V2,
    ENDPOINT_APP_TLM,
    ENDPOINT_MY_VEHICLES,
    ENDPOINT_TLM,
    ENDPOINT_VEHICLE_LIST,
    ENDPOINT_VEHICLE_MODEL_DISPLAY,
)
from aioabrp.exceptions import (
    AbrpApiError,
    AbrpAuthError,
    AbrpForbiddenError,
    AbrpScopeError,
)
from aioabrp.models import (
    AbrpVehicle,
    ChargingState,
    DrivingState,
    Location,
    Metric,
    Provider,
    Telemetry,
    TelemetryPoint,
    VehicleModelDisplay,
    VehicleRelation,
)

API_KEY = "mock-partner-key"
ACCESS_TOKEN = "mock-access-token"
VEHICLE_ID = 123456789012
VEHICLE_NAME = "Rivian R2 2027 Standard Long Range"
VEHICLE_MODEL = "rivian:r2:26:ncma91:rwd:w21"

DISPLAY_TYPECODE = "rivian:r1s:22:large"

VEHICLES_URL = f"{API_BASE_V2}/{ENDPOINT_MY_VEHICLES}?include=shared"
CATALOG_URL = f"{API_BASE_V2}/{ENDPOINT_VEHICLE_LIST}"
ONE_SHOT_URL = f"{API_BASE_V2}/{ENDPOINT_TLM}/{VEHICLE_ID}"


def display_url(typecode: str = DISPLAY_TYPECODE) -> str:
    """Build the vehicle-model display URL for ``typecode`` (path-encoded)."""
    return (
        f"{API_BASE_V2}/{ENDPOINT_VEHICLE_MODEL_DISPLAY}"
        f"/{quote(typecode, safe='')}/display"
    )


# ---------- fixtures and builders --------------------------------------------


@pytest.fixture(name="session")
async def session_fixture() -> AsyncIterator[aiohttp.ClientSession]:
    async with aiohttp.ClientSession() as websession:
        yield websession


@pytest.fixture(name="client")
def client_fixture(session: aiohttp.ClientSession) -> AbrpClient:
    return AbrpClient(session, API_KEY, StaticAuth(ACCESS_TOKEN))


@pytest.fixture(name="mock_api")
def mock_api_fixture() -> Iterator[aioresponses]:
    with aioresponses() as mocked:
        yield mocked


class RaisingAuth(AbstractAuth):
    """An auth stub whose token getter always raises."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    async def async_get_access_token(self) -> str:
        raise self._exc


def build_vehicle_item(**overrides: Any) -> dict[str, Any]:
    """Build one ``UserVehicleListItem`` as returned by /2/abrp/me/vehicles."""
    item: dict[str, Any] = {
        "id": VEHICLE_ID,
        "name": VEHICLE_NAME,
        "typecode": VEHICLE_MODEL,
        "relation": "OWNED",
        "model": {**build_display_record(), "isCustom": False},
    }
    item.update(overrides)
    return item


def build_catalog_record(**overrides: Any) -> dict[str, Any]:
    """Build a ``vehicles[i]`` wire record (v2 catalog is all-camelCase)."""
    base: dict[str, Any] = {
        "typecode": "rivian:r1t-quad:22:135",
        "manufacturer": "Rivian",
        "model": "R1T",
        "title": "Rivian R1T Quad-Motor (2022, 135 kWh)",
        "startYear": 2022,
        "endYear": None,
        "batteryCapacityWh": 135000,
    }
    base.update(overrides)
    return base


def build_catalog_response(
    vehicles: list[Any] | None = None,
) -> dict[str, Any]:
    """Wrap ``vehicles`` in the bare JSON returned by /2/vehicle/_list."""
    if vehicles is None:
        vehicles = [build_catalog_record()]
    return {"display": [], "options": [], "vehicles": vehicles}


_WIRE_TO_SNAKE: dict[str, str] = {
    "manufacturer": "manufacturer",
    "model": "model",
    "title": "title",
    "startYear": "start_year",
    "endYear": "end_year",
    "batteryCapacityWh": "battery_capacity_wh",
}


def build_display_record(**overrides: Any) -> dict[str, Any]:
    """Build a ``VehicleModelDisplay`` wire record (v2 is all-camelCase)."""
    base: dict[str, Any] = {
        "manufacturer": "Rivian",
        "model": "R1S",
        "years": "2022-2023",
        "startYear": 2022,
        "endYear": 2023,
        "title": "R1S Adventure",
    }
    base.update(overrides)
    return base


async def _get_single_catalog_entry(
    client: AbrpClient, mock_api: aioresponses, record: dict[str, Any]
) -> Any:
    """Serve one catalog record through the endpoint and return its entry."""
    mock_api.get(CATALOG_URL, payload=build_catalog_response([record]))
    catalog = await client.async_get_catalog()
    assert len(catalog) == 1
    return next(iter(catalog.values()))


# ---------- async_get_vehicles ------------------------------------------------


async def test_get_vehicles_parses_owned_shared_and_missing_model(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """Items parse in wire order with display and relation; paint is unset."""
    custom_model = {
        **build_display_record(title="My R1S"),
        "isCustom": True,
        "baseTypecode": DISPLAY_TYPECODE,
    }
    items = [
        build_vehicle_item(),
        build_vehicle_item(id=2, name="", relation="SHARED", model=custom_model),
        build_vehicle_item(id=3, typecode="custom:unknown"),
    ]
    del items[2]["model"]  # typecode did not resolve
    mock_api.get(VEHICLES_URL, payload=items)

    owned, shared, unresolved = await client.async_get_vehicles()

    assert owned == AbrpVehicle(
        vehicle_id=VEHICLE_ID,
        name=VEHICLE_NAME,
        vehicle_model=VEHICLE_MODEL,
        paint=None,
        display=VehicleModelDisplay(
            manufacturer="Rivian",
            model="R1S",
            years="2022-2023",
            title="R1S Adventure",
            start_year=2022,
            end_year=2023,
        ),
        relation=VehicleRelation.OWNED,
    )
    assert shared.name is None  # "" means unset
    assert shared.relation is VehicleRelation.SHARED
    assert shared.display is not None
    assert shared.display.title == "My R1S"
    assert unresolved.vehicle_model == "custom:unknown"
    assert unresolved.display is None


async def test_get_vehicles_empty(client: AbrpClient, mock_api: aioresponses) -> None:
    """An empty garage returns an empty list (not None, not an exception)."""
    mock_api.get(VEHICLES_URL, payload=[])

    assert await client.async_get_vehicles() == []


async def test_get_vehicles_request_shape(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """GET with shared vehicles included and both v2 auth headers."""
    mock_api.get(VEHICLES_URL, payload=[])

    await client.async_get_vehicles()

    calls = mock_api.requests[("GET", URL(VEHICLES_URL))]
    assert len(calls) == 1
    kwargs = calls[0].kwargs
    assert kwargs["headers"] == {
        "Accept": "application/json",
        "X-API-KEY": API_KEY,
        "X-ABRP-SESSION": ACCESS_TOKEN,
    }
    assert kwargs["timeout"] == aiohttp.ClientTimeout(total=30)


async def test_get_vehicles_degrades_unknown_relation_and_bad_model(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """Optional enrichment never fails the garage: it degrades to None."""
    items = [
        build_vehicle_item(relation="BORROWED", model={"title": "no manufacturer"}),
        build_vehicle_item(id=2, model=None),
    ]
    mock_api.get(VEHICLES_URL, payload=items)

    first, second = await client.async_get_vehicles()

    assert (first.relation, first.display) == (None, None)
    assert first.vehicle_model == VEHICLE_MODEL
    assert second.display is None


@pytest.mark.parametrize(
    ("mock_kwargs", "error"),
    [
        pytest.param({"status": HTTPStatus.UNAUTHORIZED}, AbrpAuthError, id="401"),
        pytest.param({"status": HTTPStatus.FORBIDDEN}, AbrpForbiddenError, id="403"),
        pytest.param(
            {"status": HTTPStatus.INTERNAL_SERVER_ERROR}, AbrpApiError, id="500"
        ),
        pytest.param(
            {"exception": aiohttp.ClientError("boom")}, AbrpApiError, id="client_error"
        ),
        pytest.param({"body": "<html>502</html>"}, AbrpApiError, id="not_json"),
        pytest.param({"body": "null"}, AbrpApiError, id="null"),
        pytest.param({"payload": {"result": []}}, AbrpApiError, id="dict"),
    ],
)
async def test_get_vehicles_failures(
    client: AbrpClient,
    mock_api: aioresponses,
    mock_kwargs: dict[str, Any],
    error: type[Exception],
) -> None:
    """401 raises AbrpAuthError; 403, other HTTP, transport, shape errors API."""
    mock_api.get(VEHICLES_URL, **mock_kwargs)

    with pytest.raises(error):
        await client.async_get_vehicles()


@pytest.mark.parametrize(
    "item",
    [
        pytest.param({"name": "no id", "typecode": "x"}, id="missing_id"),
        pytest.param({"id": None, "typecode": "x"}, id="id_null"),
        pytest.param({"id": "42", "typecode": "x"}, id="id_str"),
        pytest.param({"id": 1.5, "typecode": "x"}, id="id_float"),
        pytest.param({"id": True, "typecode": "x"}, id="id_bool"),
        pytest.param({"id": 1}, id="missing_typecode"),
        pytest.param({"id": 1, "typecode": None}, id="typecode_null"),
        pytest.param({"id": 1, "typecode": ""}, id="typecode_empty"),
        pytest.param(42, id="item_non_dict"),
    ],
)
async def test_get_vehicles_malformed_item(
    client: AbrpClient, mock_api: aioresponses, item: Any
) -> None:
    """A malformed vehicle item surfaces as AbrpApiError."""
    mock_api.get(VEHICLES_URL, payload=[item])

    with pytest.raises(AbrpApiError):
        await client.async_get_vehicles()


# ---------- async_get_catalog --------------------------------------------------


async def test_get_catalog_happy_path_keyed_by_typecode(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """Catalog records parse into CatalogEntry values keyed by typecode."""
    records = [
        build_catalog_record(),
        build_catalog_record(
            typecode="tesla:model3:19:75",
            manufacturer="Tesla",
            model="Model 3",
            title=None,
            startYear=2019,
            endYear=2020,
            batteryCapacityWh=75000,
        ),
    ]
    mock_api.get(CATALOG_URL, payload=build_catalog_response(records))

    catalog = await client.async_get_catalog()

    assert set(catalog) == {"rivian:r1t-quad:22:135", "tesla:model3:19:75"}
    rivian = catalog["rivian:r1t-quad:22:135"]
    assert rivian.typecode == "rivian:r1t-quad:22:135"
    assert rivian.manufacturer == "Rivian"
    assert rivian.model == "R1T"
    assert rivian.title == "Rivian R1T Quad-Motor (2022, 135 kWh)"
    assert rivian.start_year == 2022
    assert rivian.end_year is None
    assert rivian.battery_capacity_wh == 135000
    tesla = catalog["tesla:model3:19:75"]
    assert tesla.end_year == 2020
    assert tesla.title is None


async def test_get_catalog_request_shape(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """Outgoing request: GET + Accept/X-API-KEY/X-ABRP-SESSION + 30s budget."""
    mock_api.get(CATALOG_URL, payload=build_catalog_response([]))

    await client.async_get_catalog()

    calls = mock_api.requests[("GET", URL(CATALOG_URL))]
    assert len(calls) == 1
    kwargs = calls[0].kwargs
    assert kwargs["headers"] == {
        "Accept": "application/json",
        "X-API-KEY": API_KEY,
        "X-ABRP-SESSION": ACCESS_TOKEN,
    }
    assert kwargs["timeout"] == aiohttp.ClientTimeout(total=30)


@pytest.mark.parametrize(
    ("field", "wire_value", "expected"),
    [
        pytest.param("manufacturer", "", None, id="manufacturer_empty_string"),
        pytest.param("model", "", None, id="model_empty_string"),
        pytest.param("title", "", None, id="title_empty_string"),
        pytest.param("manufacturer", "   ", None, id="manufacturer_whitespace_only"),
        pytest.param("model", "   ", None, id="model_whitespace_only"),
        pytest.param("title", "   ", None, id="title_whitespace_only"),
        pytest.param("manufacturer", "  Rivian  ", "Rivian", id="manufacturer_padded"),
        pytest.param("model", "  R1S  ", "R1S", id="model_padded"),
        pytest.param("title", "  Dual Motor  ", "Dual Motor", id="title_padded"),
    ],
)
async def test_get_catalog_whitespace_normalisation(
    client: AbrpClient,
    mock_api: aioresponses,
    field: str,
    wire_value: str,
    expected: str | None,
) -> None:
    """Catalog string fields normalise empty/whitespace-only/padded at parse.

    Single-site normalisation contract: ``""`` and ``"   "`` collapse to
    None; ``"  X  "`` is stripped once at the parse boundary so no
    downstream display consumer ever sees padding artefacts.
    """
    entry = await _get_single_catalog_entry(
        client, mock_api, build_catalog_record(**{field: wire_value})
    )

    assert getattr(entry, _WIRE_TO_SNAKE[field]) == expected


@pytest.mark.parametrize(
    ("field", "wire_value"),
    [
        pytest.param("startYear", True, id="startyear_bool_true_rejected"),
        pytest.param("startYear", False, id="startyear_bool_false_rejected"),
        pytest.param("startYear", "2022", id="startyear_string_int_rejected"),
        pytest.param("startYear", 2022.0, id="startyear_float_rejected"),
        pytest.param("endYear", True, id="endyear_bool_rejected"),
        pytest.param("batteryCapacityWh", 135000.0, id="capacity_float_rejected"),
        pytest.param("batteryCapacityWh", "135000", id="capacity_string_rejected"),
    ],
)
async def test_get_catalog_strict_int_rejects_wrong_types(
    client: AbrpClient, mock_api: aioresponses, field: str, wire_value: Any
) -> None:
    """Strict int typing: bool ⊂ int in Python, so bools are rejected up front.

    Strings-that-look-like-ints and floats also collapse to None so
    upstream type drift fails loudly rather than masquerading as a value.
    """
    entry = await _get_single_catalog_entry(
        client, mock_api, build_catalog_record(**{field: wire_value})
    )

    assert getattr(entry, _WIRE_TO_SNAKE[field]) is None


@pytest.mark.parametrize(
    "field",
    [
        pytest.param("manufacturer", id="manufacturer_missing"),
        pytest.param("model", id="model_missing"),
        pytest.param("title", id="title_missing"),
        pytest.param("startYear", id="start_year_missing"),
        pytest.param("endYear", id="end_year_missing"),
        pytest.param("batteryCapacityWh", id="battery_capacity_missing"),
    ],
)
async def test_get_catalog_missing_optional_field_is_none(
    client: AbrpClient, mock_api: aioresponses, field: str
) -> None:
    """An absent optional catalog field falls through cleanly to None."""
    record = build_catalog_record()
    del record[field]

    entry = await _get_single_catalog_entry(client, mock_api, record)

    assert getattr(entry, _WIRE_TO_SNAKE[field]) is None


@pytest.mark.parametrize(
    "field",
    [
        pytest.param("manufacturer", id="manufacturer_null"),
        pytest.param("startYear", id="start_year_null"),
        pytest.param("batteryCapacityWh", id="battery_capacity_null"),
    ],
)
async def test_get_catalog_explicit_null_is_none(
    client: AbrpClient, mock_api: aioresponses, field: str
) -> None:
    """A wire field explicitly ``null`` parses to None, never ``"None"``."""
    entry = await _get_single_catalog_entry(
        client, mock_api, build_catalog_record(**{field: None})
    )

    assert getattr(entry, _WIRE_TO_SNAKE[field]) is None


@pytest.mark.parametrize(
    "bad_record",
    [
        pytest.param({"manufacturer": "Rivian"}, id="typecode_missing"),
        pytest.param(build_catalog_record(typecode=""), id="typecode_empty"),
        pytest.param(build_catalog_record(typecode=123), id="typecode_non_string"),
        pytest.param("just a string", id="record_non_dict"),
        pytest.param(None, id="record_null"),
    ],
)
async def test_get_catalog_skips_unusable_records(
    client: AbrpClient, mock_api: aioresponses, bad_record: Any
) -> None:
    """Unkeyable/non-dict records are skipped; the good record still lands."""
    mock_api.get(
        CATALOG_URL,
        payload=build_catalog_response([bad_record, build_catalog_record()]),
    )

    catalog = await client.async_get_catalog()

    assert set(catalog) == {"rivian:r1t-quad:22:135"}


async def test_get_catalog_wraps_timeout_as_api_error(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """A naked TimeoutError from the catalog GET wraps to AbrpApiError.

    A bare ``ClientTimeout(total=...)`` budget exceedance raises naked
    ``asyncio.TimeoutError`` which is NOT a ClientError subclass; the
    catch band must name it explicitly. ``__cause__`` preserves the
    original for diagnostics.
    """
    mock_api.get(CATALOG_URL, exception=TimeoutError("catalog budget exceeded"))

    with pytest.raises(AbrpApiError) as excinfo:
        await client.async_get_catalog()

    assert isinstance(excinfo.value.__cause__, TimeoutError)


@pytest.mark.parametrize(
    "mock_kwargs",
    [
        pytest.param({"body": "null"}, id="null"),
        pytest.param({"payload": []}, id="empty_list"),
        pytest.param({"payload": [{"typecode": "tesla:model3"}]}, id="list_of_dicts"),
        pytest.param({"payload": "just a string"}, id="string"),
        pytest.param({"payload": 42}, id="integer"),
    ],
)
async def test_get_catalog_rejects_non_dict_payload(
    client: AbrpClient, mock_api: aioresponses, mock_kwargs: dict[str, Any]
) -> None:
    """A JSON-valid non-dict catalog body raises AbrpApiError."""
    mock_api.get(CATALOG_URL, **mock_kwargs)

    with pytest.raises(AbrpApiError):
        await client.async_get_catalog()


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param({}, id="vehicles_missing"),
        pytest.param({"vehicles": None}, id="vehicles_null"),
        pytest.param({"vehicles": "oops"}, id="vehicles_str"),
    ],
)
async def test_get_catalog_malformed_vehicles_key(
    client: AbrpClient, mock_api: aioresponses, payload: dict[str, Any]
) -> None:
    """Missing/non-list ``vehicles`` in the catalog body raises AbrpApiError."""
    mock_api.get(CATALOG_URL, payload=payload)

    with pytest.raises(AbrpApiError):
        await client.async_get_catalog()


@pytest.mark.parametrize(
    ("status", "error"),
    [
        pytest.param(HTTPStatus.UNAUTHORIZED, AbrpAuthError, id="401"),
        pytest.param(HTTPStatus.FORBIDDEN, AbrpForbiddenError, id="403"),
    ],
)
async def test_get_catalog_http_auth_failure(
    client: AbrpClient,
    mock_api: aioresponses,
    status: HTTPStatus,
    error: type[Exception],
) -> None:
    """A 401 from the catalog endpoint is AbrpAuthError; a 403 is forbidden."""
    mock_api.get(CATALOG_URL, status=status)

    with pytest.raises(error):
        await client.async_get_catalog()


async def test_get_catalog_http_500_raises_api_error(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """A 5xx from the catalog endpoint raises AbrpApiError."""
    mock_api.get(CATALOG_URL, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    with pytest.raises(AbrpApiError):
        await client.async_get_catalog()


# ---------- async_get_current_telemetry ---------------------------------------


async def test_get_current_telemetry_happy_path_typed(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """The one-shot payload extracts into a typed Telemetry."""
    payload = {
        "soc": {
            "frac": 0.5,
            "time": "2026-05-25T12:00:00Z",
            "provider": "RIVIAN_STREAM",
        },
        "power": {"w": 5000.0},
        "chargingState": {"state": "CHARGING_DC"},
        "location": {"lat": 57.7, "long": 11.9},
    }
    mock_api.get(ONE_SHOT_URL, payload=payload)

    result = await client.async_get_current_telemetry(VEHICLE_ID)

    assert isinstance(result, Telemetry)
    # Present metrics are accessible via named fields.
    assert result.soc is not None
    assert result.soc.value == 50.0
    assert result.soc.time == datetime(2026, 5, 25, 12, 0, tzinfo=UTC)
    assert result.soc.time.tzinfo is not None
    assert result.soc.provider == "RIVIAN_STREAM"
    assert result.power is not None
    assert result.power.value == 5000.0
    assert result.power.time is None
    assert result.power.provider is None
    assert result.charging_state is not None
    assert result.charging_state.value is ChargingState.CHARGING_DC
    assert result.location is not None
    assert result.location.value == Location(lat=57.7, lon=11.9)
    # Metrics absent from the payload are None.
    assert result.voltage is None
    assert result.soe is None
    # items() yields the four present metrics.
    present_metrics = {metric for metric, _ in result.items()}
    assert present_metrics == {
        Metric.SOC,
        Metric.POWER,
        Metric.CHARGING_STATE,
        Metric.LOCATION,
    }


async def test_one_shot_clamps_future_block_time(
    client: AbrpClient, mock_api: aioresponses, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A future-dated one-shot block time is clamped to now (stateless).

    The one-shot getter does no monotonicity gating, but a future wire
    ``time`` must not escape the lib — a future-dated snapshot fed back
    as a stream seed would otherwise poison the gate. The clock seam is
    patched via the module attribute so the clamp site's
    ``_clock._now()`` read sees the fixed ``now``.
    """
    now = datetime(2026, 5, 25, 12, 0, tzinfo=UTC)
    monkeypatch.setattr(_clock, "_now", lambda: now)
    future = now + timedelta(hours=6)
    payload = {
        "soc": {
            "frac": 0.5,
            "time": future.isoformat().replace("+00:00", "Z"),
            "provider": "RIVIAN_STREAM",
        },
    }
    mock_api.get(ONE_SHOT_URL, payload=payload)

    result = await client.async_get_current_telemetry(VEHICLE_ID)

    assert result.soc is not None
    assert result.soc.value == 50.0
    assert result.soc.time == now


async def test_get_current_telemetry_empty_payload(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """``{}`` body is valid "no data yet" and extracts to an empty Telemetry."""
    mock_api.get(ONE_SHOT_URL, payload={})

    result = await client.async_get_current_telemetry(VEHICLE_ID)
    assert isinstance(result, Telemetry)
    assert result == Telemetry()


async def test_get_current_telemetry_request_shape(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """Outgoing request: GET + Accept/X-API-KEY/X-ABRP-SESSION + 30s budget."""
    mock_api.get(ONE_SHOT_URL, payload={})

    await client.async_get_current_telemetry(VEHICLE_ID)

    calls = mock_api.requests[("GET", URL(ONE_SHOT_URL))]
    assert len(calls) == 1
    kwargs = calls[0].kwargs
    assert kwargs["headers"] == {
        "Accept": "application/json",
        "X-API-KEY": API_KEY,
        "X-ABRP-SESSION": ACCESS_TOKEN,
    }
    assert kwargs["timeout"] == aiohttp.ClientTimeout(total=30)


async def test_get_current_telemetry_malformed_json(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """A malformed JSON one-shot body wraps as AbrpApiError."""
    mock_api.get(ONE_SHOT_URL, status=200, body="<html>oops</html>")

    with pytest.raises(AbrpApiError) as excinfo:
        await client.async_get_current_telemetry(VEHICLE_ID)

    assert isinstance(excinfo.value.__cause__, ValueError)


@pytest.mark.parametrize(
    "mock_kwargs",
    [
        pytest.param({"body": "null"}, id="null"),
        pytest.param({"payload": []}, id="empty_list"),
        pytest.param({"payload": [{"soc": {"frac": 0.5}}]}, id="list_of_dicts"),
        pytest.param({"payload": "just a string"}, id="string"),
        pytest.param({"payload": 42}, id="integer"),
    ],
)
async def test_get_current_telemetry_rejects_non_dict_payload(
    client: AbrpClient, mock_api: aioresponses, mock_kwargs: dict[str, Any]
) -> None:
    """A JSON-valid non-dict one-shot body raises AbrpApiError."""
    mock_api.get(ONE_SHOT_URL, **mock_kwargs)

    with pytest.raises(AbrpApiError):
        await client.async_get_current_telemetry(VEHICLE_ID)


@pytest.mark.parametrize(
    ("status", "error"),
    [
        pytest.param(HTTPStatus.UNAUTHORIZED, AbrpAuthError, id="401"),
        pytest.param(HTTPStatus.FORBIDDEN, AbrpForbiddenError, id="403"),
    ],
)
async def test_get_current_telemetry_http_auth_failure(
    client: AbrpClient,
    mock_api: aioresponses,
    status: HTTPStatus,
    error: type[Exception],
) -> None:
    """A 401 from the one-shot endpoint is AbrpAuthError; a 403 is forbidden."""
    mock_api.get(ONE_SHOT_URL, status=status)

    with pytest.raises(error):
        await client.async_get_current_telemetry(VEHICLE_ID)


@pytest.mark.parametrize(
    "mock_kwargs",
    [
        pytest.param({"status": HTTPStatus.INTERNAL_SERVER_ERROR}, id="500"),
        pytest.param({"exception": aiohttp.ClientError("boom")}, id="client_error"),
    ],
)
async def test_get_current_telemetry_transport_failure(
    client: AbrpClient, mock_api: aioresponses, mock_kwargs: dict[str, Any]
) -> None:
    """5xx HTTP and ClientError both surface as AbrpApiError."""
    mock_api.get(ONE_SHOT_URL, **mock_kwargs)

    with pytest.raises(AbrpApiError):
        await client.async_get_current_telemetry(VEHICLE_ID)


async def test_get_current_telemetry_unknown_charging_state_dedup(
    session: aiohttp.ClientSession,
    mock_api: aioresponses,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The chargingState warning dedup set is instance-scoped on the client.

    Two one-shots on one client warn once for the same unknown state;
    a second client instance owns its own set and warns again.
    """
    payload = {"chargingState": {"state": "FOO"}}
    for _ in range(3):
        mock_api.get(ONE_SHOT_URL, payload=payload)

    client_a = AbrpClient(session, API_KEY, StaticAuth(ACCESS_TOKEN))
    client_b = AbrpClient(session, API_KEY, StaticAuth(ACCESS_TOKEN))

    with caplog.at_level(logging.WARNING, logger="aioabrp._extract"):
        assert await client_a.async_get_current_telemetry(VEHICLE_ID) == Telemetry()
        assert await client_a.async_get_current_telemetry(VEHICLE_ID) == Telemetry()
        assert await client_b.async_get_current_telemetry(VEHICLE_ID) == Telemetry()

    warnings = [
        record
        for record in caplog.records
        if record.levelno == logging.WARNING and "FOO" in record.getMessage()
    ]
    assert len(warnings) == 2


# ---------- async_get_vehicle_model_display -----------------------------------


async def test_get_vehicle_model_display_happy_path(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """A full display record parses into a populated VehicleModelDisplay."""
    mock_api.get(display_url(), payload=build_display_record())

    display = await client.async_get_vehicle_model_display(DISPLAY_TYPECODE)

    assert isinstance(display, VehicleModelDisplay)
    assert display.manufacturer == "Rivian"
    assert display.model == "R1S"
    assert display.years == "2022-2023"
    assert display.title == "R1S Adventure"
    assert display.start_year == 2022
    assert display.end_year == 2023


@pytest.mark.parametrize(
    ("overrides", "expected_years", "expected_start", "expected_end"),
    [
        pytest.param(
            {"years": "2019", "startYear": 2019, "endYear": 2019},
            "2019",
            2019,
            2019,
            id="single_year",
        ),
        pytest.param(
            {"years": "2021+", "startYear": 2021},
            "2021+",
            2021,
            None,
            id="open_ended_range_no_end_year",
        ),
        pytest.param(
            {"years": "Unreleased"},
            "Unreleased",
            None,
            None,
            id="non_numeric_no_parsed_years",
        ),
    ],
)
async def test_get_vehicle_model_display_year_variants(
    client: AbrpClient,
    mock_api: aioresponses,
    overrides: dict[str, Any],
    expected_years: str,
    expected_start: int | None,
    expected_end: int | None,
) -> None:
    """The raw ``years`` string is preserved; omitted parsed years are None.

    The server omits ``startYear``/``endYear`` for open-ended ("2021+") or
    non-numeric ("Unreleased") values, which must surface as None without
    disturbing the always-present raw ``years`` string.
    """
    record = build_display_record(**overrides)
    for key in ("startYear", "endYear"):
        if key not in overrides:
            record.pop(key, None)
    mock_api.get(display_url(), payload=record)

    display = await client.async_get_vehicle_model_display(DISPLAY_TYPECODE)

    assert display.years == expected_years
    assert display.start_year == expected_start
    assert display.end_year == expected_end


async def test_get_vehicle_model_display_request_shape(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """Outgoing request: GET + Accept/X-API-KEY/X-ABRP-SESSION + 30s budget."""
    mock_api.get(display_url(), payload=build_display_record())

    await client.async_get_vehicle_model_display(DISPLAY_TYPECODE)

    calls = mock_api.requests[("GET", URL(display_url()))]
    assert len(calls) == 1
    kwargs = calls[0].kwargs
    assert kwargs["headers"] == {
        "Accept": "application/json",
        "X-API-KEY": API_KEY,
        "X-ABRP-SESSION": ACCESS_TOKEN,
    }
    assert kwargs["timeout"] == aiohttp.ClientTimeout(total=30)


async def test_get_vehicle_model_display_encodes_typecode_path_segment(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """A typecode is percent-encoded into a single path segment.

    A structural character such as ``/`` must not split the typecode into
    extra path segments; ``raw_path`` keeps the encoded ``%2F`` rather than
    a literal slash. (Colons normalise away under yarl, so a slash is the
    observable proof that encoding is applied.)
    """
    typecode = "maker/model:25"
    mock_api.get(display_url(typecode), payload=build_display_record())

    await client.async_get_vehicle_model_display(typecode)

    (requested_url,) = (url for _method, url in mock_api.requests)
    assert "maker%2Fmodel" in requested_url.raw_path
    assert requested_url.raw_path.endswith("/display")


@pytest.mark.parametrize(
    "record",
    [
        pytest.param({"manufacturer": None}, id="manufacturer_null"),
        pytest.param({"model": None}, id="model_null"),
        pytest.param({"years": None}, id="years_null"),
        pytest.param({"title": None}, id="title_null"),
        pytest.param({"manufacturer": 123}, id="manufacturer_non_string"),
        pytest.param({"years": 2022}, id="years_non_string"),
    ],
)
async def test_get_vehicle_model_display_malformed_required_field(
    client: AbrpClient, mock_api: aioresponses, record: dict[str, Any]
) -> None:
    """A missing/null/non-string required field raises AbrpApiError."""
    payload = build_display_record(**record)
    for key, value in record.items():
        if value is None:
            del payload[key]
    mock_api.get(display_url(), payload=payload)

    with pytest.raises(AbrpApiError):
        await client.async_get_vehicle_model_display(DISPLAY_TYPECODE)


@pytest.mark.parametrize(
    ("field", "wire_value"),
    [
        pytest.param("startYear", True, id="startyear_bool_rejected"),
        pytest.param("startYear", "2022", id="startyear_string_rejected"),
        pytest.param("startYear", 2022.0, id="startyear_float_rejected"),
        pytest.param("endYear", True, id="endyear_bool_rejected"),
    ],
)
async def test_get_vehicle_model_display_strict_year_typing(
    client: AbrpClient, mock_api: aioresponses, field: str, wire_value: Any
) -> None:
    """Wrong-typed parsed-year fields collapse to None (strict int typing)."""
    mock_api.get(display_url(), payload=build_display_record(**{field: wire_value}))

    display = await client.async_get_vehicle_model_display(DISPLAY_TYPECODE)

    snake = "start_year" if field == "startYear" else "end_year"
    assert getattr(display, snake) is None


@pytest.mark.parametrize(
    ("status", "error"),
    [
        pytest.param(HTTPStatus.UNAUTHORIZED, AbrpAuthError, id="401"),
        pytest.param(HTTPStatus.FORBIDDEN, AbrpForbiddenError, id="403"),
    ],
)
async def test_get_vehicle_model_display_http_auth_failure(
    client: AbrpClient,
    mock_api: aioresponses,
    status: HTTPStatus,
    error: type[Exception],
) -> None:
    """A 401 from the display endpoint is AbrpAuthError; a 403 is forbidden."""
    mock_api.get(display_url(), status=status)

    with pytest.raises(error):
        await client.async_get_vehicle_model_display(DISPLAY_TYPECODE)


@pytest.mark.parametrize(
    "status",
    [
        pytest.param(HTTPStatus.NOT_FOUND, id="404_unknown_typecode"),
        pytest.param(HTTPStatus.INTERNAL_SERVER_ERROR, id="500"),
    ],
)
async def test_get_vehicle_model_display_http_error_raises_api_error(
    client: AbrpClient, mock_api: aioresponses, status: HTTPStatus
) -> None:
    """Non-2xx other than 401/403 raises AbrpApiError.

    An unknown typecode (404) has no None/empty not-found sentinel — it
    surfaces as AbrpApiError like every other sibling method, leaving any
    soft not-found handling to the consumer.
    """
    mock_api.get(display_url(), status=status)

    with pytest.raises(AbrpApiError):
        await client.async_get_vehicle_model_display(DISPLAY_TYPECODE)


@pytest.mark.parametrize(
    "mock_kwargs",
    [
        pytest.param({"body": "null"}, id="null"),
        pytest.param({"payload": []}, id="list"),
        pytest.param({"payload": "just a string"}, id="string"),
    ],
)
async def test_get_vehicle_model_display_rejects_non_dict_payload(
    client: AbrpClient, mock_api: aioresponses, mock_kwargs: dict[str, Any]
) -> None:
    """A JSON-valid non-dict display body raises AbrpApiError."""
    mock_api.get(display_url(), **mock_kwargs)

    with pytest.raises(AbrpApiError):
        await client.async_get_vehicle_model_display(DISPLAY_TYPECODE)


# ---------- auth containment ---------------------------------------------------

_METHOD_CALLS: list[Any] = [
    pytest.param(lambda client: client.async_get_vehicles(), id="vehicles"),
    pytest.param(lambda client: client.async_get_catalog(), id="catalog"),
    pytest.param(
        lambda client: client.async_get_current_telemetry(VEHICLE_ID),
        id="telemetry",
    ),
    pytest.param(
        lambda client: client.async_get_vehicle_model_display(DISPLAY_TYPECODE),
        id="vehicle_model_display",
    ),
    pytest.param(
        lambda client: client.async_send_telemetry(VEHICLE_ID, soc_point()),
        id="send_telemetry",
    ),
]


@pytest.mark.parametrize("call", _METHOD_CALLS)
async def test_auth_error_from_token_getter_propagates(
    session: aiohttp.ClientSession,
    mock_api: aioresponses,
    call: Callable[[AbrpClient], Awaitable[object]],
) -> None:
    """AbrpAuthError from the token getter propagates untouched, pre-HTTP.

    The token fetch happens OUTSIDE the (ClientError, TimeoutError) band:
    a terminal auth failure must reach the caller as-is, and no HTTP
    request may be issued without a token.
    """
    client = AbrpClient(session, API_KEY, RaisingAuth(AbrpAuthError("revoked")))

    with pytest.raises(AbrpAuthError, match="revoked"):
        await call(client)

    assert not mock_api.requests


@pytest.mark.parametrize("call", _METHOD_CALLS)
async def test_client_response_error_from_token_getter_propagates(
    session: aiohttp.ClientSession,
    mock_api: aioresponses,
    call: Callable[[AbrpClient], Awaitable[object]],
) -> None:
    """A refresh ClientResponseError is NOT laundered into AbrpApiError.

    The consumer's token refresh may fail with aiohttp errors of its own;
    those belong to the consumer's error taxonomy, not the library's, so
    they must cross the client boundary unchanged.
    """
    exc = aiohttp.ClientResponseError(
        request_info=Mock(), history=(), status=500, message="refresh blew up"
    )
    client = AbrpClient(session, API_KEY, RaisingAuth(exc))

    with pytest.raises(aiohttp.ClientResponseError) as excinfo:
        await call(client)

    assert excinfo.value is exc
    assert not mock_api.requests


# ---------- async_send_telemetry ---------------------------------------------

SEND_URL = f"{API_BASE_V2}/{ENDPOINT_APP_TLM}/{VEHICLE_ID}"
# 06:30:15 at +02:00 is 04:30:15 UTC; the wire time is always UTC with "Z".
SEND_AT = datetime(2026, 11, 9, 6, 30, 15, tzinfo=timezone(timedelta(hours=2)))
SEND_AT_WIRE = "2026-11-09T04:30:15Z"


def soc_point(**overrides: Any) -> TelemetryPoint:
    """Build a minimal valid point (one soc reading) for send tests."""
    kwargs: dict[str, Any] = {
        "provider": Provider.APP_OBD,
        "time": SEND_AT,
        "soc_frac": 0.55,
    }
    kwargs.update(overrides)
    return TelemetryPoint(**kwargs)


def sent_body(mock_api: aioresponses) -> Any:
    """Decode the JSON body of the single POST to SEND_URL."""
    calls = mock_api.requests.get(("POST", URL(SEND_URL)), [])
    assert len(calls) == 1
    return json.loads(calls[0].kwargs["data"])


async def test_send_telemetry_wire_shape(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """One POST with both v2 auth headers; set fields only, unit keys, UTC Z."""
    mock_api.post(SEND_URL, payload={"mapInfo": {"region": "europe"}})
    point = TelemetryPoint(
        provider=Provider.APP_OBD,
        time=SEND_AT,
        soc_frac=0.55,
        power_w=-11_000.0,
        location=Location(59.3, 18.0),
        charging_state=ChargingState.CHARGING_AC,
    )

    result = await client.async_send_telemetry(VEHICLE_ID, point)  # type: ignore[func-returns-value]

    assert result is None
    assert sent_body(mock_api) == {
        "provider": "APP_OBD",
        "soc": {"frac": 0.55, "time": SEND_AT_WIRE},
        "power": {"w": -11_000.0, "time": SEND_AT_WIRE},
        "location": {"lat": 59.3, "long": 18.0, "time": SEND_AT_WIRE},
        "chargingState": {"state": "CHARGING_AC", "time": SEND_AT_WIRE},
    }
    kwargs = mock_api.requests[("POST", URL(SEND_URL))][0].kwargs
    assert kwargs["allow_redirects"] is False
    assert kwargs["headers"]["X-API-KEY"] == API_KEY
    assert kwargs["headers"]["X-ABRP-SESSION"] == ACCESS_TOKEN
    assert kwargs["headers"]["Content-Type"] == "application/json"


async def test_send_telemetry_maps_every_field_to_the_spec(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """Every typed field lands on the spec's InputPoint key and unit key."""
    mock_api.post(SEND_URL)
    point = TelemetryPoint(
        provider=Provider.APP_OBD,
        time=SEND_AT,
        soc_frac=0.5,
        soh_frac=0.97,
        power_w=1.0,
        hvac_power_w=2.0,
        voltage_v=3.0,
        current_a=4.0,
        soe_wh=5.0,
        battery_capacity_wh=6.0,
        charging_energy_added_wh=7.0,
        odometer_m=8.0,
        elevation_m=9.0,
        estimated_battery_range_m=10.0,
        speed_ms=11.0,
        heading_degrees=-12.0,
        battery_temperature_c=13.0,
        battery_temperature_min_c=14.0,
        battery_temperature_max_c=15.0,
        cabin_temperature_c=16.0,
        cabin_set_point_c=17.0,
        external_temperature_c=18,  # an int is a valid number
        charging_state=ChargingState.NOT_CHARGING,
        driving_state=DrivingState.DRIVE,
        location=Location(-33.9, 151.2),
    )

    await client.async_send_telemetry(VEHICLE_ID, point)

    t = SEND_AT_WIRE
    assert sent_body(mock_api) == {
        "provider": "APP_OBD",
        "soc": {"frac": 0.5, "time": t},
        "soh": {"frac": 0.97, "time": t},
        "power": {"w": 1.0, "time": t},
        "hvacPower": {"w": 2.0, "time": t},
        "voltage": {"v": 3.0, "time": t},
        "current": {"a": 4.0, "time": t},
        "soe": {"wh": 5.0, "time": t},
        "batteryCapacity": {"wh": 6.0, "time": t},
        "chargingEnergyAdded": {"wh": 7.0, "time": t},
        "odometer": {"m": 8.0, "time": t},
        "elevation": {"m": 9.0, "time": t},
        "estimatedBatteryRange": {"m": 10.0, "time": t},
        "speed": {"ms": 11.0, "time": t},
        "heading": {"degrees": -12.0, "time": t},
        "batteryTemperature": {"c": 13.0, "time": t},
        "batteryTemperatureMin": {"c": 14.0, "time": t},
        "batteryTemperatureMax": {"c": 15.0, "time": t},
        "cabinTemperature": {"c": 16.0, "time": t},
        "cabinSetPoint": {"c": 17.0, "time": t},
        "externalTemperature": {"c": 18, "time": t},
        "chargingState": {"state": "NOT_CHARGING", "time": t},
        "drivingState": {"state": "DRIVE", "time": t},
        "location": {"lat": -33.9, "long": 151.2, "time": t},
    }


async def test_send_telemetry_extra_is_merged(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """Dicts without a time get the point's; dicts with one and scalars pass."""
    mock_api.post(SEND_URL)
    new_metric = {"x": 1}
    point = soc_point(
        extra={
            "newMetric": new_metric,
            "timedMetric": {"x": 2, "time": "2026-01-01T00:00:00Z"},
            "flag": True,
        }
    )
    new_metric["x"] = 99  # mutating the caller's dict later changes nothing

    await client.async_send_telemetry(VEHICLE_ID, point)

    assert sent_body(mock_api) == {
        "provider": "APP_OBD",
        "soc": {"frac": 0.55, "time": SEND_AT_WIRE},
        "newMetric": {"x": 1, "time": SEND_AT_WIRE},
        "timedMetric": {"x": 2, "time": "2026-01-01T00:00:00Z"},
        "flag": True,
    }
    assert new_metric == {"x": 99}  # the caller's dict got no "time" added


async def test_send_telemetry_extra_only_point(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """A point may carry only extra entries; it copies, compares and pickles."""
    mock_api.post(SEND_URL)
    inner = [1]
    point = TelemetryPoint(
        provider=Provider.APP_OBD, time=SEND_AT, extra={"newMetric": {"x": inner}}
    )
    inner.append(2)  # nested values are copied too
    point.extra["provider"] = "SPOOF"  # type: ignore[index]  # typed keys win

    assert point != TelemetryPoint(
        provider=Provider.APP_OBD, time=SEND_AT, extra={"newMetric": {"x": [2]}}
    )
    assert copy.deepcopy(point) == pickle.loads(pickle.dumps(point)) == point
    assert "newMetric" not in repr(point)  # extra may carry anything
    assert "59.3" not in repr(soc_point(location=Location(59.3, 18.0)))

    await client.async_send_telemetry(VEHICLE_ID, point)

    assert sent_body(mock_api) == {
        "provider": "APP_OBD",
        "newMetric": {"x": [1], "time": SEND_AT_WIRE},
    }


@pytest.mark.parametrize("key", ["soc", "chargingState", "location", "provider"])
def test_send_telemetry_extra_cannot_override_typed_fields(key: str) -> None:
    with pytest.raises(ValueError, match=key):
        soc_point(extra={key: {"x": 1}})


@pytest.mark.parametrize(
    ("provider", "wire"),
    [
        pytest.param(Provider.APP_AUTO, "APP_AUTO", id="enum"),
        pytest.param("APP_SOMETHING_NEW", "APP_SOMETHING_NEW", id="str"),
    ],
)
async def test_send_telemetry_provider(
    client: AbrpClient, mock_api: aioresponses, provider: Provider | str, wire: str
) -> None:
    mock_api.post(SEND_URL)

    await client.async_send_telemetry(VEHICLE_ID, soc_point(provider=provider))

    assert sent_body(mock_api)["provider"] == wire


@pytest.mark.parametrize(
    ("overrides", "error", "names"),
    [
        pytest.param({"time": datetime(2026, 1, 1)}, ValueError, "time", id="naive"),
        pytest.param(
            {"time": "2026-01-01T00:00:00Z"}, TypeError, "time", id="time_str"
        ),
        pytest.param({"soc_frac": None}, ValueError, "metric", id="empty"),
        pytest.param({"soc_frac": True}, TypeError, "soc_frac", id="bool"),
        pytest.param({"soc_frac": "0.5"}, TypeError, "soc_frac", id="str_number"),
        pytest.param({"soc_frac": math.nan}, ValueError, "soc_frac", id="nan"),
        pytest.param({"power_w": math.inf}, ValueError, "power_w", id="inf"),
        pytest.param(
            {"charging_state": "CHARGING_AC"},
            TypeError,
            "charging_state",
            id="enum_str",
        ),
        pytest.param({"location": (59.3, 18.0)}, TypeError, "location", id="tuple"),
        pytest.param(
            {"location": Location(math.nan, 18.0)}, ValueError, "location", id="loc_nan"
        ),
        pytest.param(
            {"location": Location(59.3, math.inf)}, ValueError, "location", id="loc_inf"
        ),
        pytest.param({"soc_frac": 10**400}, ValueError, "soc_frac", id="huge_int"),
        pytest.param({"provider": ""}, ValueError, "provider", id="provider_empty"),
        pytest.param({"provider": None}, TypeError, "provider", id="provider_none"),
        pytest.param({"extra": {1: {}}}, TypeError, "extra", id="extra_key"),
    ],
)
def test_send_telemetry_rejects_bad_points(
    overrides: dict[str, Any], error: type[Exception], names: str
) -> None:
    """Rejected when the point is built, so no request can be sent."""
    with pytest.raises(error, match=names) as excinfo:
        soc_point(**overrides)
    assert "0.5" not in str(excinfo.value)  # messages name fields, not values


@pytest.mark.parametrize(
    "vehicle_id", [pytest.param(True, id="bool"), pytest.param("42", id="str")]
)
async def test_send_telemetry_rejects_bad_vehicle_id(
    client: AbrpClient, mock_api: aioresponses, vehicle_id: Any
) -> None:
    with pytest.raises(TypeError, match="vehicle_id"):
        await client.async_send_telemetry(vehicle_id, soc_point())
    assert not mock_api.requests


@pytest.mark.parametrize(
    ("extra", "error"),
    [
        pytest.param({"blob": object()}, TypeError, id="unserializable"),
        pytest.param({"blob": {"x": math.nan}}, ValueError, id="nan"),
    ],
)
async def test_send_telemetry_bad_extra_fails_before_auth(
    session: aiohttp.ClientSession,
    mock_api: aioresponses,
    extra: dict[str, Any],
    error: type[Exception],
) -> None:
    """A body that is not valid JSON raises before the token is even fetched."""
    client = AbrpClient(session, API_KEY, RaisingAuth(AbrpAuthError("not reached")))

    with pytest.raises(error):
        await client.async_send_telemetry(VEHICLE_ID, soc_point(extra=extra))
    assert not mock_api.requests


@pytest.mark.parametrize(
    "mock_kwargs",
    [
        pytest.param({"status": HTTPStatus.OK, "body": "not json"}, id="200_text"),
        pytest.param({"status": HTTPStatus.ACCEPTED}, id="202"),
        pytest.param({"status": HTTPStatus.NO_CONTENT}, id="204"),
    ],
)
async def test_send_telemetry_any_2xx_succeeds(
    client: AbrpClient, mock_api: aioresponses, mock_kwargs: dict[str, Any]
) -> None:
    mock_api.post(SEND_URL, **mock_kwargs)

    await client.async_send_telemetry(VEHICLE_ID, soc_point())


@pytest.mark.parametrize(
    ("mock_kwargs", "error", "message"),
    [
        pytest.param(
            {"status": 400, "body": "soc.frac:\n  out of range"},
            AbrpApiError,
            "HTTP 400: soc.frac: out of range",
            id="400",
        ),
        pytest.param(
            {"status": 400, "body": '{"message": "soc.frac out of range"}'},
            AbrpApiError,
            "HTTP 400: soc.frac out of range",
            id="400_json",
        ),
        pytest.param(
            {"status": 400, "body": b"\xff\xfe bad"},
            AbrpApiError,
            "HTTP 400: ",
            id="400_not_utf8",
        ),
        pytest.param(
            {"status": 400, "body": f"echo {ACCESS_TOKEN} {API_KEY}"},
            AbrpApiError,
            "HTTP 400: echo",
            id="400_echoes_secrets",
        ),
        pytest.param({"status": 401}, AbrpAuthError, "HTTP 401", id="401"),
        pytest.param(
            {"status": 403, "body": "insufficient_scope"},
            AbrpAuthError,
            "HTTP 403: insufficient_scope",
            id="403",
        ),
        pytest.param(
            {
                "status": 403,
                # A challenge that is not insufficient_scope stays forbidden.
                "headers": {"WWW-Authenticate": 'Bearer realm="abrp"'},
                "body": '{"message":"not allowed to view vehicle"}',
            },
            AbrpForbiddenError,
            "HTTP 403: not allowed to view vehicle",
            id="403_forbidden",
        ),
        pytest.param({"status": 404}, AbrpApiError, "HTTP 404", id="404"),
        pytest.param({"status": 302}, AbrpApiError, "HTTP 302", id="redirect"),
        pytest.param({"status": 500}, AbrpApiError, "HTTP 500", id="500"),
        pytest.param(
            {"exception": aiohttp.ClientError("boom")},
            AbrpApiError,
            "boom",
            id="client",
        ),
        pytest.param({"exception": TimeoutError()}, AbrpApiError, "", id="timeout"),
    ],
)
async def test_send_telemetry_errors(
    client: AbrpClient,
    mock_api: aioresponses,
    mock_kwargs: dict[str, Any],
    error: type[Exception],
    message: str,
) -> None:
    mock_api.post(SEND_URL, **mock_kwargs)

    with pytest.raises(error) as excinfo:
        await client.async_send_telemetry(VEHICLE_ID, soc_point())

    text = str(excinfo.value)
    assert text.startswith(message)
    assert ACCESS_TOKEN not in text
    assert API_KEY not in text
    assert "0.55" not in text  # the request body never leaks


async def test_send_telemetry_truncates_long_error_text(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    mock_api.post(SEND_URL, status=400, body="x" * 5000)

    with pytest.raises(AbrpApiError) as excinfo:
        await client.async_send_telemetry(VEHICLE_ID, soc_point())

    assert str(excinfo.value) == "HTTP 400: " + "x" * 200


SCOPE_CHALLENGE = {
    "WWW-Authenticate": 'Bearer error="insufficient_scope", scope="tlm:write"'
}


@pytest.mark.parametrize(
    ("mock_kwargs", "scope"),
    [
        pytest.param(
            {
                "status": 403,
                "headers": SCOPE_CHALLENGE,
                "body": '{"message":"insufficient_scope"}',
            },
            "tlm:write",
            id="header",
        ),
        pytest.param(
            {
                "status": 403,
                "headers": {"WWW-Authenticate": 'Bearer error="insufficient_scope"'},
            },
            None,
            id="header_no_scope",
        ),
        pytest.param({"status": 403, "body": "insufficient_scope"}, None, id="body"),
    ],
)
async def test_send_telemetry_missing_scope_raises_scope_error(
    client: AbrpClient,
    mock_api: aioresponses,
    mock_kwargs: dict[str, Any],
    scope: str | None,
) -> None:
    """A 403 for a missing scope is an AbrpScopeError (still an AbrpAuthError)."""
    mock_api.post(SEND_URL, **mock_kwargs)

    with pytest.raises(AbrpScopeError) as excinfo:
        await client.async_send_telemetry(VEHICLE_ID, soc_point())

    assert isinstance(excinfo.value, AbrpAuthError)
    assert excinfo.value.scope == scope


@pytest.mark.parametrize(
    "mock_kwargs",
    [
        pytest.param(
            {
                "status": 401,
                "headers": {"WWW-Authenticate": 'Bearer error="invalid_token"'},
            },
            id="401",
        ),
        pytest.param(
            {"status": 401, "headers": SCOPE_CHALLENGE}, id="401_scope_header"
        ),
    ],
)
async def test_send_telemetry_other_auth_failures_are_not_scope_errors(
    client: AbrpClient, mock_api: aioresponses, mock_kwargs: dict[str, Any]
) -> None:
    mock_api.post(SEND_URL, **mock_kwargs)

    with pytest.raises(AbrpAuthError) as excinfo:
        await client.async_send_telemetry(VEHICLE_ID, soc_point())

    assert not isinstance(excinfo.value, AbrpScopeError)


async def test_send_telemetry_forbidden_is_not_an_auth_error(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """A 403 for a valid session (vehicle not owned or shared) must not reauth."""
    mock_api.post(
        SEND_URL, status=403, body='{"message":"not allowed to view vehicle"}'
    )

    with pytest.raises(AbrpApiError) as excinfo:
        await client.async_send_telemetry(VEHICLE_ID, soc_point())

    assert isinstance(excinfo.value, AbrpForbiddenError)
    assert not isinstance(excinfo.value, AbrpAuthError)


async def test_read_missing_scope_raises_scope_error(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """The read calls map the same 403 challenge to AbrpScopeError."""
    mock_api.get(VEHICLES_URL, status=403, headers=SCOPE_CHALLENGE)

    with pytest.raises(AbrpScopeError) as excinfo:
        await client.async_get_vehicles()

    assert excinfo.value.scope == "tlm:write"
    assert str(excinfo.value) == "HTTP 403"
