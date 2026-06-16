"""Tests for aioabrp.client: the request/response AbrpClient.

Endpoint mocks use aioresponses against a real ``aiohttp.ClientSession``.
The wire surface under test:

* ``POST /1/session/get_tlm`` — garage enumeration (v1 envelope)
* ``GET /2/vehicle/_list`` — vehicle catalog (bare JSON)
* ``GET /2/tlm/{vehicle_id}`` — one-shot telemetry (bare JSON -> typed)
* ``GET /2/vehicle-model/by-typecode/{typecode}/display`` — display metadata

Auth-vs-generic error routing on the v1 envelope uses the word-bounded
keyword heuristic on the ``error`` text.
"""

import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import UTC, datetime, timedelta
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
    API_BASE_V1,
    API_BASE_V2,
    ENDPOINT_GET_TLM,
    ENDPOINT_TLM,
    ENDPOINT_VEHICLE_LIST,
    ENDPOINT_VEHICLE_MODEL_DISPLAY,
)
from aioabrp.exceptions import AbrpApiError, AbrpAuthError
from aioabrp.models import (
    AbrpVehicle,
    ChargingState,
    Location,
    Metric,
    Telemetry,
    VehicleModelDisplay,
)

API_KEY = "mock-partner-key"
ACCESS_TOKEN = "mock-access-token"
VEHICLE_ID = 941349991303
VEHICLE_NAME = "Rivian R2 2027 Standard Long Range"
VEHICLE_MODEL = "rivian:r2:26:ncma91:rwd:w21"
PAINT = "WHITE"

DISPLAY_TYPECODE = "rivian:r1s:22:large"

VEHICLES_URL = f"{API_BASE_V1}/{ENDPOINT_GET_TLM}"
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


def build_vehicle_record(
    vehicle_id: int = VEHICLE_ID,
    name: str | None = VEHICLE_NAME,
    vehicle_model: str = VEHICLE_MODEL,
    paint: str | None = PAINT,
) -> dict[str, Any]:
    """Build one ``result`` array entry as returned by /1/session/get_tlm.

    A trimmed mirror of the live wire record: the identity fields the
    parser consumes plus a few of the extraneous columns the real
    endpoint emits, so the parser is exercised against realistic noise.
    """
    return {
        "vehicle_id": vehicle_id,
        "car_model": vehicle_model,
        "name": name,
        "paint": paint,
        "tlm_type": None,
        "always_log": False,
        "is_connected": False,
        "owner_id": 2755291,
    }


def build_garage_response(
    records: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Wrap ``records`` in the v1 envelope returned by /1/session/get_tlm."""
    if records is None:
        records = [build_vehicle_record()]
    return {
        "status": "ok",
        "result": records,
        "extra": {"settings_update_time": 0, "settings_version": 0},
    }


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


async def test_get_vehicles_happy_path(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """Two vehicles parse in order; a null nickname is tolerated."""
    records = [
        build_vehicle_record(),
        build_vehicle_record(vehicle_id=2, name=None, paint=None),
    ]
    mock_api.post(VEHICLES_URL, payload=build_garage_response(records))

    vehicles = await client.async_get_vehicles()

    assert len(vehicles) == 2
    first, second = vehicles
    assert isinstance(first, AbrpVehicle)
    assert first.vehicle_id == VEHICLE_ID
    assert first.name == VEHICLE_NAME
    assert first.vehicle_model == VEHICLE_MODEL
    assert first.paint == PAINT
    assert second.vehicle_id == 2
    assert second.name is None
    assert second.paint is None


async def test_get_vehicles_empty(client: AbrpClient, mock_api: aioresponses) -> None:
    """An empty garage returns an empty list (not None, not an exception)."""
    mock_api.post(VEHICLES_URL, payload=build_garage_response([]))

    assert await client.async_get_vehicles() == []


async def test_get_vehicles_request_shape(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """Outgoing request: POST + ``APIKEY`` header + ``session_id`` body."""
    mock_api.post(VEHICLES_URL, payload=build_garage_response())

    await client.async_get_vehicles()

    calls = mock_api.requests[("POST", URL(VEHICLES_URL))]
    assert len(calls) == 1
    kwargs = calls[0].kwargs
    assert kwargs["json"] == {"session_id": ACCESS_TOKEN}
    assert kwargs["headers"] == {"Authorization": f"APIKEY {API_KEY}"}
    assert kwargs["timeout"] == aiohttp.ClientTimeout(total=30)


@pytest.mark.parametrize(
    "error_text",
    [
        pytest.param("session expired", id="session_and_expired"),
        pytest.param("invalid session", id="invalid_and_session"),
        pytest.param("Authentication failed", id="auth"),
        pytest.param("Token expired", id="token_and_expired"),
        pytest.param("session_id invalid", id="session_id_underscore"),
        pytest.param("auth_required", id="auth_required_underscore"),
        pytest.param("authorization failed", id="authorization"),
        pytest.param("invalid_credentials", id="invalid_credentials"),
    ],
)
async def test_get_vehicles_envelope_auth_error(
    client: AbrpClient, mock_api: aioresponses, error_text: str
) -> None:
    """``status:error`` with auth-heuristic text raises AbrpAuthError."""
    mock_api.post(VEHICLES_URL, payload={"status": "error", "error": error_text})

    with pytest.raises(AbrpAuthError):
        await client.async_get_vehicles()


@pytest.mark.parametrize(
    "error_text",
    [
        pytest.param("Backend overloaded", id="backend"),
        pytest.param("Rate limit reached", id="rate_limit"),
        pytest.param("Internal failure", id="internal"),
        pytest.param("invalid vehicle_model", id="invalid_other_word"),
    ],
)
async def test_get_vehicles_envelope_api_error(
    client: AbrpClient, mock_api: aioresponses, error_text: str
) -> None:
    """``status:error`` without auth-heuristic text raises AbrpApiError."""
    mock_api.post(VEHICLES_URL, payload={"status": "error", "error": error_text})

    with pytest.raises(AbrpApiError):
        await client.async_get_vehicles()


@pytest.mark.parametrize(
    "status",
    [
        pytest.param(HTTPStatus.UNAUTHORIZED, id="401"),
        pytest.param(HTTPStatus.FORBIDDEN, id="403"),
    ],
)
async def test_get_vehicles_http_auth_failure(
    client: AbrpClient, mock_api: aioresponses, status: HTTPStatus
) -> None:
    """A 401/403 from the garage endpoint raises AbrpAuthError."""
    mock_api.post(VEHICLES_URL, status=status)

    with pytest.raises(AbrpAuthError):
        await client.async_get_vehicles()


@pytest.mark.parametrize(
    "mock_kwargs",
    [
        pytest.param({"status": HTTPStatus.INTERNAL_SERVER_ERROR}, id="500"),
        pytest.param({"status": HTTPStatus.BAD_GATEWAY}, id="502"),
        pytest.param({"status": HTTPStatus.SERVICE_UNAVAILABLE}, id="503"),
        pytest.param({"exception": aiohttp.ClientError("boom")}, id="client_error"),
    ],
)
async def test_get_vehicles_transport_failure(
    client: AbrpClient, mock_api: aioresponses, mock_kwargs: dict[str, Any]
) -> None:
    """5xx HTTP and ClientError both surface as AbrpApiError."""
    mock_api.post(VEHICLES_URL, **mock_kwargs)

    with pytest.raises(AbrpApiError):
        await client.async_get_vehicles()


@pytest.mark.parametrize(
    "mock_kwargs",
    [
        pytest.param({"body": "null"}, id="null"),
        pytest.param({"payload": []}, id="empty_list"),
        pytest.param(
            {"payload": [build_vehicle_record(vehicle_id=1)]}, id="list_of_dicts"
        ),
        pytest.param({"payload": "just a string"}, id="string"),
        pytest.param({"payload": 42}, id="integer"),
    ],
)
async def test_get_vehicles_rejects_non_dict_payload(
    client: AbrpClient, mock_api: aioresponses, mock_kwargs: dict[str, Any]
) -> None:
    """A JSON-valid non-dict garage body raises AbrpApiError, not AttributeError."""
    mock_api.post(VEHICLES_URL, **mock_kwargs)

    with pytest.raises(AbrpApiError):
        await client.async_get_vehicles()


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param({"status": "ok"}, id="missing_result_key"),
        pytest.param({"status": "ok", "result": "oops"}, id="result_is_str"),
        pytest.param({"status": "ok", "result": None}, id="result_is_null"),
        pytest.param({"status": "ok", "result": {"x": 1}}, id="result_is_dict"),
    ],
)
async def test_get_vehicles_malformed_success_envelope(
    client: AbrpClient, mock_api: aioresponses, payload: dict[str, Any]
) -> None:
    """``status:ok`` with missing/non-list ``result`` raises AbrpApiError."""
    mock_api.post(VEHICLES_URL, payload=payload)

    with pytest.raises(AbrpApiError):
        await client.async_get_vehicles()


@pytest.mark.parametrize(
    "malformed_body",
    [
        pytest.param("<html><body>502 Bad Gateway</body></html>", id="html"),
        pytest.param('{"result": ', id="truncated_json"),
    ],
)
async def test_get_vehicles_malformed_json_raises_api_error(
    client: AbrpClient, mock_api: aioresponses, malformed_body: str
) -> None:
    """Malformed JSON garage body wraps as AbrpApiError with ValueError cause."""
    mock_api.post(VEHICLES_URL, status=200, body=malformed_body)

    with pytest.raises(AbrpApiError) as excinfo:
        await client.async_get_vehicles()

    assert isinstance(excinfo.value.__cause__, ValueError)


async def test_get_vehicles_empty_body_raises_api_error(
    client: AbrpClient, mock_api: aioresponses
) -> None:
    """A truncated/empty 200 body raises AbrpApiError.

    aiohttp's ``response.json()`` short-circuits an empty body to
    ``None`` (no ``JSONDecodeError``), so this lands on the non-dict
    shape guard rather than the malformed-JSON wrap — either way the
    public boundary type is AbrpApiError.
    """
    mock_api.post(VEHICLES_URL, status=200, body="")

    with pytest.raises(AbrpApiError):
        await client.async_get_vehicles()


@pytest.mark.parametrize(
    "record",
    [
        pytest.param({"name": "no ids"}, id="missing_vehicle_id"),
        pytest.param({"vehicle_id": None, "car_model": "x"}, id="vehicle_id_null"),
        pytest.param(
            {"vehicle_id": "not-an-int", "car_model": "x"}, id="vehicle_id_str"
        ),
        pytest.param({"vehicle_id": 1}, id="missing_car_model"),
        pytest.param(42, id="record_non_dict"),
    ],
)
async def test_get_vehicles_malformed_record(
    client: AbrpClient, mock_api: aioresponses, record: Any
) -> None:
    """A malformed ``result`` record surfaces as AbrpApiError."""
    mock_api.post(VEHICLES_URL, payload=build_garage_response([record]))

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
    "status",
    [
        pytest.param(HTTPStatus.UNAUTHORIZED, id="401"),
        pytest.param(HTTPStatus.FORBIDDEN, id="403"),
    ],
)
async def test_get_catalog_http_auth_failure(
    client: AbrpClient, mock_api: aioresponses, status: HTTPStatus
) -> None:
    """A 401/403 from the catalog endpoint raises AbrpAuthError."""
    mock_api.get(CATALOG_URL, status=status)

    with pytest.raises(AbrpAuthError):
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
    "status",
    [
        pytest.param(HTTPStatus.UNAUTHORIZED, id="401"),
        pytest.param(HTTPStatus.FORBIDDEN, id="403"),
    ],
)
async def test_get_current_telemetry_http_auth_failure(
    client: AbrpClient, mock_api: aioresponses, status: HTTPStatus
) -> None:
    """A 401/403 from the one-shot endpoint raises AbrpAuthError."""
    mock_api.get(ONE_SHOT_URL, status=status)

    with pytest.raises(AbrpAuthError):
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
    "status",
    [
        pytest.param(HTTPStatus.UNAUTHORIZED, id="401"),
        pytest.param(HTTPStatus.FORBIDDEN, id="403"),
    ],
)
async def test_get_vehicle_model_display_http_auth_failure(
    client: AbrpClient, mock_api: aioresponses, status: HTTPStatus
) -> None:
    """A 401/403 from the display endpoint raises AbrpAuthError."""
    mock_api.get(display_url(), status=status)

    with pytest.raises(AbrpAuthError):
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
