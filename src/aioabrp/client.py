"""Request/response client for the ABRP v1/v2 endpoints.

Wire surface:

* ``POST /1/session/get_tlm`` — garage enumeration.
  Header ``Authorization: APIKEY <api_key>``, body
  ``{"session_id": "<access token>"}``, response envelope
  ``{"status": "ok"|"error", "result"|"error": ...}``. The v1 endpoint
  returns ``200 OK`` for both successful calls and most business-level
  failures; auth and validation failures arrive as
  ``200 {"status": "error", "error": "<text>"}``. The auth-vs-generic
  split is a text-matching heuristic on the error string until ABRP
  exposes machine-readable error codes.
* ``GET /2/vehicle/_list`` — vehicle catalog (bare JSON, no envelope).
* ``GET /2/tlm/{vehicle_id}`` — one-shot telemetry snapshot (bare JSON),
  extracted into the library's typed event shape.

The v2 endpoints split auth across two headers: the static partner key
in ``X-API-KEY`` and the per-user session token in ``X-ABRP-SESSION``.

Failure containment: every method fetches the access token via the
injected :class:`~aioabrp.auth.AbstractAuth` BEFORE entering its
``(ClientError, TimeoutError)`` wrap band, so a terminal
:class:`~aioabrp.exceptions.AbrpAuthError` from the getter — or any
consumer-side refresh failure such as ``aiohttp.ClientResponseError`` —
propagates to the caller untouched instead of being laundered into
:class:`~aioabrp.exceptions.AbrpApiError`.
"""

import re
from collections.abc import Mapping
from http import HTTPStatus
from typing import Any

from aiohttp import ClientError, ClientSession, ClientTimeout

from ._extract import extract_metrics
from .auth import AbstractAuth
from .const import (
    API_BASE_V1,
    API_BASE_V2,
    ENDPOINT_GET_TLM,
    ENDPOINT_TLM,
    ENDPOINT_VEHICLE_LIST,
    HEADER_ABRP_SESSION,
    HEADER_API_KEY,
    ONE_SHOT_TIMEOUT_SECONDS,
)
from .exceptions import AbrpApiError, AbrpAuthError
from .models import AbrpVehicle, CatalogEntry, Telemetry

# Heuristic match against the v1 envelope ``error`` text. The keywords are
# word-bounded to avoid matching unrelated business errors that happen to
# contain a substring (e.g. "invalid vehicle_model" matches a bare "invalid").
# Compound forms like ``session_id`` / ``auth_required`` are their own
# alternatives because the ``_`` is a regex word character and would block
# the shorter ``\bsession\b`` / ``\bauth\b`` boundary match.
_AUTH_ERROR_RE = re.compile(
    r"\b(?:"
    r"session|session_id|token|expired|unauthorized"
    r"|authentication|authorization|auth_required|auth_failed"
    r"|invalid_credentials"
    r")\b",
    re.IGNORECASE,
)


class AbrpClient:
    """Stateless async client for the ABRP v1 garage + v2 catalog/telemetry APIs.

    ``api_key`` is the static Iternio partner key (used in v1's
    ``Authorization: APIKEY <key>`` header and in v2's ``X-API-KEY``
    header). The per-user access token is fetched fresh from ``auth`` on
    every call — the v1 endpoint accepts it in the request body as
    ``session_id``; the v2 endpoints expect it in ``X-ABRP-SESSION``.

    The only instance state is the dedup set for the
    unrecognized-chargingState warning — one per client instance, never
    module-global, so warning state cannot leak across accounts.
    """

    def __init__(
        self,
        websession: ClientSession,
        api_key: str,
        auth: AbstractAuth,
    ) -> None:
        """Initialize the client with a shared session and injected auth."""
        self._websession = websession
        self._api_key = api_key
        self._auth = auth
        self._unknown_charging_states_seen: set[str] = set()

    async def async_get_vehicles(self) -> list[AbrpVehicle]:
        """Return the authenticated user's garage.

        Raises:
            AbrpAuthError: HTTP 401/403, or 200 envelope with auth-flavoured
                ``error`` text (or, untouched, from the token getter).
            AbrpApiError: any other HTTP/transport/parse failure, or 200
                envelope with non-auth ``error`` text.

        """
        token = await self._auth.async_get_access_token()
        url = f"{API_BASE_V1}/{ENDPOINT_GET_TLM}"
        headers = {"Authorization": f"APIKEY {self._api_key}"}
        body = {"session_id": token}
        try:
            # Explicit budget deliberately diverges from the HA source — a
            # standalone lib can't assume the consumer session's default.
            async with self._websession.post(
                url,
                headers=headers,
                json=body,
                timeout=ClientTimeout(total=ONE_SHOT_TIMEOUT_SECONDS),
            ) as response:
                if response.status in (
                    HTTPStatus.UNAUTHORIZED,
                    HTTPStatus.FORBIDDEN,
                ):
                    raise AbrpAuthError(f"HTTP {response.status}")
                # 401/403 only; other 4xx → AbrpApiError. Revisit if ABRP
                # starts returning auth-flavoured 4xx with structured bodies
                # (current v1 surfaces auth failures as 200 + status:"error").
                if response.status >= HTTPStatus.BAD_REQUEST:
                    raise AbrpApiError(f"HTTP {response.status}")
                try:
                    payload: Any = await response.json()
                except ValueError as err:
                    raise AbrpApiError(
                        f"malformed JSON in garage response: {err}"
                    ) from err
        except (ClientError, TimeoutError) as err:
            raise AbrpApiError(str(err)) from err

        if not isinstance(payload, dict):
            raise AbrpApiError(
                f"unexpected garage payload shape: {type(payload).__name__}"
            )
        if payload.get("status") != "ok":
            error_text = str(payload.get("error", "unknown"))
            if _AUTH_ERROR_RE.search(error_text):
                raise AbrpAuthError(error_text)
            raise AbrpApiError(error_text)

        records = payload.get("result")
        if not isinstance(records, list):
            raise AbrpApiError("missing or malformed 'result' in success response")
        return [_parse_vehicle(record) for record in records]

    async def async_get_catalog(self) -> dict[str, CatalogEntry]:
        """Fetch the v2 vehicle catalog from ``GET /2/vehicle/_list``.

        Returns a dict keyed by typecode for O(1) lookup. The catalog is
        ~850 KB (~1100 entries, no pagination) and stable enough that
        fetching once per consumer lifetime is appropriate.

        Auth: dual-header (``X-API-KEY`` partner key + ``X-ABRP-SESSION``
        access token), same shape as the telemetry endpoints and
        probe-confirmed on the catalog endpoint.

        Raises:
            AbrpAuthError: HTTP 401/403 (session rejected by ABRP), or,
                untouched, from the token getter.
            AbrpApiError: any other 4xx/5xx HTTP, transport, timeout, or
                parse failure. Consumers may treat this as non-fatal and
                degrade to an empty catalog.

        """
        payload = await self._get_v2_json(
            f"{API_BASE_V2}/{ENDPOINT_VEHICLE_LIST}", what="catalog"
        )
        vehicles_raw = payload.get("vehicles")
        if not isinstance(vehicles_raw, list):
            raise AbrpApiError("missing or malformed 'vehicles' in catalog response")
        catalog: dict[str, CatalogEntry] = {}
        for record in vehicles_raw:
            if not isinstance(record, dict):
                continue
            entry = _parse_catalog_entry(record)
            if entry is not None:
                catalog[entry.typecode] = entry
        return catalog

    async def async_get_current_telemetry(self, vehicle_id: int) -> Telemetry:
        """Fetch and extract the current telemetry snapshot for one vehicle.

        ``GET /2/tlm/{vehicle_id}`` with ``Accept: application/json``
        returns the bare wire frame (the single-vehicle endpoint scopes
        ``vehicleId`` via the path). An empty response body ``{}`` is a
        valid "no metric data yet" answer and extracts to a ``Telemetry``
        with all fields ``None``.

        The frame runs through :func:`aioabrp._extract.extract_metrics`,
        so the result is the same typed :class:`~aioabrp.models.Telemetry`
        shape a telemetry stream's ``on_update`` delivers. No
        monotonicity gating happens here — any seeding/merge policy
        against stream events belongs to the consumer.

        Raises:
            AbrpAuthError: HTTP 401/403 (session rejected by ABRP), or,
                untouched, from the token getter.
            AbrpApiError: any other 4xx/5xx HTTP, transport, timeout, or
                parse failure.

        """
        payload = await self._get_v2_json(
            f"{API_BASE_V2}/{ENDPOINT_TLM}/{vehicle_id}", what="one-shot"
        )
        extracted = extract_metrics(
            payload,
            unknown_charging_states_seen=self._unknown_charging_states_seen,
        )
        return Telemetry(**{metric.value: value for metric, value in extracted.items()})

    async def _get_v2_json(self, url: str, *, what: str) -> dict[str, Any]:
        """GET a v2 endpoint and return its bare-JSON dict payload.

        Shared request ladder for the dual-header v2 endpoints: 401/403
        raises :class:`AbrpAuthError`; any other non-2xx, transport,
        timeout, parse, or non-dict-shape failure raises
        :class:`AbrpApiError` (``what`` names the endpoint in messages).
        The token fetch stays OUTSIDE the ``(ClientError, TimeoutError)``
        band so getter exceptions propagate to the caller untouched.
        """
        token = await self._auth.async_get_access_token()
        headers = {
            "Accept": "application/json",
            HEADER_API_KEY: self._api_key,
            HEADER_ABRP_SESSION: token,
        }
        try:
            async with self._websession.get(
                url,
                headers=headers,
                timeout=ClientTimeout(total=ONE_SHOT_TIMEOUT_SECONDS),
            ) as response:
                if response.status in (
                    HTTPStatus.UNAUTHORIZED,
                    HTTPStatus.FORBIDDEN,
                ):
                    raise AbrpAuthError(f"HTTP {response.status}")
                if response.status >= HTTPStatus.BAD_REQUEST:
                    raise AbrpApiError(f"HTTP {response.status}")
                try:
                    payload: Any = await response.json()
                except ValueError as err:
                    raise AbrpApiError(
                        f"malformed JSON in {what} response: {err}"
                    ) from err
        except (ClientError, TimeoutError) as err:
            # A bare ``ClientTimeout(total=...)`` raises naked
            # ``asyncio.TimeoutError`` which is NOT a ``ClientError`` subclass,
            # so the catch band must name it explicitly. Wrapping at the client
            # boundary keeps the caller's ``(AbrpAuthError, AbrpApiError)``
            # handling engaged when the endpoint hangs past the budget.
            raise AbrpApiError(str(err)) from err

        if not isinstance(payload, dict):
            raise AbrpApiError(
                f"unexpected {what} payload shape: {type(payload).__name__}"
            )
        return payload


def _parse_vehicle(record: dict[str, Any]) -> AbrpVehicle:
    """Parse one ``result`` array entry into an :class:`AbrpVehicle`.

    Only the v1 identity fields are surfaced (raw wire fields; any
    catalog enrichment or display policy is the consumer's concern).
    ``name`` stays nullable defensively — some live-API records carry no
    nickname.
    """
    try:
        name = record.get("name")
        paint = record.get("paint")
        return AbrpVehicle(
            vehicle_id=int(record["vehicle_id"]),
            name=str(name) if name is not None else None,
            vehicle_model=str(record["car_model"]),
            paint=str(paint) if paint is not None else None,
        )
    # AttributeError covers non-dict records (``record.get`` on a scalar) —
    # deliberate divergence from the HA source, which leaks that shape.
    except (AttributeError, KeyError, TypeError, ValueError) as err:
        raise AbrpApiError(f"malformed vehicle record: {err}") from err


def _str_or_none(value: Any) -> str | None:
    """Normalise an optional catalog string field.

    Non-strings collapse to ``None``. Strings are stripped at the parse
    boundary; the result is ``None`` if nothing remains, otherwise the
    trimmed token. Empty (``""``) and whitespace-only (``"   "``) inputs
    therefore both collapse to ``None``, and whitespace-padded inputs
    (``"  Rivian  "``) are normalised once at parse time rather than
    leaking padding into every downstream consumer.

    Load-bearing contract: any string that survives this helper must be
    presentable verbatim in a composed display string (consumers — e.g.
    a device-card model composer — use ``entry.manufacturer and
    entry.model`` as a truthy filter and interpolate the values
    directly). Single-site normalisation guarantees every such consumer
    sees the same trimmed shape, so display strings never carry upstream
    padding artefacts.
    """
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _int_or_none(value: Any) -> int | None:
    """Normalise an optional catalog int field.

    Accepts ``int`` strictly: ``bool`` is rejected up front because
    ``bool ⊂ int`` in Python (``isinstance(True, int) is True``), and
    silently coercing ``True → 1`` for ``startYear`` / ``endYear`` /
    ``batteryCapacityWh`` would surface a nonsense value downstream.
    Floats, strings-that-look-like-ints, and other types also collapse to
    ``None`` so upstream type drift fails loudly (the field reads as
    unknown) rather than masquerading as a real value.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _parse_catalog_entry(record: Mapping[str, Any]) -> CatalogEntry | None:
    """Parse one ``vehicles[i]`` wire record into a :class:`CatalogEntry`.

    Returns ``None`` when the record's ``typecode`` is missing, empty, or
    non-string — those entries can't participate in the typecode-keyed
    dict that :meth:`AbrpClient.async_get_catalog` builds. Per-field
    typing on the optional columns runs through :func:`_str_or_none` /
    :func:`_int_or_none` so empty strings, ``null``s, and wrong types all
    collapse cleanly to ``None``.
    """
    typecode = record.get("typecode")
    if not isinstance(typecode, str) or not typecode:
        return None
    return CatalogEntry(
        typecode=typecode,
        manufacturer=_str_or_none(record.get("manufacturer")),
        model=_str_or_none(record.get("model")),
        title=_str_or_none(record.get("title")),
        start_year=_int_or_none(record.get("startYear")),
        end_year=_int_or_none(record.get("endYear")),
        battery_capacity_wh=_int_or_none(record.get("batteryCapacityWh")),
    )
