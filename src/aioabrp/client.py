"""Request/response client for the ABRP v2 endpoints.

Wire surface:

* ``GET /2/abrp/me/vehicles?include=shared`` — the caller's garage: owned
  vehicles, then vehicles shared with the caller, each with the display
  metadata of its model (bare JSON array).
* ``GET /2/vehicle/_list`` — vehicle catalog (bare JSON, no envelope).
* ``GET /2/tlm/{vehicle_id}`` — one-shot telemetry snapshot (bare JSON),
  extracted into the library's typed event shape.
* ``GET /2/vehicle-model/by-typecode/{typecode}/display`` — per-typecode
  display metadata (bare JSON, no envelope).
* ``POST /2/app/tlm/{vehicle_id}`` — send one telemetry point (JSON
  ``InputPoint`` body; the response body is ignored).

All endpoints split auth across two headers: the static partner key
in ``X-API-KEY`` and the per-user session token in ``X-ABRP-SESSION``.

Failure containment: every method fetches the access token via the
injected :class:`~aioabrp.auth.AbstractAuth` BEFORE entering its
``(ClientError, TimeoutError)`` wrap band, so a terminal
:class:`~aioabrp.exceptions.AbrpAuthError` from the getter — or any
consumer-side refresh failure such as ``aiohttp.ClientResponseError`` —
propagates to the caller untouched instead of being laundered into
:class:`~aioabrp.exceptions.AbrpApiError`.
"""

import json
import re
from collections.abc import Mapping
from http import HTTPStatus
from typing import Any
from urllib.parse import quote

from aiohttp import ClientError, ClientResponse, ClientSession, ClientTimeout

# ``_clock`` is imported as a module (not ``from ._clock import _now``) so the
# clamp site reads ``_clock._now()`` via attribute access — tests monkeypatch
# ``aioabrp._clock._now`` and a name-bound import would capture the original
# lambda and defeat the patch. Do not "clean up" to a name-bound import.
from . import _clock
from ._clock import clamp_future_times
from ._extract import extract_metrics
from .auth import AbstractAuth
from .const import (
    API_BASE_V2,
    ENDPOINT_APP_TLM,
    ENDPOINT_MY_VEHICLES,
    ENDPOINT_TLM,
    ENDPOINT_VEHICLE_LIST,
    ENDPOINT_VEHICLE_MODEL_DISPLAY,
    HEADER_ABRP_SESSION,
    HEADER_API_KEY,
    ONE_SHOT_TIMEOUT_SECONDS,
)
from .exceptions import (
    AbrpApiError,
    AbrpAuthError,
    AbrpError,
    AbrpForbiddenError,
    AbrpScopeError,
)
from .models import (
    AbrpVehicle,
    CatalogEntry,
    Telemetry,
    TelemetryPoint,
    VehicleModelDisplay,
    VehicleRelation,
)

# The scope a ``WWW-Authenticate: Bearer error="insufficient_scope"`` names.
_SCOPE_RE = re.compile(r'scope="([^"]*)"')

_RELATIONS = {relation.name: relation for relation in VehicleRelation}


class AbrpClient:
    """Stateless async client for the ABRP v2 garage, catalog and telemetry APIs.

    ``api_key`` is the static Iternio partner key, sent in ``X-API-KEY``.
    The per-user access token is fetched fresh from ``auth`` on every call
    and sent in ``X-ABRP-SESSION``.

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
        self._unknown_driving_states_seen: set[str] = set()

    async def async_get_vehicles(self) -> list[AbrpVehicle]:
        """Return the caller's garage, shared vehicles included.

        One request returns every vehicle with the display metadata of its
        model, so no per-typecode display lookup is needed. Vehicles come
        in ABRP's order (currently owned first, then shared, each by id).

        Raises:
            AbrpAuthError: HTTP 401 (session rejected by ABRP), or,
                untouched, from the token getter.
            AbrpApiError: any other HTTP, transport, timeout, parse, or
                malformed-item failure.

        """
        payload = await self._get_v2(
            f"{API_BASE_V2}/{ENDPOINT_MY_VEHICLES}",
            what="garage",
            params={"include": "shared"},
        )
        if not isinstance(payload, list):
            raise AbrpApiError(
                f"unexpected garage payload shape: {type(payload).__name__}"
            )
        return [_parse_vehicle(item) for item in payload]

    async def async_get_catalog(self) -> dict[str, CatalogEntry]:
        """Fetch the v2 vehicle catalog from ``GET /2/vehicle/_list``.

        Returns a dict keyed by typecode for O(1) lookup. The catalog is
        ~850 KB (~1100 entries, no pagination) and stable enough that
        fetching once per consumer lifetime is appropriate.

        Auth: dual-header (``X-API-KEY`` partner key + ``X-ABRP-SESSION``
        access token), same shape as the telemetry endpoints and
        probe-confirmed on the catalog endpoint.

        Raises:
            AbrpAuthError: HTTP 401 (session rejected by ABRP), or,
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
        against stream events belongs to the consumer — but future block
        times ARE clamped to now (same as the stream), so a future-dated
        snapshot cannot escape the lib or poison a stream's gate when fed
        back as a seed.

        Raises:
            AbrpAuthError: HTTP 401 (session rejected by ABRP), or,
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
            unknown_driving_states_seen=self._unknown_driving_states_seen,
        )
        # No gating here (stateless one-shot), but future block times are
        # clamped to now — same as the stream — so a future-dated snapshot
        # cannot escape the lib and is safe to feed back as a stream seed.
        extracted = clamp_future_times(extracted, _clock._now())
        return Telemetry(**{metric.value: value for metric, value in extracted.items()})

    async def async_get_vehicle_model_display(
        self, typecode: str
    ) -> VehicleModelDisplay:
        """Fetch display metadata for one typecode.

        ``GET /2/vehicle-model/by-typecode/{typecode}/display`` returns the
        bare ``VehicleModelDisplay`` JSON (no envelope): manufacturer, model,
        the raw model-year string, the server-parsed start/end years (when
        derivable), and the display title. ``typecode`` is percent-encoded
        into the path segment so a structural character cannot escape it.

        Raises:
            AbrpAuthError: HTTP 401 (session rejected by ABRP), or,
                untouched, from the token getter.
            AbrpApiError: any other 4xx/5xx HTTP (including 404 for an unknown
                typecode — there is no None/not-found sentinel; catch this to
                degrade), transport, timeout, parse, or malformed-record
                failure.

        """
        payload = await self._get_v2_json(
            f"{API_BASE_V2}/{ENDPOINT_VEHICLE_MODEL_DISPLAY}"
            f"/{quote(typecode, safe='')}/display",
            what="vehicle-model display",
        )
        return _parse_vehicle_model_display(payload)

    async def async_send_telemetry(
        self, vehicle_id: int, point: TelemetryPoint
    ) -> None:
        """Send one telemetry point (``POST /2/app/tlm/{vehicle_id}``).

        The token must be allowed to write telemetry. Any 2xx is success.

        Raises:
            TypeError: ``vehicle_id`` is not an int, or ``extra`` is not
                JSON-serializable (raised before any request).
            ValueError: ``extra`` holds a non-finite float.
            AbrpScopeError: HTTP 403 ``insufficient_scope``; the token lacks
                the scope in ``.scope`` (``tlm:write``) and needs re-authorizing.
            AbrpAuthError: HTTP 401 (session rejected), or from the token getter.
            AbrpForbiddenError: any other HTTP 403, e.g. a vehicle neither owned
                nor shared; an AbrpApiError, so reauth is not the fix.
            AbrpApiError: any other non-2xx, transport or timeout failure,
                with ABRP's error text when it sent one.

        """
        if isinstance(vehicle_id, bool) or not isinstance(vehicle_id, int):
            raise TypeError("vehicle_id must be an int")
        body = json.dumps(point._to_wire(), allow_nan=False)
        await self._post_v2(f"{API_BASE_V2}/{ENDPOINT_APP_TLM}/{vehicle_id}", body)

    async def _get_v2_json(self, url: str, *, what: str) -> dict[str, Any]:
        """GET a v2 endpoint whose payload must be a JSON object."""
        payload = await self._get_v2(url, what=what)
        if not isinstance(payload, dict):
            raise AbrpApiError(
                f"unexpected {what} payload shape: {type(payload).__name__}"
            )
        return payload

    async def _get_v2(
        self, url: str, *, what: str, params: Mapping[str, str] | None = None
    ) -> Any:
        """GET a v2 endpoint and return its decoded JSON payload.

        Shared request ladder for the dual-header v2 endpoints: non-2xx
        statuses map as in :func:`_http_error`; transport, timeout, or
        parse failure raises :class:`AbrpApiError` (``what``
        names the endpoint in messages). The token fetch stays OUTSIDE the
        ``(ClientError, TimeoutError)`` band so getter exceptions propagate
        to the caller untouched.
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
                params=params,
                timeout=ClientTimeout(total=ONE_SHOT_TIMEOUT_SECONDS),
            ) as response:
                if response.status >= HTTPStatus.BAD_REQUEST:
                    raise _http_error(response, f"HTTP {response.status}")
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
        return payload

    async def _post_v2(self, url: str, body: str) -> None:
        """POST a JSON body to a v2 endpoint; any 2xx is success.

        Redirects are not followed: a followed POST becomes a GET whose 200
        would look like a successful write.
        """
        token = await self._auth.async_get_access_token()
        headers = {
            "Content-Type": "application/json",
            HEADER_API_KEY: self._api_key,
            HEADER_ABRP_SESSION: token,
        }
        try:
            async with self._websession.post(
                url,
                data=body,
                headers=headers,
                allow_redirects=False,
                timeout=ClientTimeout(total=ONE_SHOT_TIMEOUT_SECONDS),
            ) as response:
                if HTTPStatus.OK <= response.status < HTTPStatus.MULTIPLE_CHOICES:
                    return
                raw = await response.read()
        except (ClientError, TimeoutError) as err:
            raise AbrpApiError(str(err)) from err
        detail = _error_text(raw, (token, self._api_key))
        message = f"HTTP {response.status}" + (f": {detail}" if detail else "")
        raise _http_error(response, message, body=detail)


def _http_error(response: ClientResponse, message: str, *, body: str = "") -> AbrpError:
    """Map a non-2xx response to its error.

    401 is a rejected session; 403 ``insufficient_scope`` is AbrpScopeError.
    Any other 403 comes with a valid session (ABRP answers a bad one with
    401), so it is AbrpForbiddenError, which reauth would not fix.
    """
    if response.status == HTTPStatus.UNAUTHORIZED:
        return AbrpAuthError(message)
    if response.status != HTTPStatus.FORBIDDEN:
        return AbrpApiError(message)
    challenge = response.headers.get("WWW-Authenticate", "")
    if "insufficient_scope" not in challenge and "insufficient_scope" not in body:
        return AbrpForbiddenError(message)
    scope = _SCOPE_RE.search(challenge)
    return AbrpScopeError(message, scope.group(1) if scope else None)


def _error_text(raw: bytes, secrets: tuple[str, ...]) -> str:
    """Return ABRP's error message as one short line, secrets redacted.

    ABRP sends ``{"message": "..."}``; other bodies are used as text.
    """
    text = raw.decode("utf-8", "replace")
    try:
        payload = json.loads(text)
    except ValueError:
        payload = None
    if isinstance(payload, dict) and isinstance(payload.get("message"), str):
        text = payload["message"]
    text = " ".join(text.split())
    for secret in secrets:
        if secret:
            text = text.replace(secret, "<redacted>")
    return text[:200]


def _parse_vehicle(item: Any) -> AbrpVehicle:
    """Parse one ``UserVehicleListItem`` into an :class:`AbrpVehicle`.

    ``id`` (an integer) and ``typecode`` (a non-empty string) are required:
    a missing or malformed one raises :class:`AbrpApiError`. ``name`` maps
    ``""`` (unset) and non-strings to ``None``. The
    optional enrichment never fails the garage: an unknown ``relation`` or
    an absent or malformed ``model`` becomes ``None``. Extra model fields
    (``isCustom``, ``baseTypecode``) are ignored; ``display`` is what ABRP
    shows either way.
    """
    try:
        vehicle_id = item["id"]
        if isinstance(vehicle_id, bool) or not isinstance(vehicle_id, int):
            raise TypeError(f"id must be an integer, got {vehicle_id!r}")
        typecode = item["typecode"]
        if not isinstance(typecode, str) or not typecode:
            raise TypeError(f"typecode must be a non-empty string, got {typecode!r}")
        name = item.get("name")
        relation = item.get("relation")
        model = item.get("model")
    # AttributeError covers non-dict items (``item.get`` on a scalar).
    except (AttributeError, KeyError, TypeError, ValueError) as err:
        raise AbrpApiError(f"malformed vehicle item: {err}") from err
    display: VehicleModelDisplay | None = None
    if isinstance(model, Mapping):
        try:
            display = _parse_vehicle_model_display(model)
        except AbrpApiError:
            display = None
    return AbrpVehicle(
        vehicle_id=vehicle_id,
        name=_str_or_none(name),
        vehicle_model=typecode,
        display=display,
        relation=_RELATIONS.get(relation) if isinstance(relation, str) else None,
    )


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


def _require_str(record: Mapping[str, Any], key: str) -> str:
    """Read a wire-required string field, raising on absence/wrong type.

    Unlike :func:`_str_or_none` (which collapses bad input to ``None`` for the
    catalog's optional columns), the display schema marks these fields
    required, so type drift fails loudly as :class:`AbrpApiError` rather than
    masquerading as a missing value.
    """
    value = record.get(key)
    if not isinstance(value, str):
        raise AbrpApiError(
            f"malformed vehicle-model display record: "
            f"'{key}' must be a string, got {type(value).__name__}"
        )
    return value


def _parse_vehicle_model_display(record: Mapping[str, Any]) -> VehicleModelDisplay:
    """Parse the bare display payload into a :class:`VehicleModelDisplay`.

    The four required strings run through :func:`_require_str` (loud failure
    on absence/wrong type); ``start_year`` / ``end_year`` run through
    :func:`_int_or_none`, so the server omitting them for open-ended or
    non-numeric ``years`` cleanly surfaces as ``None``.
    """
    return VehicleModelDisplay(
        manufacturer=_require_str(record, "manufacturer"),
        model=_require_str(record, "model"),
        years=_require_str(record, "years"),
        title=_require_str(record, "title"),
        start_year=_int_or_none(record.get("startYear")),
        end_year=_int_or_none(record.get("endYear")),
    )
