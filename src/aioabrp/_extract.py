"""Wire frame -> typed metric extraction. Internal module.

Converts one telemetry wire frame (a one-shot ``GET /2/tlm/{id}`` payload
or an SSE frame body — see :mod:`aioabrp._wire_types`) into the library's
typed event shape ``dict[Metric, MetricValue]`` (internal; the public
``on_update`` / ``async_get_current_telemetry`` boundary packs this into a
:class:`~aioabrp.models.Telemetry`).

Tolerance matrix shared by every metric: the extractors tolerate every
shape the server might emit for an unavailable metric — missing key,
``null`` block, empty dict, ``null`` leaf, non-numeric leaf, and
bool-as-number (bool is a subclass of ``int`` in Python) — by omitting
the metric. Extraction never raises on frame content. Display rounding
is the consumer's concern.

Per-metric load-bearing notes (ported with the extractors):

* soc / soh — ``frac`` arrives as a 0.0-1.0 fraction; surfaced x100 on
  the familiar 0-100 % scale. soh is intentionally NOT clamped at 100 —
  a post-recalibration overshoot (``frac > 1.0``) is meaningful drift
  downstream statistics are meant to capture, so flattening it would
  lose signal.
* battery_temperature — °C with NO lower-bound filter: winter operation
  (sub-zero pack temps) is a real wire shape, not a degenerate one.
  Distinct from any cabin or external temperature ABRP might surface in
  future fields.
* odometer / range / elevation — native meters: a canonical,
  unit-flip-safe scale; km rendering is display policy, not extraction
  policy.
* speed / calibrated_max_speed — native m/s; speed_factor is a raw
  dimensionless multiplier (leaf ``frac`` but NOT surfaced x100); heading
  is degrees; current is amps; charging_energy_added / hvac_power keep
  Wh / W. None of these are derived or converted (1:1 wire mirror).
* calibrated_confidence — a 1- or 4-element float array surfaced as a
  tuple, all-or-nothing on any bad element. map_info — a struct with
  independently-optional subfields (raw m/s speed limit); region drift
  degrades that subfield to ``None`` without dropping the block.
"""

import logging
import math
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from .models import (
    ChargingState,
    DrivingState,
    Location,
    MapInfo,
    Metric,
    MetricValue,
    Region,
)

_LOGGER = logging.getLogger(__name__)

# Wire key for each metric. Most metrics are named identically on the
# wire, but a handful of fields camelCase on the wire while the Metric
# enum stays snake_case. Pinned by tests/test_extract.py against the
# keep-set in _wire_types.py so the column cannot drift from the wire.
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
    Metric.CABIN_SET_POINT: "cabinSetPoint",
    Metric.CABIN_TEMPERATURE: "cabinTemperature",
    Metric.CALIBRATED_MAX_SPEED: "calibratedMaxSpeed",
    Metric.CHARGING_ENERGY_ADDED: "chargingEnergyAdded",
    Metric.CURRENT: "current",
    Metric.DRIVING_STATE: "drivingState",
    Metric.ELEVATION: "elevation",
    Metric.EXTERNAL_TEMPERATURE: "externalTemperature",
    Metric.HEADING: "heading",
    Metric.HVAC_POWER: "hvacPower",
    Metric.MAP_INFO: "mapInfo",
    Metric.SPEED: "speed",
    Metric.SPEED_FACTOR: "speedFactor",
    Metric.CALIBRATED_CONFIDENCE: "calibratedConfidence",
}

# Numeric leaf key under each metric's wire block (units in the module
# docstring: percent-after-x100, W, V, Wh, m, Wh/km, °C).
_NUMERIC_LEAF_KEYS: dict[Metric, str] = {
    Metric.SOC: "frac",
    Metric.POWER: "w",
    Metric.VOLTAGE: "v",
    Metric.SOE: "wh",
    Metric.ODOMETER: "m",
    Metric.CALIBRATED_REF_CONS: "wh_per_km",
    Metric.BATTERY_CAPACITY: "wh",
    Metric.SOH: "frac",
    Metric.RANGE: "m",
    Metric.BATTERY_TEMPERATURE: "c",
    Metric.CABIN_SET_POINT: "c",
    Metric.CABIN_TEMPERATURE: "c",
    Metric.EXTERNAL_TEMPERATURE: "c",
    Metric.CALIBRATED_MAX_SPEED: "ms",
    Metric.SPEED: "ms",
    Metric.CHARGING_ENERGY_ADDED: "wh",
    Metric.CURRENT: "a",
    Metric.ELEVATION: "m",
    Metric.HEADING: "degrees",
    Metric.HVAC_POWER: "w",
    # speed_factor is a dimensionless 0-2 multiplier; its wire leaf is
    # ``frac`` but it is deliberately NOT in _FRACTION_METRICS — it must
    # pass through unscaled, never surfaced x100.
    Metric.SPEED_FACTOR: "frac",
}

# Metrics whose wire leaf is a 0.0-1.0 fraction surfaced x100 (soh
# deliberately unclamped — see the module docstring). speed_factor and
# calibrated_confidence also carry a ``frac`` leaf but are intentionally
# absent here: they are raw multipliers/confidences, not percentages.
_FRACTION_METRICS = frozenset({Metric.SOC, Metric.SOH})

# Wire enum member -> ChargingState for the categorical ``chargingState``
# field. Closed-enum: every member ABRP's v2 spec emits has an entry
# (mirrors the ``ChargingStateValue`` Literal in _wire_types.py). An
# unrecognized/future member maps to None — the metric is omitted, never
# surfaced as a raw string (see :func:`_charging_state`).
_CHARGING_STATES: dict[str, ChargingState] = {
    "CHARGING_AC": ChargingState.CHARGING_AC,
    "CHARGING_DC": ChargingState.CHARGING_DC,
    "CHARGING_UNKNOWN": ChargingState.CHARGING_UNKNOWN,
    "NOT_CHARGING": ChargingState.NOT_CHARGING,
    "PLUGGED_IN": ChargingState.PLUGGED_IN,
}

# Wire enum member -> DrivingState for the categorical ``drivingState``
# field. Closed-enum, same contract as ``_CHARGING_STATES``: an
# unrecognized/future member maps to None (metric omitted + one warning
# per caller-owned dedup set — see :func:`_driving_state`).
_DRIVING_STATES: dict[str, DrivingState] = {
    "PARK": DrivingState.PARK,
    "REVERSE": DrivingState.REVERSE,
    "NEUTRAL": DrivingState.NEUTRAL,
    "DRIVE": DrivingState.DRIVE,
}

# Wire enum member -> Region for the ``mapInfo.region`` subfield. Unlike
# the top-level categorical metrics, an unrecognized region degrades to
# ``None`` on the MapInfo struct (the rest of the block is preserved) and
# does NOT warn — region is one optional subfield, not the whole metric.
_REGIONS: dict[str, Region] = {
    "AFRICA": Region.AFRICA,
    "ASIA": Region.ASIA,
    "AUSTRALIA": Region.AUSTRALIA,
    "CENTRAL_AMERICA": Region.CENTRAL_AMERICA,
    "EUROPE": Region.EUROPE,
    "NORTH_AMERICA": Region.NORTH_AMERICA,
    "SOUTH_AMERICA": Region.SOUTH_AMERICA,
    "OCEANIA": Region.OCEANIA,
}


def parse_block_time(block: Mapping[str, Any]) -> datetime | None:
    """Return the block's ``time`` as a tz-aware datetime, or ``None``.

    Returns ``None`` whenever:

    * the block has no ``time`` key — defense; every keep-set block
      carries one in production but absence MUST NOT crash extraction;
    * ``time`` is not a string (e.g. an opaque ``int`` marker);
    * the string is malformed, or structurally well-formed but invalid
      (e.g. ``2026-13-01T00:00:00Z``, ``2026-02-30T...``) —
      :meth:`datetime.fromisoformat` raises :class:`ValueError` for
      both shapes; caught here to keep call sites branch-free;
    * the parsed datetime is naive (no tz suffix in the string) — naive
      vs. aware comparison would raise :class:`TypeError` at the
      monotonicity-gate comparison site. ABRP's wire format always
      carries a ``Z`` suffix per the captured rollup sample
      (2026-05-25), so rejecting naive strings filters wire-shape
      regressions only.

    Defensive return-``None`` (rather than fail-loud) is load-bearing:
    callers rely on ``None`` to bypass the monotonicity gate and fall
    through to "adopt incoming" — today's contract.
    """
    raw = block.get("time")
    if not isinstance(raw, str):
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed


def is_clean_provider_str(value: object) -> bool:
    r"""Return True iff ``value`` is a non-empty, unpadded string.

    Single REJECT-ONLY contract for the provider-rejection guard, shared
    by :func:`extract_metrics`'s per-block provider gate and any
    consumer-side restore/persistence guard that needs the symmetric
    semantics. An upstream that pads its enum strings is a wire-shape
    regression we want loud, not silently normalised — so the guard
    rejects padding rather than stripping.

    **ASCII-whitespace contract.** The
    ``value == value.strip()`` check uses :meth:`str.strip` with no
    argument, which only strips characters for which
    ``str.isspace()`` returns True. Several Unicode characters
    commonly used as padding — ``U+200B`` (ZWS), ``U+200C`` (ZWNJ),
    ``U+200D`` (ZWJ), ``U+FEFF`` (BOM) — return False from
    ``isspace()`` and therefore survive both this guard and
    ``.strip()``: a ``"\u200bDERIVED"`` value would slip through as
    "clean". That gap is intentional. ABRP's ``Provider`` enum is
    closed and ASCII-only (see the spec at
    https://api.iternio.com/swagger-ui/spec/prod/IternioPlanning.out.yaml);
    a Unicode-whitespace-padded provider value would be an upstream
    regression we want surfaced as a downstream mismatch / loud
    failure of the matching ``Provider`` literal, not silently
    sanitised at the boundary. ``NBSP`` (``U+00A0``) and other
    in-``isspace`` Unicode whitespace at edges behave differently:
    ``.strip()`` removes them, so ``value != value.strip()`` and the
    guard REJECTS them. That asymmetry vs. ZWS-family codepoints is
    also acceptable given the closed-ASCII contract — both shapes
    (slip-through-then-mismatch-downstream for ZWS-family, loud-
    rejection-at-boundary for NBSP-family) surface upstream regressions.
    """
    return isinstance(value, str) and bool(value) and value == value.strip()


def _clean_provider(block: Mapping[str, Any]) -> str | None:
    """Return the block's upstream provider string, or ``None``.

    Symmetric-reject boundary: every non-string AND the empty string AND
    any whitespace-only or leading-/trailing-padded string map to
    ``None`` via :func:`is_clean_provider_str`. Per-metric ``provider``
    is a ``NotRequired`` claim on each ``WithTimeAndProvider`` block
    (see :mod:`aioabrp._wire_types`); absent / null / non-string /
    empty / whitespace-padded are all treated as "no usable provider on
    this block". Consumers that stamp providers may keep their prior
    value on ``None`` (sticky-on-omission — providers don't flip
    mid-stream in normal operation).
    """
    provider = block.get("provider")
    if is_clean_provider_str(provider):
        return provider
    return None


def _coerce_finite_number(value: object) -> float | None:
    """Coerce a wire scalar to a finite ``float``, or ``None``.

    The single numeric tolerance matrix shared by every numeric path
    (leaf extraction and the ``calibratedConfidence`` array): non-numeric,
    bool (a subclass of ``int`` in Python — the explicit check matters),
    and non-finite values (``json.loads`` accepts the non-standard
    ``NaN``/``Infinity`` tokens; neither is a usable metric value) all map
    to ``None``. Never raises. Accepted values are coerced so the result
    is always a runtime ``float`` regardless of wire int/float spelling.
    """
    if (
        not isinstance(value, int | float)
        or isinstance(value, bool)
        or not math.isfinite(value)
    ):
        return None
    return float(value)


def _float_leaf(block: Mapping[str, Any], leaf_key: str) -> float | None:
    """Return the numeric leaf ``block[leaf_key]`` as a float, or ``None``.

    Thin wrapper over :func:`_coerce_finite_number` for the (missing leaf
    -> ``None``) lookup case; the tolerance matrix lives in one place.
    """
    return _coerce_finite_number(block.get(leaf_key))


_CATEGORICAL_OMITTED_WARNING = (
    "%sUnrecognized ABRP %s %r; the %s metric will be omitted for this "
    "value until aioabrp adds it"
)


def _enum_state[E](
    block: Mapping[str, Any],
    mapping: dict[str, E],
    unknown_seen: set[str],
    log_name: str | None,
    *,
    wire_field: str,
    metric_name: str,
) -> E | None:
    """Map a closed categorical ``{state: <member>}`` block to a member enum.

    Backs both ``chargingState`` and ``drivingState`` (one mapping +
    dedup set each — the sets are caller-owned, one per client/stream
    instance and never shared between the two enums, so drift in one
    categorical metric can neither leak across accounts nor suppress the
    other metric's warning). Tolerates every degenerate leaf shape
    (missing / null / non-string ``state``) by returning ``None`` —
    consistent with the absent/malformed -> omitted contract the numeric
    extractors share (non-dict blocks are already rejected by
    :func:`extract_metrics`). An unrecognized non-empty member also maps
    to ``None`` (the enum is closed; a raw string must never leak into
    the typed event) and logs a WARNING once per ``unknown_seen`` set so
    upstream enum drift leaves a runtime breadcrumb. The warning carries
    only the member token (and the optional ``log_name``), never any
    other frame content.
    """
    state = block.get("state")
    if not isinstance(state, str):
        return None
    member = mapping.get(state)
    if member is None and state and state not in unknown_seen:
        unknown_seen.add(state)
        _LOGGER.warning(
            _CATEGORICAL_OMITTED_WARNING,
            f"{log_name}: " if log_name else "",
            wire_field,
            state,
            metric_name,
        )
    return member


def _location(block: Mapping[str, Any]) -> Location | None:
    """Extract a GPS coordinate pair, or ``None`` when unavailable.

    The wire spells longitude ``long`` (spec ``Location`` block — see
    :mod:`aioabrp._wire_types`); the model field is ``lon``. Tolerance
    mirrors the numeric matrix: either leaf missing / null / non-numeric
    / bool omits the whole metric (a half-coordinate is meaningless).
    """
    lat = _float_leaf(block, "lat")
    long = _float_leaf(block, "long")
    if lat is None or long is None:
        return None
    return Location(lat=lat, lon=long)


def _calibrated_confidence(block: Mapping[str, Any]) -> tuple[float, ...] | None:
    """Extract the ``calibratedConfidence`` array as a tuple of floats, or ``None``.

    The wire leaf ``frac`` is a list of numbers (1 or 4 in practice).
    All-or-nothing, consistent with :func:`_location`: a missing / null /
    non-list / empty leaf, or ANY element that fails the shared numeric
    tolerance matrix (:func:`_coerce_finite_number` — null, non-numeric,
    bool, non-finite), omits the whole metric: a partially-decoded
    confidence vector is meaningless. Surviving elements are coerced so
    the tuple is always runtime ``float``s. The library does not interpret,
    range-clamp, or length-check the values (consumer policy), matching
    the no-derived-logic contract.
    """
    frac = block.get("frac")
    if not isinstance(frac, list) or not frac:
        return None
    values: list[float] = []
    for item in frac:
        number = _coerce_finite_number(item)
        if number is None:
            return None
        values.append(number)
    return tuple(values)


def _map_info(block: Mapping[str, Any]) -> MapInfo | None:
    """Extract the ``mapInfo`` struct, or ``None`` when it carries no data.

    Unlike every other metric, ``mapInfo`` has no single required leaf;
    each subfield is independently optional and tolerated per-field:

    * ``region`` — a closed enum; an unrecognized / non-string value
      degrades to ``None`` while the rest of the struct survives (region
      is one subfield, not the whole metric, so it is NOT dropped/warned
      like the top-level categorical metrics);
    * ``country_3`` / ``address`` — kept iff a string, else ``None``;
    * ``speedLimitMs`` — the shared numeric tolerance matrix (raw m/s);
    * ``isFreeSpeedZone`` — kept iff a genuine ``bool``, else ``None``.

    When the block is a dict but EVERY subfield is absent / malformed the
    metric is omitted (return ``None``) — same "presence = data present"
    contract the other extractors hold, so a content-free ``mapInfo``
    block never surfaces an all-``None`` struct.
    """
    raw_region = block.get("region")
    region = _REGIONS.get(raw_region) if isinstance(raw_region, str) else None

    raw_country = block.get("country_3")
    country_3 = raw_country if isinstance(raw_country, str) else None

    raw_address = block.get("address")
    address = raw_address if isinstance(raw_address, str) else None

    speed_limit_ms = _float_leaf(block, "speedLimitMs")

    raw_zone = block.get("isFreeSpeedZone")
    is_free_speed_zone = raw_zone if isinstance(raw_zone, bool) else None

    if (
        region is None
        and country_3 is None
        and address is None
        and speed_limit_ms is None
        and is_free_speed_zone is None
    ):
        return None
    return MapInfo(
        region=region,
        country_3=country_3,
        address=address,
        speed_limit_ms=speed_limit_ms,
        is_free_speed_zone=is_free_speed_zone,
    )


def extract_metrics(
    frame: Mapping[str, Any],
    *,
    unknown_charging_states_seen: set[str],
    unknown_driving_states_seen: set[str],
    log_name: str | None = None,
) -> dict[Metric, MetricValue[Any]]:
    """Extract every present, well-formed metric from one wire frame.

    Iterates :data:`WIRE_KEYS`; a metric appears in the result only when
    its wire block is a dict and its value survives the tolerance matrix
    (module docstring). Each emitted :class:`MetricValue` carries the
    block's tz-aware ``time`` (or ``None`` — see
    :func:`parse_block_time`) and clean ``provider`` (or ``None`` — see
    :func:`is_clean_provider_str`).

    ``unknown_charging_states_seen`` / ``unknown_driving_states_seen`` are
    the caller-owned dedup sets for the unrecognized-``chargingState`` /
    ``drivingState`` warnings: one warning per state per set, and the two
    enums use SEPARATE sets so drift in one never suppresses the other.
    Keep one pair of sets per client/stream instance so multi-account
    consumers do not share dedup state. ``log_name`` prefixes those
    warnings when given; a warning contains nothing from the frame but
    the unrecognized enum-member string itself (never the map address or
    any other payload content).
    """
    result: dict[Metric, MetricValue[Any]] = {}
    for metric, wire_key in WIRE_KEYS.items():
        block = frame.get(wire_key)
        if not isinstance(block, dict):
            # Missing key / null block / non-dict block: metric absent.
            continue
        value: (
            float
            | ChargingState
            | DrivingState
            | Location
            | MapInfo
            | tuple[float, ...]
            | None
        )
        if metric is Metric.CHARGING_STATE:
            value = _enum_state(
                block,
                _CHARGING_STATES,
                unknown_charging_states_seen,
                log_name,
                wire_field="chargingState",
                metric_name="charging_state",
            )
        elif metric is Metric.DRIVING_STATE:
            value = _enum_state(
                block,
                _DRIVING_STATES,
                unknown_driving_states_seen,
                log_name,
                wire_field="drivingState",
                metric_name="driving_state",
            )
        elif metric is Metric.LOCATION:
            value = _location(block)
        elif metric is Metric.MAP_INFO:
            value = _map_info(block)
        elif metric is Metric.CALIBRATED_CONFIDENCE:
            value = _calibrated_confidence(block)
        else:
            value = _float_leaf(block, _NUMERIC_LEAF_KEYS[metric])
            if value is not None and metric in _FRACTION_METRICS:
                value = value * 100
        if value is None:
            continue
        result[metric] = MetricValue(
            value=value,
            time=parse_block_time(block),
            provider=_clean_provider(block),
        )
    return result
