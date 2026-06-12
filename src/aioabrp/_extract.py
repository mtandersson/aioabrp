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
* odometer / range — native meters: a canonical, unit-flip-safe scale;
  km rendering is display policy, not extraction policy.
"""

import logging
import math
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from .models import ChargingState, Location, Metric, MetricValue

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
}

# Metrics whose wire leaf is a 0.0-1.0 fraction surfaced x100 (soh
# deliberately unclamped — see the module docstring).
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


def _float_leaf(block: Mapping[str, Any], leaf_key: str) -> float | None:
    """Return the numeric leaf ``block[leaf_key]`` as a float, or ``None``.

    Shared numeric tolerance matrix: missing leaf, ``null`` leaf,
    non-numeric leaf, bool (a subclass of ``int`` in Python — the
    explicit check matters), and non-finite values (``json.loads``
    accepts the non-standard ``NaN``/``Infinity`` tokens; neither is a
    usable metric value) all map to ``None``. Never raises. Accepted
    leaves are coerced so :class:`MetricValue.value` is always a runtime
    ``float`` regardless of wire int/float spelling.
    """
    value = block.get(leaf_key)
    if (
        not isinstance(value, int | float)
        or isinstance(value, bool)
        or not math.isfinite(value)
    ):
        return None
    return float(value)


def _charging_state(
    block: Mapping[str, Any],
    unknown_charging_states_seen: set[str],
    log_name: str | None,
) -> ChargingState | None:
    """Map the categorical ``chargingState`` block to a :class:`ChargingState`.

    Tolerates every degenerate leaf shape (missing / null / non-string
    ``state``) by returning ``None`` — consistent with the
    absent/malformed -> omitted contract the numeric extractors share
    (non-dict blocks are already rejected by :func:`extract_metrics`).
    An unrecognized non-empty member also maps to ``None`` (the enum is
    closed; a raw string must never leak into the typed event) and logs
    a WARNING once per ``unknown_charging_states_seen`` set so upstream
    enum drift leaves a runtime breadcrumb. The dedup set is
    caller-owned — one per client/stream instance, never module-global —
    so warning state cannot leak across accounts.
    """
    state = block.get("state")
    if not isinstance(state, str):
        return None
    member = _CHARGING_STATES.get(state)
    if member is None and state and state not in unknown_charging_states_seen:
        unknown_charging_states_seen.add(state)
        _LOGGER.warning(
            "%sUnrecognized ABRP chargingState %r; the charging_state metric "
            "will be omitted for this state until aioabrp adds it",
            f"{log_name}: " if log_name else "",
            state,
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


def extract_metrics(
    frame: Mapping[str, Any],
    *,
    unknown_charging_states_seen: set[str],
    log_name: str | None = None,
) -> dict[Metric, MetricValue[Any]]:
    """Extract every present, well-formed metric from one wire frame.

    Iterates :data:`WIRE_KEYS`; a metric appears in the result only when
    its wire block is a dict and its value survives the tolerance matrix
    (module docstring). Each emitted :class:`MetricValue` carries the
    block's tz-aware ``time`` (or ``None`` — see
    :func:`parse_block_time`) and clean ``provider`` (or ``None`` — see
    :func:`is_clean_provider_str`).

    ``unknown_charging_states_seen`` is the caller-owned dedup set for
    the unrecognized-chargingState warning: one warning per state per
    set. Keep one set per client/stream instance so multi-account
    consumers do not share dedup state. ``log_name`` prefixes that
    warning when given; the warning contains nothing from the frame but
    the state string itself.
    """
    result: dict[Metric, MetricValue[Any]] = {}
    for metric, wire_key in WIRE_KEYS.items():
        block = frame.get(wire_key)
        if not isinstance(block, dict):
            # Missing key / null block / non-dict block: metric absent.
            continue
        value: float | ChargingState | Location | None
        if metric is Metric.CHARGING_STATE:
            value = _charging_state(block, unknown_charging_states_seen, log_name)
        elif metric is Metric.LOCATION:
            value = _location(block)
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
