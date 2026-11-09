"""Public typed models for aioabrp."""

import copy
import math
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import Enum, StrEnum
from typing import Any


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
    CABIN_SET_POINT = "cabin_set_point"
    CABIN_TEMPERATURE = "cabin_temperature"
    CALIBRATED_MAX_SPEED = "calibrated_max_speed"
    CHARGING_ENERGY_ADDED = "charging_energy_added"
    CURRENT = "current"
    DRIVING_STATE = "driving_state"
    ELEVATION = "elevation"
    EXTERNAL_TEMPERATURE = "external_temperature"
    HEADING = "heading"
    HVAC_POWER = "hvac_power"
    MAP_INFO = "map_info"
    SPEED = "speed"
    SPEED_FACTOR = "speed_factor"
    CALIBRATED_CONFIDENCE = "calibrated_confidence"
    BATTERY_TEMPERATURE_MIN = "battery_temperature_min"
    BATTERY_TEMPERATURE_MAX = "battery_temperature_max"


class ChargingState(StrEnum):
    """Categorical charging state (closed enum mirroring the wire members)."""

    CHARGING_AC = "charging_ac"
    CHARGING_DC = "charging_dc"
    CHARGING_UNKNOWN = "charging_unknown"
    NOT_CHARGING = "not_charging"
    PLUGGED_IN = "plugged_in"


class DrivingState(StrEnum):
    """Categorical driving state / current gear (closed enum mirroring the wire)."""

    PARK = "park"
    REVERSE = "reverse"
    NEUTRAL = "neutral"
    DRIVE = "drive"


class VehicleRelation(StrEnum):
    """How the caller relates to a garage vehicle.

    Member names mirror the wire values (``OWNED``, ``SHARED``); the values
    are lowercase like the other enums, so ``VehicleRelation["OWNED"]``
    works and ``VehicleRelation("OWNED")`` does not.
    """

    OWNED = "owned"
    SHARED = "shared"


class Region(StrEnum):
    """Coarse world region of the vehicle position (closed enum, mirrors wire)."""

    AFRICA = "africa"
    ASIA = "asia"
    AUSTRALIA = "australia"
    CENTRAL_AMERICA = "central_america"
    EUROPE = "europe"
    NORTH_AMERICA = "north_america"
    SOUTH_AMERICA = "south_america"
    OCEANIA = "oceania"


@dataclass(frozen=True, slots=True)
class Location:
    """A GPS coordinate pair."""

    lat: float
    lon: float


@dataclass(frozen=True, slots=True)
class MapInfo:
    """Additional map-related context for the vehicle position.

    Every subfield is independently optional: the wire ``mapInfo`` block
    carries no required leaf, so any subfield absent / malformed on the
    wire surfaces as ``None`` while the rest of the block is preserved.
    ``speed_limit_ms`` keeps the wire's raw m/s unit (no conversion).
    """

    region: Region | None
    country_3: str | None
    address: str | None
    speed_limit_ms: float | None
    is_free_speed_zone: bool | None


@dataclass(frozen=True, slots=True)
class MetricValue[T]:
    """One extracted metric value.

    Generic over the value type ``T``: ``float`` for numeric metrics,
    ``ChargingState`` / ``DrivingState`` for the categorical metrics,
    ``Location`` for the GPS pair, ``MapInfo`` for the map-context struct,
    and ``tuple[float, ...]`` for ``calibrated_confidence`` (a 1- or
    4-element array). Units keep the raw ABRP wire scale (no conversion):
    percent (soc/soh - frac surfaced x100), W (power/hvac_power), V, A
    (current), Wh (soe/battery_capacity/charging_energy_added), m
    (odometer/range/elevation), m/s (speed/calibrated_max_speed),
    degrees (heading), °C (battery incl. min/max cell, cabin and external
    temperatures), Wh/km
    (calibrated_ref_cons), and dimensionless fractions (speed_factor,
    calibrated_confidence). ``time`` is the wire block's tz-aware
    timestamp or ``None``; ``provider`` is the clean upstream provider
    string or ``None``.
    """

    value: T
    time: datetime | None
    provider: str | None


@dataclass(frozen=True, slots=True)
class Telemetry:
    """A typed telemetry frame: one optional ``MetricValue`` per metric.

    Mirrors the wire's ``OutputPoint`` (a struct of named optional blocks).
    A field is ``None`` when the metric was absent from the frame; a stream
    frame is a sparse delta, so most fields are ``None`` on any given update.
    """

    soc: MetricValue[float] | None = None
    power: MetricValue[float] | None = None
    voltage: MetricValue[float] | None = None
    soe: MetricValue[float] | None = None
    odometer: MetricValue[float] | None = None
    calibrated_ref_cons: MetricValue[float] | None = None
    battery_capacity: MetricValue[float] | None = None
    soh: MetricValue[float] | None = None
    range: MetricValue[float] | None = None
    battery_temperature: MetricValue[float] | None = None
    charging_state: MetricValue[ChargingState] | None = None
    location: MetricValue[Location] | None = None
    cabin_set_point: MetricValue[float] | None = None
    cabin_temperature: MetricValue[float] | None = None
    calibrated_max_speed: MetricValue[float] | None = None
    charging_energy_added: MetricValue[float] | None = None
    current: MetricValue[float] | None = None
    driving_state: MetricValue[DrivingState] | None = None
    elevation: MetricValue[float] | None = None
    external_temperature: MetricValue[float] | None = None
    heading: MetricValue[float] | None = None
    hvac_power: MetricValue[float] | None = None
    map_info: MetricValue[MapInfo] | None = None
    speed: MetricValue[float] | None = None
    speed_factor: MetricValue[float] | None = None
    calibrated_confidence: MetricValue[tuple[float, ...]] | None = None
    battery_temperature_min: MetricValue[float] | None = None
    battery_temperature_max: MetricValue[float] | None = None

    def items(self) -> Iterator[tuple[Metric, MetricValue[Any]]]:
        """Yield ``(Metric, MetricValue)`` for every present (non-None) field.

        The bridge is ``metric.value`` — every ``Metric`` member's value is
        exactly the field name on this dataclass.
        """
        for metric in Metric:
            value = getattr(self, metric.value)
            if value is not None:
                yield metric, value

    def merge(self, delta: Telemetry) -> Telemetry:
        """Return a copy of ``self`` with ``delta``'s present fields overlaid.

        Absent (``None``) delta fields leave ``self``'s field untouched.
        Pure structural overlay — accumulation policy is the consumer's.
        """
        updates = {metric.value: value for metric, value in delta.items()}
        return replace(self, **updates) if updates else self


class ConnectionState(Enum):
    """Stream connection state.

    Plain Enum — values never cross the wire, unlike Metric/ChargingState.
    """

    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    AUTH_FAILED = "auth_failed"


@dataclass(frozen=True, slots=True)
class ConnectionEvent:
    """A stream connection state change."""

    state: ConnectionState
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class CatalogEntry:
    """One catalog vehicle template from ``GET /2/vehicle/_list``.

    The v2 catalog endpoint returns ~1100 vehicle templates indexed by
    ``typecode``. Each entry carries nameplate metadata (maker, model, title,
    year window, battery capacity). Optional fields normalise at the parse
    boundary so empty strings and wrong types collapse to ``None`` instead
    of poisoning downstream consumers.
    """

    typecode: str
    manufacturer: str | None
    model: str | None
    title: str | None
    start_year: int | None
    end_year: int | None
    battery_capacity_wh: int | None


@dataclass(frozen=True, slots=True)
class VehicleModelDisplay:
    """Display metadata for one vehicle model, resolved from a typecode.

    Returned by ``GET /2/vehicle-model/by-typecode/{typecode}/display``. The
    four wire-required strings (``manufacturer``, ``model``, ``years``,
    ``title``) are always present; ``start_year`` / ``end_year`` are the
    server's convenience parse of ``years`` and are ``None`` when not
    derivable — open-ended ranges (``"2021+"`` → no ``end_year``) or
    non-numeric values (``"Unreleased"`` → neither). The raw ``years`` string
    is preserved verbatim so consumers can present or re-parse it themselves.
    """

    manufacturer: str
    model: str
    years: str
    title: str
    start_year: int | None
    end_year: int | None

    @property
    def model_name(self) -> str:
        """Composed model label without the make, e.g. ``"R2 2026-2027 Long Range"``.

        Build formula: ``model`` + optional `` {start_year}-{end_year}`` (or
        `` {start_year}`` alone when the end year is missing or equal to the
        start) + optional `` {title}`` (the trim). The year segment is dropped
        whenever ``start_year`` is ``None`` — covering both "no years" and the
        open-ended "end-year-only" case. ``title`` is stripped and dropped when
        blank; ``model`` is used verbatim. The raw ``years`` string is never
        consulted. The pair is never reordered or range-checked, so a
        server-side ``end_year < start_year`` renders verbatim (``"2024-2020"``)
        rather than being normalised.

        Suits consumers that keep the make in a field of its own, pairing this
        with :attr:`manufacturer`.
        """
        parts = [self.model]
        if self.start_year is not None:
            if self.end_year is None or self.end_year == self.start_year:
                parts.append(str(self.start_year))
            else:
                parts.append(f"{self.start_year}-{self.end_year}")
        if title := self.title.strip():
            parts.append(title)
        return " ".join(parts)

    @property
    def display_name(self) -> str:
        """Composed device-card label, e.g. ``"Rivian R2 2026-2027 Long Range"``.

        ``manufacturer`` joined verbatim to :attr:`model_name` by a single
        space — see that property for the year and trim rules. Because the join
        is unconditional, a blank ``manufacturer`` yields a leading space.

        Always returns a ``str`` (never ``None``), unlike the nullable
        :attr:`AbrpIdentity.display_name` field.
        """
        return f"{self.manufacturer} {self.model_name}"


@dataclass(frozen=True, slots=True)
class AbrpVehicle:
    """One vehicle in the caller's garage.

    From ``GET /2/abrp/me/vehicles?include=shared``, in ABRP's order
    (currently owned vehicles first, then shared ones, each by id).

    * ``name`` is the owner's nickname, ``None`` when unset.
    * ``vehicle_model`` is the typecode: a catalogue typecode or a custom
      model id.
    * ``paint`` is always ``None``: the garage endpoint does not return it.
      The field is kept for compatibility.
    * ``display`` is what ABRP shows for the vehicle's model, ``None`` when
      the typecode does not resolve (or the record is malformed).
    * ``relation`` is ``None`` for a relation this version does not know.
    """

    vehicle_id: int
    name: str | None
    vehicle_model: str
    paint: str | None = None
    display: VehicleModelDisplay | None = None
    relation: VehicleRelation | None = None


class Provider(StrEnum):
    """Source of a sent telemetry point; a plain str is accepted too."""

    APP_LOCATION = "APP_LOCATION"
    APP_OBD = "APP_OBD"
    APP_AUTOMOTIVE = "APP_AUTOMOTIVE"
    APP_AUTO = "APP_AUTO"
    ABRP_OTA = "ABRP_OTA"


# Field -> (InputPoint metric key, unit key).
_NUMERIC_WIRE: dict[str, tuple[str, str]] = {
    "soc_frac": ("soc", "frac"),
    "soh_frac": ("soh", "frac"),
    "power_w": ("power", "w"),
    "hvac_power_w": ("hvacPower", "w"),
    "voltage_v": ("voltage", "v"),
    "current_a": ("current", "a"),
    "soe_wh": ("soe", "wh"),
    "battery_capacity_wh": ("batteryCapacity", "wh"),
    "charging_energy_added_wh": ("chargingEnergyAdded", "wh"),
    "odometer_m": ("odometer", "m"),
    "elevation_m": ("elevation", "m"),
    "estimated_battery_range_m": ("estimatedBatteryRange", "m"),
    "speed_ms": ("speed", "ms"),
    "heading_degrees": ("heading", "degrees"),
    "battery_temperature_c": ("batteryTemperature", "c"),
    "battery_temperature_min_c": ("batteryTemperatureMin", "c"),
    "battery_temperature_max_c": ("batteryTemperatureMax", "c"),
    "cabin_temperature_c": ("cabinTemperature", "c"),
    "cabin_set_point_c": ("cabinSetPoint", "c"),
    "external_temperature_c": ("externalTemperature", "c"),
}
_STATE_WIRE: dict[str, tuple[str, type[StrEnum]]] = {
    "charging_state": ("chargingState", ChargingState),
    "driving_state": ("drivingState", DrivingState),
}
_RESERVED_WIRE_KEYS = frozenset(
    {"provider", "location"}
    | {key for key, _ in _NUMERIC_WIRE.values()}
    | {key for key, _ in _STATE_WIRE.values()}
)


def _check_number(name: str, value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"{name} must be an int or float")
    try:
        finite = math.isfinite(value)
    except OverflowError:
        finite = False
    if not finite:
        raise ValueError(f"{name} must be finite")


def _wire_time(value: datetime) -> str:
    """Format ``value`` as ISO 8601 UTC with a ``Z`` suffix."""
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class TelemetryPoint:
    """One telemetry point for :meth:`AbrpClient.async_send_telemetry`.

    All metrics share one timezone-aware ``time``. Field names carry ABRP's
    wire unit and values are sent unconverted (``soc_frac`` 0.55 = 55 %).
    ``extra`` holds spec fields newer than this library; it is deep-copied,
    and a mapping value without ``"time"`` gets the point's time. Types
    are checked here, ranges are left to ABRP.
    """

    provider: Provider | str
    time: datetime
    soc_frac: float | None = None
    soh_frac: float | None = None
    power_w: float | None = None
    hvac_power_w: float | None = None
    voltage_v: float | None = None
    current_a: float | None = None
    soe_wh: float | None = None
    battery_capacity_wh: float | None = None
    charging_energy_added_wh: float | None = None
    odometer_m: float | None = None
    elevation_m: float | None = None
    estimated_battery_range_m: float | None = None
    speed_ms: float | None = None
    heading_degrees: float | None = None
    battery_temperature_c: float | None = None
    battery_temperature_min_c: float | None = None
    battery_temperature_max_c: float | None = None
    cabin_temperature_c: float | None = None
    cabin_set_point_c: float | None = None
    external_temperature_c: float | None = None
    charging_state: ChargingState | None = None
    driving_state: DrivingState | None = None
    location: Location | None = field(default=None, repr=False)
    extra: Mapping[str, Any] = field(default_factory=dict, repr=False, hash=False)

    def __post_init__(self) -> None:
        """Check types and copy ``extra``."""
        if not isinstance(self.provider, str):
            raise TypeError("provider must be a Provider or str")
        if not self.provider:
            raise ValueError("provider must not be empty")
        if not isinstance(self.time, datetime):
            raise TypeError("time must be a datetime")
        if self.time.utcoffset() is None:
            raise ValueError("time must be timezone-aware")
        has_metric = False
        for name in _NUMERIC_WIRE:
            value = getattr(self, name)
            if value is not None:
                _check_number(name, value)
                has_metric = True
        for name, (_, enum_type) in _STATE_WIRE.items():
            value = getattr(self, name)
            if value is not None:
                if not isinstance(value, enum_type):
                    raise TypeError(f"{name} must be a {enum_type.__name__}")
                has_metric = True
        if self.location is not None:
            if not isinstance(self.location, Location):
                raise TypeError("location must be a Location")
            _check_number("location", self.location.lat)
            _check_number("location", self.location.lon)
            has_metric = True
        if not isinstance(self.extra, Mapping):
            raise TypeError("extra must be a mapping")
        for key in self.extra:
            if not isinstance(key, str):
                raise TypeError("extra keys must be str")
            if key in _RESERVED_WIRE_KEYS:
                raise ValueError(f"extra key {key!r} is a typed field; set it there")
        if not has_metric and not self.extra:
            raise ValueError("a point needs at least one metric or extra entry")
        copied = copy.deepcopy(
            {k: dict(v) if isinstance(v, Mapping) else v for k, v in self.extra.items()}
        )
        object.__setattr__(self, "extra", copied)

    def _to_wire(self) -> dict[str, Any]:
        """Return the ``InputPoint`` JSON body for this point."""
        time = _wire_time(self.time)
        body: dict[str, Any] = {"provider": str(self.provider)}
        for name, (key, unit) in _NUMERIC_WIRE.items():
            value = getattr(self, name)
            if value is not None:
                body[key] = {unit: value, "time": time}
        for name, (key, _) in _STATE_WIRE.items():
            value = getattr(self, name)
            if value is not None:
                body[key] = {"state": value.name, "time": time}
        if self.location is not None:
            body["location"] = {
                "lat": self.location.lat,
                "long": self.location.lon,
                "time": time,
            }
        for key, value in self.extra.items():
            if key in _RESERVED_WIRE_KEYS:  # typed fields win
                continue
            body[key] = {"time": time, **value} if isinstance(value, dict) else value
        return body
