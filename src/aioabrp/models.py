"""Public typed models for aioabrp."""

from collections.abc import Iterator
from dataclasses import dataclass, replace
from datetime import datetime
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
    degrees (heading), °C (battery/cabin/external temperatures), Wh/km
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
class AbrpVehicle:
    """One vehicle from the v1 garage enumeration (raw wire fields only).

    All fields come from ``POST /1/session/get_tlm``. ``name`` stays
    nullable defensively (some live-API records have no nickname).
    """

    vehicle_id: int
    name: str | None
    vehicle_model: str
    paint: str | None


@dataclass(frozen=True, slots=True)
class CatalogEntry:
    """One catalog vehicle template from ``GET /2/vehicle/_list``.

    The v2 catalog endpoint returns ~1100 vehicle templates indexed by
    ``typecode``. Each entry carries nameplate metadata (maker, model, title,
    year window, battery capacity) that the v1 garage endpoint does not
    expose. Optional fields normalise at the parse boundary so empty strings
    and wrong types collapse to ``None`` instead of poisoning downstream
    consumers.
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
