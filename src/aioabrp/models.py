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
class MetricValue[T]:
    """One extracted metric value.

    Generic over the value type ``T`` (``float`` for numeric metrics,
    ``ChargingState`` for the categorical metric, ``Location`` for the GPS
    pair). Units are fixed per metric: percent (soc/soh), W, V, Wh, m, °C,
    Wh/km. ``time`` is the wire block's tz-aware timestamp or ``None``;
    ``provider`` is the clean upstream provider string or ``None``.
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
