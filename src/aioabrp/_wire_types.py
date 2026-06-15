r"""Pruned TypedDicts mirroring the iternio v2 swagger telemetry payloads.

DO NOT HAND-EDIT individual class bodies. The classes below are derived
from ``datamodel-code-generator``'s output for ABRP's v2 OpenAPI spec; the
file is the post-codegen prune to the keep-set we actually consume.

Keep-set methodology
--------------------
Transitively reachable from the two telemetry frame containers the
library consumes (``OutputPoint`` for one-shot telemetry payloads,
``OutputPointWithVehicleId`` for SSE frames) given the surfaced
``OutputPoint`` keys actually read by:

* every metric extractor registered in :mod:`aioabrp._extract` — soc,
  power, voltage, soe, odometer, calibratedRefCons, batteryCapacity,
  soh, estimatedBatteryRange, batteryTemperature, chargingState,
  cabinSetPoint, cabinTemperature, calibratedMaxSpeed,
  chargingEnergyAdded, current, drivingState, elevation,
  externalTemperature, heading, hvacPower, speed, speedFactor,
  calibratedConfidence, mapInfo;
* ``location`` — carried on the SSE wire and consumed by the
  :attr:`aioabrp.Metric.LOCATION` extractor in :mod:`aioabrp._extract`.

That's 26 surfaced keys → 26 leaf+Output pairs + the mixin chain
(``WithTime``, ``WithTimeAndProvider``, ``Provider``, ``DateTimeString``)
+ a handful of numeric type aliases (plus the ``ChargingStateValue`` /
``DrivingStateValue`` / ``Region`` enum aliases for the categorical
leaves). The full swagger graph emits ~415 classes; the keep-set is a
fraction of that. ``OutputPoint`` mirrors ABRP's telemetry payload 1:1.

The exhaustive keep-set is also pinned by ``tests/test_wire_types.py``:
an ``OutputPoint`` cardinality assertion fails RED if a future regen
re-introduces a pruned field, and per-leaf assertions fail RED on
codegen-driven wire-key drift (e.g. ``frac`` renamed to ``percent``).

Regen procedure on swagger drift
--------------------------------
The swagger spec is published at
``https://api.iternio.com/swagger-ui/spec/prod/IternioPlanning.out.yaml``
(verified live 2026-06-11; tracks production). When a regen is required:

1. Download a fresh copy and re-run codegen against it::

       curl -fsSL \
           https://api.iternio.com/swagger-ui/spec/prod/IternioPlanning.out.yaml \
           -o /tmp/iternio_v2_swagger.yaml
       uvx --from datamodel-code-generator datamodel-codegen \
           --input /tmp/iternio_v2_swagger.yaml \
           --input-file-type openapi \
           --output _generated_full.py \
           --output-model-type typing.TypedDict \
           --target-python-version 3.14 \
           --use-double-quotes \
           --use-standard-collections \
           --use-union-operator

2. Diff ``_generated_full.py`` against this file's keep-set. For any
   new field on ``OutputPoint`` we want to surface, hand-copy the leaf
   pair (``Xxx`` + ``XxxOutput``) and any new numeric type alias it
   references into this file, and add the new entry to
   ``OutputPoint``'s body. If a hand-copied leaf re-introduces a
   parent-key overlap (TypedDict multi-inheritance key collision),
   restore the ``# mypy: disable-error-code=misc`` directive at line
   1 — the current keep-set has zero collisions, so the directive is
   not present.

3. Add the corresponding row to ``_KEEP_SET_LEAVES`` and to
   ``_EXPECTED_OUTPUT_POINT_KEYS`` in ``tests/test_wire_types.py``;
   re-run the test suite — the cardinality + per-leaf assertions
   re-anchor the keep-set.

4. Discard ``_generated_full.py``. ``datamodel-code-generator`` is a
   developer tool, NOT a runtime dependency, and the library ships
   only the pruned file checked in here.

The cardinality assertion in ``test_wire_types.py`` is the
swagger-drift canary: skipping step 2-3 (a regen-without-prune) restores
3,132 LOC and the test fails immediately.
"""

from typing import Literal, NotRequired, TypedDict

type DateTimeString = str


class WithTime(TypedDict):
    time: DateTimeString


type Provider = Literal[
    "RIVIAN_PLAN",
    "TESLA_FLEET_STREAM",
    "TESLA_FLEET_POLL",
    "HIGHMOBILITY_MQTT",
    "ENODE_PUSH",
    "APP_LOCATION",
    "APP_OBD",
    "APP_AUTOMOTIVE",
    "TLM_API",
    "TRONITY_POLL",
    "RIVIAN_POLL",
    "DEBUG",
    "APP_AUTO",
    "EXTERNAL_CALIBRATION",
    "RIVIAN_STREAM",
    "DERIVED",
]


class WithTimeAndProvider(WithTime):
    provider: NotRequired[Provider]


# Numeric type aliases — codegen emits each leaf's wire field with its
# upstream alias rather than the bare ``float`` / ``int``; preserved
# verbatim so a future regen-diff highlights only meaningful drift.
type Lat = float
type Long = float
type FracWiggle = float
type EnergyWh = float
type PowerW = float
type TemperatureC = float
type Degrees = float
type FracItem = float


class Coordinates(TypedDict):
    lat: Lat
    long: Long


class Location(Coordinates):
    pass


class LocationOutput(WithTimeAndProvider, Location):
    pass


class BatteryCapacity(TypedDict):
    wh: EnergyWh


class BatteryCapacityOutput(WithTimeAndProvider, BatteryCapacity):
    pass


class BatteryTemperature(TypedDict):
    c: TemperatureC


class BatteryTemperatureOutput(WithTimeAndProvider, BatteryTemperature):
    pass


class CalibratedRefCons(TypedDict):
    wh_per_km: float


class CalibratedRefConsOutput(WithTimeAndProvider, CalibratedRefCons):
    pass


type ChargingStateValue = Literal[
    "CHARGING_AC",
    "CHARGING_DC",
    "CHARGING_UNKNOWN",
    "NOT_CHARGING",
    "PLUGGED_IN",
]


class ChargingState(TypedDict):
    state: ChargingStateValue


class ChargingStateOutput(WithTimeAndProvider, ChargingState):
    pass


class EstimatedBatteryRange(TypedDict):
    m: float


class EstimatedBatteryRangeOutput(WithTimeAndProvider, EstimatedBatteryRange):
    pass


class Odometer(TypedDict):
    m: float


class OdometerOutput(WithTimeAndProvider, Odometer):
    pass


class Power(TypedDict):
    w: PowerW


class PowerOutput(WithTimeAndProvider, Power):
    pass


class Soc(TypedDict):
    frac: FracWiggle


class SocOutput(WithTimeAndProvider, Soc):
    pass


class Soe(TypedDict):
    wh: EnergyWh


class SoeOutput(WithTimeAndProvider, Soe):
    pass


class Soh(TypedDict):
    frac: FracWiggle


class SohOutput(WithTimeAndProvider, Soh):
    pass


class Voltage(TypedDict):
    v: float


class VoltageOutput(WithTimeAndProvider, Voltage):
    pass


class CabinSetPoint(TypedDict):
    c: TemperatureC


class CabinSetPointOutput(WithTimeAndProvider, CabinSetPoint):
    pass


class CabinTemperature(TypedDict):
    c: TemperatureC


class CabinTemperatureOutput(WithTimeAndProvider, CabinTemperature):
    pass


class ExternalTemperature(TypedDict):
    c: TemperatureC


class ExternalTemperatureOutput(WithTimeAndProvider, ExternalTemperature):
    pass


class ChargingEnergyAdded(TypedDict):
    wh: EnergyWh


class ChargingEnergyAddedOutput(WithTimeAndProvider, ChargingEnergyAdded):
    pass


class Current(TypedDict):
    a: float


class CurrentOutput(WithTimeAndProvider, Current):
    pass


class Elevation(TypedDict):
    m: float


class ElevationOutput(WithTimeAndProvider, Elevation):
    pass


class Heading(TypedDict):
    degrees: Degrees


class HeadingOutput(WithTimeAndProvider, Heading):
    pass


class HvacPower(TypedDict):
    w: PowerW


class HvacPowerOutput(WithTimeAndProvider, HvacPower):
    pass


class Speed(TypedDict):
    ms: float


class SpeedOutput(WithTimeAndProvider, Speed):
    pass


class MaxSpeed(TypedDict):
    ms: float


class MaxSpeedOutput(WithTimeAndProvider, MaxSpeed):
    pass


class SpeedFactor(TypedDict):
    frac: float


class SpeedFactorOutput(WithTimeAndProvider, SpeedFactor):
    pass


class CalibratedConfidence(TypedDict):
    frac: list[FracItem]


class CalibratedConfidenceOutput(WithTimeAndProvider, CalibratedConfidence):
    pass


type DrivingStateValue = Literal[
    "PARK",
    "REVERSE",
    "NEUTRAL",
    "DRIVE",
]


class DrivingState(TypedDict):
    state: DrivingStateValue


class DrivingStateOutput(WithTimeAndProvider, DrivingState):
    pass


type Region = Literal[
    "AFRICA",
    "ASIA",
    "AUSTRALIA",
    "CENTRAL_AMERICA",
    "EUROPE",
    "NORTH_AMERICA",
    "SOUTH_AMERICA",
    "OCEANIA",
]


class MapInfo(TypedDict):
    region: NotRequired[Region]
    country_3: NotRequired[str]
    address: NotRequired[str]
    speedLimitMs: NotRequired[float]
    isFreeSpeedZone: NotRequired[bool]


class MapInfoOutput(WithTimeAndProvider, MapInfo):
    pass


class OutputPoint(TypedDict):
    batteryCapacity: NotRequired[BatteryCapacityOutput]
    batteryTemperature: NotRequired[BatteryTemperatureOutput]
    cabinSetPoint: NotRequired[CabinSetPointOutput]
    cabinTemperature: NotRequired[CabinTemperatureOutput]
    calibratedConfidence: NotRequired[CalibratedConfidenceOutput]
    calibratedMaxSpeed: NotRequired[MaxSpeedOutput]
    calibratedRefCons: NotRequired[CalibratedRefConsOutput]
    chargingEnergyAdded: NotRequired[ChargingEnergyAddedOutput]
    chargingState: NotRequired[ChargingStateOutput]
    current: NotRequired[CurrentOutput]
    drivingState: NotRequired[DrivingStateOutput]
    elevation: NotRequired[ElevationOutput]
    estimatedBatteryRange: NotRequired[EstimatedBatteryRangeOutput]
    externalTemperature: NotRequired[ExternalTemperatureOutput]
    heading: NotRequired[HeadingOutput]
    hvacPower: NotRequired[HvacPowerOutput]
    location: NotRequired[LocationOutput]
    mapInfo: NotRequired[MapInfoOutput]
    odometer: NotRequired[OdometerOutput]
    power: NotRequired[PowerOutput]
    soc: NotRequired[SocOutput]
    soe: NotRequired[SoeOutput]
    soh: NotRequired[SohOutput]
    speed: NotRequired[SpeedOutput]
    speedFactor: NotRequired[SpeedFactorOutput]
    voltage: NotRequired[VoltageOutput]


class OutputPointWithVehicleId(OutputPoint):
    vehicleId: int
