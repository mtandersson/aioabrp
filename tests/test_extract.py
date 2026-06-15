"""Tests for aioabrp._extract: wire frame -> typed metric extraction."""

import logging
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest

from aioabrp._extract import (
    WIRE_KEYS,
    extract_metrics,
    is_clean_provider_str,
    parse_block_time,
)
from aioabrp.models import (
    ChargingState,
    DrivingState,
    Location,
    MapInfo,
    Metric,
    MetricValue,
    Region,
)


def _extract(frame: dict[str, Any]) -> dict[Metric, MetricValue[Any]]:
    """Run extract_metrics with throwaway dedup sets."""
    return extract_metrics(
        frame,
        unknown_charging_states_seen=set(),
        unknown_driving_states_seen=set(),
    )


def test_wire_keys_cover_every_metric() -> None:
    assert set(WIRE_KEYS) == set(Metric)
    assert WIRE_KEYS[Metric.CALIBRATED_REF_CONS] == "calibratedRefCons"
    assert WIRE_KEYS[Metric.BATTERY_CAPACITY] == "batteryCapacity"
    assert WIRE_KEYS[Metric.RANGE] == "estimatedBatteryRange"
    assert WIRE_KEYS[Metric.BATTERY_TEMPERATURE] == "batteryTemperature"
    assert WIRE_KEYS[Metric.CHARGING_STATE] == "chargingState"
    # camelCase wire keys for the 14 added metrics.
    assert WIRE_KEYS[Metric.CABIN_SET_POINT] == "cabinSetPoint"
    assert WIRE_KEYS[Metric.CALIBRATED_MAX_SPEED] == "calibratedMaxSpeed"
    assert WIRE_KEYS[Metric.CHARGING_ENERGY_ADDED] == "chargingEnergyAdded"
    assert WIRE_KEYS[Metric.EXTERNAL_TEMPERATURE] == "externalTemperature"
    assert WIRE_KEYS[Metric.HVAC_POWER] == "hvacPower"
    assert WIRE_KEYS[Metric.DRIVING_STATE] == "drivingState"
    assert WIRE_KEYS[Metric.MAP_INFO] == "mapInfo"
    assert WIRE_KEYS[Metric.CALIBRATED_CONFIDENCE] == "calibratedConfidence"
    assert WIRE_KEYS[Metric.SPEED_FACTOR] == "speedFactor"


# ---------- null-safety matrix -----------------------------------------------
#
# Defense-in-depth for every degenerate wire shape: a missing isinstance
# guard would propagate a TypeError out of extract_metrics on a live SSE
# frame. Pure functions over the wire shape; rows and ids ported from the
# originating integration's test suite, re-targeted at extract_metrics
# presence/absence.


@pytest.mark.parametrize(
    ("metric", "frame", "expected"),
    [
        # Happy path.
        pytest.param(Metric.SOC, {"soc": {"frac": 0.5}}, 50.0, id="soc_ok"),
        pytest.param(Metric.POWER, {"power": {"w": 1234.0}}, 1234.0, id="power_ok"),
        pytest.param(Metric.VOLTAGE, {"voltage": {"v": 400.0}}, 400.0, id="voltage_ok"),
        # Key absent — most common upstream-delta case.
        pytest.param(Metric.SOC, {}, None, id="soc_absent"),
        pytest.param(Metric.POWER, {}, None, id="power_absent"),
        pytest.param(Metric.VOLTAGE, {}, None, id="voltage_absent"),
        # Key present but value is null (upstream sentinel).
        pytest.param(Metric.SOC, {"soc": None}, None, id="soc_null"),
        pytest.param(Metric.POWER, {"power": None}, None, id="power_null"),
        pytest.param(Metric.VOLTAGE, {"voltage": None}, None, id="voltage_null"),
        # Key present but value is empty dict (no inner field).
        pytest.param(Metric.SOC, {"soc": {}}, None, id="soc_empty"),
        pytest.param(Metric.POWER, {"power": {}}, None, id="power_empty"),
        pytest.param(Metric.VOLTAGE, {"voltage": {}}, None, id="voltage_empty"),
        # Inner numeric leaf is null — ``null * 100`` would TypeError if the
        # isinstance guard were absent.
        pytest.param(Metric.SOC, {"soc": {"frac": None}}, None, id="soc_inner_null"),
        pytest.param(Metric.POWER, {"power": {"w": None}}, None, id="power_inner_null"),
        pytest.param(
            Metric.VOLTAGE, {"voltage": {"v": None}}, None, id="voltage_inner_null"
        ),
        # Inner leaf is the wrong type — bool is a subclass of int in Python,
        # so the explicit ``isinstance(_, bool)`` check matters.
        pytest.param(Metric.SOC, {"soc": {"frac": True}}, None, id="soc_inner_bool"),
        pytest.param(
            Metric.POWER, {"power": {"w": "5000"}}, None, id="power_inner_str"
        ),
        # Non-finite leaf — json.loads accepts the non-standard NaN/Infinity
        # tokens; a NaN is "metric omitted", never a value.
        pytest.param(
            Metric.SOC, {"soc": {"frac": float("nan")}}, None, id="soc_inner_nan"
        ),
        # ENUM charging_state: every degenerate / unrecognized shape is
        # omitted (never a raw string — the enum is closed).
        pytest.param(Metric.CHARGING_STATE, {}, None, id="charging_state_absent"),
        pytest.param(
            Metric.CHARGING_STATE,
            {"chargingState": None},
            None,
            id="charging_state_null",
        ),
        pytest.param(
            Metric.CHARGING_STATE,
            {"chargingState": {}},
            None,
            id="charging_state_empty",
        ),
        pytest.param(
            Metric.CHARGING_STATE,
            {"chargingState": {"time": "2026-05-24T12:00:00Z"}},
            None,
            id="charging_state_missing_state",
        ),
        pytest.param(
            Metric.CHARGING_STATE,
            {"chargingState": {"state": None}},
            None,
            id="charging_state_inner_null",
        ),
        pytest.param(
            Metric.CHARGING_STATE,
            {"chargingState": {"state": ""}},
            None,
            id="charging_state_inner_empty_string",
        ),
        pytest.param(
            Metric.CHARGING_STATE,
            {"chargingState": {"state": 123}},
            None,
            id="charging_state_inner_int",
        ),
        pytest.param(
            Metric.CHARGING_STATE,
            {"chargingState": {"state": "FOO"}},
            None,
            id="charging_state_unrecognized_member",
        ),
    ],
)
def test_null_safety(
    metric: Metric, frame: dict[str, Any], expected: float | None
) -> None:
    """Every degenerate wire shape omits the metric; never raises."""
    result = _extract(frame)
    if expected is None:
        assert metric not in result
    else:
        assert result[metric].value == expected


@pytest.mark.parametrize(
    ("wire_member", "expected"),
    [
        ("CHARGING_AC", ChargingState.CHARGING_AC),
        ("CHARGING_DC", ChargingState.CHARGING_DC),
        ("CHARGING_UNKNOWN", ChargingState.CHARGING_UNKNOWN),
        ("NOT_CHARGING", ChargingState.NOT_CHARGING),
        ("PLUGGED_IN", ChargingState.PLUGGED_IN),
    ],
)
def test_charging_state_member_mapping(
    wire_member: str, expected: ChargingState
) -> None:
    result = _extract({"chargingState": {"state": wire_member}})
    assert result[Metric.CHARGING_STATE].value is expected


# ---------- full happy-path frame -------------------------------------------


def test_happy_path_all_metrics() -> None:
    """All 26 metrics extract from one frame with time + provider."""
    extra = {"time": "2026-05-25T12:00:00Z", "provider": "RIVIAN_STREAM"}
    frame: dict[str, Any] = {
        "vehicleId": 1,
        "soc": {"frac": 0.5, **extra},
        "power": {"w": 1234.0, **extra},
        "voltage": {"v": 400.0, **extra},
        "soe": {"wh": 60000.0, **extra},
        "odometer": {"m": 1000.5, **extra},
        "calibratedRefCons": {"wh_per_km": 180.0, **extra},
        "batteryCapacity": {"wh": 120000.0, **extra},
        "soh": {"frac": 0.97, **extra},
        "estimatedBatteryRange": {"m": 250000.0, **extra},
        # Sub-zero pack temp: winter operation is a real wire shape — the
        # extractor must not apply a lower-bound filter.
        "batteryTemperature": {"c": -4.5, **extra},
        "chargingState": {"state": "CHARGING_DC", **extra},
        "location": {"lat": 57.7, "long": 11.97, **extra},
        "cabinSetPoint": {"c": 21.0, **extra},
        "cabinTemperature": {"c": 19.5, **extra},
        "calibratedMaxSpeed": {"ms": 55.0, **extra},
        "chargingEnergyAdded": {"wh": 8000.0, **extra},
        "current": {"a": -150.0, **extra},
        "drivingState": {"state": "DRIVE", **extra},
        "elevation": {"m": 42.0, **extra},
        "externalTemperature": {"c": 12.0, **extra},
        "heading": {"degrees": 270.0, **extra},
        "hvacPower": {"w": 1500.0, **extra},
        "speed": {"ms": 27.5, **extra},
        # Raw multiplier — must surface unscaled (NOT x100).
        "speedFactor": {"frac": 0.9, **extra},
        "calibratedConfidence": {"frac": [0.8, 0.85, 0.9, 0.95], **extra},
        "mapInfo": {
            "region": "EUROPE",
            "country_3": "SWE",
            "address": "Kungsgatan",
            "speedLimitMs": 25.0,
            "isFreeSpeedZone": False,
            **extra,
        },
    }
    result = _extract(frame)
    assert set(result) == set(Metric)

    expected_values: dict[Metric, object] = {
        Metric.SOC: 50.0,
        Metric.POWER: 1234.0,
        Metric.VOLTAGE: 400.0,
        Metric.SOE: 60000.0,
        Metric.ODOMETER: 1000.5,
        Metric.CALIBRATED_REF_CONS: 180.0,
        Metric.BATTERY_CAPACITY: 120000.0,
        Metric.SOH: pytest.approx(97.0),
        Metric.RANGE: 250000.0,
        Metric.BATTERY_TEMPERATURE: -4.5,
        Metric.CHARGING_STATE: ChargingState.CHARGING_DC,
        Metric.LOCATION: Location(lat=57.7, lon=11.97),
        Metric.CABIN_SET_POINT: 21.0,
        Metric.CABIN_TEMPERATURE: 19.5,
        Metric.CALIBRATED_MAX_SPEED: 55.0,
        Metric.CHARGING_ENERGY_ADDED: 8000.0,
        Metric.CURRENT: -150.0,
        Metric.DRIVING_STATE: DrivingState.DRIVE,
        Metric.ELEVATION: 42.0,
        Metric.EXTERNAL_TEMPERATURE: 12.0,
        Metric.HEADING: 270.0,
        Metric.HVAC_POWER: 1500.0,
        Metric.SPEED: 27.5,
        Metric.SPEED_FACTOR: 0.9,
        Metric.CALIBRATED_CONFIDENCE: (0.8, 0.85, 0.9, 0.95),
        Metric.MAP_INFO: MapInfo(
            region=Region.EUROPE,
            country_3="SWE",
            address="Kungsgatan",
            speed_limit_ms=25.0,
            is_free_speed_zone=False,
        ),
    }
    expected_time = datetime(2026, 5, 25, 12, tzinfo=UTC)
    for metric, expected in expected_values.items():
        mv = result[metric]
        assert mv.value == expected, metric
        assert mv.time == expected_time, metric
        assert mv.provider == "RIVIAN_STREAM", metric


def test_soh_overshoot_not_clamped() -> None:
    """A post-recalibration ``frac > 1.0`` surfaces >100% — never clamped."""
    result = _extract({"soh": {"frac": 1.02}})
    assert result[Metric.SOH].value == pytest.approx(102.0)


def test_int_leaf_accepted_and_coerced_to_float() -> None:
    """A wire int on the shared numeric path surfaces as a runtime float."""
    value = _extract({"odometer": {"m": 1000}})[Metric.ODOMETER].value
    assert value == 1000.0
    assert isinstance(value, float)


# ---------- location tolerance matrix (NEW extractor; the plan is the spec) -


def test_location_ok() -> None:
    result = _extract({"location": {"lat": 57.7, "long": 11.97}})
    assert result[Metric.LOCATION].value == Location(lat=57.7, lon=11.97)


def test_location_int_leaves_coerced_to_float() -> None:
    value = _extract({"location": {"lat": 57, "long": 12}})[Metric.LOCATION].value
    assert value == Location(lat=57.0, lon=12.0)
    assert isinstance(value, Location)
    assert isinstance(value.lat, float)
    assert isinstance(value.lon, float)


@pytest.mark.parametrize(
    "frame",
    [
        pytest.param({}, id="location_absent"),
        pytest.param({"location": None}, id="location_null"),
        pytest.param({"location": {}}, id="location_empty"),
        pytest.param({"location": [57.7, 11.97]}, id="location_non_dict"),
        pytest.param({"location": {"lat": 57.7}}, id="location_lat_only"),
        pytest.param({"location": {"long": 11.97}}, id="location_long_only"),
        pytest.param(
            {"location": {"lat": None, "long": 11.97}}, id="location_lat_null"
        ),
        pytest.param(
            {"location": {"lat": True, "long": 11.97}}, id="location_lat_bool"
        ),
        pytest.param(
            {"location": {"lat": "57.7", "long": 11.97}}, id="location_lat_string"
        ),
    ],
)
def test_location_degenerate_shapes_omitted(frame: dict[str, Any]) -> None:
    assert Metric.LOCATION not in _extract(frame)


# ---------- block time handling ----------------------------------------------


@pytest.mark.parametrize(
    ("block", "expected"),
    [
        pytest.param(
            {"time": "2026-05-25T12:00:00Z"},
            datetime(2026, 5, 25, 12, tzinfo=UTC),
            id="zulu_suffix",
        ),
        pytest.param(
            {"time": "2026-05-25T12:00:00+02:00"},
            datetime(2026, 5, 25, 12, tzinfo=timezone(timedelta(hours=2))),
            id="explicit_offset",
        ),
        pytest.param({}, None, id="time_absent"),
        pytest.param({"time": None}, None, id="time_null"),
        pytest.param({"time": 12345}, None, id="time_int_marker"),
        pytest.param({"time": "not a date"}, None, id="time_malformed"),
        pytest.param({"time": "2026-05-25T12:00:00"}, None, id="time_naive"),
        pytest.param({"time": "2026-13-01T00:00:00Z"}, None, id="time_month_13"),
        pytest.param({"time": "2026-02-30T00:00:00Z"}, None, id="time_feb_30"),
    ],
)
def test_parse_block_time(block: dict[str, Any], expected: datetime | None) -> None:
    assert parse_block_time(block) == expected


@pytest.mark.parametrize(
    "raw_time",
    [
        pytest.param(12345, id="int_marker"),
        pytest.param("2026-05-25T12:00:00", id="naive"),
        pytest.param("2026-13-01T00:00:00Z", id="month_13"),
    ],
)
def test_unparseable_time_still_adopts_value(raw_time: object) -> None:
    """A bad block time never suppresses the value — only the timestamp."""
    result = _extract({"soc": {"frac": 0.5, "time": raw_time}})
    mv = result[Metric.SOC]
    assert mv.value == 50.0
    assert mv.time is None


def test_aware_time_attached_to_metric_value() -> None:
    result = _extract({"soc": {"frac": 0.5, "time": "2026-05-25T12:00:00Z"}})
    assert result[Metric.SOC].time == datetime(2026, 5, 25, 12, tzinfo=UTC)


# ---------- provider handling -------------------------------------------------


@pytest.mark.parametrize(
    ("block", "expected"),
    [
        pytest.param(
            {"frac": 0.5, "provider": "RIVIAN_STREAM"},
            "RIVIAN_STREAM",
            id="clean",
        ),
        pytest.param({"frac": 0.5, "provider": ""}, None, id="empty_string"),
        pytest.param({"frac": 0.5, "provider": "  padded "}, None, id="padded"),
        pytest.param({"frac": 0.5, "provider": 123}, None, id="non_string"),
        pytest.param({"frac": 0.5, "provider": None}, None, id="null"),
        pytest.param({"frac": 0.5}, None, id="missing"),
    ],
)
def test_provider_per_block(block: dict[str, Any], expected: str | None) -> None:
    mv = _extract({"soc": block})[Metric.SOC]
    assert mv.value == 50.0
    assert mv.provider == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("RIVIAN_STREAM", True, id="clean"),
        pytest.param("", False, id="empty"),
        pytest.param(" DERIVED", False, id="leading_space"),
        pytest.param("DERIVED ", False, id="trailing_space"),
        pytest.param("\t DERIVED \n", False, id="ascii_whitespace_mix"),
        # NBSP is ``isspace`` -> stripped -> guard REJECTS (loud at boundary).
        pytest.param("\u00a0DERIVED", False, id="nbsp_padded_rejected"),
        # ZWS-family survives ``.strip()`` -> intentionally "clean" (the
        # mismatch surfaces downstream against the closed ASCII enum).
        pytest.param("\u200bDERIVED", True, id="zws_padded_slips_through"),
        pytest.param(None, False, id="none"),
        pytest.param(123, False, id="int"),
        pytest.param(True, False, id="bool"),
    ],
)
def test_is_clean_provider_str(value: object, expected: bool) -> None:
    assert is_clean_provider_str(value) is expected


# ---------- unknown chargingState warning dedup (caller-owned set) -----------


def test_unknown_charging_state_warns_once_per_set(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """One shared set -> exactly one warning across repeated frames."""
    caplog.set_level(logging.WARNING, logger="aioabrp._extract")
    seen: set[str] = set()
    frame = {"chargingState": {"state": "FOO"}}
    assert Metric.CHARGING_STATE not in extract_metrics(
        frame,
        unknown_charging_states_seen=seen,
        unknown_driving_states_seen=set(),
    )
    assert Metric.CHARGING_STATE not in extract_metrics(
        frame,
        unknown_charging_states_seen=seen,
        unknown_driving_states_seen=set(),
    )
    warnings = [r for r in caplog.records if "FOO" in r.getMessage()]
    assert len(warnings) == 1
    assert seen == {"FOO"}


def test_unknown_charging_state_dedup_is_per_set_not_global(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Two caller-owned sets (two instances) each warn once — no module-global."""
    caplog.set_level(logging.WARNING, logger="aioabrp._extract")
    frame = {"chargingState": {"state": "FOO"}}
    extract_metrics(
        frame,
        unknown_charging_states_seen=set(),
        unknown_driving_states_seen=set(),
    )
    extract_metrics(
        frame,
        unknown_charging_states_seen=set(),
        unknown_driving_states_seen=set(),
    )
    warnings = [r for r in caplog.records if "FOO" in r.getMessage()]
    assert len(warnings) == 2


def test_unknown_charging_state_warning_log_name_prefix(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger="aioabrp._extract")
    frame = {
        "chargingState": {"state": "FOO"},
        "soc": {"frac": 0.5, "provider": "RIVIAN_STREAM"},
    }
    extract_metrics(
        frame,
        unknown_charging_states_seen=set(),
        unknown_driving_states_seen=set(),
        log_name="acct-1",
    )
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    assert message.startswith("acct-1")
    assert "FOO" in message
    # PII contract: nothing from the frame but the state string itself.
    assert "RIVIAN_STREAM" not in message
    assert "0.5" not in message


def test_known_charging_state_never_warns(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger="aioabrp._extract")
    seen: set[str] = set()
    result = extract_metrics(
        {"chargingState": {"state": "NOT_CHARGING"}},
        unknown_charging_states_seen=seen,
        unknown_driving_states_seen=set(),
    )
    assert result[Metric.CHARGING_STATE].value is ChargingState.NOT_CHARGING
    assert not caplog.records
    assert not seen


# ---------- added scalar metrics: units + tolerance --------------------------


@pytest.mark.parametrize(
    ("metric", "frame", "expected"),
    [
        # Happy path, raw wire units (no conversion).
        pytest.param(
            Metric.CABIN_SET_POINT, {"cabinSetPoint": {"c": 21.0}}, 21.0, id="cabin_sp"
        ),
        pytest.param(
            Metric.CABIN_TEMPERATURE,
            {"cabinTemperature": {"c": 19.5}},
            19.5,
            id="cabin_temp",
        ),
        pytest.param(
            Metric.EXTERNAL_TEMPERATURE,
            {"externalTemperature": {"c": -3.0}},
            -3.0,
            id="ext_temp_subzero",
        ),
        pytest.param(
            Metric.CALIBRATED_MAX_SPEED,
            {"calibratedMaxSpeed": {"ms": 55.0}},
            55.0,
            id="max_speed_ms",
        ),
        pytest.param(Metric.SPEED, {"speed": {"ms": 27.5}}, 27.5, id="speed_ms"),
        pytest.param(
            Metric.CHARGING_ENERGY_ADDED,
            {"chargingEnergyAdded": {"wh": 8000.0}},
            8000.0,
            id="energy_added_wh",
        ),
        pytest.param(
            Metric.CURRENT, {"current": {"a": -150.0}}, -150.0, id="current_a"
        ),
        pytest.param(
            Metric.ELEVATION, {"elevation": {"m": 42.0}}, 42.0, id="elevation_m"
        ),
        pytest.param(
            Metric.HEADING, {"heading": {"degrees": 270.0}}, 270.0, id="heading"
        ),
        pytest.param(
            Metric.HVAC_POWER, {"hvacPower": {"w": 1500.0}}, 1500.0, id="hvac_w"
        ),
        # Degenerate shapes omit (shares the numeric tolerance matrix).
        pytest.param(Metric.HEADING, {}, None, id="heading_absent"),
        pytest.param(Metric.HEADING, {"heading": None}, None, id="heading_null"),
        pytest.param(Metric.HEADING, {"heading": {}}, None, id="heading_empty"),
        pytest.param(
            Metric.HEADING,
            {"heading": {"degrees": None}},
            None,
            id="heading_inner_null",
        ),
        pytest.param(
            Metric.HEADING,
            {"heading": {"degrees": True}},
            None,
            id="heading_inner_bool",
        ),
        pytest.param(Metric.SPEED, {"speed": {"ms": "27"}}, None, id="speed_inner_str"),
        pytest.param(
            Metric.CURRENT,
            {"current": {"a": float("inf")}},
            None,
            id="current_inner_inf",
        ),
    ],
)
def test_added_scalar_metrics(
    metric: Metric, frame: dict[str, Any], expected: float | None
) -> None:
    result = _extract(frame)
    if expected is None:
        assert metric not in result
    else:
        assert result[metric].value == expected


def test_speed_factor_passes_through_unscaled() -> None:
    """speed_factor uses leaf ``frac`` but is a raw multiplier — NOT x100."""
    result = _extract({"speedFactor": {"frac": 0.9}})
    assert result[Metric.SPEED_FACTOR].value == 0.9


def test_added_scalar_int_coerced_to_float() -> None:
    value = _extract({"elevation": {"m": 42}})[Metric.ELEVATION].value
    assert value == 42.0
    assert isinstance(value, float)


# ---------- driving_state (categorical, own dedup set) -----------------------


@pytest.mark.parametrize(
    ("wire_member", "expected"),
    [
        ("PARK", DrivingState.PARK),
        ("REVERSE", DrivingState.REVERSE),
        ("NEUTRAL", DrivingState.NEUTRAL),
        ("DRIVE", DrivingState.DRIVE),
    ],
)
def test_driving_state_member_mapping(wire_member: str, expected: DrivingState) -> None:
    result = _extract({"drivingState": {"state": wire_member}})
    assert result[Metric.DRIVING_STATE].value is expected


@pytest.mark.parametrize(
    "frame",
    [
        pytest.param({}, id="absent"),
        pytest.param({"drivingState": None}, id="null"),
        pytest.param({"drivingState": {}}, id="empty"),
        pytest.param({"drivingState": {"state": None}}, id="inner_null"),
        pytest.param({"drivingState": {"state": ""}}, id="inner_empty_string"),
        pytest.param({"drivingState": {"state": 123}}, id="inner_int"),
        pytest.param({"drivingState": {"state": "FLYING"}}, id="unrecognized_member"),
    ],
)
def test_driving_state_degenerate_shapes_omitted(frame: dict[str, Any]) -> None:
    assert Metric.DRIVING_STATE not in _extract(frame)


def test_unknown_driving_state_warns_once_per_set(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger="aioabrp._extract")
    seen: set[str] = set()
    frame = {"drivingState": {"state": "FLYING"}}
    for _ in range(2):
        assert Metric.DRIVING_STATE not in extract_metrics(
            frame,
            unknown_charging_states_seen=set(),
            unknown_driving_states_seen=seen,
        )
    warnings = [r for r in caplog.records if "FLYING" in r.getMessage()]
    assert len(warnings) == 1
    assert seen == {"FLYING"}


def test_driving_and_charging_drift_use_separate_sets(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A driving-state set must not suppress a charging-state warning, or vice versa."""
    caplog.set_level(logging.WARNING, logger="aioabrp._extract")
    charging_seen: set[str] = set()
    driving_seen: set[str] = set()
    frame = {
        "chargingState": {"state": "WARP"},
        "drivingState": {"state": "WARP"},
    }
    extract_metrics(
        frame,
        unknown_charging_states_seen=charging_seen,
        unknown_driving_states_seen=driving_seen,
    )
    messages = [r.getMessage() for r in caplog.records]
    # The same wire token "WARP" drifts on both enums — each warns once,
    # tracked in its own set.
    assert charging_seen == {"WARP"}
    assert driving_seen == {"WARP"}
    assert sum("chargingState" in m for m in messages) == 1
    assert sum("drivingState" in m for m in messages) == 1


def test_unknown_driving_state_warning_is_token_only(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger="aioabrp._extract")
    frame = {
        "drivingState": {"state": "FLYING", "provider": "RIVIAN_STREAM"},
        "mapInfo": {"address": "Kungsgatan"},
    }
    extract_metrics(
        frame,
        unknown_charging_states_seen=set(),
        unknown_driving_states_seen=set(),
        log_name="acct-2",
    )
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    assert message.startswith("acct-2")
    assert "FLYING" in message
    # PII contract: never the address or any other payload content.
    assert "Kungsgatan" not in message
    assert "RIVIAN_STREAM" not in message


# ---------- calibrated_confidence (array -> tuple) ---------------------------


@pytest.mark.parametrize(
    ("frame", "expected"),
    [
        pytest.param(
            {"calibratedConfidence": {"frac": [0.8]}}, (0.8,), id="single_value"
        ),
        pytest.param(
            {"calibratedConfidence": {"frac": [0.8, 0.85, 0.9, 0.95]}},
            (0.8, 0.85, 0.9, 0.95),
            id="four_values",
        ),
        pytest.param(
            {"calibratedConfidence": {"frac": [1, 0]}},
            (1.0, 0.0),
            id="ints_coerced_to_float",
        ),
    ],
)
def test_calibrated_confidence_happy(
    frame: dict[str, Any], expected: tuple[float, ...]
) -> None:
    value = _extract(frame)[Metric.CALIBRATED_CONFIDENCE].value
    assert value == expected
    assert all(isinstance(v, float) for v in value)


@pytest.mark.parametrize(
    "frame",
    [
        pytest.param({}, id="absent"),
        pytest.param({"calibratedConfidence": None}, id="null"),
        pytest.param({"calibratedConfidence": {}}, id="empty_block"),
        pytest.param({"calibratedConfidence": {"frac": None}}, id="leaf_null"),
        pytest.param({"calibratedConfidence": {"frac": []}}, id="empty_array"),
        pytest.param({"calibratedConfidence": {"frac": 0.8}}, id="leaf_not_a_list"),
        pytest.param(
            {"calibratedConfidence": {"frac": [0.8, None]}}, id="element_null"
        ),
        pytest.param({"calibratedConfidence": {"frac": [0.8, "x"]}}, id="element_str"),
        pytest.param(
            {"calibratedConfidence": {"frac": [0.8, True]}}, id="element_bool"
        ),
        pytest.param(
            {"calibratedConfidence": {"frac": [0.8, float("nan")]}},
            id="element_nan",
        ),
    ],
)
def test_calibrated_confidence_degenerate_shapes_omitted(
    frame: dict[str, Any],
) -> None:
    assert Metric.CALIBRATED_CONFIDENCE not in _extract(frame)


# ---------- map_info (struct, per-subfield tolerance) ------------------------


def test_map_info_full_block() -> None:
    result = _extract(
        {
            "mapInfo": {
                "region": "EUROPE",
                "country_3": "SWE",
                "address": "Kungsgatan",
                "speedLimitMs": 25.0,
                "isFreeSpeedZone": False,
            }
        }
    )
    assert result[Metric.MAP_INFO].value == MapInfo(
        region=Region.EUROPE,
        country_3="SWE",
        address="Kungsgatan",
        speed_limit_ms=25.0,
        is_free_speed_zone=False,
    )


def test_map_info_partial_block_keeps_present_subfields() -> None:
    result = _extract({"mapInfo": {"country_3": "USA", "speedLimitMs": 30}})
    assert result[Metric.MAP_INFO].value == MapInfo(
        region=None,
        country_3="USA",
        address=None,
        speed_limit_ms=30.0,
        is_free_speed_zone=None,
    )


def test_map_info_unknown_region_degrades_to_none_keeping_rest() -> None:
    """An unrecognized region nulls only that subfield — the block survives."""
    result = _extract({"mapInfo": {"region": "ATLANTIS", "country_3": "SWE"}})
    assert result[Metric.MAP_INFO].value == MapInfo(
        region=None,
        country_3="SWE",
        address=None,
        speed_limit_ms=None,
        is_free_speed_zone=None,
    )


def test_map_info_falsy_valid_subfields_are_present_not_omitted() -> None:
    """speed_limit_ms=0.0 and is_free_speed_zone=False are real values, not 'absent'."""
    result = _extract({"mapInfo": {"speedLimitMs": 0.0, "isFreeSpeedZone": False}})
    value = result[Metric.MAP_INFO].value
    assert value.speed_limit_ms == 0.0
    assert value.is_free_speed_zone is False


@pytest.mark.parametrize(
    "frame",
    [
        pytest.param({}, id="absent"),
        pytest.param({"mapInfo": None}, id="null"),
        pytest.param({"mapInfo": {}}, id="empty_block"),
        pytest.param({"mapInfo": [1, 2]}, id="non_dict"),
        # Dict present but every subfield malformed -> omit (no all-None struct).
        pytest.param(
            {
                "mapInfo": {
                    "region": "ATLANTIS",
                    "country_3": 7,
                    "address": None,
                    "speedLimitMs": "fast",
                    "isFreeSpeedZone": "yes",
                }
            },
            id="all_subfields_malformed",
        ),
    ],
)
def test_map_info_omitted_when_no_subfield_survives(frame: dict[str, Any]) -> None:
    assert Metric.MAP_INFO not in _extract(frame)


def test_map_info_is_free_speed_zone_requires_genuine_bool() -> None:
    """The boolean subfield rejects truthy non-bools (1/"true") -> None."""
    result = _extract({"mapInfo": {"isFreeSpeedZone": 1, "country_3": "SWE"}})
    assert result[Metric.MAP_INFO].value.is_free_speed_zone is None


def test_added_struct_metrics_carry_time_and_provider() -> None:
    """map_info / calibrated_confidence flow through the same time+provider path."""
    extra = {"time": "2026-05-25T12:00:00Z", "provider": "RIVIAN_STREAM"}
    result = _extract(
        {
            "mapInfo": {"country_3": "SWE", **extra},
            "calibratedConfidence": {"frac": [0.9], **extra},
        }
    )
    for metric in (Metric.MAP_INFO, Metric.CALIBRATED_CONFIDENCE):
        mv = result[metric]
        assert mv.time == datetime(2026, 5, 25, 12, tzinfo=UTC)
        assert mv.provider == "RIVIAN_STREAM"
