"""Tests for aioabrp's public models, exceptions, and auth helpers."""

import dataclasses
from datetime import datetime

import pytest

from aioabrp import (
    AbrpApiError,
    AbrpAuthError,
    AbrpError,
    AbrpVehicle,
    CatalogEntry,
    ChargingState,
    ConnectionEvent,
    ConnectionState,
    DrivingState,
    Location,
    MapInfo,
    Metric,
    MetricValue,
    Region,
    StaticAuth,
    Telemetry,
    VehicleModelDisplay,
)


def _mv[T](
    value: T,
    t: str = "2026-05-25T00:00:00+00:00",
    provider: str = "RIVIAN_STREAM",
) -> MetricValue[T]:
    return MetricValue(value=value, time=datetime.fromisoformat(t), provider=provider)


def test_exception_hierarchy() -> None:
    assert issubclass(AbrpAuthError, AbrpError)
    assert issubclass(AbrpApiError, AbrpError)
    assert issubclass(AbrpError, Exception)


def test_metric_values_match_registry_keys() -> None:
    assert {m.value for m in Metric} == {
        "soc",
        "power",
        "voltage",
        "soe",
        "odometer",
        "calibrated_ref_cons",
        "battery_capacity",
        "soh",
        "range",
        "battery_temperature",
        "charging_state",
        "location",
        "cabin_set_point",
        "cabin_temperature",
        "calibrated_max_speed",
        "charging_energy_added",
        "current",
        "driving_state",
        "elevation",
        "external_temperature",
        "heading",
        "hvac_power",
        "map_info",
        "speed",
        "speed_factor",
        "calibrated_confidence",
    }


def test_every_metric_has_a_telemetry_field() -> None:
    """The Telemetry.items() bridge: every Metric value names a field."""
    field_names = {f.name for f in dataclasses.fields(Telemetry)}
    assert {m.value for m in Metric} <= field_names


def test_charging_state_members() -> None:
    assert {c.value for c in ChargingState} == {
        "charging_ac",
        "charging_dc",
        "charging_unknown",
        "not_charging",
        "plugged_in",
    }


def test_driving_state_members() -> None:
    assert {d.value for d in DrivingState} == {
        "park",
        "reverse",
        "neutral",
        "drive",
    }


def test_region_members() -> None:
    assert {r.value for r in Region} == {
        "africa",
        "asia",
        "australia",
        "central_america",
        "europe",
        "north_america",
        "south_america",
        "oceania",
    }


def test_map_info_carries_optional_subfields() -> None:
    info = MapInfo(
        region=Region.EUROPE,
        country_3="SWE",
        address="Kungsgatan",
        speed_limit_ms=25.0,
        is_free_speed_zone=False,
    )
    assert info.region is Region.EUROPE
    assert info.country_3 == "SWE"
    assert info.address == "Kungsgatan"
    assert info.speed_limit_ms == 25.0
    assert info.is_free_speed_zone is False
    # Every subfield is independently optional.
    assert (
        MapInfo(
            region=None,
            country_3=None,
            address=None,
            speed_limit_ms=None,
            is_free_speed_zone=None,
        ).region
        is None
    )


@pytest.mark.parametrize(
    "instance",
    [
        MetricValue(value=1.0, time=None, provider=None),
        Location(lat=1.0, lon=2.0),
        ConnectionEvent(state=ConnectionState.CONNECTED, reason=None),
        AbrpVehicle(vehicle_id=1, name=None, vehicle_model="x", paint=None),
        CatalogEntry(
            typecode="t",
            manufacturer=None,
            model=None,
            title=None,
            start_year=None,
            end_year=None,
            battery_capacity_wh=None,
        ),
        Telemetry(soc=MetricValue(value=1.0, time=None, provider=None)),
    ],
)
def test_models_are_frozen(instance: object) -> None:
    field = dataclasses.fields(instance)[0].name  # type: ignore[arg-type]
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(instance, field, "nope")


async def test_static_auth_returns_token() -> None:
    auth = StaticAuth("tok-123")
    assert await auth.async_get_access_token() == "tok-123"


def test_empty_telemetry_has_all_none_and_no_items() -> None:
    tlm = Telemetry()
    assert tlm.soc is None
    assert tlm.location is None
    assert list(tlm.items()) == []


def test_items_yields_only_present_fields_keyed_by_metric() -> None:
    tlm = Telemetry(soc=_mv(80.0), power=_mv(-1000.0))
    assert dict(tlm.items()) == {Metric.SOC: tlm.soc, Metric.POWER: tlm.power}


def test_typed_fields_carry_their_value_type() -> None:
    info = MapInfo(
        region=Region.EUROPE,
        country_3="SWE",
        address="Kungsgatan",
        speed_limit_ms=25.0,
        is_free_speed_zone=False,
    )
    tlm = Telemetry(
        soc=_mv(80.0),
        charging_state=_mv(ChargingState.CHARGING_DC),
        location=_mv(Location(lat=1.0, lon=2.0)),
        driving_state=_mv(DrivingState.DRIVE),
        calibrated_confidence=_mv((0.8, 0.85, 0.9, 0.95)),
        map_info=_mv(info),
    )
    assert tlm.soc is not None
    assert tlm.soc.value == 80.0
    assert tlm.charging_state is not None
    assert tlm.charging_state.value is ChargingState.CHARGING_DC
    assert tlm.location is not None
    assert tlm.location.value == Location(lat=1.0, lon=2.0)
    assert tlm.driving_state is not None
    assert tlm.driving_state.value is DrivingState.DRIVE
    assert tlm.calibrated_confidence is not None
    assert tlm.calibrated_confidence.value == (0.8, 0.85, 0.9, 0.95)
    assert tlm.map_info is not None
    assert tlm.map_info.value is info


def test_merge_overlays_present_delta_fields_and_keeps_others() -> None:
    base = Telemetry(soc=_mv(80.0), power=_mv(-1000.0))
    delta = Telemetry(soc=_mv(81.0))
    merged = base.merge(delta)
    assert merged.soc is not None
    assert merged.soc.value == 81.0
    assert merged.power is base.power
    assert base.soc is not None
    assert base.soc.value == 80.0


def test_merge_empty_delta_returns_equivalent() -> None:
    base = Telemetry(soc=_mv(80.0))
    assert base.merge(Telemetry()) is base


def _make_display(
    *,
    manufacturer: str = "Rivian",
    model: str = "R1S",
    years: str = "2025",
    title: str = "Dual Motor",
    start_year: int | None = 2025,
    end_year: int | None = None,
) -> VehicleModelDisplay:
    return VehicleModelDisplay(
        manufacturer=manufacturer,
        model=model,
        years=years,
        title=title,
        start_year=start_year,
        end_year=end_year,
    )


@pytest.mark.parametrize(
    ("display", "expected"),
    [
        pytest.param(
            _make_display(start_year=2024, end_year=2025),
            "Rivian R1S 2024-2025 Dual Motor",
            id="both_years_present_yields_range",
        ),
        pytest.param(
            _make_display(years="2024", start_year=2024, end_year=2024),
            "Rivian R1S 2024 Dual Motor",
            id="equal_years_collapse_to_bare_year",
        ),
        # Deliberate, not a bug: the server's pair is authoritative, never
        # reordered or range-checked. Normalising would invent a year window
        # ABRP never reported. See VehicleModelDisplay.model_name.
        pytest.param(
            _make_display(start_year=2024, end_year=2020),
            "Rivian R1S 2024-2020 Dual Motor",
            id="inverted_years_render_verbatim",
        ),
        pytest.param(
            _make_display(start_year=2025, end_year=None),
            "Rivian R1S 2025 Dual Motor",
            id="start_year_only_yields_bare_year",
        ),
        pytest.param(
            _make_display(start_year=None, end_year=2023),
            "Rivian R1S Dual Motor",
            id="end_year_only_drops_year_segment",
        ),
        pytest.param(
            _make_display(start_year=None, end_year=None),
            "Rivian R1S Dual Motor",
            id="both_years_missing_drops_year_segment",
        ),
        pytest.param(
            _make_display(title="", start_year=2025),
            "Rivian R1S 2025",
            id="empty_title_yields_no_title_segment",
        ),
        pytest.param(
            _make_display(title="   ", start_year=2025),
            "Rivian R1S 2025",
            id="whitespace_title_dropped",
        ),
        pytest.param(
            _make_display(title="  Dual Motor  ", start_year=2025),
            "Rivian R1S 2025 Dual Motor",
            id="padded_title_stripped",
        ),
        pytest.param(
            _make_display(years="2099", start_year=None, end_year=None),
            "Rivian R1S Dual Motor",
            id="years_string_field_ignored",
        ),
        # `years` disagreeing with the parsed pair is what pins "never
        # consulted" — every other case that reaches the year segment has
        # `years` coincidentally equal to `start_year`.
        pytest.param(
            _make_display(years="2099", start_year=2025, end_year=None),
            "Rivian R1S 2025 Dual Motor",
            id="years_string_loses_to_parsed_start_year",
        ),
        pytest.param(
            _make_display(manufacturer="", start_year=2025, title=""),
            " R1S 2025",
            id="manufacturer_joined_verbatim_no_strip",
        ),
    ],
)
def test_vehicle_model_display_name(
    display: VehicleModelDisplay,
    expected: str,
) -> None:
    assert display.display_name == expected


# (id, display, expected model_name) — mirrors the display_name cases above with
# the make stripped. Shared so the delegation invariant runs over the same inputs.
_MODEL_NAME_CASES: list[tuple[str, VehicleModelDisplay, str]] = [
    (
        "both_years_present_yields_range",
        _make_display(start_year=2024, end_year=2025),
        "R1S 2024-2025 Dual Motor",
    ),
    (
        "equal_years_collapse_to_bare_year",
        _make_display(years="2024", start_year=2024, end_year=2024),
        "R1S 2024 Dual Motor",
    ),
    # Deliberate, not a bug — see the note in the display_name grid above.
    (
        "inverted_years_render_verbatim",
        _make_display(start_year=2024, end_year=2020),
        "R1S 2024-2020 Dual Motor",
    ),
    (
        "start_year_only_yields_bare_year",
        _make_display(start_year=2025, end_year=None),
        "R1S 2025 Dual Motor",
    ),
    (
        "end_year_only_drops_year_segment",
        _make_display(start_year=None, end_year=2023),
        "R1S Dual Motor",
    ),
    (
        "both_years_missing_drops_year_segment",
        _make_display(start_year=None, end_year=None),
        "R1S Dual Motor",
    ),
    (
        "empty_title_yields_no_title_segment",
        _make_display(title="", start_year=2025),
        "R1S 2025",
    ),
    (
        "whitespace_title_dropped",
        _make_display(title="   ", start_year=2025),
        "R1S 2025",
    ),
    (
        "padded_title_stripped",
        _make_display(title="  Dual Motor  ", start_year=2025),
        "R1S 2025 Dual Motor",
    ),
    (
        "years_string_field_ignored",
        _make_display(years="2099", start_year=None, end_year=None),
        "R1S Dual Motor",
    ),
    # See the note in the display_name grid above.
    (
        "years_string_loses_to_parsed_start_year",
        _make_display(years="2099", start_year=2025, end_year=None),
        "R1S 2025 Dual Motor",
    ),
    (
        "manufacturer_absent_from_model_name",
        _make_display(manufacturer="", start_year=2025, title=""),
        "R1S 2025",
    ),
]


@pytest.mark.parametrize(
    ("display", "expected"),
    [
        pytest.param(display, expected, id=id_)
        for id_, display, expected in _MODEL_NAME_CASES
    ],
)
def test_vehicle_model_model_name(
    display: VehicleModelDisplay,
    expected: str,
) -> None:
    assert display.model_name == expected


@pytest.mark.parametrize(
    "display",
    [pytest.param(display, id=id_) for id_, display, _ in _MODEL_NAME_CASES],
)
def test_vehicle_model_display_name_is_manufacturer_plus_model_name(
    display: VehicleModelDisplay,
) -> None:
    assert display.display_name == f"{display.manufacturer} {display.model_name}"
