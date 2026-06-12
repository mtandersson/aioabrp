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
    Location,
    Metric,
    MetricValue,
    StaticAuth,
    Telemetry,
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
    }


def test_charging_state_members() -> None:
    assert {c.value for c in ChargingState} == {
        "charging_ac",
        "charging_dc",
        "charging_unknown",
        "not_charging",
        "plugged_in",
    }


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
    tlm = Telemetry(
        soc=_mv(80.0),
        charging_state=_mv(ChargingState.CHARGING_DC),
        location=_mv(Location(lat=1.0, lon=2.0)),
    )
    assert tlm.soc is not None
    assert tlm.soc.value == 80.0
    assert tlm.charging_state is not None
    assert tlm.charging_state.value is ChargingState.CHARGING_DC
    assert tlm.location is not None
    assert tlm.location.value == Location(lat=1.0, lon=2.0)


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
