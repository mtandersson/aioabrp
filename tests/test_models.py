"""Tests for aioabrp's public models, exceptions, and auth helpers."""

import dataclasses

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
)


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
    ],
)
def test_models_are_frozen(instance: object) -> None:
    field = dataclasses.fields(instance)[0].name  # type: ignore[arg-type]
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(instance, field, "nope")


async def test_static_auth_returns_token() -> None:
    auth = StaticAuth("tok-123")
    assert await auth.async_get_access_token() == "tok-123"
