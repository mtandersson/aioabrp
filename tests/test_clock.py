"""Tests for the pure clock-clamp helpers in aioabrp._clock."""

from datetime import UTC, datetime

from aioabrp._clock import _clamp_time, clamp_future_times
from aioabrp.models import Location, Metric, MetricValue

NOW = datetime(2026, 6, 12, 12, 0, 0, tzinfo=UTC)
PAST = datetime(2026, 6, 12, 11, 0, 0, tzinfo=UTC)
FUTURE = datetime(2026, 6, 12, 13, 0, 0, tzinfo=UTC)


def test_clamp_time_passes_through_none_and_past_and_equal() -> None:
    assert _clamp_time(None, NOW) is None
    assert _clamp_time(PAST, NOW) == PAST
    assert _clamp_time(NOW, NOW) == NOW


def test_clamp_time_rewrites_future_to_now() -> None:
    assert _clamp_time(FUTURE, NOW) == NOW


def test_clamp_future_times_rewrites_only_future_entries() -> None:
    extracted = {
        Metric.SOC: MetricValue(value=50.0, time=FUTURE, provider="P"),
        Metric.POWER: MetricValue(value=10.0, time=PAST, provider=None),
        Metric.VOLTAGE: MetricValue(value=400.0, time=None, provider=None),
    }
    out = clamp_future_times(extracted, NOW)
    assert out[Metric.SOC] == MetricValue(value=50.0, time=NOW, provider="P")
    assert out[Metric.POWER] == MetricValue(value=10.0, time=PAST, provider=None)
    assert out[Metric.VOLTAGE] == MetricValue(value=400.0, time=None, provider=None)


def test_clamp_future_times_is_pure_no_clock_read() -> None:
    loc = MetricValue(value=Location(lat=1.0, lon=2.0), time=FUTURE, provider=None)
    out = clamp_future_times({Metric.LOCATION: loc}, NOW)
    assert out[Metric.LOCATION].time == NOW
    assert out[Metric.LOCATION].value == Location(lat=1.0, lon=2.0)
