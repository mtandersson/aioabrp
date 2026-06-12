"""Wall-clock seam and pure future-time clamp helpers.

Shared by the stream gate, the one-shot getter, and seed validation so a
future-dated wire ``time`` can never escape the library or poison the
monotonicity gate. The clock is a single module-level seam (mirroring
``stream._sleep``): tests monkeypatch ``aioabrp._clock._now`` once and it
governs every clamp site. The clamp helpers are PURE — callers pass the
``now`` snapshot in — so exactly one ``_now()`` read happens per SSE frame,
per construction, and per one-shot call.
"""

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, overload

from .models import Metric, MetricValue


# Single monkeypatch target for all clock-dependent behaviour. Returns a
# tz-aware UTC datetime to match the extractor's tz-aware wire times.
def _now() -> datetime:
    return datetime.now(UTC)


@overload
def _clamp_time(t: datetime, now: datetime) -> datetime: ...
@overload
def _clamp_time(t: None, now: datetime) -> None: ...
def _clamp_time(t: datetime | None, now: datetime) -> datetime | None:
    """Return ``min(t, now)``; ``None`` passes through (no ordering claim)."""
    if t is None or t <= now:
        return t
    return now


def clamp_future_times(
    extracted: dict[Metric, MetricValue[Any]], now: datetime
) -> dict[Metric, MetricValue[Any]]:
    """Rewrite any future block ``time`` to ``now``; everything else untouched."""
    out: dict[Metric, MetricValue[Any]] = {}
    for metric, v in extracted.items():
        clamped = _clamp_time(v.time, now)
        out[metric] = v if clamped is v.time else replace(v, time=clamped)
    return out
