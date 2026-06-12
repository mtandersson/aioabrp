"""Tests pinning aioabrp's public API surface."""

import aioabrp

# The PLAN.md Workstream A public surface — exact set, no more, no less.
EXPECTED_PUBLIC_API = {
    "AbstractAuth",
    "StaticAuth",
    "AbrpClient",
    "TelemetryStream",
    "AbrpVehicle",
    "CatalogEntry",
    "Metric",
    "MetricValue",
    "Telemetry",
    "ChargingState",
    "Location",
    "ConnectionState",
    "ConnectionEvent",
    "AbrpError",
    "AbrpAuthError",
    "AbrpApiError",
    "__version__",
}


def test_all_is_exactly_the_planned_surface() -> None:
    assert set(aioabrp.__all__) == EXPECTED_PUBLIC_API


def test_every_public_name_imports_from_top_level() -> None:
    for name in aioabrp.__all__:
        assert getattr(aioabrp, name) is not None, name


def test_star_import_exposes_exactly_the_public_surface() -> None:
    namespace: dict[str, object] = {}
    exec("from aioabrp import *", namespace)
    imported = set(namespace) - {"__builtins__"}
    assert imported == EXPECTED_PUBLIC_API


def test_no_private_module_or_internal_symbol_leaks() -> None:
    for private_module in ("_wire_types", "_extract", "_sse"):
        assert private_module not in aioabrp.__all__
    # Internal symbols must not be re-exported at the top level.
    for internal in (
        "extract_metrics",
        "parse_block_time",
        "is_clean_provider_str",
        "iter_sse_events",
        "parse_sse_event",
        "OutputPoint",
        "WIRE_KEYS",
    ):
        assert internal not in aioabrp.__all__
        assert not hasattr(aioabrp, internal), internal
