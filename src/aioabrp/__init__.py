"""Async Python client for the A Better Routeplanner (ABRP) / Iternio telemetry API."""

from importlib.metadata import PackageNotFoundError, version

from .auth import AbrpIdentity, AbstractAuth, StaticAuth, parse_unverified_identity
from .client import AbrpClient
from .exceptions import (
    AbrpApiError,
    AbrpAuthError,
    AbrpError,
    AbrpForbiddenError,
    AbrpScopeError,
)
from .models import (
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
    Provider,
    Region,
    Telemetry,
    TelemetryPoint,
    VehicleModelDisplay,
    VehicleRelation,
)
from .stream import TelemetryStream

try:
    # Resolved from the installed package metadata, which hatch-vcs derives
    # from the git tag at build time. There is no version literal in source.
    __version__ = version("aioabrp")
except PackageNotFoundError:  # pragma: no cover - only when running uninstalled
    __version__ = "0.0.0"

__all__ = [
    "AbrpApiError",
    "AbrpAuthError",
    "AbrpClient",
    "AbrpError",
    "AbrpForbiddenError",
    "AbrpIdentity",
    "AbrpScopeError",
    "AbrpVehicle",
    "AbstractAuth",
    "CatalogEntry",
    "ChargingState",
    "ConnectionEvent",
    "ConnectionState",
    "DrivingState",
    "Location",
    "MapInfo",
    "Metric",
    "MetricValue",
    "Provider",
    "Region",
    "StaticAuth",
    "Telemetry",
    "TelemetryPoint",
    "TelemetryStream",
    "VehicleModelDisplay",
    "VehicleRelation",
    "__version__",
    "parse_unverified_identity",
]
