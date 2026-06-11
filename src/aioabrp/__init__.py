"""Async Python client for the A Better Routeplanner (ABRP) / Iternio telemetry API."""

from .auth import AbstractAuth, StaticAuth
from .exceptions import AbrpApiError, AbrpAuthError, AbrpError
from .models import (
    AbrpVehicle,
    CatalogEntry,
    ChargingState,
    ConnectionEvent,
    ConnectionState,
    Location,
    Metric,
    MetricValue,
)

__version__ = "0.1.0"

__all__ = [
    "AbrpApiError",
    "AbrpAuthError",
    "AbrpError",
    "AbrpVehicle",
    "AbstractAuth",
    "CatalogEntry",
    "ChargingState",
    "ConnectionEvent",
    "ConnectionState",
    "Location",
    "Metric",
    "MetricValue",
    "StaticAuth",
    "__version__",
]
