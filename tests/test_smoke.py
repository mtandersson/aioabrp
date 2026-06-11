"""Smoke tests proving the packaging and test toolchain works end-to-end."""

from importlib.metadata import version

import aiohttp
from aioresponses import aioresponses

import aioabrp


def test_version() -> None:
    """The package imports and its version matches the installed metadata."""
    assert aioabrp.__version__ == version("aioabrp")


async def test_aioresponses_mocks_aiohttp_get() -> None:
    """Verify aioresponses can mock an aiohttp GET on this Python version."""
    with aioresponses() as mocked:
        mocked.get("https://api.example.invalid/ping", payload={"ok": True})
        async with (
            aiohttp.ClientSession() as session,
            session.get("https://api.example.invalid/ping") as response,
        ):
            assert response.status == 200
            assert await response.json() == {"ok": True}
