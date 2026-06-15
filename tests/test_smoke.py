"""Smoke tests proving the packaging and test toolchain works end-to-end."""

from importlib.metadata import version

import aiohttp
from aioresponses import aioresponses
from packaging.version import Version

import aioabrp


def test_version() -> None:
    """The installed package exposes a real, well-formed version.

    With the version derived from the git tag via hatch-vcs, ``__version__``
    and the installed metadata resolve from the same source, so equality is
    tautological. The load-bearing checks are that the package is actually
    installed under its declared name (not the uninstalled ``0.0.0``
    fallback) and that the resolved version is valid PEP 440.
    """
    assert aioabrp.__version__ == version("aioabrp")
    assert aioabrp.__version__ != "0.0.0"
    # Parses as a valid PEP 440 version (raises InvalidVersion otherwise).
    Version(aioabrp.__version__)


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
