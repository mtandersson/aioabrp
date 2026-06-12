"""Injected authentication for aioabrp.

The library never sees refresh tokens or OAuth endpoints; the consumer
owns the token lifecycle and hands the library a fresh access token via
:class:`AbstractAuth`.
"""

from abc import ABC, abstractmethod


class AbstractAuth(ABC):
    """Provides a fresh access token on demand.

    Implementations MUST raise :class:`aioabrp.AbrpAuthError` for terminal
    auth failure (e.g. a revoked refresh token). Any other exception is
    treated as transient by the library.
    """

    @abstractmethod
    async def async_get_access_token(self) -> str:
        """Return a valid access token."""


class StaticAuth(AbstractAuth):
    """A fixed-token :class:`AbstractAuth` for scripts and pre-OAuth flows."""

    def __init__(self, token: str) -> None:
        """Store the fixed token."""
        self._token = token

    async def async_get_access_token(self) -> str:
        """Return the fixed token."""
        return self._token
