"""Injected authentication for aioabrp.

The library never sees refresh tokens or OAuth endpoints; the consumer
owns the token lifecycle and hands the library a fresh access token via
:class:`AbstractAuth`.
"""

import base64
import binascii
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass

from .exceptions import AbrpAuthError


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


@dataclass(frozen=True)
class AbrpIdentity:
    """Identity extracted from an ABRP OIDC ``id_token``."""

    subject: str
    """OIDC ``sub`` claim — stable and non-empty; use as the entry unique id."""

    display_name: str | None
    """``name`` -> ``email`` claim chain, stripped; ``None`` if neither qualifies."""


def parse_unverified_identity(id_token: str) -> AbrpIdentity:
    """Extract identity from an ABRP ``id_token`` WITHOUT verifying its signature.

    Skipping signature verification is safe here because the token arrives over
    TLS directly from the issuer during the OAuth code exchange; we only read
    claims and never make a trust decision on the token itself. This keeps the
    library dependency-free (no JWT/crypto stack). The ``unverified`` in the name
    is the contract: callers must not treat the result as authenticated.

    Raises :class:`AbrpAuthError` on any malformed token or a missing/empty
    ``sub``. A ``None`` ``display_name`` is normal, not an error.
    """
    try:
        payload_b64 = id_token.split(".")[1]
        # Restore base64 padding; urlsafe b64 in JWTs omits trailing ``=``.
        payload_b64 += "=" * (-len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
    except (AttributeError, IndexError, TypeError, ValueError, binascii.Error) as exc:
        # AttributeError/TypeError cover a non-``str`` argument (e.g. None/bytes);
        # the rest cover a malformed/undecodable token. binascii.Error and
        # json.JSONDecodeError are both ValueError subclasses (listed for clarity).
        raise AbrpAuthError("Malformed id_token") from exc

    # ``sub`` is an opaque issuer-assigned id: keep it verbatim, never strip it
    # (unlike ``display_name`` below). Only reject missing/non-str/empty values.
    subject = payload.get("sub") if isinstance(payload, dict) else None
    if not isinstance(subject, str) or not subject:
        raise AbrpAuthError("id_token has no usable 'sub' claim")

    display_name: str | None = None
    for key in ("name", "email"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            display_name = value.strip()
            break

    return AbrpIdentity(subject=subject, display_name=display_name)
