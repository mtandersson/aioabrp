"""Exceptions raised by aioabrp."""


class AbrpError(Exception):
    """Base for all aioabrp errors."""


class AbrpAuthError(AbrpError):
    """Authentication or session failure (terminal — do not retry)."""


class AbrpScopeError(AbrpAuthError):
    """The token lacks a scope the endpoint needs (HTTP 403 insufficient_scope).

    ``scope`` is the scope ABRP names as required, or ``None``. Re-authorizing
    with that scope fixes it; the token itself is still valid.
    """

    def __init__(self, message: str, scope: str | None = None) -> None:
        """Store the message and the required scope."""
        super().__init__(message)
        self.scope = scope


class AbrpApiError(AbrpError):
    """Non-auth API failure (transport, malformed response, business error)."""


class AbrpForbiddenError(AbrpApiError):
    """HTTP 403 for a valid session, e.g. a vehicle neither owned nor shared.

    Not an :class:`AbrpAuthError`: re-authenticating does not fix it.
    """
