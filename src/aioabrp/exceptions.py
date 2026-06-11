"""Exceptions raised by aioabrp."""


class AbrpError(Exception):
    """Base for all aioabrp errors."""


class AbrpAuthError(AbrpError):
    """Authentication or session failure (terminal — do not retry)."""


class AbrpApiError(AbrpError):
    """Non-auth API failure (transport, malformed response, business error)."""
