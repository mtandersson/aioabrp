"""Tests for aioabrp.auth: identity extraction from the ABRP OIDC id_token.

``parse_unverified_identity`` decodes (without signature verification) the
payload segment of an ``id_token`` to yield an :class:`AbrpIdentity`:

* ``subject`` — the OIDC ``sub`` claim, a stable, non-empty unique id.
* ``display_name`` — the ``name`` -> ``email`` display-claim chain, stripped,
  or ``None`` when neither qualifies.

Any malformed token, or a missing/empty ``sub``, raises ``AbrpAuthError``.
Tokens are hand-built locally so the tests need no real identity provider.
"""

import base64
import json
from typing import Any

import pytest

from aioabrp.auth import AbrpIdentity, parse_unverified_identity
from aioabrp.exceptions import AbrpAuthError


def _make_token(payload: dict[str, Any]) -> str:
    """Build a fake 3-segment JWT (``hdr.payload.sig``) from a claims dict.

    The payload is base64url-encoded with ``=`` padding stripped, exactly as
    real JWTs emit it, exercising the function's padding-restoration path.
    """
    raw = json.dumps(payload).encode()
    payload_b64 = base64.urlsafe_b64encode(raw).rstrip(b"=").decode()
    return f"hdr.{payload_b64}.sig"


def test_subject_and_name() -> None:
    identity = parse_unverified_identity(
        _make_token({"sub": "user-123", "name": "Alice", "email": "a@example.com"})
    )
    assert identity == AbrpIdentity(subject="user-123", display_name="Alice")


def test_email_used_when_no_name() -> None:
    identity = parse_unverified_identity(
        _make_token({"sub": "user-123", "email": "a@example.com"})
    )
    assert identity.display_name == "a@example.com"


def test_display_name_none_when_no_name_or_email() -> None:
    identity = parse_unverified_identity(_make_token({"sub": "user-123"}))
    assert identity.subject == "user-123"
    assert identity.display_name is None


def test_whitespace_name_falls_through_to_email() -> None:
    identity = parse_unverified_identity(
        _make_token({"sub": "user-123", "name": "   ", "email": "a@example.com"})
    )
    assert identity.display_name == "a@example.com"


def test_non_string_name_is_ignored() -> None:
    identity = parse_unverified_identity(
        _make_token({"sub": "user-123", "name": 123, "email": "a@example.com"})
    )
    assert identity.display_name == "a@example.com"


def test_display_name_is_stripped() -> None:
    identity = parse_unverified_identity(
        _make_token({"sub": "user-123", "name": " Alice "})
    )
    assert identity.display_name == "Alice"


def test_unicode_display_name_preserved() -> None:
    # Guards the json/base64 round-trip against an encoding-breaking refactor.
    identity = parse_unverified_identity(
        _make_token({"sub": "user-123", "name": "José 🚗"})
    )
    assert identity.display_name == "José 🚗"


def test_whitespace_only_sub_is_preserved_verbatim() -> None:
    # ``sub`` is opaque: kept as-is, never stripped (unlike ``display_name``).
    identity = parse_unverified_identity(_make_token({"sub": "   "}))
    assert identity.subject == "   "


def test_payload_needing_base64_padding_decodes() -> None:
    # This payload's base64 length is not a multiple of 4, so the function must
    # restore the stripped ``=`` padding to decode it.
    token = _make_token({"sub": "ab"})
    payload_b64 = token.split(".")[1]
    assert len(payload_b64) % 4 != 0
    assert parse_unverified_identity(token).subject == "ab"


def test_missing_sub_raises() -> None:
    with pytest.raises(AbrpAuthError):
        parse_unverified_identity(_make_token({"name": "Alice"}))


def test_empty_sub_raises() -> None:
    with pytest.raises(AbrpAuthError):
        parse_unverified_identity(_make_token({"sub": ""}))


def test_non_string_sub_raises() -> None:
    with pytest.raises(AbrpAuthError):
        parse_unverified_identity(_make_token({"sub": 12345}))


def test_non_str_token_raises() -> None:
    # A non-``str`` argument is normalized to AbrpAuthError, not a raw TypeError.
    with pytest.raises(AbrpAuthError):
        parse_unverified_identity(None)  # type: ignore[arg-type]


def test_not_a_jwt_raises() -> None:
    with pytest.raises(AbrpAuthError):
        parse_unverified_identity("not-a-jwt")


def test_corrupt_base64_payload_raises() -> None:
    with pytest.raises(AbrpAuthError) as excinfo:
        parse_unverified_identity("hdr.!!!not base64!!!.sig")
    # The underlying decode error is chained as the cause (repo convention).
    assert excinfo.value.__cause__ is not None


def test_non_json_payload_raises() -> None:
    payload_b64 = base64.urlsafe_b64encode(b"not json").rstrip(b"=").decode()
    with pytest.raises(AbrpAuthError):
        parse_unverified_identity(f"hdr.{payload_b64}.sig")


def test_json_payload_not_an_object_raises() -> None:
    # Valid JSON, but a list rather than an object -> no usable ``sub``.
    payload_b64 = base64.urlsafe_b64encode(b"[1, 2, 3]").rstrip(b"=").decode()
    with pytest.raises(AbrpAuthError):
        parse_unverified_identity(f"hdr.{payload_b64}.sig")
