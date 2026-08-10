"""Validation of the per-execution sandbox tokens minted by chat-backend.

Design of record: ``docs/code-execution-security.md`` §4 in genetics-results-suite.

Two properties matter more than anything else in this file:

1. **It does not inherit ``require_auth``'s fail-open.** The shared-secret path deliberately
   keeps serving when ``INTERNAL_API_SECRET`` is unset so a mid-rollout cluster does not go
   dark. That is unacceptable for the sandbox, which is the one caller whose input is
   attacker-authored. A sandbox-shaped bearer is rejected when the signing key is missing, and
   ``require_sandbox_config`` refuses to start the process at all when the sandbox is deployed
   and either secret is missing.

2. **A sandbox-shaped bearer is discriminated on the JOSE ``alg`` header, never on dots.**
   "Three dot-separated segments" also matches every RS256 Google Identity Token; routing on
   it would 401 that entire class of caller (latent here, immediately fatal in results-api).
   Reading the unverified header is safe because it only *selects* a validator — this module
   pins ``algorithms=["HS256"]`` and its own key regardless of what the header claimed, so a
   forged ``alg`` changes which validator rejects a token, not whether it is rejected. It is
   not licence to pass the header's ``alg`` to the decoder, nor to trust ``kid`` or ``iss``
   for anything beyond routing.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import os
import sys
import time
from dataclasses import dataclass

import jwt

logger = logging.getLogger(__name__)

AUDIENCE = "db-api"
ISSUER = "chat-backend"
ALGORITHM = "HS256"

# claims the token is worthless without; `scope` is required to be present but its value is
# not yet interpreted (a hook for later per-view narrowing)
REQUIRED_CLAIMS = ["iss", "aud", "sub", "sid", "jti", "iat", "exp"]

# reject a token whose iat is further in the past than its whole lifetime, so a signing key
# that leaks cannot be used to backdate long-lived tokens
MAX_TOKEN_AGE_SECONDS = 300

# tolerance for minter/verifier clock skew, applied by PyJWT to `exp`, `nbf` and `iat`. The
# 300s ttl already absorbs skew in the past direction; `iat` has no such slack forward, where
# PyJWT >= 2.10 raises ImmatureSignatureError the moment iat > now — and chat-backend and this
# service are separate pods on (soon) separate node pools. Deliberately not applied to the
# MAX_TOKEN_AGE_SECONDS check below, which stays exact.
LEEWAY_SECONDS = 5

SANDBOX_TOKEN_SIGNING_KEY = os.environ.get("SANDBOX_TOKEN_SIGNING_KEY", "")

# a separate required input, deliberately not derived from the signing key being present:
# keying the startup check on the key would leave the both-unset case wide open, where
# db-api boots fail-open and the sandbox reaches it by sending no Authorization header at all
SANDBOX_ENABLED = os.environ.get("SANDBOX_ENABLED", "").strip().lower() in {
    "1",
    "true",
    "yes",
}


@dataclass(frozen=True)
class SandboxPrincipal:
    """An authenticated sandbox execution. ``jti`` is the execution id."""

    user: str
    session_id: str
    execution_id: str
    scope: str
    expires_at: int


class SandboxTokenError(Exception):
    """The bearer was sandbox-shaped and did not validate. Always a hard 401."""


def _b64url_decode(segment: str) -> bytes:
    return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))


def is_sandbox_shaped(token: str) -> bool:
    """True when the bearer's JOSE header declares HS256 — the sandbox token's algorithm.

    Anything else (an RS256 Google Identity Token, an opaque token, junk) is not routed here
    and continues down the pre-existing auth paths untouched.
    """
    if not token or token.count(".") != 2:
        return False
    try:
        header = json.loads(_b64url_decode(token.split(".", 1)[0]))
    except (ValueError, binascii.Error, UnicodeDecodeError):
        return False
    return isinstance(header, dict) and header.get("alg") == ALGORITHM


def verify_sandbox_token(token: str) -> SandboxPrincipal:
    """Validate a sandbox-shaped bearer, or raise :class:`SandboxTokenError`.

    Never falls through to another auth path: a malformed sandbox token must not degrade into
    "is this string equal to the shared secret", which would be a downgrade path.
    """
    if not SANDBOX_TOKEN_SIGNING_KEY:
        raise SandboxTokenError("SANDBOX_TOKEN_SIGNING_KEY is not set")

    try:
        claims = jwt.decode(
            token,
            SANDBOX_TOKEN_SIGNING_KEY,
            algorithms=[ALGORITHM],
            audience=AUDIENCE,
            issuer=ISSUER,
            options={"require": REQUIRED_CLAIMS},
            leeway=LEEWAY_SECONDS,
        )
    except jwt.InvalidTokenError as exc:
        raise SandboxTokenError(f"{type(exc).__name__}: {exc}") from exc

    scope = claims.get("scope")
    if not scope:
        raise SandboxTokenError("missing scope claim")

    # `options={"require": ...}` rejects only missing/null claims, so an empty string passes it
    # and yields a principal attributing the query to nobody. Attribution is the point of the
    # token, so assert the minter's invariant here rather than trusting it.
    for name in ("sub", "sid", "jti"):
        if not claims.get(name):
            raise SandboxTokenError(f"empty {name} claim")

    # PyJWT treats a list `aud` as membership, so {"aud": ["db-api", "results-api"]} would
    # validate at BOTH services — single-destination binding must be a property of the
    # validator, not merely of the minter
    if not isinstance(claims["aud"], str):
        raise SandboxTokenError("aud must be a single string")

    # PyJWT accepts an iat arbitrarily far in the past as long as exp is in the future; a
    # token minted with a long ttl would otherwise outlive the design's 5-minute window
    if int(claims["iat"]) < int(time.time()) - MAX_TOKEN_AGE_SECONDS:
        raise SandboxTokenError("iat too far in the past")

    return SandboxPrincipal(
        user=str(claims["sub"]),
        session_id=str(claims["sid"]),
        execution_id=str(claims["jti"]),
        scope=str(scope),
        expires_at=int(claims["exp"]),
    )


def require_sandbox_config(internal_api_secret: str) -> None:
    """Refuse to start mis-configured while the sandbox is deployed.

    ``SANDBOX_ENABLED`` tracks the sandbox Deployment, not the signing key. Rules 1-5 of the
    design all fire on "a sandbox-shaped bearer", and nothing obliges the sandbox to send one
    — a script that simply omits ``Authorization`` would fall into ``require_auth``'s
    unset-secret early return and be authorized with no ``sid``, ``sub`` or ``jti`` to
    attribute it to anyone. So both secrets are mandatory once the sandbox exists.
    """
    if not SANDBOX_ENABLED:
        return
    missing = [
        name
        for name, value in (
            ("INTERNAL_API_SECRET", internal_api_secret),
            ("SANDBOX_TOKEN_SIGNING_KEY", SANDBOX_TOKEN_SIGNING_KEY),
        )
        if not value
    ]
    if missing:
        logger.error(
            "SANDBOX_ENABLED is true but %s unset: refusing to start fail-open while the "
            "sandbox can reach this service",
            " and ".join(missing),
        )
        sys.exit(1)
