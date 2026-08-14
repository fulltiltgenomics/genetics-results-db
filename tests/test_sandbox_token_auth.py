"""Tests for the per-execution sandbox token path in require_auth.

The design of record is docs/code-execution-security.md section 4 in genetics-results-suite.
The properties that matter are all failure properties: a sandbox-shaped bearer must never
degrade into the shared-secret comparison, must never be accepted when the signing key is
unset, and must never authorize a token minted for the other service.

api.main constructs a BigQuery client at module scope, which is offline (no RPC until a query
runs), so every case here is decided by require_auth before a handler executes.
"""

import base64
import importlib
import json
import os
import sys
import time

import jwt
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

SECRET = "test-internal-secret"
SIGNING_KEY = "test-sandbox-signing-key-that-is-32-bytes+"

# app-level dependencies run after routing, so an unknown path 404s before require_auth ever
# fires; /openapi.json is a real route and is deliberately behind the same dependency
PROTECTED_PATH = "/openapi.json"


def _mint(key=SIGNING_KEY, audience="db-api", issuer="chat-backend", age=0, ttl=300, **over):
    iat = int(time.time()) - age
    claims = {
        "iss": issuer,
        "aud": audience,
        "sub": "user@finngen.fi",
        "sid": "session-abc",
        "jti": "exec-123",
        "iat": iat,
        "exp": iat + ttl,
        "scope": "query:views",
    }
    for k, v in over.items():
        if v is None:
            claims.pop(k, None)
        else:
            claims[k] = v
    return jwt.encode(claims, key, algorithm="HS256")


def _reload(monkeypatch, **env):
    """Re-import api.main under `env`, with every variable it touches restored afterwards.

    Only INTERNAL_API_SECRET still has to be in place *before* the import, and only because
    api.main reads it once at startup to ASCII-validate it and to seed its
    `_authentication_was_configured` latch. The two sandbox variables are read per call
    (genetics-results-suite-l7z), so a test that only needs to change one of those should
    monkeypatch it against the shared `client` fixture instead of reloading anything.

    Restoring still cannot be a context manager that unsets on the way out — the reloaded module
    outlives this call and its app keeps serving. It goes through `monkeypatch` instead, whose
    undo runs when the caller's fixture tears down.

    Restoring is not tidiness. This used to mutate os.environ directly and put nothing back, so
    a `_reload()` that popped INTERNAL_API_SECRET left it popped for everything that ran next —
    and since api.main reads the secret per request (genetics-results-suite-xi6), every
    still-live module-scoped client shared by the other auth tests would have gone on consulting
    the emptied variable. PROJECT_ID is set here too and was equally unrestored.

    Pass the test's own `monkeypatch`, or a `pytest.MonkeyPatch()` that a higher-scoped fixture
    undoes itself — a function-scoped monkeypatch cannot be requested from a module fixture.
    """
    for key in ("INTERNAL_API_SECRET", "SANDBOX_TOKEN_SIGNING_KEY", "SANDBOX_ENABLED"):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        if value is not None:
            monkeypatch.setenv(key, value)
    monkeypatch.setenv("PROJECT_ID", os.environ.get("PROJECT_ID", "test-project"))
    sys.modules.pop("api.main", None)

    return importlib.import_module("api.main")


# module-scoped: re-importing api.main reloads the dataset YAML, and a later _reload() in
# another test cannot disturb this app — it keeps its own module objects and their globals
@pytest.fixture(scope="module")
def client():
    mp = pytest.MonkeyPatch()
    main = _reload(mp, INTERNAL_API_SECRET=SECRET, SANDBOX_TOKEN_SIGNING_KEY=SIGNING_KEY)
    yield TestClient(main.app, raise_server_exceptions=False)
    mp.undo()


def _get(client, token):
    return client.get(PROTECTED_PATH, headers={"Authorization": f"Bearer {token}"})


# --- the happy path, and the claims it depends on -----------------------------------------


def test_valid_sandbox_token_is_authorized(client):
    assert _get(client, _mint()).status_code != 401


def test_expired_token_is_rejected(client):
    """5-minute lifetime is the whole reason a leaked token is worth little."""
    assert _get(client, _mint(age=400, ttl=300)).status_code == 401


def test_backdated_iat_is_rejected(client):
    """PyJWT accepts an ancient iat as long as exp is future; a long-ttl token must not work."""
    assert _get(client, _mint(age=3600, ttl=7200)).status_code == 401


@pytest.mark.parametrize("claim", ["iss", "aud", "sub", "sid", "jti", "iat", "exp", "scope"])
def test_every_required_claim_is_required(client, claim):
    assert _get(client, _mint(**{claim: None})).status_code == 401


def test_foreign_issuer_is_rejected(client):
    assert _get(client, _mint(issuer="someone-else")).status_code == 401


# --- audience binding: a results-api token must not work here ------------------------------


def test_results_api_token_cannot_be_replayed_at_db_api(client):
    """Same signing key, same everything, different aud. This is the replay control."""
    assert _get(client, _mint(audience="results-api")).status_code == 401


def test_multi_audience_token_is_rejected(client):
    """PyJWT reads a list `aud` as membership, so this would otherwise validate at BOTH
    services and one-token-per-audience would be a minter property, not a validator one."""
    assert _get(client, _mint(audience=["db-api", "results-api"])).status_code == 401


def test_single_element_list_audience_is_also_rejected(client):
    assert _get(client, _mint(audience=["db-api"])).status_code == 401


# --- attribution: the token must name somebody -----------------------------------------------


@pytest.mark.parametrize("claim", ["sub", "sid", "jti"])
def test_empty_attribution_claim_is_rejected(client, claim):
    """`options={"require": ...}` only rejects missing/null; an empty string attributes the
    query to nobody, which defeats the point of the credential."""
    assert _get(client, _mint(**{claim: ""})).status_code == 401


# --- clock skew ------------------------------------------------------------------------------


def test_freshly_minted_token_from_a_slightly_fast_minter_is_accepted(client):
    """iat 3s in the future: PyJWT >= 2.10 raises ImmatureSignatureError with leeway=0, and the
    minter and this service are different pods with different clocks."""
    assert _get(client, _mint(age=-3)).status_code != 401


def test_leeway_does_not_widen_the_300s_iat_bound(client):
    """The MAX_TOKEN_AGE_SECONDS check is separate from the decoder and stays exact."""
    assert _get(client, _mint(age=299, ttl=600)).status_code != 401
    assert _get(client, _mint(age=301, ttl=600)).status_code == 401


# --- signature -----------------------------------------------------------------------------


def test_wrong_signing_key_is_rejected(client):
    assert _get(client, _mint(key="a-different-key-also-32-bytes-long!")).status_code == 401


def test_tampered_payload_is_rejected(client):
    token = _mint()
    header, payload, signature = token.split(".")
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    claims["sub"] = "attacker@example.com"
    forged = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    assert _get(client, f"{header}.{forged}.{signature}").status_code == 401


def test_alg_none_is_rejected(client):
    """The decoder pins algorithms=["HS256"]; an unsigned token is not a sandbox token."""
    unsigned = jwt.encode({"iss": "chat-backend"}, key="", algorithm="none")
    assert _get(client, unsigned).status_code == 401


# --- no fallthrough, no downgrade ----------------------------------------------------------


def test_sandbox_shaped_bearer_never_reaches_the_shared_secret_comparison(client):
    """A malformed sandbox token must not degrade into 'is this string equal to the secret'."""
    assert _get(client, "not.a.jwt").status_code == 401
    assert _get(client, SECRET).status_code != 401  # the shared secret still works


def test_signing_key_unset_rejects_every_sandbox_token(client, monkeypatch):
    """Fail closed, not warn-and-continue. Non-HS256 callers are unaffected.

    No reload: the key is read per call (genetics-results-suite-l7z), so unsetting it on a
    live app is the same condition as never having set it — and a strictly better test of it,
    since it is the app the rest of this file uses.

    The 200 first is what makes that true. Without it the test would also pass against an app
    whose sandbox path was already dead for some other reason, and the `delenv` would be
    proving nothing.
    """
    assert _get(client, _mint()).status_code != 401
    monkeypatch.delenv("SANDBOX_TOKEN_SIGNING_KEY")
    assert _get(client, _mint()).status_code == 401
    assert _get(client, SECRET).status_code != 401


def test_sandbox_token_bypasses_the_fail_open_early_return(monkeypatch):
    """With INTERNAL_API_SECRET unset db-api serves anyone — but not a bad sandbox token.

    Rule 1 of the design: route the sandbox-shaped bearer before the unset-secret early
    return, so the fail-open branch is unreachable for such a request.
    """
    main = _reload(monkeypatch, SANDBOX_TOKEN_SIGNING_KEY=SIGNING_KEY)
    c = TestClient(main.app, raise_server_exceptions=False)
    assert c.get(PROTECTED_PATH).status_code != 401  # fail-open, unchanged
    assert _get(c, _mint(key="wrong")).status_code == 401
    assert _get(c, _mint()).status_code != 401


# --- when the signing key is read (genetics-results-suite-l7z) -----------------------------


def test_the_signing_key_is_read_per_request_not_once_at_import(monkeypatch):
    """Import first, set the variable second — the ordering pytest collection actually produces.

    Fails against the old module-scope snapshot, which froze the key to "" for the life of the
    process and rejected every sandbox token forever after. The hazard is the order-dependence
    itself: which test file imported api.sandbox_auth first decided what the sandbox path did.
    """
    main = _reload(monkeypatch, INTERNAL_API_SECRET=SECRET)  # signing key unset at import
    c = TestClient(main.app, raise_server_exceptions=False)
    assert _get(c, _mint()).status_code == 401

    monkeypatch.setenv("SANDBOX_TOKEN_SIGNING_KEY", SIGNING_KEY)
    assert _get(c, _mint()).status_code != 401


def test_a_changed_or_removed_signing_key_can_only_reject_more(client, monkeypatch):
    """Why this accessor is NOT latched, unlike api.main's `_internal_api_secret`.

    That one latches because "" there means "authentication was never configured" and takes a
    fail-OPEN early return, so a per-request read would let an emptied variable *disable* auth.
    The signing key has no such value: every state of it except the exact minting key rejects.
    Both runtime transitions are therefore strictly stricter, which is what this pins — removing
    the key 401s sandbox tokens without opening anything, and rotating it invalidates tokens
    minted under the old key rather than accepting them.
    """
    assert _get(client, _mint()).status_code != 401

    monkeypatch.delenv("SANDBOX_TOKEN_SIGNING_KEY")
    assert _get(client, _mint()).status_code == 401
    assert client.get(PROTECTED_PATH).status_code == 401  # no fail-open appeared
    assert _get(client, SECRET).status_code != 401  # the shared-secret path is untouched

    rotated = "a-rotated-sandbox-signing-key-32-bytes+"
    monkeypatch.setenv("SANDBOX_TOKEN_SIGNING_KEY", rotated)
    assert _get(client, _mint()).status_code == 401
    assert _get(client, _mint(key=rotated)).status_code != 401


# --- keys the crypto layer itself refuses --------------------------------------------------

# PyJWT's HMAC prepare_key refuses a PEM outright (InvalidKeyError, a PyJWTError *sibling* of
# InvalidTokenError, not a subclass) rather than using it as HMAC material
PEM_SHAPED_KEY = (
    "-----BEGIN PUBLIC KEY-----\n"
    "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEZm9vYmFyYmF6cXV1eA==\n"
    "-----END PUBLIC KEY-----\n"
)

# what os.environ hands back for a variable holding a non-UTF-8 byte (surrogateescape); PyJWT
# utf-8-encodes a str key, and surrogates are not encodable -> UnicodeEncodeError
SURROGATE_KEY = "sandbox-signing-key-\udcff-from-non-utf8-bytes"

CRYPTO_REFUSED_KEYS = pytest.mark.parametrize(
    "key", [PEM_SHAPED_KEY, SURROGATE_KEY], ids=["pem", "surrogate"]
)


@CRYPTO_REFUSED_KEYS
def test_a_key_the_crypto_layer_refuses_is_a_401_not_a_500(client, monkeypatch, key):
    """A degenerate signing key must reject the caller, not fault the request.

    Both of these escaped the old `except jwt.InvalidTokenError` and surfaced as 500. Nobody was
    admitted either way, but a 500 skips require_auth's `endpoint_access` rejection line, so the
    operator sees an unattributed fault where an auth failure happened.
    """
    monkeypatch.setenv("SANDBOX_TOKEN_SIGNING_KEY", key)
    assert _get(client, _mint()).status_code == 401
    assert _get(client, SECRET).status_code != 401  # other callers unaffected


@CRYPTO_REFUSED_KEYS
def test_verify_sandbox_token_raises_only_sandbox_token_error(monkeypatch, key):
    """The docstring's "always a hard 401" as an assertion: nothing else escapes this call.

    pytest.raises pins the type — anything the crypto layer raises that is not translated fails
    here rather than being caught, which is what the widened `except` exists to guarantee.
    """
    from api import sandbox_auth

    monkeypatch.setenv("SANDBOX_TOKEN_SIGNING_KEY", key)
    with pytest.raises(sandbox_auth.SandboxTokenError):
        sandbox_auth.verify_sandbox_token(_mint())


# --- discrimination on alg, not on dots ----------------------------------------------------


def test_rs256_bearer_is_not_treated_as_a_sandbox_token():
    """A three-segment RS256 JWT is what every Google Identity Token looks like.

    db-api has no such caller today, so this is latent here — but routing on dot count is the
    mistake that would 401 that entire class in results-api, and the discriminator is shared.
    """
    from api import sandbox_auth

    header = base64.urlsafe_b64encode(b'{"alg":"RS256","typ":"JWT"}').decode().rstrip("=")
    assert sandbox_auth.is_sandbox_shaped(f"{header}.payload.signature") is False
    assert sandbox_auth.is_sandbox_shaped(_mint()) is True


@pytest.mark.parametrize("bearer", ["", "opaque-token", "a.b", "a.b.c.d", "!!!.!!!.!!!"])
def test_non_jwt_bearers_are_not_sandbox_shaped(bearer):
    from api import sandbox_auth

    assert sandbox_auth.is_sandbox_shaped(bearer) is False


# --- fail-closed startup -------------------------------------------------------------------


@pytest.mark.parametrize(
    "internal,signing",
    [
        ("", SIGNING_KEY),   # the case an earlier draft covered
        (SECRET, ""),        # signing key missing
        ("", ""),            # the both-unset case rule 6 exists for
    ],
)
def test_refuses_to_start_when_the_sandbox_is_deployed_and_a_secret_is_missing(
    monkeypatch, internal, signing
):
    """No sys.modules juggling: `require_sandbox_config` reads both variables when CALLED
    (genetics-results-suite-l7z), so setting them on the already-imported module is enough.
    Against the old import-time snapshot this file's own import would have frozen
    SANDBOX_ENABLED to false and none of these three cases could fire at all."""
    from api import sandbox_auth

    monkeypatch.setenv("SANDBOX_ENABLED", "true")
    monkeypatch.setenv("SANDBOX_TOKEN_SIGNING_KEY", signing)

    with pytest.raises(SystemExit) as exc:
        sandbox_auth.require_sandbox_config(internal)
    assert exc.value.code == 1


def test_starts_when_the_sandbox_is_deployed_and_both_secrets_are_present(monkeypatch):
    from api import sandbox_auth

    monkeypatch.setenv("SANDBOX_ENABLED", "true")
    monkeypatch.setenv("SANDBOX_TOKEN_SIGNING_KEY", SIGNING_KEY)
    sandbox_auth.require_sandbox_config(SECRET)  # must not raise


def test_sandbox_not_deployed_leaves_the_existing_behaviour_alone(monkeypatch):
    from api import sandbox_auth

    monkeypatch.delenv("SANDBOX_ENABLED", raising=False)
    sandbox_auth.require_sandbox_config("")  # must not raise
