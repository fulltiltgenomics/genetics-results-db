"""Tests for the shared-secret authentication on the query API.

Imports api.main, which constructs a BigQuery client at module scope — that is offline
(no RPC until a query runs), so these tests never reach BigQuery: every case is decided
by require_auth before a handler executes.
"""

import os
import sys

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

SECRET = "test-internal-secret"


@pytest.fixture(scope="module")
def client():
    os.environ["INTERNAL_API_SECRET"] = SECRET
    os.environ.setdefault("PROJECT_ID", "test-project")
    from api.main import app

    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def auth():
    return {"Authorization": f"Bearer {SECRET}"}


def test_health_needs_no_auth(client):
    """kubelet probes and the monitor CronJob poll /health with no credentials."""
    assert client.get("/health").status_code == 200


@pytest.mark.parametrize("path", ["/openapi.json", "/docs", "/redoc"])
def test_docs_require_auth(client, path):
    """FastAPI's own docs routes bypass app-level dependencies; these are re-declared."""
    assert client.get(path).status_code == 401


def test_docs_served_with_auth(client, auth):
    assert client.get("/openapi.json", headers=auth).status_code == 200


@pytest.mark.parametrize(
    "headers",
    [None, {"Authorization": "Bearer wrong"}, {"Authorization": SECRET}],
    ids=["missing", "wrong-secret", "no-bearer-prefix"],
)
def test_query_rejected_without_valid_secret(client, headers):
    resp = client.post("/query", json={"sql": "SELECT 1"}, headers=headers)
    assert resp.status_code == 401


def test_query_passes_authentication_with_secret(client, auth):
    """Authorization must not be what stops the request; BigQuery may still reject the SQL."""
    resp = client.post("/query", json={"sql": "SELECT 1"}, headers=auth)
    assert resp.status_code != 401


@pytest.mark.parametrize(
    "token",
    ["sécret", "пароль", "test-internal-secret​"],
    ids=["latin-1", "cyrillic", "zero-width-suffix"],
)
def test_non_ascii_bearer_is_401_not_500(client, token):
    """`hmac.compare_digest` raises TypeError when either side is a str with non-ASCII, so
    comparing the raw bearer would turn a bad credential into a 500. None of these are
    sandbox-shaped, so the sandbox branch declines them and they reach the comparison.

    Sent as raw bytes because that is what a header is on the wire — httpx refuses to encode
    a non-ASCII str, while starlette latin-1-decodes the bytes back into a non-ASCII str, so
    a client that is not httpx can reach this and a test that is must go around it.
    """
    header = {"Authorization": f"Bearer {token}".encode("utf-8")}
    resp = client.post("/query", json={"sql": "SELECT 1"}, headers=header)
    assert resp.status_code == 401


def _request_with_raw_bearer(raw: bytes):
    """A starlette Request whose Authorization header carries exactly these wire bytes.

    Hand-built rather than driven through TestClient because TestClient cannot express it:
    `starlette/testclient.py` does `value.encode()` (utf-8) on httpx's already-decoded header
    str, so latin-1 wire bytes are silently rewritten to utf-8 before the app sees them and
    every case below would collapse into the utf-8 one.
    """
    from fastapi import Request

    return Request(
        {
            "type": "http",
            "method": "POST",
            "scheme": "http",
            "server": ("testserver", 80),
            "path": "/query",
            "query_string": b"",
            "headers": [(b"authorization", raw)],
        }
    )


def test_which_wire_bytes_authenticate_a_non_ascii_secret(client, monkeypatch):
    """Pin the accept/reject map over RAW wire bytes — what genetics-results-suite-ctq changed.

    Starlette latin-1-decodes the raw header bytes, so re-encoding the presented token with
    latin-1 undoes that decode exactly. Which *client* that suits is not universal: node/undici
    and python-requests put the latin-1 form on the wire, aiohttp the utf-8 form, and httpx
    refuses to send either. `_require_ascii_secret` refuses a non-ASCII secret at startup, so
    this map is unreachable in a real deployment; the comparison is still reachable, so it is
    pinned here rather than through the app.

    Injected by standing in for the accessor rather than by setting the environment: since
    genetics-results-suite-xi6 `require_auth` reads through `_internal_api_secret`, which
    validates what it returns, so a non-ASCII value can no longer reach the comparison via the
    environment — that is the property the test below pins. Replacing the accessor puts the
    codec pairing under test without weakening it.
    """
    import api.main as main

    monkeypatch.setattr(main, "_internal_api_secret", lambda: "sécret")
    # utf-8 on the wire (aiohttp-shaped): authenticates now, 401 before ctq
    main.require_auth(_request_with_raw_bearer(b"Bearer s\xc3\xa9cret"))
    # latin-1 on the wire (node/undici- and requests-shaped): 401 now, authenticated before ctq
    with pytest.raises(HTTPException) as exc:
        main.require_auth(_request_with_raw_bearer(b"Bearer s\xe9cret"))
    assert exc.value.status_code == 401


def test_the_ascii_guard_fires_only_on_a_non_ascii_secret(client, monkeypatch):
    """genetics-results-suite-ctq: the invariant require_auth relies on is enforced, not merely
    documented. Absent and empty are the dev/test configuration and stay silent.

    Since genetics-results-suite-xi6 the secret is read per request, so the invariant has to
    hold at REQUEST time too: validating only the import-time snapshot would leave `require_auth`
    comparing against bytes nothing ever checked. `_internal_api_secret` is the only way to
    obtain the value, and it refuses a non-ASCII one — asserted here on the accessor, so the
    property survives however the reading is arranged.

    The two times behave differently ON PURPOSE, and the difference is the whole failure mode:
      * at import `_require_ascii_secret` RAISES, so the pod never passes readiness and the
        rollout stalls with the old pods still serving (genetics-results-suite-ctq);
      * at request time the accessor FAILS CLOSED (401) and must never raise. A RuntimeError out
        of a FastAPI dependency is a 500 for every call, including the one holding the correct
        credential, on a pod that stays Ready because `/health` returns before the read. A
        non-ASCII value can only get there by an in-process mutation, which no deployment does.
    """
    import api.main as main

    with pytest.raises(RuntimeError, match="INTERNAL_API_SECRET"):
        main._require_ascii_secret("sécret")
    main._require_ascii_secret(SECRET)
    main._require_ascii_secret("")

    monkeypatch.setenv("INTERNAL_API_SECRET", "sécret")
    assert main._internal_api_secret() is None  # refuse, do not raise
    resp = client.get("/openapi.json", headers={"Authorization": f"Bearer {SECRET}"})
    assert resp.status_code == 401  # not 500, and not admitted

    monkeypatch.setenv("INTERNAL_API_SECRET", SECRET)
    assert main._internal_api_secret() == SECRET
    assert client.get("/openapi.json", headers={"Authorization": f"Bearer {SECRET}"}).status_code == 200
