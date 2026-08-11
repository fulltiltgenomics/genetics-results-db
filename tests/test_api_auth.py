"""Tests for the shared-secret authentication on the query API.

Imports api.main, which constructs a BigQuery client at module scope — that is offline
(no RPC until a query runs), so these tests never reach BigQuery: every case is decided
by require_auth before a handler executes.
"""

import os
import sys

import pytest
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
