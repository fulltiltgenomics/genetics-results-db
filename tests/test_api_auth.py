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
