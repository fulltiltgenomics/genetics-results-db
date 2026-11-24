"""Tests for db-api's per-execution (`jti`) request-count and concurrency gate.

Design of record: docs/code-execution-security.md section 4 in genetics-results-suite; the rule
as implemented is documented in api/sandbox_budget.py.

The aggregate BYTE budget in api/main.py (`SANDBOX_AGGREGATE_BYTES_BUDGET`) bounds what one
execution may make BigQuery scan. It bounds nothing about what that execution costs *this pod*,
because the paths that run no BigQuery job charge it nothing: `/health`, `/docs`, `/redoc`,
`/openapi.json`, an unmatched path, `/schema` on a categorical-value cache hit, `/stats`'
metadata loop. db-api is `replicas: 1` and is on the browser's chat path, so those are what
these limits exist for.

The rules both services enforce identically are tests/sandbox_contract.py's `BudgetContract`,
collected here as `TestBudgetContract`. The properties under test in this file are db-api's own:

  * a request carrying no sandbox token is bound by NONE of them, with every limit set to 1 —
    chat-backend, mcp-server and the `/health` probe must be unaffected, which is the single
    most important property here;
  * an unmatched path is COUNTED and RELEASES its slot — the assertion that pins the gate to
    middleware rather than to a dependency teardown;
  * a slot is released when the handler raises;
  * the sweep evicts only an execution whose token is dead;
  * the gate is registered on the real app, and refuses an inverted concurrency pair at import.
"""

import functools
import importlib
import os
import sys
import time

import pytest
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.testclient import TestClient
from sandbox_contract import (
    SIGNING_KEY,
    BudgetContract,
    add_budget_routes,
    bearer,
    call_asgi,
    concurrent,
    get,
    mint_token,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api import sandbox_auth, sandbox_budget  # noqa: E402

SECRET = "test-internal-secret"

_HANDLER_CALLS: list[str] = []

_mint = functools.partial(mint_token, audience="db-api")


@pytest.fixture(autouse=True)
def sandbox_config(monkeypatch):
    monkeypatch.setenv("SANDBOX_TOKEN_SIGNING_KEY", SIGNING_KEY)
    monkeypatch.setenv("INTERNAL_API_SECRET", SECRET)


@pytest.fixture(autouse=True)
def clean_state():
    sandbox_budget.reset()
    _HANDLER_CALLS.clear()
    yield
    sandbox_budget.reset()
    _HANDLER_CALLS.clear()


# --- harness --------------------------------------------------------------------------------


def _auth(request: Request) -> None:
    """A stand-in for api.main.require_auth with the same *shape and outcomes*: an app-level
    dependency, so it is solved after routing and not at all for an unmatched path; it accepts
    the shared secret and a valid sandbox token, and 401s everything else."""
    header = request.headers.get("Authorization", "")
    if header == f"Bearer {SECRET}":
        return
    token = header[7:] if header.startswith("Bearer ") else ""
    if sandbox_auth.is_sandbox_shaped(token):
        try:
            sandbox_auth.verify_sandbox_token(token)
            return
        except sandbox_auth.SandboxTokenError:
            pass
    raise HTTPException(status_code=401, detail="unauthorized")


@pytest.fixture
def client():
    api = FastAPI(dependencies=[Depends(_auth)])
    api.add_middleware(sandbox_budget.SandboxBudgetMiddleware)
    add_budget_routes(api, _HANDLER_CALLS)

    @api.get("/raises")
    async def raises():
        _HANDLER_CALLS.append("raises")
        raise RuntimeError("handler exploded")

    return api


class TestBudgetContract(BudgetContract):
    budget = sandbox_budget
    audience = "db-api"
    handler_calls = _HANDLER_CALLS
    spent_after_one_request = ("SANDBOX_MAX_REQUESTS_PER_EXECUTION", 1)
    blob_size = 1

    @staticmethod
    def still_charged(entry):
        return entry.requests == 1


# --- 1. non-sandbox traffic is untouched. This is the property that keeps chat working. ------


def test_a_non_sandbox_caller_is_bound_by_none_of_them(client, monkeypatch):
    """chat-backend and mcp-server authenticate with INTERNAL_API_SECRET. With every limit at 1
    they must still see no accounting at all — getting this wrong takes production chat down."""
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_REQUESTS_PER_EXECUTION", 1)
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_CONCURRENT_REQUESTS", 1)
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL", 1)
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_TRACKED_EXECUTIONS", 1)

    for _ in range(5):
        assert get(client, "/blob", SECRET, n=100).status_code == 200

    second, held = concurrent(client, SECRET, SECRET)
    assert (second.status_code, held.status_code) == (200, 200)
    assert sandbox_budget._executions == {}, "no entry is created for a non-sandbox caller"


def test_a_token_for_the_other_service_is_not_accounted(client):
    """`aud: results-api` fails verify_sandbox_token here, so it buys no slot on this pod."""
    assert get(client, "/blob", _mint(aud="results-api"), n=10).status_code == 401
    assert sandbox_budget._executions == {}


# --- 2. nothing may strand a slot -----------------------------------------------------------


def test_an_unmatched_route_is_counted_and_releases_its_slot(client, monkeypatch):
    """This is the assertion that pins the gate to MIDDLEWARE rather than a dependency.

    `require_auth` is an app-level `Depends`, solved per MATCHED route; an unmatched path 404s
    out of the router with no dependency ever entered. A dependency placement would therefore
    neither count this request nor release its slot, and the leak would be permanent —
    `_sweep_locked` refuses to evict an entry with `in_flight > 0`.
    """
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_CONCURRENT_REQUESTS", 1)
    token = _mint()

    assert get(client, "/no-such-route", token).status_code == 404
    entry = sandbox_budget.snapshot("exec-123")
    assert entry is not None, "an unmatched path must still be counted"
    assert entry.requests == 1
    assert entry.in_flight == 0, "an unmatched route must not strand a concurrency slot"
    assert get(client, "/blob", token, n=1).status_code == 200, "and the slot is reusable"


def test_a_raising_handler_releases_its_slot(client, monkeypatch):
    """The release is in a `finally`; an exception on the way out must not leak the slot."""
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_CONCURRENT_REQUESTS", 1)
    token = _mint()

    # Starlette answers the 500 and then re-raises the handler's error for the server to log
    resp = call_asgi(
        client,
        "GET",
        "/raises",
        headers=bearer(token),
        tolerate=lambda exc: isinstance(exc, RuntimeError) and str(exc) == "handler exploded",
    )
    assert resp.status_code == 500
    entry = sandbox_budget.snapshot("exec-123")
    assert (entry.requests, entry.in_flight) == (1, 0)
    assert get(client, "/blob", token, n=1).status_code == 200


# --- 3. the sweep drops only a dead execution ------------------------------------------------


def test_cleanup_drops_only_executions_whose_token_can_no_longer_authenticate(client):
    get(client, "/blob", _mint(jti="live"), n=1)
    get(client, "/blob", _mint(jti="expired", ttl=1), n=1)

    # far enough past `exp` that verify_sandbox_token's own leeway cannot still accept it
    with sandbox_budget._lock:
        sandbox_budget._sweep_locked(time.time() + 1 + sandbox_budget.sandbox_auth.LEEWAY_SECONDS + 1)

    assert sandbox_budget.snapshot("expired") is None
    assert sandbox_budget.snapshot("live").requests == 1, (
        "evicting a live execution silently resets its counters — the fail-open direction"
    )


# --- 4. the shipped defaults, and the gate as actually registered ---------------------------


def test_the_defaults_are_the_documented_numbers():
    """Documented in api/sandbox_budget.py and in docs/code-execution-security.md section 4."""
    assert sandbox_budget.SANDBOX_MAX_REQUESTS_PER_EXECUTION == 1000
    assert sandbox_budget.SANDBOX_MAX_CONCURRENT_REQUESTS == 4
    assert sandbox_budget.SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL == 8
    assert sandbox_budget.SANDBOX_MAX_TRACKED_EXECUTIONS == 4096


@pytest.mark.parametrize(
    ("total", "per_execution", "refused"),
    [
        ("4", "8", True),  # inverted
        ("8", "8", False),  # equal is the intended boundary: one execution may use the pod
        ("9", "8", False),
    ],
)
def test_an_inverted_concurrency_pair_is_refused_at_import(
    monkeypatch, total, per_execution, refused
):
    """A pod-wide bound *below* the per-execution one is refused the same way every other bad
    value is — at import, so the pod fails to start rather than serving a configuration whose
    per-execution number an execution can never reach and whose manifest is therefore a lie.

    The check runs once, at import, and nothing re-runs it, so this has to re-execute the
    module. The re-import is undone in `finally`: `api.main` and every other test in this file
    hold a reference to the *original* module object, and leaving a second copy in `sys.modules`
    would silently decouple the middleware from the constants the other tests monkeypatch.
    """
    monkeypatch.setenv("SANDBOX_MAX_CONCURRENT_REQUESTS", per_execution)
    monkeypatch.setenv("SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL", total)
    original = sys.modules["api.sandbox_budget"]
    del sys.modules["api.sandbox_budget"]
    try:
        if refused:
            with pytest.raises(
                ValueError,
                match=(
                    r"SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL \(4\) must be >= "
                    r"SANDBOX_MAX_CONCURRENT_REQUESTS \(8\)"
                ),
            ):
                importlib.import_module("api.sandbox_budget")
        else:
            reloaded = importlib.import_module("api.sandbox_budget")
            assert reloaded.SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL == int(total)
    finally:
        sys.modules["api.sandbox_budget"] = original
        sys.modules["api"].sandbox_budget = original


def test_the_gate_is_registered_on_the_real_app_and_covers_an_unauthenticated_route(
    monkeypatch,
):
    """/health is in `_UNAUTHENTICATED_PATHS`, so `require_auth` returns early for it and the
    byte budget never sees it — the gate is the only thing that bounds a loop of them."""
    monkeypatch.setenv("PROJECT_ID", os.environ.get("PROJECT_ID", "test-project"))
    sys.modules.pop("api.main", None)
    main = importlib.import_module("api.main")
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_REQUESTS_PER_EXECUTION", 2)

    with TestClient(main.app, raise_server_exceptions=False) as http:
        headers = {"Authorization": f"Bearer {_mint(jti='exec-health')}"}
        statuses = [http.get("/health", headers=headers).status_code for _ in range(4)]

    assert statuses == [200, 200, 429, 429]
    sys.modules.pop("api.main", None)
