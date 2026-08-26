"""Tests for db-api's per-execution (`jti`) request-count and concurrency gate.

Design of record: docs/code-execution-security.md section 4 in genetics-results-suite; the rule
as implemented is documented in api/sandbox_budget.py.

The aggregate BYTE budget in api/main.py (`SANDBOX_AGGREGATE_BYTES_BUDGET`) bounds what one
execution may make BigQuery scan. It bounds nothing about what that execution costs *this pod*,
because the paths that run no BigQuery job charge it nothing: `/health`, `/docs`, `/redoc`,
`/openapi.json`, an unmatched path, `/schema` on a categorical-value cache hit, `/stats`'
metadata loop. db-api is `replicas: 1` and is on the browser's chat path, so those are what
these limits exist for. The properties under test:

  * a request carrying no sandbox token is bound by NONE of them, with every limit set to 1 —
    chat-backend, mcp-server and the `/health` probe must be unaffected, which is the single
    most important property here;
  * the request-count limit answers 429 on its own, before any handler work;
  * concurrency is *rejected*, not queued, per execution and pod-wide;
  * an unmatched path is COUNTED and RELEASES its slot — the assertion that pins the gate to
    middleware rather than to a dependency teardown;
  * a slot is released when the handler raises;
  * the counter map is bounded and its sweep can never evict a live execution.
"""

import asyncio
import importlib
import json
import os
import sys
import time
from urllib.parse import urlencode

import jwt
import pytest
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api import sandbox_auth, sandbox_budget  # noqa: E402

SECRET = "test-internal-secret"
SIGNING_KEY = "test-sandbox-signing-key-that-is-32-bytes+"

_HANDLER_CALLS: list[str] = []


def _mint(jti="exec-123", ttl=300, **over):
    iat = int(time.time())
    claims = {
        "iss": "chat-backend",
        "aud": "db-api",
        "sub": "user@finngen.fi",
        "sid": "session-abc",
        "jti": jti,
        "iat": iat,
        "exp": iat + ttl,
        "scope": "query:views",
    }
    claims.update(over)
    return jwt.encode(claims, SIGNING_KEY, algorithm="HS256")


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


class _Response:
    def __init__(self, status: int, body: bytes):
        self.status_code, self.content = status, body

    def json(self):
        return json.loads(self.content)


def _scope(path: str, query: str, headers):
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": query.encode(),
        "root_path": "",
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers],
        "client": ("testclient", 50000),
        "server": ("testserver", 80),
        "state": {},
    }


async def _acall(api, path, query="", headers=()) -> _Response:
    """Drive the ASGI app directly: TestClient cannot hold one request open while it issues
    another, which every concurrency assertion below needs."""
    messages = []
    received = []

    async def receive():
        if not received:
            received.append(True)
            return {"type": "http.request", "body": b"", "more_body": False}
        await asyncio.Event().wait()

    async def send(message):
        messages.append(message)

    try:
        await api(_scope(path, query, headers), receive, send)
    except RuntimeError as exc:  # the /raises endpoint, re-raised by ServerErrorMiddleware
        if str(exc) != "handler exploded":
            raise
    start = next(m for m in messages if m["type"] == "http.response.start")
    body = b"".join(
        bytes(m.get("body", b"")) for m in messages if m["type"] == "http.response.body"
    )
    return _Response(start["status"], body)


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

    gate = asyncio.Event()
    entered = asyncio.Event()

    @api.get("/blob")
    async def blob(n: int = 10):
        _HANDLER_CALLS.append("blob")
        return JSONResponse({"payload": "x" * n})

    @api.get("/slow")
    async def slow():
        _HANDLER_CALLS.append("slow")
        entered.set()
        await gate.wait()
        return JSONResponse({"ok": True})

    @api.get("/raises")
    async def raises():
        _HANDLER_CALLS.append("raises")
        raise RuntimeError("handler exploded")

    api.state.gate, api.state.entered = gate, entered
    return api


def _get(client, path, token=None, **params):
    headers = [("Authorization", f"Bearer {token}")] if token else []
    return asyncio.run(_acall(client, path, urlencode(params), headers))


# --- 1. non-sandbox traffic is untouched. This is the property that keeps chat working. ------


def test_a_non_sandbox_caller_is_bound_by_none_of_them(client, monkeypatch):
    """chat-backend and mcp-server authenticate with INTERNAL_API_SECRET. With every limit at 1
    they must still see no accounting at all — getting this wrong takes production chat down."""
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_REQUESTS_PER_EXECUTION", 1)
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_CONCURRENT_REQUESTS", 1)
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL", 1)
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_TRACKED_EXECUTIONS", 1)

    for _ in range(5):
        assert _get(client, "/blob", SECRET, n=100).status_code == 200

    second, held = _concurrent(client, SECRET, SECRET)
    assert (second.status_code, held.status_code) == (200, 200)
    assert sandbox_budget._executions == {}, "no entry is created for a non-sandbox caller"


def test_an_unauthenticated_request_creates_no_accounting(client, monkeypatch):
    """The kubelet and the monitor CronJob poll /health with no credentials at all."""
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_REQUESTS_PER_EXECUTION", 1)
    for _ in range(3):
        assert _get(client, "/blob", n=10).status_code == 401
    assert sandbox_budget._executions == {}


def test_an_invalid_sandbox_token_is_not_accounted(client):
    """The gate's own resolver must not 401 or account for it; require_auth owns that."""
    forged = jwt.encode({"aud": "db-api"}, "wrong-key", algorithm="HS256")
    assert _get(client, "/blob", forged, n=10).status_code == 401
    assert sandbox_budget._executions == {}


def test_a_token_for_the_other_service_is_not_accounted(client):
    """`aud: results-api` fails verify_sandbox_token here, so it buys no slot on this pod."""
    assert _get(client, "/blob", _mint(aud="results-api"), n=10).status_code == 401
    assert sandbox_budget._executions == {}


# --- 2. the request-count limit -------------------------------------------------------------


def test_the_request_count_limit_stops_a_loop(client, monkeypatch):
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_REQUESTS_PER_EXECUTION", 3)
    token = _mint()

    statuses = [_get(client, "/blob", token, n=1).status_code for _ in range(5)]

    assert statuses == [200, 200, 200, 429, 429]


def test_the_request_count_429_names_its_limit(client, monkeypatch):
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_REQUESTS_PER_EXECUTION", 1)
    token = _mint()
    _get(client, "/blob", token, n=1)
    resp = _get(client, "/blob", token, n=1)

    assert resp.status_code == 429
    payload = resp.json()
    assert payload["code"] == "sandbox_request_count"
    assert (payload["limit"], payload["observed"]) == (1, 1)
    assert "Request limit for this execution" in payload["detail"]


def test_a_rejection_costs_no_handler_work(client, monkeypatch):
    """Admitted before routing, so a spent execution costs no BigQuery job."""
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_REQUESTS_PER_EXECUTION", 1)
    token = _mint()
    _get(client, "/blob", token, n=1)
    _HANDLER_CALLS.clear()

    assert _get(client, "/blob", token, n=1).status_code == 429
    assert _HANDLER_CALLS == []


def test_the_count_is_per_execution_not_per_pod(client, monkeypatch):
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_REQUESTS_PER_EXECUTION", 1)
    assert _get(client, "/blob", _mint(jti="exec-a"), n=1).status_code == 200
    assert _get(client, "/blob", _mint(jti="exec-a"), n=1).status_code == 429
    assert _get(client, "/blob", _mint(jti="exec-b"), n=1).status_code == 200


# --- 3. concurrency: rejected, never queued -------------------------------------------------


def _concurrent(client, first_token, second_token):
    """Hold one request inside its handler, then issue a second and read its answer."""

    async def scenario():
        headers = [("Authorization", f"Bearer {first_token}")]
        held = asyncio.create_task(_acall(client, "/slow", "", headers))
        await asyncio.wait_for(client.state.entered.wait(), timeout=5)
        try:
            second = await _acall(
                client, "/blob", "n=1", [("Authorization", f"Bearer {second_token}")]
            )
        finally:
            client.state.gate.set()
        return second, await asyncio.wait_for(held, timeout=5)

    return asyncio.run(scenario())


def test_a_second_concurrent_request_is_rejected_not_queued(client, monkeypatch):
    """Queueing would burn the sandbox's 60-120s wall clock on a wait the script cannot see."""
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_CONCURRENT_REQUESTS", 1)
    token = _mint()

    second, held = _concurrent(client, token, token)

    assert second.status_code == 429
    payload = second.json()
    assert payload["code"] == "sandbox_concurrency"
    assert (payload["limit"], payload["observed"]) == (1, 1)
    assert held.status_code == 200, "the held request still completes normally"
    assert sandbox_budget.snapshot("exec-123").in_flight == 0


def test_the_pod_wide_concurrency_limit_covers_distinct_executions(client, monkeypatch):
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_CONCURRENT_REQUESTS", 10)
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL", 1)

    second, _held = _concurrent(client, _mint(jti="exec-a"), _mint(jti="exec-b"))

    assert second.status_code == 429
    assert second.json()["code"] == "sandbox_concurrency_pod"


def test_the_slot_is_released_so_the_next_request_succeeds(client, monkeypatch):
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_CONCURRENT_REQUESTS", 1)
    token = _mint()

    for _ in range(3):
        assert _get(client, "/blob", token, n=1).status_code == 200


# --- 4. nothing may strand a slot -----------------------------------------------------------


def test_an_unmatched_route_is_counted_and_releases_its_slot(client, monkeypatch):
    """This is the assertion that pins the gate to MIDDLEWARE rather than a dependency.

    `require_auth` is an app-level `Depends`, solved per MATCHED route; an unmatched path 404s
    out of the router with no dependency ever entered. A dependency placement would therefore
    neither count this request nor release its slot, and the leak would be permanent —
    `_sweep_locked` refuses to evict an entry with `in_flight > 0`.
    """
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_CONCURRENT_REQUESTS", 1)
    token = _mint()

    assert _get(client, "/no-such-route", token).status_code == 404
    entry = sandbox_budget.snapshot("exec-123")
    assert entry is not None, "an unmatched path must still be counted"
    assert entry.requests == 1
    assert entry.in_flight == 0, "an unmatched route must not strand a concurrency slot"
    assert _get(client, "/blob", token, n=1).status_code == 200, "and the slot is reusable"


def test_a_raising_handler_releases_its_slot(client, monkeypatch):
    """The release is in a `finally`; an exception on the way out must not leak the slot."""
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_CONCURRENT_REQUESTS", 1)
    token = _mint()

    assert _get(client, "/raises", token).status_code == 500
    entry = sandbox_budget.snapshot("exec-123")
    assert (entry.requests, entry.in_flight) == (1, 0)
    assert _get(client, "/blob", token, n=1).status_code == 200


# --- 5. the map is bounded, and its sweep never evicts a live execution ----------------------


def test_cleanup_drops_only_executions_whose_token_can_no_longer_authenticate(client):
    _get(client, "/blob", _mint(jti="live"), n=1)
    _get(client, "/blob", _mint(jti="expired", ttl=1), n=1)

    # far enough past `exp` that verify_sandbox_token's own leeway cannot still accept it
    with sandbox_budget._lock:
        sandbox_budget._sweep_locked(time.time() + 1 + sandbox_budget.sandbox_auth.LEEWAY_SECONDS + 1)

    assert sandbox_budget.snapshot("expired") is None
    assert sandbox_budget.snapshot("live").requests == 1, (
        "evicting a live execution silently resets its counters — the fail-open direction"
    )


def test_cleanup_never_evicts_an_execution_with_a_request_in_flight(client):
    async def scenario():
        headers = [("Authorization", f"Bearer {_mint(jti='inflight', ttl=1)}")]
        held = asyncio.create_task(_acall(client, "/slow", "", headers))
        await asyncio.wait_for(client.state.entered.wait(), timeout=5)
        with sandbox_budget._lock:
            sandbox_budget._sweep_locked(time.time() + 3600)
        survived = sandbox_budget.snapshot("inflight")
        client.state.gate.set()
        await asyncio.wait_for(held, timeout=5)
        return survived

    survived = asyncio.run(scenario())
    assert survived is not None and survived.in_flight == 1


def test_a_full_tracker_refuses_a_new_execution_rather_than_evicting_a_live_one(
    client, monkeypatch
):
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_TRACKED_EXECUTIONS", 1)
    _get(client, "/blob", _mint(jti="exec-a"), n=1)

    resp = _get(client, "/blob", _mint(jti="exec-b"), n=1)

    assert resp.status_code == 429
    assert resp.json()["code"] == "sandbox_execution_tracker_full"
    assert sandbox_budget.snapshot("exec-a").requests == 1

    # the slot frees itself once the incumbent's token expires, with no operator action. This
    # must use a *new* jti: re-using "exec-a" takes admit's `entry is not None` path and touches
    # neither the sweep nor the tracker-full check.
    with sandbox_budget._lock:
        sandbox_budget._executions["exec-a"].expires_at = int(time.time()) - 3600
    assert _get(client, "/blob", _mint(jti="exec-c"), n=1).status_code == 200
    assert sandbox_budget.snapshot("exec-a") is None, "the expired incumbent was swept"


# --- 6. the shipped defaults, and the gate as actually registered ---------------------------


def test_the_defaults_are_the_documented_numbers():
    """Documented in api/sandbox_budget.py and in docs/code-execution-security.md section 4."""
    assert sandbox_budget.SANDBOX_MAX_REQUESTS_PER_EXECUTION == 1000
    assert sandbox_budget.SANDBOX_MAX_CONCURRENT_REQUESTS == 4
    assert sandbox_budget.SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL == 8
    assert sandbox_budget.SANDBOX_MAX_TRACKED_EXECUTIONS == 4096


@pytest.mark.parametrize("raw", ["0", "-1", "", "not-a-number", "4.5"])
def test_a_nonsense_limit_fails_loudly_rather_than_rejecting_everything(monkeypatch, raw):
    """Every limit is a ceiling compared with `>=`, so 0 or a negative silently turns it into
    "reject every sandbox request" — a total outage no health check would attribute to a typo."""
    monkeypatch.setenv("SANDBOX_MAX_REQUESTS_PER_EXECUTION", raw)
    with pytest.raises(ValueError, match="positive integer"):
        sandbox_budget._env_int("SANDBOX_MAX_REQUESTS_PER_EXECUTION", 1000)


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
