"""Tests that WHEN db-api reads INTERNAL_API_SECRET cannot decide whether auth runs.

genetics-results-suite-xi6. api.main used to snapshot the variable at module import, so the
value require_auth compared against was fixed by whoever imported the module first. pytest
imports every test module at COLLECTION time, before any fixture sets the variable, so an
ordinary shuffled run could freeze the secret to "" and send require_auth down its fail-open
branch: at `--randomly-seed=2662673150` the nine tests in test_api_auth that assert 401 got 200
and **failed** — 9 failed, 87 passed.

**The defect was a seed-dependent RED suite, not a silently green one.** The bead note this
change was written from claimed the reverse ("nine auth tests passed for the wrong reason; a
green suite that had never exercised the auth path"), and that is backwards: a test asserting
401 that receives 200 fails, loudly and by name. The correction is written out here rather than
quietly dropped, because the false version was the stated justification for the change.

The honest justification is smaller but real: the auth path's behaviour depended on test
collection ORDER, so a shuffled run could go red for reasons unrelated to whatever was being
changed, and a future module-scope `import api.main` in any test file would freeze the secret
and break an unrelated file. Reading per request removes ordering as an input entirely — the
ordering is written out below rather than left to a seed, so each case pins its property
whatever order the rest of the suite happens to run in.

The fail-open branch itself is deliberately kept: it is documented local-dev behaviour
(README "unset by default, which disables authentication") and unreachable in the deployment,
where the env var comes from a non-optional secretKeyRef. What is pinned below is that it is
unreachable whenever a secret IS set — including after the fact, once this process has
observed one (`test_a_configured_process_never_fails_open_afterwards`).
"""

import os
import sys

import pytest
from fastapi import HTTPException, Request
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from test_sandbox_token_auth import PROTECTED_PATH, SECRET, _reload  # noqa: E402


def _request(bearer=None):
    headers = [(b"authorization", f"Bearer {bearer}".encode("latin-1"))] if bearer else []
    return Request(
        {
            "type": "http",
            "method": "GET",
            "scheme": "http",
            "server": ("testserver", 80),
            "path": PROTECTED_PATH,
            "query_string": b"",
            "headers": headers,
        }
    )


@pytest.fixture
def restore_module():
    """Every case here re-imports api.main; leave a secret-set module object in sys.modules.

    Without this, a case that imported with the variable unset would leave that module cached
    and export its own failure into whatever runs next — the very coupling this file exists to
    rule out.

    Only the sys.modules entry is this fixture's business. It does not hand the session a
    secret-set *environment*: autouse function fixtures tear down last, so conftest's
    `restore_auth_env` runs after this and puts the environment back to whatever the session
    had. The private MonkeyPatch below is undone immediately for the same reason — by the time
    it matters the import has already happened.
    """
    yield
    mp = pytest.MonkeyPatch()
    try:
        _reload(mp, INTERNAL_API_SECRET=SECRET)
    finally:
        mp.undo()


def test_the_secret_is_read_per_request_not_once_at_import(restore_module, monkeypatch):
    """The original hazard, minimally: import first, set the variable second.

    Fails if api.main snapshots the value at import — the module then holds "" forever and
    require_auth returns instead of raising, which is how a shuffled run turned nine auth tests
    red. Nothing about pytest is required to reach it: any process that imports api.main before
    the environment is complete gets an unauthenticated service.
    """
    main = _reload(monkeypatch)  # imported with INTERNAL_API_SECRET unset, as collection does
    monkeypatch.setenv("INTERNAL_API_SECRET", SECRET)

    with pytest.raises(HTTPException) as exc:
        main.require_auth(_request())
    assert exc.value.status_code == 401

    with pytest.raises(HTTPException):
        main.require_auth(_request("wrong"))

    verified = _request(SECRET)
    main.require_auth(verified)
    assert verified.state.principal == main._INTERNAL_PRINCIPAL


@pytest.mark.parametrize(
    "import_before_secret", [True, False], ids=["import-then-set", "set-then-import"]
)
def test_fail_open_is_unreachable_when_a_secret_is_set(
    restore_module, monkeypatch, import_before_secret
):
    """Both orderings, end to end through the app: a set secret means auth runs, full stop.

    The `unauthenticated` principal is asserted absent as well as the 401s: it is the only
    externally visible mark the fail-open branch leaves, so a run that silently served
    everything would otherwise look identical to one that authenticated everything.
    """
    if import_before_secret:
        main = _reload(monkeypatch)
        monkeypatch.setenv("INTERNAL_API_SECRET", SECRET)
    else:
        main = _reload(monkeypatch, INTERNAL_API_SECRET=SECRET)

    client = TestClient(main.app, raise_server_exceptions=False)
    assert client.get(PROTECTED_PATH).status_code == 401
    assert client.get(PROTECTED_PATH, headers={"Authorization": "Bearer wrong"}).status_code == 401
    assert client.get(
        PROTECTED_PATH, headers={"Authorization": f"Bearer {SECRET}"}
    ).status_code == 200

    verified = _request(SECRET)
    main.require_auth(verified)
    fields = main._access_log_fields(verified, PROTECTED_PATH, "GET")
    assert fields["principal"] == main._INTERNAL_PRINCIPAL != "unauthenticated"


@pytest.mark.parametrize("disable", [None, ""], ids=["deleted", "emptied"])
def test_a_configured_process_never_fails_open_afterwards(restore_module, monkeypatch, disable):
    """Reading per request may ENABLE authentication at runtime; it must never DISABLE it.

    The old module global bought this for free — no in-process mutation of os.environ could
    turn a secret-set app into a fail-open one, because the app held its own copy. A plain
    per-request read gives that away: emptying or deleting the variable would admit every
    caller with `principal=None`. Not reachable under a running pod (an immutable environ, and
    nothing in `api/` writes os.environ), but it is routine in-process — which is exactly why
    tests/conftest.py has to restore these variables at all.

    So `_internal_api_secret` latches: once this process has seen a non-empty secret, a later
    empty one is a fail-CLOSED condition. Asserted through the app rather than the accessor,
    because 401 vs "admitted with principal=None" is the property, not the return value.
    """
    main = _reload(monkeypatch, INTERNAL_API_SECRET=SECRET)
    client = TestClient(main.app, raise_server_exceptions=False)
    auth = {"Authorization": f"Bearer {SECRET}"}
    assert client.get(PROTECTED_PATH, headers=auth).status_code == 200

    if disable is None:
        monkeypatch.delenv("INTERNAL_API_SECRET")
    else:
        monkeypatch.setenv("INTERNAL_API_SECRET", disable)

    assert client.get(PROTECTED_PATH).status_code == 401
    assert client.get(PROTECTED_PATH, headers=auth).status_code == 401

    denied = _request()
    with pytest.raises(HTTPException) as exc:
        main.require_auth(denied)
    assert exc.value.status_code == 401
    assert denied.state.principal is None  # refused, not admitted as an anonymous principal

    monkeypatch.setenv("INTERNAL_API_SECRET", SECRET)
    assert client.get(PROTECTED_PATH, headers=auth).status_code == 200


def test_a_non_ascii_secret_still_stops_the_pod_at_startup(restore_module, monkeypatch):
    """Reading per request must not move the ASCII check to request time (genetics-results-suite-ctq).

    Startup is the good failure mode: the pod never passes readiness and the rollout stalls with
    the old pods still serving. That property is unchanged, and this pins it.

    The request-time half is pinned in test_api_auth: the accessor must NOT raise there, because
    a RuntimeError out of a FastAPI dependency is a 500 for every call including the one holding
    the right credential — on a pod kubelet keeps Ready, since /health returns before the read.
    A non-ASCII value at request time can only come from a runtime mutation, so it fails closed.
    """
    with pytest.raises(RuntimeError, match="INTERNAL_API_SECRET"):
        _reload(monkeypatch, INTERNAL_API_SECRET="sécret")
