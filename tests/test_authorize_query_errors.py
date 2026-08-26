"""Every way `authorize_query` can refuse must leave nothing executed, with a status that says
whose fault it was — 4xx when the caller's statement is the problem, 5xx when BigQuery is.

The gate dry-runs the caller's statement and rejects it unless it is a plain SELECT over the
exposed tables. Three of its four refusal paths come out of BigQuery as exceptions rather than
as a parsed job, and an exception type it does not name escapes the endpoint as a 500. That is
not only a wrong status code: the /query caller is a chat/agent path with retry logic, which
retries a 500 and not a 400, so a typo'd table name turns into a retry storm.

genetics-results-suite-4h6.53 already widened the catch to `NotFound` when it deleted the SQL
rewriting. `Forbidden` — a table that exists but the service account may not read — was left,
and could not be triggered live because the SA has project-level dataViewer. Hence a stub: no
BigQuery, no credentials, runs by default. This repo has no CI, so a test gated on live
credentials guards nothing (same reasoning as test_no_sql_rewriting.py).

Each case asserts BOTH halves. The status code alone would pass for a gate that answered 400
and then ran the query anyway, so every case also asserts what reached BigQuery: exactly one
call, the dry run, and never the execution.
"""

import os
import sys

import pytest
from fastapi.testclient import TestClient
from google.api_core.exceptions import BadRequest, Forbidden, NotFound

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from test_sandbox_token_auth import _reload  # noqa: E402

SQL = "SELECT * FROM some_table"


class _Ref:
    def __init__(self, project, dataset_id, table_id):
        self.project, self.dataset_id, self.table_id = project, dataset_id, table_id


class _Row:
    def values(self):
        return (1,)


class _Result:
    total_rows = 1
    schema = [type("F", (), {"name": "n"})()]

    def __iter__(self):
        return iter([_Row()])


class _Job:
    def __init__(self, statement_type, referenced_tables):
        self.statement_type = statement_type
        self.referenced_tables = referenced_tables
        self.total_bytes_processed = 1024

    def result(self):
        return _Result()


class GateBQ:
    """Fails the dry run with `raises`, or returns a probe describing `statement_type`.

    Records every query() call so a test can assert the execution never happened.
    """

    def __init__(self, raises=None, statement_type="SELECT", referenced_tables=()):
        self.calls = []
        self._raises = raises
        self._statement_type = statement_type
        self._referenced_tables = list(referenced_tables)

    def query(self, sql, job_config=None):
        self.calls.append((sql, job_config))
        if self._raises is not None:
            raise self._raises
        return _Job(self._statement_type, self._referenced_tables)


@pytest.fixture(scope="module")
def main():
    mp = pytest.MonkeyPatch()
    module = _reload(mp)
    yield module
    mp.undo()


@pytest.fixture
def client(main):
    return TestClient(main.app)


def _install(main, monkeypatch, **kwargs):
    fake = GateBQ(**kwargs)
    monkeypatch.setattr(main, "bq_client", fake)
    return fake


def _assert_only_the_dry_run_ran(fake):
    assert len(fake.calls) == 1, f"expected the dry run alone, got {len(fake.calls)} queries"
    assert fake.calls[0][1].dry_run is True


def test_not_found_from_the_dry_run_is_a_400_and_nothing_runs(main, client, monkeypatch):
    """A fully-qualified table that does not exist. This was the reported 500."""
    fake = _install(
        main, monkeypatch, raises=NotFound("Not found: Table proj:ds.nope was not found")
    )

    resp = client.post("/query", json={"sql": SQL})

    assert resp.status_code == 400, resp.text
    assert "not found" in resp.json()["detail"].lower()
    _assert_only_the_dry_run_ran(fake)


def test_bad_request_from_the_dry_run_is_a_400_and_nothing_runs(main, client, monkeypatch):
    """Syntax errors and the bytes-billed ceiling both arrive this way."""
    fake = _install(main, monkeypatch, raises=BadRequest("Syntax error: Unexpected end"))

    resp = client.post("/query", json={"sql": SQL})

    assert resp.status_code == 400, resp.text
    _assert_only_the_dry_run_ran(fake)


def test_access_denied_from_the_dry_run_is_a_403_and_nothing_runs(main, client, monkeypatch):
    """A table that exists but the SA may not read: refuse, do not 500."""
    fake = _install(
        main,
        monkeypatch,
        raises=Forbidden(
            "Access Denied: Table secret-project:secret_dataset.payroll: "
            "User does not have permission to query table",
            errors=[{"reason": "accessDenied"}],
        ),
    )

    resp = client.post("/query", json={"sql": SQL})

    assert resp.status_code == 403, resp.text
    _assert_only_the_dry_run_ran(fake)


@pytest.mark.parametrize("errors", [None, [], [{}], [{"reason": "quotaExceeded"}]])
def test_a_403_that_is_not_access_denied_is_a_503_and_nothing_runs(
    main, client, monkeypatch, errors
):
    """BigQuery answers 403 for `quotaExceeded`, `billingNotEnabled` and `blocked` too, and
    `quotaExceeded` is not retried by the client, so it arrives here as a plain `Forbidden`.
    Reporting a degraded service as the caller's bad query would be a 4xx nothing retries and
    nothing alerts on. A missing or empty `errors` is not evidence of a denial either."""
    kwargs = {} if errors is None else {"errors": errors}
    fake = _install(main, monkeypatch, raises=Forbidden("Quota exceeded", **kwargs))

    resp = client.post("/query", json={"sql": SQL})

    assert resp.status_code == 503, resp.text
    assert "outside the exposed set" not in resp.text
    _assert_only_the_dry_run_ran(fake)


def test_the_403_does_not_claim_the_table_was_outside_the_exposed_set(main, client, monkeypatch):
    """An `accessDenied` on an *allow-listed* table is reachable — an IAM edit, an expired
    condition, a policy tag — and the dry run raised, so there is no `referencedTables` to tell
    the two apart. The refusal may therefore not assert which one it was."""
    fake = _install(
        main,
        monkeypatch,
        raises=Forbidden("Access Denied: Table x", errors=[{"reason": "accessDenied"}]),
    )

    detail = client.post("/query", json={"sql": SQL}).json()["detail"]

    assert "may reference" in detail
    assert detail.count("references tables outside") == 0
    _assert_only_the_dry_run_ran(fake)


def test_forbidden_does_not_echo_the_table_bigquery_named(main, client, monkeypatch):
    """The denial message identifies a fully-qualified table the caller may never have written
    — for a bare name it is this service's own project and dataset — and there is nothing the
    caller could do with it. It is logged instead."""
    fake = _install(
        main,
        monkeypatch,
        raises=Forbidden(
            "Access Denied: Table secret-project:secret_dataset.payroll: "
            "User does not have permission to query table",
            errors=[{"reason": "accessDenied"}],
        ),
    )

    body = client.post("/query", json={"sql": SQL}).text

    for leaked in ("secret-project", "secret_dataset", "payroll", "Access Denied"):
        assert leaked not in body, f"BigQuery's denial text reached the caller: {leaked!r}"
    _assert_only_the_dry_run_ran(fake)


def test_non_select_is_still_a_400_and_nothing_runs(main, client, monkeypatch):
    """The widened except must not have moved the statement-type check."""
    fake = _install(main, monkeypatch, statement_type="DELETE")

    resp = client.post("/query", json={"sql": "DELETE FROM credible_sets_v WHERE TRUE"})

    assert resp.status_code == 400, resp.text
    _assert_only_the_dry_run_ran(fake)


def test_table_outside_the_allow_list_is_still_a_403_and_nothing_runs(main, client, monkeypatch):
    """The gate's whole purpose; project-level dataViewer means only this stops the read."""
    fake = _install(
        main,
        monkeypatch,
        referenced_tables=[_Ref("other-project", "other_dataset", "payroll")],
    )

    resp = client.post("/query", json={"sql": "SELECT * FROM `other-project.other_dataset.payroll`"})

    assert resp.status_code == 403, resp.text
    _assert_only_the_dry_run_ran(fake)


def test_an_allowed_select_still_passes_the_gate(main, client, monkeypatch):
    """The control: without this the suite would pass if the gate refused everything."""
    fake = _install(
        main,
        monkeypatch,
        referenced_tables=[_Ref(main.PROJECT_ID, main.DATASET_ID, "datasets_v")],
    )

    resp = client.post("/query", json={"sql": "SELECT * FROM datasets_v"})

    assert resp.status_code == 200, resp.text
    assert len(fake.calls) == 2
    assert fake.calls[1][1].dry_run is False
