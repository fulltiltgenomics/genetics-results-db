"""Tests for the per-credential row and byte caps on /query.

Design of record: docs/code-execution-security.md section 4 in genetics-results-suite.

The properties that matter:
  * the tight limits are the **default** — a sandbox token, and an unauthenticated caller on
    a fail-open deployment, both get them. Only a request verified against
    INTERNAL_API_SECRET is relaxed to the operator-configured MAX_ROWS/MAX_BYTES_BILLED, so a
    caller can never widen its limits by presenting a weaker credential or none at all;
  * the row cap is applied in the handler, not by the class-level `le=MAX_ROWS` on
    `QueryRequest`, which cannot vary per credential;
  * the aggregate per-`jti` byte budget answers 429, never a truncated result.
"""

import os
import sys
import time

import jwt
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from test_sandbox_token_auth import SECRET, SIGNING_KEY, _mint, _reload  # noqa: E402

GB = 1024**3


class _Ref:
    def __init__(self, project, dataset_id, table_id):
        self.project, self.dataset_id, self.table_id = project, dataset_id, table_id


class _Row:
    def __init__(self, n):
        self._n = n

    def values(self):
        return (self._n, "x")


class _Result:
    def __init__(self, total_rows):
        self.total_rows = total_rows
        self.schema = [type("F", (), {"name": "n"})(), type("F", (), {"name": "s"})()]

    def __iter__(self):
        return (_Row(i) for i in range(self.total_rows))


class _Job:
    def __init__(self, job_config, total_rows, bytes_processed):
        self.job_config = job_config
        self.statement_type = "SELECT"
        self.total_bytes_processed = bytes_processed
        self._total_rows = total_rows

    @property
    def referenced_tables(self):
        return [_Ref("test-project", "genetics_results", "credible_sets_v")]

    def result(self):
        return _Result(self._total_rows)


class FakeBQ:
    """Records every job config it is handed so the tests can assert on the caps."""

    def __init__(self, total_rows=10, bytes_processed=1 * GB):
        self.total_rows = total_rows
        self.bytes_processed = bytes_processed
        self.configs = []

    def query(self, sql, job_config=None):
        self.configs.append(job_config)
        return _Job(job_config, self.total_rows, self.bytes_processed)

    @property
    def dry_run_config(self):
        return self.configs[0]

    @property
    def execution_config(self):
        return self.configs[1]


@pytest.fixture(scope="module")
def main():
    mp = pytest.MonkeyPatch()
    module = _reload(mp, INTERNAL_API_SECRET=SECRET, SANDBOX_TOKEN_SIGNING_KEY=SIGNING_KEY)
    module.MAX_ROWS = 100_000
    module.MAX_BYTES_BILLED = 100 * GB
    module._RELAXED_CAPS = module._Caps(100_000, 100 * GB, None)
    yield module
    mp.undo()


@pytest.fixture
def client(main):
    return TestClient(main.app)


@pytest.fixture(autouse=True)
def clean_budget(main):
    main._jti_bytes.clear()
    yield
    main._jti_bytes.clear()


def _fake(main, monkeypatch, **kwargs):
    fake = FakeBQ(**kwargs)
    monkeypatch.setattr(main, "bq_client", fake)
    return fake


def _post(client, token, **body):
    return client.post(
        "/query",
        json={"sql": "SELECT 1 FROM credible_sets_v", **body},
        headers={"Authorization": f"Bearer {token}"},
    )


# --- the tight limits are the default ------------------------------------------------------


def test_sandbox_gets_the_tight_byte_cap_and_row_cap(main, client, monkeypatch):
    fake = _fake(main, monkeypatch, total_rows=60_000)
    resp = _post(client, _mint(), max_rows=100_000)

    assert resp.status_code == 200
    assert fake.execution_config.maximum_bytes_billed == main.SANDBOX_MAX_BYTES_BILLED == 50 * GB
    assert fake.dry_run_config.maximum_bytes_billed == 50 * GB
    assert len(resp.json()["rows"]) == main.SANDBOX_MAX_ROWS == 25_000
    assert resp.json()["truncated"] is True


def test_sandbox_asking_for_fewer_rows_than_the_cap_still_gets_what_it_asked_for(
    main, client, monkeypatch
):
    """The cap clamps down; it must not become a floor a script can raise itself to."""
    _fake(main, monkeypatch, total_rows=60_000)
    resp = _post(client, _mint(), max_rows=10)
    assert len(resp.json()["rows"]) == 10


def test_no_credential_on_a_fail_open_deployment_gets_the_tight_limits(monkeypatch):
    """db-api's fail-open branch (INTERNAL_API_SECRET unset) leaves principal None. That is a
    weaker credential than the shared secret and must not buy looser limits."""
    module = _reload(monkeypatch, SANDBOX_TOKEN_SIGNING_KEY=SIGNING_KEY)
    fake = FakeBQ(total_rows=60_000)
    monkeypatch.setattr(module, "bq_client", fake)

    resp = TestClient(module.app).post(
        "/query", json={"sql": "SELECT 1 FROM credible_sets_v", "max_rows": 100_000}
    )

    assert resp.status_code == 200
    assert fake.execution_config.maximum_bytes_billed == module.SANDBOX_MAX_BYTES_BILLED
    assert len(resp.json()["rows"]) == module.SANDBOX_MAX_ROWS


# --- and are relaxed only for the verified shared secret ------------------------------------


def test_shared_secret_gets_the_operator_configured_limits(main, client, monkeypatch):
    fake = _fake(main, monkeypatch, total_rows=60_000)
    resp = _post(client, SECRET, max_rows=100_000)

    assert resp.status_code == 200
    assert fake.execution_config.maximum_bytes_billed == main.MAX_BYTES_BILLED == 100 * GB
    assert len(resp.json()["rows"]) == 60_000
    assert resp.json()["truncated"] is False


def test_the_row_cap_is_applied_in_the_handler_not_by_the_pydantic_field(main):
    """`max_rows` carries a class-level `le=MAX_ROWS` evaluated once at model definition time.
    It is identical for every caller, so it cannot be the place the per-credential cap lives."""
    field = main.QueryRequest.model_fields["max_rows"]
    bound = next(m.le for m in field.metadata if hasattr(m, "le"))
    assert bound == main.MAX_ROWS > main.SANDBOX_MAX_ROWS
    # a sandbox request well under that bound is still clamped, which only the handler can do
    assert main.QueryRequest(sql="SELECT 1", max_rows=50_000).max_rows == 50_000


# --- the aggregate per-jti budget -----------------------------------------------------------


def test_aggregate_budget_returns_429_rather_than_truncating(main, client, monkeypatch):
    _fake(main, monkeypatch, total_rows=10, bytes_processed=80 * GB)
    token = _mint()

    for i in range(2):
        assert _post(client, token).status_code == 200, f"query {i} should fit in the budget"

    # 160 GB spent, a third 80 GB query would exceed the 200 GB budget
    resp = _post(client, token)
    assert resp.status_code == 429
    assert "budget" in resp.json()["detail"].lower()
    assert main._jti_bytes["exec-123"] == 160 * GB, "a rejected query must not be charged"


def test_the_budget_is_per_jti_not_per_user_or_session(main, client, monkeypatch):
    _fake(main, monkeypatch, bytes_processed=150 * GB)
    assert _post(client, _mint(jti="exec-a")).status_code == 200
    assert _post(client, _mint(jti="exec-a")).status_code == 429
    assert _post(client, _mint(jti="exec-b")).status_code == 200


def test_the_shared_secret_is_not_charged_to_the_budget(main, client, monkeypatch):
    _fake(main, monkeypatch, bytes_processed=150 * GB)
    for _ in range(4):
        assert _post(client, SECRET).status_code == 200
    assert not main._jti_bytes


def test_a_dry_run_is_not_charged(main, client, monkeypatch):
    _fake(main, monkeypatch, bytes_processed=150 * GB)
    for _ in range(4):
        assert _post(client, _mint(), dry_run=True).status_code == 200
    assert not main._jti_bytes


def test_the_charge_is_reconciled_to_the_bytes_actually_billed(main, client, monkeypatch):
    """The pre-flight charge uses the dry run's estimate; a cache hit bills less."""

    fake = _fake(main, monkeypatch, bytes_processed=10 * GB)
    original_query = fake.query

    def query(sql, job_config=None):
        job = original_query(sql, job_config=job_config)
        if not job_config.dry_run:
            job.total_bytes_processed = 0  # served from cache
        return job

    monkeypatch.setattr(fake, "query", query)
    assert _post(client, _mint()).status_code == 200
    assert main._jti_bytes["exec-123"] == 0


def test_the_counter_is_bounded_so_a_flood_of_jtis_cannot_grow_it(main):
    for i in range(main._JTI_BUDGET_LRU_SIZE + 50):
        main._charge_aggregate(f"exec-{i}", 1)
    assert len(main._jti_bytes) == main._JTI_BUDGET_LRU_SIZE
