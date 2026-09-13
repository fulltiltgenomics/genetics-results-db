"""Caps, charging and refunds on db-api's BigQuery paths *other than* /query.

Design of record: docs/code-execution-security.md section 4 in genetics-results-suite.

The aggregate per-`jti` budget is documented as covering every query of one execution, but it
was charged from /query alone. /schema, /stats and /tables/{t}/sample also submit jobs, none of
them is cached at the HTTP layer, and each took the per-credential byte cap without ever being
charged — so a script could loop them for its whole wall clock entirely outside the budget.
Worse, /schema's distinct-value scans passed no request at all and so ran at the *relaxed*
100 GB ceiling, twice the sandbox per-query cap.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from test_query_caps import (  # noqa: E402
    GB,
    _fake,
    _post,
    _Result,
    clean_budget,  # noqa: F401  (fixture)
    client,  # noqa: F401  (fixture)
    main,  # noqa: F401  (fixture)
)
from test_sandbox_token_auth import (  # noqa: E402,F401
    SECRET,
    SIGNING_KEY,
    _mint,
    _reload,
)


class _InternalJob:
    def __init__(self, job_config, rows, bytes_processed):
        self.job_config = job_config
        self.total_bytes_processed = bytes_processed
        self._rows = rows

    def result(self):
        return self._rows


class _TableMeta:
    num_rows = 7
    description = ""
    schema: list = []


class FakeInternalBQ:
    """Fake for the service's own scans, which have no dry run to price them with."""

    def __init__(self, rows, bytes_processed=10 * GB, fail=None):
        self.rows = rows
        self.bytes_processed = bytes_processed
        self.fail = fail
        self.configs = []

    def query(self, sql, job_config=None):
        self.configs.append(job_config)
        if self.fail is not None:
            raise self.fail
        return _InternalJob(job_config, self.rows, self.bytes_processed)

    def get_table(self, ref):
        return _TableMeta()


def _get(client, path, token):
    return client.get(path, headers={"Authorization": f"Bearer {token}"})


# --- /stats and /tables/{t}/sample -----------------------------------------------------------


def test_stats_is_charged_to_the_aggregate_budget(main, client, monkeypatch):
    row = type("R", (), {"dataset": "d", "data_type": "t", "count": 1})()
    fake = FakeInternalBQ([row], bytes_processed=30 * GB)
    monkeypatch.setattr(main, "bq_client", fake)

    assert _get(client, "/stats", _mint()).status_code == 200
    assert fake.configs[-1].maximum_bytes_billed == main.SANDBOX_MAX_BYTES_BILLED
    assert main._jti_bytes["exec-123"] == 30 * GB


def test_stats_429s_once_the_execution_budget_is_spent(main, client, monkeypatch):
    row = type("R", (), {"dataset": "d", "data_type": "t", "count": 1})()
    monkeypatch.setattr(main, "bq_client", FakeInternalBQ([row], bytes_processed=30 * GB))
    main._charge_aggregate_spent("exec-123", main.SANDBOX_AGGREGATE_BYTES_BUDGET)

    resp = _get(client, "/stats", _mint())
    assert resp.status_code == 429
    assert "budget" in str(resp.json()["detail"]).lower()


def test_sample_is_capped_and_charged(main, client, monkeypatch):
    fake = FakeInternalBQ(_Result(3), bytes_processed=20 * GB)
    monkeypatch.setattr(main, "bq_client", fake)

    assert _get(client, "/tables/credible_sets_v/sample", _mint()).status_code == 200
    assert fake.configs[-1].maximum_bytes_billed == main.SANDBOX_MAX_BYTES_BILLED
    assert main._jti_bytes["exec-123"] == 20 * GB


def test_sample_is_neither_tightened_nor_charged_for_the_shared_secret(main, client, monkeypatch):
    fake = FakeInternalBQ(_Result(3), bytes_processed=20 * GB)
    monkeypatch.setattr(main, "bq_client", fake)

    assert _get(client, "/tables/credible_sets_v/sample", SECRET).status_code == 200
    assert fake.configs[-1].maximum_bytes_billed == main.MAX_BYTES_BILLED == 100 * GB
    assert not main._jti_bytes


# --- /schema's distinct-value scans ----------------------------------------------------------


@pytest.fixture
def one_categorical_column(main, monkeypatch):
    monkeypatch.setattr(main, "_CATEGORICAL_COLUMNS", {"credible_sets_v": {"dataset": None}})
    main._VALUES_CACHE.clear()
    yield
    main._VALUES_CACHE.clear()


def test_schema_value_scans_run_at_the_triggering_callers_cap_and_are_charged(
    main, monkeypatch, one_categorical_column
):
    fake = FakeInternalBQ([{"dataset": ["a", "b"]}], bytes_processed=40 * GB)
    monkeypatch.setattr(main, "bq_client", fake)
    caps = main._Caps(main.SANDBOX_MAX_ROWS, main.SANDBOX_MAX_BYTES_BILLED, "exec-123")

    assert main._get_categorical_values("credible_sets_v", caps) == {"dataset": ["a", "b"]}
    assert fake.configs[0].maximum_bytes_billed == main.SANDBOX_MAX_BYTES_BILLED == 50 * GB
    assert main._jti_bytes["exec-123"] == 40 * GB


def test_a_scan_over_the_triggering_callers_ceiling_leaves_the_cache_for_the_next_caller(
    main, monkeypatch, one_categorical_column
):
    """Why a per-credential ceiling on the shared cache is safe: a job that exceeds it fails
    and populates nothing, so the next caller retries the scan under its own limits."""
    monkeypatch.setattr(
        main, "bq_client", FakeInternalBQ([], fail=main.BadRequest("bytes billed limit exceeded"))
    )
    tight = main._Caps(main.SANDBOX_MAX_ROWS, main.SANDBOX_MAX_BYTES_BILLED, "exec-123")
    assert main._get_categorical_values("credible_sets_v", tight) == {}
    assert "credible_sets_v" not in main._VALUES_CACHE

    monkeypatch.setattr(main, "bq_client", FakeInternalBQ([{"dataset": ["a"]}]))
    assert main._get_categorical_values("credible_sets_v", main._RELAXED_CAPS) == {"dataset": ["a"]}


def test_schema_scans_at_the_sandbox_ceiling_over_http(
    main, client, monkeypatch, one_categorical_column
):
    fake = FakeInternalBQ([{"dataset": ["a"]}], bytes_processed=5 * GB)
    monkeypatch.setattr(main, "bq_client", fake)

    assert _get(client, "/schema?table=credible_sets_v", _mint()).status_code == 200
    assert fake.configs, "the cache miss must have issued at least one job"
    assert all(c.maximum_bytes_billed == main.SANDBOX_MAX_BYTES_BILLED for c in fake.configs)
    assert main._jti_bytes["exec-123"] == 5 * GB * len(fake.configs)


# --- the counter bound, and refunds ----------------------------------------------------------


def test_the_reject_path_also_trims_the_counter(main):
    """A rejected charge still inserts — a 0-valued entry for a `jti` that never spent
    anything — so trimming only on the allowed branch left the bound unreal."""
    over = main.SANDBOX_AGGREGATE_BYTES_BUDGET + 1
    for i in range(main._JTI_BUDGET_LRU_SIZE + 50):
        allowed, _ = main._charge_aggregate(f"exec-{i}", over)
        assert allowed is False
    assert len(main._jti_bytes) == main._JTI_BUDGET_LRU_SIZE


def test_a_failed_query_refunds_its_pre_flight_charge(main, client, monkeypatch):
    """The charge is made from the dry run's estimate, before the job runs. A job that never
    runs bills nothing, so a script's syntax errors must not consume its execution budget."""
    fake = _fake(main, monkeypatch, bytes_processed=10 * GB)
    original_query = fake.query

    def query(sql, job_config=None):
        if job_config.dry_run:
            return original_query(sql, job_config=job_config)
        raise main.BadRequest("Syntax error")

    monkeypatch.setattr(fake, "query", query)
    assert _post(client, _mint()).status_code == 400
    assert main._jti_bytes["exec-123"] == 0
