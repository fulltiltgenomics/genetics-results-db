"""The caller's SQL must reach BigQuery byte-for-byte — no parsing, no rewriting.

This is the guard for a regression that has now come back THREE TIMES. db-api used to
rewrite the SQL text with a regex, replacing a bare name after FROM/JOIN with the
fully-qualified view. Each round closed one lexical case and revealed the next: a CTE
aliased to a table name, then a *backticked* CTE declaration, then BigQuery's decoding of
escape sequences inside backticked identifiers plus backtick pairing desynchronised by
`` `a\\`b` ``. Every round produced the identical failure — the caller's own CTE silently
replaced by the 889-row view, returned with HTTP 200. No error, no warning, wrong answer.

The fix was to delete the rewrite and let `default_dataset` on the job config make BigQuery
resolve bare names, so SQL scoping is applied by the thing that defines it.

The behavioural tests for that live in test_query_name_resolution.py, but they SKIP unless
LIVE_BQ_* is pointed at a real dataset, and this repo has no CI — so on most runs nothing
holds the line. Hence this module: it needs no BigQuery and no environment, and it asserts
the only thing that can bring the whole class back, which is somebody reintroducing SQL
parsing. Do not delete it as redundant with the live tests; it is what runs by default.
"""

import os
import sys

import pytest
from fastapi.testclient import TestClient
from google.cloud import bigquery

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from test_sandbox_token_auth import _reload  # noqa: E402

# names of the successive rewrite implementations and their helpers; each was deleted after
# it was found to be emulating a scoping rule it could not get right
REWRITE_SYMBOLS = (
    "_qualify_tables",
    "_cte_names",
    "_QUALIFY_TARGETS",
    "_SQL_NOISE",
    "_IDENT_NOISE",
    "_blank_noise",
    "_CTE_SCAN",
)

# a query every one of the deleted rewrites got wrong: a CTE named after an exposed view,
# declared backticked, with the view name also appearing in a comment
TRICKY_SQL = "WITH `datasets_v` AS (SELECT 1 AS n)\nSELECT n FROM datasets_v -- FROM credible_sets_v\n"


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
    statement_type = "SELECT"
    total_bytes_processed = 1024

    def __init__(self, project, dataset_id):
        self.referenced_tables = [_Ref(project, dataset_id, "datasets_v")]

    def result(self):
        return _Result()


class RecordingBQ:
    """Records the exact SQL string and job config of every query() call."""

    def __init__(self, project, dataset_id):
        self.calls = []
        self._project, self._dataset_id = project, dataset_id

    def query(self, sql, job_config=None):
        self.calls.append((sql, job_config))
        return _Job(self._project, self._dataset_id)


@pytest.fixture(scope="module")
def main():
    mp = pytest.MonkeyPatch()
    module = _reload(mp)
    yield module
    mp.undo()


@pytest.fixture
def recorder(main, monkeypatch):
    fake = RecordingBQ(main.PROJECT_ID, main.DATASET_ID)
    monkeypatch.setattr(main, "bq_client", fake)
    return fake


def test_no_sql_parsing_helper_has_been_reintroduced(main):
    """Any of these coming back means the emulator is back, whatever it is called."""
    present = [name for name in REWRITE_SYMBOLS if hasattr(main, name)]
    assert present == [], f"SQL-rewriting helpers reintroduced in api.main: {present}"


def test_query_endpoint_sends_the_callers_sql_unmodified(main, recorder):
    """Both the dry-run probe and the execution must see the caller's string, byte for byte."""
    resp = TestClient(main.app).post("/query", json={"sql": TRICKY_SQL})

    assert resp.status_code == 200, resp.text
    assert len(recorder.calls) == 2
    for sql, _ in recorder.calls:
        assert sql == TRICKY_SQL


def test_dry_run_probe_and_execution_both_carry_a_default_dataset(main, recorder):
    """`default_dataset` is what replaced the rewrite; without it bare names cannot resolve."""
    TestClient(main.app).post("/query", json={"sql": TRICKY_SQL})

    for _, job_config in recorder.calls:
        assert job_config.default_dataset is not None
        assert job_config.default_dataset.project == main.PROJECT_ID
        assert job_config.default_dataset.dataset_id == main.DATASET_ID


def test_probes_default_dataset_comes_from_the_callers_config_not_a_constant(main, recorder):
    """A probe that resolves names differently from the execution is a gate bypass: the
    allow-list is checked against the dry run's referencedTables, so the statement that gets
    authorized has to be the statement that runs."""
    caller_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=1024,
        default_dataset="other-project.other_dataset",
    )
    main.authorize_query("SELECT 1", caller_config)

    dry_config = recorder.calls[0][1]
    assert dry_config.dry_run is True
    assert dry_config.default_dataset.project == "other-project"
    assert dry_config.default_dataset.dataset_id == "other_dataset"
    assert dry_config.default_dataset != main._DEFAULT_DATASET
