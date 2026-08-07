"""Tests for load_table's (job, rows_written) contract.

Regression guard for "Loaded None rows": the staging path finishes with a SELECT
into a destination table, which BigQuery does not treat as DML and publishes no
rows-written statistic for, so the count cannot be read back off that job and has
to be carried out of load_table explicitly.

Client-free: the BigQuery client is a stub, so no project or network is needed.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import load_data  # noqa: E402


STAGED = 506961


class _FakeLoadJob:
    def __init__(self, output_rows):
        self.output_rows = output_rows
        self.errors = None
        self.result_calls = 0

    def result(self):
        self.result_calls += 1
        return self


class _FakeQueryJob:
    """A QueryJob writing to a destination exposes neither of the row-count fields."""

    def __init__(self):
        self.errors = None
        self.num_dml_affected_rows = None  # not DML, so never populated

    def result(self):
        # a RowIterator over the destination table; its total_rows describes the
        # table AFTER the write, which is why it cannot be used as rows-written
        return MagicMock(total_rows=999_999)


@pytest.fixture
def client(monkeypatch):
    c = MagicMock()
    c.load_table_from_uri.side_effect = lambda uri, table, job_config=None: (
        _FakeLoadJob(STAGED) if "staging" in table else _FakeLoadJob(1234)
    )
    c.query.return_value = _FakeQueryJob()
    return c


def test_direct_path_defers_the_count_to_the_caller(client):
    """The LoadJob is returned un-awaited so main() can still report job.errors."""
    job, rows = load_data.load_table(
        client, "gs://b/f.tsv.gz", "p.d.credible_sets", "credible_sets"
    )

    assert rows is None
    assert job.result_calls == 0, "awaiting here would lose job.errors on failure"
    # main() resolves it off the completed job, as it always did
    job.result()
    assert job.output_rows == 1234


def test_staging_path_returns_the_staged_row_count(client):
    """hla_associations is a CHR_STRING_TABLE and injects `dataset`, so it stages."""
    job, rows = load_data.load_table(
        client,
        "gs://b/finngen_hla.tsv.gz",
        "p.d.hla_associations",
        "hla_associations",
        const_columns={"dataset": "finngen_hla"},
    )

    assert rows == STAGED
    # the projection is an unfiltered 1:1 SELECT over staging, so staged == written
    assert not hasattr(job, "output_rows")
    assert job.num_dml_affected_rows is None
    assert job.result().total_rows != rows, "destination total is not rows-written"


def test_staging_count_survives_the_old_getattr_lookups(client):
    """The two fields the previous code consulted are both absent — hence None."""
    job, rows = load_data.load_table(
        client,
        "gs://b/finngen_hla.tsv.gz",
        "p.d.hla_associations",
        "hla_associations",
        const_columns={"dataset": "finngen_hla"},
    )

    old_style = getattr(job, "output_rows", None)
    if old_style is None:
        old_style = getattr(job, "num_dml_affected_rows", None)
    assert old_style is None
    assert rows == STAGED


@pytest.mark.parametrize(
    "table_type",
    sorted(load_data.CHR_STRING_TABLES),
)
def test_every_chr_string_table_takes_the_staging_path(client, table_type):
    """These all convert chr, so none of them can report a count off its job."""
    job, rows = load_data.load_table(
        client, "gs://b/f.tsv.gz", f"p.d.{table_type}", table_type
    )
    assert rows == STAGED
