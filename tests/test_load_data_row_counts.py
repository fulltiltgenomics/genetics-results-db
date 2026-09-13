"""Tests for load_table's (job, rows_written) contract and the staging projection.

Regression guard for "Loaded None rows": the staging path ends in an INSERT whose
`num_dml_affected_rows` is the count, and load_table has to carry it out explicitly
because the caller cannot read `output_rows` off a query job.

The projection has to be DML into the existing table rather than a query job with a
destination: a destination-table WRITE_TRUNCATE replaces the schema and drops every
NOT NULL mode and column description.

Client-free: the BigQuery client is a stub, so no project or network is needed.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import NotFound

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import load_data  # noqa: E402

STAGED = 506961
# distinct from STAGED only so a test can tell which job the count was read from
INSERTED = 506960


class _FakeLoadJob:
    def __init__(self, output_rows):
        self.output_rows = output_rows
        self.errors = None
        self.result_calls = 0

    def result(self):
        self.result_calls += 1
        return self


class _FakeQueryJob:
    def __init__(self, sql):
        self.sql = sql
        self.errors = None
        self.num_dml_affected_rows = INSERTED if sql.startswith("INSERT") else None

    def result(self):
        return MagicMock(total_rows=999_999)


@pytest.fixture
def client(monkeypatch):
    c = MagicMock()
    c.load_table_from_uri.side_effect = lambda uri, table, job_config=None: (
        _FakeLoadJob(STAGED) if "staging" in table else _FakeLoadJob(1234)
    )
    c.query.side_effect = lambda sql, job_config=None: _FakeQueryJob(sql)
    return c


def _queries(client):
    return [call.args[0] for call in client.query.call_args_list]


def _direct_path_tables():
    """Tables that load directly: no chr conversion, no prefix strip, no derived columns.

    Derived from the module rather than hardcoded — `credible_sets` was the obvious
    direct-path table until it gained computed `variant`/`resource` columns, which
    silently moved it onto the staging path.
    """
    return sorted(
        t
        for t in load_data.SCHEMAS
        if t not in load_data.CHR_STRING_TABLES
        and t not in load_data.CELL_TYPE_PREFIX_TABLES
        and t not in load_data.DERIVED_COLUMNS
    )


@pytest.mark.parametrize("table_type", _direct_path_tables())
def test_direct_path_defers_the_count_to_the_caller(client, table_type):
    """The LoadJob is returned un-awaited so main() can still report job.errors."""
    job, rows = load_data.load_table(
        client, "gs://b/f.tsv.gz", f"p.d.{table_type}", table_type
    )

    assert rows is None
    assert job.result_calls == 0, "awaiting here would lose job.errors on failure"
    assert client.query.call_count == 0
    # main() resolves it off the completed job, as it always did
    job.result()
    assert job.output_rows == 1234


def test_staging_path_returns_the_insert_row_count(client):
    """hla_associations is a CHR_STRING_TABLE and injects `dataset`, so it stages."""
    job, rows = load_data.load_table(
        client,
        "gs://b/finngen_hla.tsv.gz",
        "p.d.hla_associations",
        "hla_associations",
        const_columns={"dataset": "finngen_hla"},
    )

    assert job.sql.startswith("INSERT")
    assert rows == INSERTED == job.num_dml_affected_rows
    assert not hasattr(job, "output_rows")


def test_write_truncate_truncates_then_inserts_into_the_existing_table(client):
    load_data.load_table(
        client,
        "gs://b/finngen_hla.tsv.gz",
        "p.d.hla_associations",
        "hla_associations",
        write_disposition="WRITE_TRUNCATE",
        const_columns={"dataset": "finngen_hla"},
    )

    truncate, insert = _queries(client)
    assert truncate == "TRUNCATE TABLE `p.d.hla_associations`"
    columns = ", ".join(f"`{f.name}`" for f in load_data.SCHEMAS["hla_associations"])
    assert insert.startswith(f"INSERT INTO `p.d.hla_associations` ({columns})\n")
    assert "__staging_" in insert
    for call in client.query.call_args_list:
        config = call.kwargs.get("job_config")
        assert config is None or config.destination is None
        assert config is None or config.create_disposition is None
    # the injected constant travels as a query parameter, never as a literal
    insert_config = client.query.call_args_list[1].kwargs["job_config"]
    assert [(q.name, q.value) for q in insert_config.query_parameters] == [
        ("const_dataset", "finngen_hla")
    ]


def test_write_append_issues_no_truncate(client):
    load_data.load_table(
        client,
        "gs://b/finngen_hla.tsv.gz",
        "p.d.hla_associations",
        "hla_associations",
        write_disposition="WRITE_APPEND",
        const_columns={"dataset": "finngen_hla"},
    )

    (insert,) = _queries(client)
    assert insert.startswith("INSERT INTO `p.d.hla_associations` (")


def test_missing_target_table_points_at_setup_bigquery(client):
    client.get_table.side_effect = NotFound("p.d.hla_associations")

    with pytest.raises(RuntimeError, match="scripts/setup_bigquery.sh"):
        load_data.load_table(
            client,
            "gs://b/finngen_hla.tsv.gz",
            "p.d.hla_associations",
            "hla_associations",
            write_disposition="WRITE_TRUNCATE",
            const_columns={"dataset": "finngen_hla"},
        )

    # refused before anything was staged or truncated
    assert client.load_table_from_uri.call_count == 0
    assert client.query.call_count == 0


@pytest.mark.parametrize(
    "table_type",
    sorted(load_data.CHR_STRING_TABLES),
)
def test_every_chr_string_table_takes_the_staging_path(client, table_type):
    """These all convert chr, so none of them can report a count off a load job."""
    job, rows = load_data.load_table(
        client, "gs://b/f.tsv.gz", f"p.d.{table_type}", table_type
    )
    assert rows == INSERTED
    (insert,) = _queries(client)
    assert insert.startswith(f"INSERT INTO `p.d.{table_type}` (")
