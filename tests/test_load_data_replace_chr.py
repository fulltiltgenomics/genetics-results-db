"""Tests for the per-chromosome replace that makes a sharded load rerunnable, and for the
JSON-string-to-typed-array projection that rides the same staging table.

What is pinned is the ORDER of operations: every refusal (row count, stray chromosome,
a value the typed array does not reproduce) has to land before the DELETE, because after
it the target has already lost the rows a bad shard set was about to replace.

Whether the generated SQL means what it says is not something a stub can answer; that was
checked against BigQuery with literal rows. These tests pin its shape and where it runs.

Client-free: the BigQuery client is a stub, so no project or network is needed.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import load_data  # noqa: E402
from test_load_data_row_counts import (  # noqa: E402
    INSERTED,
    STAGED,
    _FakeLoadJob,
    _FakeQueryJob,
    _queries,
)

TABLE = "gnomad_variant_annotation"
URI = "gs://b/shards/chr7.part-*.tsv.gz"


def _client(stray=0, mismatched=0):
    class _Job(_FakeQueryJob):
        def result(self):
            if not self.sql.startswith("SELECT COUNTIF"):
                return MagicMock()
            # one count per COUNTIF, in the order the loader asked for them
            counts = [stray] if "IS DISTINCT FROM" in self.sql else []
            return iter([(*counts, mismatched)])

    c = MagicMock()
    c.load_table_from_uri.side_effect = lambda uri, table, job_config=None: _FakeLoadJob(STAGED)
    c.query.side_effect = lambda sql, job_config=None: _Job(sql)
    return c


def _consequences():
    return next(f for f in load_data.SCHEMAS[TABLE] if f.name == "consequences")


def _load(client, **kwargs):
    return load_data.load_table(client, URI, f"p.d.{TABLE}", TABLE, skip_leading_rows=0, **kwargs)


def test_replace_checks_then_deletes_then_inserts():
    client = _client()
    job, rows = _load(client, replace_chr=7, expect_rows=STAGED)

    check, delete, insert = _queries(client)
    assert "__staging_" in check and "IS DISTINCT FROM 7" in check
    assert load_data.json_array_mismatch_sql(_consequences()) in check
    assert delete == f"DELETE FROM `p.d.{TABLE}` WHERE `chr` = 7"
    assert insert.startswith(f"INSERT INTO `p.d.{TABLE}` (")
    assert "CONCAT(`chr`, ':', `pos`, ':', `ref`, ':', `alt`)) AS `variant`" in insert
    assert rows == INSERTED
    client.delete_table.assert_called_once()


def test_a_row_count_mismatch_leaves_the_target_untouched():
    client = _client()
    with pytest.raises(RuntimeError, match="left untouched"):
        _load(client, replace_chr=7, expect_rows=STAGED + 1)
    assert client.query.call_count == 0
    client.delete_table.assert_called_once()


def test_a_stray_chromosome_leaves_the_target_untouched():
    client = _client(stray=3)
    with pytest.raises(RuntimeError, match="not on chr 7"):
        _load(client, replace_chr=7)
    assert [q.split()[0] for q in _queries(client)] == ["SELECT"]
    client.delete_table.assert_called_once()


def test_an_unparseable_consequences_value_leaves_the_target_untouched():
    client = _client(mismatched=2)
    with pytest.raises(RuntimeError, match="2 staged rows.*`consequences`.*left untouched"):
        _load(client, replace_chr=7)
    assert [q.split()[0] for q in _queries(client)] == ["SELECT"]
    client.delete_table.assert_called_once()


def test_the_value_check_runs_without_replace_chr_too():
    """A plain append has no DELETE to protect, but it would still load the value thinner."""
    client = _client(mismatched=1)
    with pytest.raises(RuntimeError, match="left untouched"):
        _load(client)
    (check,) = _queries(client)
    assert "IS DISTINCT FROM" not in check


def test_consequences_is_staged_as_a_string_and_projected_into_the_typed_array():
    client = _client()
    _load(client, replace_chr=7)

    staged = {
        f.name: f for f in client.load_table_from_uri.call_args.kwargs["job_config"].schema
    }
    assert (staged["consequences"].field_type, staged["consequences"].mode) == (
        "STRING",
        "NULLABLE",
    )
    insert = _queries(client)[-1]
    assert f"{load_data.json_array_projection_sql(_consequences())} AS `consequences`" in insert


def test_the_projection_reads_every_sub_field_and_keeps_the_file_order():
    field = _consequences()
    sql = load_data.json_array_projection_sql(field)

    for sub in field.fields:
        assert f"'$.{sub.name}') AS `{sub.name}`" in sql or (
            f"'$.{sub.name}') AS INT64) AS `{sub.name}`" in sql
        )
    assert "JSON_VALUE_ARRAY(e, '$.consequences')" in sql
    assert "SAFE_CAST(JSON_VALUE(e, '$.canonical') AS INT64)" in sql
    # an ARRAY subquery without ORDER BY promises no order
    assert sql.endswith("UNNEST(JSON_QUERY_ARRAY(`consequences`)) AS e WITH OFFSET AS o ORDER BY o)")


def test_the_mismatch_predicate_spares_the_null_marker_and_fails_closed():
    sql = load_data.json_array_mismatch_sql(_consequences())
    # NA is staged as NULL and is a legitimate empty array, not a mismatch
    assert sql.startswith("`consequences` IS NOT NULL AND IFNULL(")
    # a string PARSE_JSON cannot read compares as NULL, which must count as a mismatch
    assert "SAFE.PARSE_JSON(`consequences`)" in sql and sql.endswith(", TRUE)")


def test_a_sub_field_type_with_no_json_reader_is_refused_rather_than_guessed():
    from google.cloud import bigquery

    field = bigquery.SchemaField(
        "c", "RECORD", mode="REPEATED", fields=(bigquery.SchemaField("x", "FLOAT64"),)
    )
    with pytest.raises(ValueError, match="no JSON projection for c.x"):
        load_data.json_array_projection_sql(field)


def test_every_json_array_column_is_a_repeated_record_in_its_schema():
    for table, names in load_data.JSON_ARRAY_COLUMNS.items():
        fields = {f.name: f for f in load_data.SCHEMAS[table]}
        for name in names:
            assert (fields[name].field_type, fields[name].mode) == ("RECORD", "REPEATED")


def test_replace_refuses_a_truncating_disposition():
    client = _client()
    with pytest.raises(ValueError, match="WRITE_APPEND"):
        _load(client, replace_chr=7, write_disposition="WRITE_TRUNCATE")
    assert client.load_table_from_uri.call_count == 0


@pytest.mark.parametrize("kwargs", [{"replace_chr": 7}, {"expect_rows": 1}])
def test_direct_path_tables_refuse_both_options(kwargs):
    client = _client()
    with pytest.raises(ValueError, match="staging path"):
        load_data.load_table(
            client, "gs://b/f.tsv.gz", "p.d.variant_annotation", "variant_annotation", **kwargs
        )
    assert client.load_table_from_uri.call_count == 0


def test_chr_string_tables_refuse_replace():
    client = _client()
    with pytest.raises(ValueError, match="chr-string"):
        load_data.load_table(
            client, "gs://b/f.tsv.gz", "p.d.mpra", "mpra", replace_chr=7
        )


def test_json_bearing_source_is_loaded_unquoted():
    """A double quote is the CSV loader's default quote character and JSON is full of them."""
    client = _client()
    _load(client)
    config = client.load_table_from_uri.call_args.kwargs["job_config"]
    assert config.quote_character == ""
    assert "variant" not in [f.name for f in config.schema]


def test_other_tables_keep_the_default_quoting():
    client = _client()
    load_data.load_table(client, "gs://b/f.tsv.gz", "p.d.mpra", "mpra")
    config = client.load_table_from_uri.call_args.kwargs["job_config"]
    assert config.quote_character is None
    assert config.allow_quoted_newlines is True
