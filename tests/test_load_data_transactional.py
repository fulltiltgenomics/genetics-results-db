"""Tests for the all-or-none multi-table replace (`--transaction`).

What is pinned is where each refusal lands relative to the one script that touches the
targets: a missing target, a bad const column or a short staged file must stop the run
before that script is sent, and every staging table is dropped either way. Whether the
script's transaction semantics hold is BigQuery's to answer and was checked against the
rehearsal dataset; these tests pin its shape.

Client-free: the BigQuery client is a stub, so no project or network is needed.
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import NotFound

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import load_data  # noqa: E402
from test_load_data_row_counts import (  # noqa: E402
    _FakeLoadJob,
    _FakeQueryJob,
    _queries,
)

ROWS = {"cellxgene_expression": 16173377, "cellxgene_datasets": 8}


def _header(table_type):
    aliases = load_data.HEADER_ALIASES.get(table_type, {})
    consts = {"release"} if table_type == "cellxgene_datasets" else set()
    return [aliases.get(f.name, f.name) for f in load_data.SCHEMAS[table_type] if f.name not in consts]


def _headers(uri):
    return _header(next(t for t in ROWS if f"/{t}" in uri))


def _client(missing=()):
    c = MagicMock()
    c.load_table_from_uri.side_effect = lambda uri, table, job_config=None: _FakeLoadJob(
        next(n for t, n in ROWS.items() if f".{t}__staging_" in str(table))
    )
    c.query.side_effect = lambda sql, job_config=None: _FakeQueryJob(sql)

    def get_table(table_id):
        if table_id.rsplit(".", 1)[1] in missing:
            raise NotFound(table_id)
        return MagicMock()

    c.get_table.side_effect = get_table
    return c


def _loads(**overrides):
    loads = [
        {"table_id": f"p.d.{t}", "table_type": t, "gcs_uri": f"gs://b/r/bq/{t}.tsv.gz",
         "expect_rows": n}
        for t, n in ROWS.items()
    ]
    loads[1]["const_columns"] = {"release": "2026-10-13-120000"}
    for i, kv in overrides.items():
        loads[int(i[1:])].update(kv)
    return loads


def _staged(client):
    return [call.args[1] for call in client.load_table_from_uri.call_args_list]


def _txn(client, loads, **kw):
    return load_data.load_tables_transactional(
        client, loads, read_header=kw.pop("read_header", _headers), **kw)


def _dropped(client):
    return [call.args[0] for call in client.delete_table.call_args_list]


def test_one_script_deletes_and_inserts_every_target_inside_a_transaction():
    client = _client()
    rows = _txn(client, _loads())
    assert rows == {f"p.d.{t}": n for t, n in ROWS.items()}
    (script,) = _queries(client)
    begin, commit = script.index("BEGIN TRANSACTION"), script.index("COMMIT TRANSACTION")
    for t, n in ROWS.items():
        delete = script.index(f"DELETE FROM `p.d.{t}` WHERE TRUE")
        insert = script.index(f"INSERT INTO `p.d.{t}`")
        check = script.index(f"(SELECT COUNT(*) FROM `p.d.{t}`) != {n}")
        assert begin < delete < insert < check < commit
    assert "ROLLBACK TRANSACTION" in script[commit:]
    assert sorted(map(str, _dropped(client))) == sorted(map(str, _staged(client)))


def test_const_column_is_a_parameter_with_a_per_table_prefix():
    client = _client()
    _txn(client, _loads())
    params = client.query.call_args.kwargs["job_config"].query_parameters
    assert [(p.name, p.value) for p in params] == [("t1_release", "2026-10-13-120000")]
    assert "@t1_release AS `release`" in _queries(client)[0]
    # the const column is not in the file, so the staged schema must not carry it
    datasets_config = client.load_table_from_uri.call_args_list[1].kwargs["job_config"]
    assert "release" not in [f.name for f in datasets_config.schema]
    # staging tables are created with an expiry, then appended to (a truncating load would
    # replace the table and its expiry with it)
    for call in client.create_table.call_args_list:
        assert call.args[0].expires is not None
    assert datasets_config.write_disposition == "WRITE_APPEND"


def test_short_file_touches_no_table_and_drops_every_staging_table():
    client = _client()
    with pytest.raises(RuntimeError, match="no table touched"):
        _txn(client, _loads(t1={"expect_rows": 9}))
    assert _queries(client) == []
    assert len(_dropped(client)) == 2 and sorted(map(str, _dropped(client))) == sorted(map(str, _staged(client)))


@pytest.mark.parametrize("mangle", [
    lambda h: [h[1], h[0], *h[2:]],     # two same-typed columns swapped
    lambda h: [*h, "extra"],            # a column the schema does not have
], ids=["swapped", "extra"])
def test_header_mismatch_touches_no_table(mangle):
    client = _client()

    def headers(uri):
        h = _headers(uri)
        return mangle(h) if "/cellxgene_expression/" in uri and uri.endswith("2.tsv.gz") else h

    loads = _loads(t0={"gcs_uri": [f"gs://b/r/bq/cellxgene_expression/{c}.tsv.gz" for c in (1, 2)]})
    with pytest.raises(RuntimeError, match="no table touched"):
        _txn(client, loads, read_header=headers)
    assert _staged(client) == [] and _queries(client) == [] and not client.create_table.called


def test_shards_are_staged_in_one_load_job():
    client = _client()
    shards = [f"gs://b/r/bq/cellxgene_expression/{c}.tsv.gz" for c in (1, 2, 23)]
    _txn(client, _loads(t0={"gcs_uri": shards}))
    assert client.load_table_from_uri.call_args_list[0].args[0] == shards


def test_sigterm_mid_staging_cancels_the_job_and_drops_staging():
    client = _client()
    job = _FakeLoadJob(0)
    job.result = MagicMock(side_effect=SystemExit(143))
    job.cancel = MagicMock()
    client.load_table_from_uri.side_effect = lambda uri, table, job_config=None: job
    with pytest.raises(SystemExit):
        _txn(client, _loads())
    job.cancel.assert_called_once()
    assert len(_dropped(client)) == 1 and _queries(client) == []


def test_missing_target_stops_before_anything_is_staged():
    client = _client(missing=("cellxgene_datasets",))
    with pytest.raises(RuntimeError, match="setup_bigquery.sh"):
        _txn(client, _loads())
    assert _staged(client) == [] and _queries(client) == []


@pytest.mark.parametrize(
    "loads, match",
    [
        ([], "distinct"),
        (_loads() + _loads()[:1], "distinct"),
        (_loads(t1={"const_columns": {"nope": "x"}}), "not in schema"),
        (_loads(t0={"table_type": "gene_annotations"}), "cannot be loaded in a transaction"),
        (_loads(t0={"table_type": "gnomad_variant_annotation"}), "cannot be loaded in a transaction"),
    ],
)
def test_refused_specs_touch_nothing(loads, match):
    client = _client()
    with pytest.raises(ValueError, match=match):
        _txn(client, loads)
    assert _staged(client) == [] and _queries(client) == []


def test_cellxgene_files_keep_the_empty_null_marker():
    """publish.py writes NULL as an empty field; a custom 'NA' marker would make BigQuery
    refuse every empty FLOAT64 (tau, the pseudobulk mean)."""
    for t in load_data.EMPTY_NULL_TABLES:
        assert t in load_data.SCHEMAS
        assert load_data._csv_load_config(t, [], 1, "WRITE_TRUNCATE").null_marker is None
    assert load_data._csv_load_config("mpra", [], 1, "WRITE_TRUNCATE").null_marker == "NA"


def test_cli_spec_reaches_the_transaction(tmp_path, monkeypatch):
    spec = tmp_path / "spec.json"
    spec.write_text(json.dumps([
        {"table": "cellxgene_datasets", "gcs_uri": "gs://b/r/bq/datasets.tsv", "expect_rows": 8,
         "const_columns": {"release": "r1"}},
    ]))
    seen = {}

    def fake(client, loads, skip):
        seen["loads"], seen["skip"] = loads, skip
        return {loads[0]["table_id"]: 8}

    monkeypatch.setattr(load_data, "load_tables_transactional", fake)
    monkeypatch.setattr(load_data.bigquery, "Client", lambda project: MagicMock())
    monkeypatch.setattr(sys, "argv", ["load_data.py", "--project", "p", "--dataset", "d",
                                      "--transaction", str(spec)])
    load_data.main()
    assert seen["loads"] == [{"table_id": "p.d.cellxgene_datasets", "table_type": "cellxgene_datasets",
                              "gcs_uri": "gs://b/r/bq/datasets.tsv", "expect_rows": 8,
                              "const_columns": {"release": "r1"}}]


def test_cli_refuses_transaction_mixed_with_single_table_options(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["load_data.py", "--project", "p", "--transaction", "x.json",
                                      "--table", "mpra"])
    with pytest.raises(SystemExit):
        load_data.main()
