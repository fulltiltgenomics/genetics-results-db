"""Categorical values leave db-api as strings, whatever BigQuery type they came from.

A BOOL column in a view's `categorical_columns` (dosage_sensitivity_v's haploinsufficient /
triplosensitive) made ARRAY_AGG return Python bools, and `_compact_categorical_values`'
collection-resource test calls `.lower()` on every flat value — so the whole view fell out of
`/schema` into its `warnings` array and `/schema?table=...` made db-api answer 503 (surfaced
as 500 through the mcp-server proxy). The values are
rendered at the point they are collected, in BigQuery's own spelling, so no consumer of a
categorical list has to care about the column's type.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from test_internal_query_caps import FakeInternalBQ  # noqa: E402
from test_query_caps import GB, client, main  # noqa: E402,F401  (fixtures)
from test_sandbox_token_auth import SECRET, _reload  # noqa: E402,F401

VIEW = "credible_sets_v"


class _Field:
    def __init__(self, name, field_type):
        self.name, self.field_type, self.mode, self.description = name, field_type, "NULLABLE", ""


class _Meta:
    num_rows = 5
    description = ""
    schema = [_Field("haploinsufficient", "BOOLEAN")]


class _SchemaBQ(FakeInternalBQ):
    def get_table(self, ref):
        return _Meta()


@pytest.fixture
def bool_categorical_column(main, monkeypatch):
    monkeypatch.setattr(main, "_CATEGORICAL_COLUMNS", {VIEW: {"haploinsufficient": None}})
    main._VALUES_CACHE.clear()
    yield
    main._VALUES_CACHE.clear()


def test_bool_distinct_values_are_collected_as_bigquery_spelling(
    main, monkeypatch, bool_categorical_column
):
    monkeypatch.setattr(main, "bq_client", _SchemaBQ([{"haploinsufficient": [True, False]}]))
    caps = main._Caps(main.MAX_ROWS, main.MAX_BYTES_BILLED, None)

    assert main._get_categorical_values(VIEW, caps) == {"haploinsufficient": ["false", "true"]}


def test_grouped_values_and_their_group_keys_are_rendered_too(main, monkeypatch):
    """The dependency value becomes a JSON object key and is prefix-tested in compaction,
    so it is rendered on the same rule as the values it groups."""
    monkeypatch.setattr(main, "_CATEGORICAL_COLUMNS", {VIEW: {"n": "haploinsufficient"}})
    main._VALUES_CACHE.clear()
    monkeypatch.setattr(main, "bq_client", _SchemaBQ([{"haploinsufficient": True, "n": [1, 2]}]))
    caps = main._Caps(main.MAX_ROWS, main.MAX_BYTES_BILLED, None)

    assert main._get_categorical_values(VIEW, caps) == {
        "n_by_haploinsufficient": {"true": ["1", "2"]}
    }
    main._VALUES_CACHE.clear()


def test_compaction_passes_rendered_bools_through_untouched(main):
    assert main._compact_categorical_values({"haploinsufficient": ["false", "true"]}) == {
        "haploinsufficient": ["false", "true"]
    }
    assert main._compact_categorical_values({"n_by_x": {"true": ["1"]}}) == {"n_by_x": {"true": ["1"]}}


def test_schema_lists_the_view_with_its_bool_allowed_values(
    main, client, monkeypatch, bool_categorical_column
):
    monkeypatch.setattr(main, "bq_client", _SchemaBQ([{"haploinsufficient": [True, False]}]))

    resp = client.get(f"/schema?table={VIEW}", headers={"Authorization": f"Bearer {SECRET}"})

    assert resp.status_code == 200
    body = resp.json()
    assert not body.get("warnings")
    (table,) = body["tables"]
    (column,) = table["columns"]
    assert column["name"] == "haploinsufficient"
    assert column["allowed_values"] == ["false", "true"]
