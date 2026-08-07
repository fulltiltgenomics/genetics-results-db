"""Unit tests for the derived scope of the registry <-> results-view cross-check.

The regression these guard: the cross-check used to union a hardcoded list of nine tables,
so a newly added table contributed no `dataset` values and its drift could not be detected.
Nothing here touches BigQuery.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from live_dataset_scope import (  # noqa: E402
    DATASET_COLUMNS,
    EXCLUDED_VIEWS,
    build_union_sql,
    parse_columns_csv,
    parse_views,
)

API_MAIN = os.path.join(os.path.dirname(__file__), "..", "api", "main.py")

COLUMNS = parse_columns_csv(
    "table_name,column_name\n"
    "credible_sets_v,dataset\n"
    "colocalization_v,dataset1\n"
    "colocalization_v,dataset2\n"
    "hla_associations_v,dataset\n"
    "phenotypes_v,dataset\n"
    "gene_annotations_v,symbol\n"
)


def test_views_are_parsed_from_the_real_api_module():
    """The scope must come from what api/main.py actually exposes, not a copy of it."""
    with open(API_MAIN) as handle:
        views = parse_views(handle.read())
    assert "credible_sets_v" in views
    assert "hla_associations_v" in views
    assert all(v.endswith("_v") for v in views)


def test_a_new_view_enters_scope_without_being_listed_anywhere():
    """The point of deriving the scope: exposing a view is enough to put it in the check."""
    sql = build_union_sql(
        ["credible_sets_v", "hla_associations_v"], COLUMNS, "proj", "ds")
    assert "`proj.ds.hla_associations_v`" in sql
    assert "`proj.ds.credible_sets_v`" in sql


def test_both_colocalization_sides_are_collected():
    """colocalization has no bare `dataset` column; collecting one side would hide half."""
    sql = build_union_sql(["colocalization_v"], COLUMNS, "proj", "ds")
    assert "SELECT dataset1 AS d" in sql
    assert "SELECT dataset2 AS d" in sql


def test_excluded_views_are_skipped_and_carry_a_reason():
    sql = build_union_sql(
        ["credible_sets_v", "phenotypes_v"], COLUMNS, "proj", "ds")
    assert "phenotypes_v" not in sql
    assert all(isinstance(reason, str) and reason for reason in EXCLUDED_VIEWS.values())


def test_self_referential_views_are_excluded():
    """phenotypes_v/datasets_v are built FROM the mapping under test; including them would
    make the cross-check confirm itself."""
    assert "phenotypes_v" in EXCLUDED_VIEWS
    assert "datasets_v" in EXCLUDED_VIEWS


def test_every_excluded_view_is_actually_exposed():
    """An exclusion for a view that no longer exists is dead weight that hides the next one."""
    with open(API_MAIN) as handle:
        views = set(parse_views(handle.read()))
    assert set(EXCLUDED_VIEWS) <= views


def test_missing_dataset_column_fails_loudly_rather_than_being_skipped():
    """Skipping a column-less view is the same fail-open hole one level down.

    `future_reference_v` stands for a view added later with no `dataset` column and no
    exclusion entry: the build must stop, not quietly narrow its own scope.
    """
    with pytest.raises(ValueError) as excinfo:
        build_union_sql(
            ["credible_sets_v", "future_reference_v"], COLUMNS, "proj", "ds")
    assert "future_reference_v" in str(excinfo.value)
    assert "EXCLUDED_VIEWS" in str(excinfo.value)


def test_empty_column_dump_fails_rather_than_producing_an_empty_scope():
    with pytest.raises(ValueError):
        build_union_sql(["credible_sets_v"], {}, "proj", "ds")


def test_all_exposed_views_are_either_excluded_or_have_a_dataset_column():
    """The live shape of the schema, asserted against the checked-in view SQL: every view
    api/main.py exposes is either excluded with a reason or contributes dataset values."""
    schemas = os.path.join(os.path.dirname(__file__), "..", "schemas")
    with open(API_MAIN) as handle:
        views = parse_views(handle.read())
    columns = {}
    for view in views:
        base = os.path.join(schemas, f"{view.removesuffix('_v')}.sql")
        if not os.path.exists(base):
            continue
        with open(base) as handle:
            text = handle.read()
        found = {c for c in DATASET_COLUMNS if f"\n  {c} STRING" in text}
        if found:
            columns[view] = found
    sql = build_union_sql(views, columns, "proj", "ds")
    assert "hla_associations_v" in sql
    assert "peak_to_gene_v" in sql


def test_bq_csv_header_is_not_mistaken_for_a_column():
    parsed = parse_columns_csv("table_name,column_name\ncredible_sets_v,dataset\n")
    assert parsed == {"credible_sets_v": {"dataset"}}


def test_unparseable_views_list_is_an_error_not_an_empty_scope():
    with pytest.raises(ValueError):
        parse_views("VIEWS_BY_NAME = {}\n")


@pytest.mark.parametrize("source", [
    'VIEWS = ["a_v", "b_v"] + EXTRA_VIEWS\n',
    'VIEWS = ["a_v", "b_v"]\nVIEWS.append("c_v")\n',
    'VIEWS = ["a_v", "b_v"]\nVIEWS.extend(EXTRA_VIEWS)\n',
    'VIEWS = ["a_v", "b_v"]\nVIEWS += ["c_v"]\n',
    'VIEWS = ["a_v", "b_v"]\nVIEWS = VIEWS + ["c_v"]\n',
], ids=["concat", "append", "extend", "augmented-assign", "rebind"])
def test_views_assembled_in_pieces_fails_rather_than_parsing_short(source):
    """A SHORT list is the dangerous outcome, not a missing one.

    Each of these used to parse to ['a_v', 'b_v'] with no error: valid SQL over fewer views,
    a loader that runs clean, and the omitted views' dataset values invisible to the check.
    """
    with pytest.raises(ValueError) as excinfo:
        parse_views(source)
    assert "VIEWS" in str(excinfo.value)


def test_annotated_views_literal_is_accepted():
    """`VIEWS: list[str] = [...]` is the same literal; the old regex rejected it outright."""
    assert parse_views('VIEWS: list[str] = ["a_v", "b_v"]\n') == ["a_v", "b_v"]


def test_the_real_api_module_parses_to_its_full_view_list():
    """Guards the parser against being strict in the wrong direction: the checks above must
    reject assembled lists without rejecting the literal api/main.py actually ships."""
    with open(API_MAIN) as handle:
        views = parse_views(handle.read())
    assert len(views) == len(set(views))
    assert len(views) >= len(EXCLUDED_VIEWS) + 10  # every exclusion, plus the result views
