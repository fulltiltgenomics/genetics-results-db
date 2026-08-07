"""Tests for /query's table-name auto-qualification.

The qualifier used to be a bare substring replace of " <table>". Once `datasets` and
`phenotypes` became table names that put two common English words into it, so any query
whose text contained them outside a table position was rewritten - string literals most
dangerously, because the result is still valid SQL that authorize_query accepts and that
returns zero rows.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture(scope="module")
def qualify():
    os.environ.setdefault("INTERNAL_API_SECRET", "test-internal-secret")
    os.environ.setdefault("PROJECT_ID", "test-project")
    from api.main import DATASET_ID, PROJECT_ID, _qualify_tables

    def _run(sql):
        return _qualify_tables(sql), f"`{PROJECT_ID}.{DATASET_ID}."

    return _run


def test_bare_table_name_is_qualified(qualify):
    out, prefix = qualify("SELECT * FROM credible_sets_v LIMIT 1")
    assert out == f"SELECT * FROM {prefix}credible_sets_v` LIMIT 1"


def test_base_table_name_redirects_to_view(qualify):
    out, prefix = qualify("SELECT * FROM datasets LIMIT 1")
    assert out == f"SELECT * FROM {prefix}datasets_v` LIMIT 1"


def test_multiline_from_is_qualified(qualify):
    """The old space-branch existed for this shape; \\s+ has to keep covering it."""
    out, prefix = qualify("SELECT *\nFROM\n  phenotypes_v\nLIMIT 1")
    assert f"FROM\n  {prefix}phenotypes_v`" in out


def test_join_is_qualified(qualify):
    out, prefix = qualify(
        "SELECT * FROM credible_sets_v cs JOIN phenotypes_v p USING (dataset, trait_original)"
    )
    assert f"JOIN {prefix}phenotypes_v` p" in out
    assert f"FROM {prefix}credible_sets_v` cs" in out


def test_lowercase_keywords_are_qualified(qualify):
    out, prefix = qualify("select * from datasets_v join phenotypes_v using (dataset)")
    assert f"from {prefix}datasets_v`" in out
    assert f"join {prefix}phenotypes_v`" in out


def test_string_literal_containing_a_table_word_is_untouched(qualify):
    """The regression: this used to run and silently return zero rows."""
    sql = "SELECT * FROM datasets_v WHERE description LIKE '%uk biobank datasets%'"
    out, prefix = qualify(sql)
    assert out.endswith("LIKE '%uk biobank datasets%'")
    assert f"FROM {prefix}datasets_v`" in out


def test_string_literal_with_phenotypes_is_untouched(qualify):
    sql = "SELECT * FROM datasets_v WHERE description LIKE '%blood phenotypes%'"
    out, _ = qualify(sql)
    assert out.endswith("LIKE '%blood phenotypes%'")


@pytest.mark.parametrize("word", ["variant_annotation", "gene_annotations", "open_chromatin"])
def test_other_table_words_survive_in_literals(qualify, word):
    sql = f"SELECT * FROM datasets_v WHERE description LIKE '%{word} notes%'"
    out, _ = qualify(sql)
    assert f"'%{word} notes%'" in out


def test_alias_and_order_by_are_untouched(qualify):
    sql = (
        "SELECT resource, COUNT(*) AS datasets FROM datasets_v "
        "GROUP BY resource ORDER BY datasets DESC"
    )
    out, prefix = qualify(sql)
    assert "AS datasets FROM" in out
    assert out.endswith("ORDER BY datasets DESC")
    assert f"FROM {prefix}datasets_v`" in out


def test_already_qualified_name_is_not_rewritten_twice(qualify):
    _, prefix = qualify("")
    sql = f"SELECT * FROM {prefix}credible_sets_v`"
    out, _ = qualify(sql)
    assert out == sql


def test_view_suffix_is_not_split_by_the_base_table_rule(qualify):
    """`credible_sets` must not match inside `credible_sets_v`."""
    out, prefix = qualify("SELECT * FROM credible_sets_v")
    assert out == f"SELECT * FROM {prefix}credible_sets_v`"


def test_comma_unnest_shape_still_qualifies(qualify):
    """datasets.yaml ships `FROM datasets_v, UNNEST(resource_aliases) AS alias`."""
    out, prefix = qualify("SELECT alias FROM datasets_v, UNNEST(resource_aliases) AS alias")
    assert out == f"SELECT alias FROM {prefix}datasets_v`, UNNEST(resource_aliases) AS alias"


def test_cte_name_is_not_qualified(qualify):
    sql = "WITH leads AS (SELECT 1) SELECT * FROM leads"
    out, _ = qualify(sql)
    assert out == sql


def test_known_residual_from_inside_a_literal_is_still_rewritten(qualify):
    """Documented limitation, asserted so a future parser-based fix notices it changed."""
    out, prefix = qualify("SELECT 'FROM datasets_v' AS s")
    assert f"'FROM {prefix}datasets_v`'" in out
