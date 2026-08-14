"""How /query resolves unqualified table names, asserted against real BigQuery.

db-api used to rewrite the caller's SQL text with a regex, replacing a bare name after
FROM/JOIN with the fully-qualified view. That regex was emulating SQL scoping, and three
rounds of fixes each closed one lexical case and revealed the next — a CTE aliased to a
table name, then a *backticked* CTE declaration, then BigQuery's decoding of escape
sequences inside backticked identifiers (`` `\\u0064atasets` `` declares a CTE genuinely
named `datasets`) and backtick pairing desynchronised by `` `a\\`b` ``. Every one of them
produced the same silent wrong answer: 889 rows of `datasets_v` where the caller asked for
their one-row CTE, returned with HTTP 200.

The rewrite is gone. `default_dataset` on the job config makes BigQuery resolve bare names,
so the scoping rules are applied by the thing that defines them. These tests therefore
assert on the ROWS COMING BACK, not on rewritten query text — a test over the text of a
rewrite can only ever restate the rewrite.

They need a live BigQuery, because the property under test is BigQuery's own name
resolution; a mock would be the emulator all over again. Run them by pointing
LIVE_BQ_PROJECT_ID (and LIVE_BQ_DATASET_ID) at a real dataset; without it they skip.

Those are deliberately NOT the app's own PROJECT_ID/DATASET_ID. Setting those in the
environment reaches every other test module, and test_query_caps mocks BigQuery with
referenced tables hardcoded under `test-project` — a real PROJECT_ID makes its allow-list
comparison answer 403 and nine unrelated tests fail. The module fixture below sets the
app variables for its own reload only.
"""

import importlib
import os
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

PROJECT_ID = os.environ.get("LIVE_BQ_PROJECT_ID")
DATASET_ID = os.environ.get("LIVE_BQ_DATASET_ID", "genetics_results")

pytestmark = pytest.mark.skipif(
    not PROJECT_ID,
    reason="live BigQuery name resolution; set LIVE_BQ_PROJECT_ID (and LIVE_BQ_DATASET_ID)",
)


@pytest.fixture(scope="module")
def client():
    """The real app against the real BigQuery, on the documented fail-open auth branch.

    INTERNAL_API_SECRET is deliberately unset: `require_auth` returns without a principal,
    which is the branch a cluster that has not yet wired the secret serves on. That keeps
    these tests about name resolution rather than about credentials — the per-credential
    caps have their own suite in test_query_caps.py.
    """
    patch = pytest.MonkeyPatch()
    for key in ("INTERNAL_API_SECRET", "SANDBOX_TOKEN_SIGNING_KEY", "SANDBOX_ENABLED"):
        patch.delenv(key, raising=False)
    patch.setenv("PROJECT_ID", PROJECT_ID)
    patch.setenv("DATASET_ID", DATASET_ID)
    sys.modules.pop("api.main", None)
    main = importlib.import_module("api.main")
    with TestClient(main.app) as test_client:
        yield test_client
    patch.undo()
    sys.modules.pop("api.main", None)


def _query(client, sql, **kwargs):
    return client.post("/query", json={"sql": sql, **kwargs})


# `datasets_v` is the table the three shadowing bugs silently served instead of the CTE.
# Its size is asserted rather than assumed so a shadowing test cannot pass by coincidence
# if the table shrinks to one row.
def test_a_bare_view_name_resolves_and_returns_rows(client):
    """The whole point of default_dataset: callers may write the short name."""
    response = _query(client, "SELECT * FROM credible_sets_v LIMIT 5")
    assert response.status_code == 200, response.text
    assert response.json()["total_rows"] == 5


def test_a_bare_base_table_name_resolves(client):
    """Base tables are in the allow-list too, and resolve the same way."""
    response = _query(client, "SELECT * FROM datasets LIMIT 3")
    assert response.status_code == 200, response.text
    assert response.json()["total_rows"] == 3


def test_the_shadowed_table_really_is_bigger_than_the_ctes_below(client):
    """Makes the shadowing assertions meaningful: `datasets` has many rows, the CTEs one."""
    response = _query(client, f"SELECT COUNT(*) AS n FROM `{PROJECT_ID}.{DATASET_ID}.datasets_v`")
    assert response.status_code == 200, response.text
    assert response.json()["rows"][0][0] > 1


@pytest.mark.parametrize("declaration", [
    "datasets",
    "`datasets`",
    "`\\u0064atasets`",
], ids=["bare", "backticked", "escaped"])
def test_a_cte_named_after_a_base_table_shadows_it(client, declaration):
    """The measured failure, all three spellings: each returned 889 rows of `datasets_v`.

    The escaped spelling is the one no regex catches — BigQuery decodes escape sequences
    inside backtick-quoted identifiers, so this declares a CTE genuinely named `datasets`
    while the query text contains no such string.
    """
    response = _query(client, f"WITH {declaration} AS (SELECT 1 AS x) SELECT * FROM datasets")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["total_rows"] == 1
    assert body["rows"] == [[1]]


@pytest.mark.parametrize("declaration", [
    "datasets_v",
    "`datasets_v`",
], ids=["bare", "backticked"])
def test_a_cte_named_after_a_view_shadows_it(client, declaration):
    response = _query(client, f"WITH {declaration} AS (SELECT 2 AS x) SELECT * FROM datasets_v")
    assert response.status_code == 200, response.text
    assert response.json()["rows"] == [[2]]


def test_a_backtick_escape_in_a_cte_name_does_not_break_anything(client):
    r"""`` `a\`b` `` contains an escaped backtick, so naive backtick pairing desynchronises
    from here on and used to erase an arbitrary later span — including a real CTE
    declaration — producing the same silent wrong answer. BigQuery just reads a name."""
    response = _query(
        client,
        r"WITH `a\`b` AS (SELECT 3 AS x), datasets AS (SELECT 4 AS x) "
        r"SELECT (SELECT x FROM `a\`b`) AS a, (SELECT x FROM datasets) AS d",
    )
    assert response.status_code == 200, response.text
    assert response.json()["rows"] == [[3, 4]]


def test_an_earlier_cte_referenced_from_a_later_one_still_shadows(client):
    response = _query(
        client,
        "WITH phenotypes AS (SELECT 5 AS x), later AS (SELECT * FROM phenotypes) "
        "SELECT * FROM later",
    )
    assert response.status_code == 200, response.text
    assert response.json()["rows"] == [[5]]


def test_a_cte_does_not_stop_a_real_view_resolving(client):
    """Shadowing must not overreach onto names the WITH clause never declared."""
    response = _query(
        client,
        "WITH leads AS (SELECT 1 AS x) SELECT COUNT(*) AS n FROM credible_sets_v, leads",
    )
    assert response.status_code == 200, response.text
    assert response.json()["rows"][0][0] > 0


def test_a_table_word_inside_a_string_literal_is_just_text(client):
    """The rewrite's original sin: `LIKE '%uk biobank datasets%'` became a qualified name
    inside the literal — valid SQL that silently matched nothing."""
    response = _query(
        client,
        "SELECT COUNT(*) AS n FROM datasets_v WHERE 'uk biobank datasets' LIKE '%datasets%'",
    )
    assert response.status_code == 200, response.text
    assert response.json()["rows"][0][0] > 0


def test_an_alias_named_after_a_table_is_not_a_table(client):
    response = _query(
        client,
        "SELECT resource, COUNT(*) AS datasets FROM datasets_v "
        "GROUP BY resource ORDER BY datasets DESC LIMIT 1",
    )
    assert response.status_code == 200, response.text
    assert response.json()["total_rows"] == 1


def test_an_explicitly_named_table_outside_the_allow_list_is_forbidden(client):
    """Explicit qualification beats the default dataset, and the allow-list still sees it:
    `referencedTables` comes back fully qualified regardless of how a name was written."""
    response = _query(
        client,
        f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.TABLES` LIMIT 1",
    )
    assert response.status_code == 403, response.text
    assert "outside the exposed set" in response.json()["detail"]


def test_an_unknown_bare_name_fails_closed_without_executing(client):
    """Absent from the default dataset, so the dry run errors and nothing runs."""
    response = _query(client, "SELECT * FROM no_such_table_here LIMIT 1")
    assert 400 <= response.status_code < 500, response.text
    assert "rows" not in response.json()


def test_a_non_select_is_rejected(client):
    response = _query(client, f"CREATE TABLE `{PROJECT_ID}.{DATASET_ID}.t` AS SELECT 1 AS x")
    assert 400 <= response.status_code < 500, response.text
    assert "SELECT" in response.json()["detail"]


def test_the_dry_run_and_the_execution_resolve_names_identically(client):
    """A gate checked under different scoping than the statement runs under is no gate.
    Both job configs carry the same default dataset; the same SQL must price and run."""
    sql = "SELECT COUNT(*) AS n FROM datasets_v"
    priced = _query(client, sql, dry_run=True)
    executed = _query(client, sql)
    assert priced.status_code == 200, priced.text
    assert executed.status_code == 200, executed.text
