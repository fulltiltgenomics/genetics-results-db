"""schemas/hla_associations_v.sql must keep selecting every hla_associations column.

hla_associations_v is the only one of the views that pins its base columns: it lists
them explicitly because five are aliased to the suite's house spelling, and SELECT *
cannot rename. That makes it the only view where adding a column to the base table
silently fails to surface -- generate_resource_sql.py's lint reads only the
CASE...END AS resource block, and monitor/bq_summary.py issues only COUNT(*), so an
under-wide select list passes both.

The test parses the .sql file rather than restating its column list, because a list
restated in the test only proves the test agrees with itself; the file is the artifact
that gets applied to BigQuery. It compares the SOURCE identifiers of the select list
(the left-hand side of `mlogp AS mlog10p`, not the alias) against
SCHEMAS["hla_associations"] in scripts/load_data.py, the in-repo authority for the
base table. It reads the select list anchored to `CREATE ... VIEW ... AS SELECT` after
stripping `--` and `/* */` comments, so commented-out text cannot stand in for the real
list; an unparseable file, a missing anchor and an unclassifiable select item are all
failures rather than skips. Offline by construction: it reads two files and imports no
BigQuery client.
"""

import importlib.util
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
VIEW_SQL = REPO_ROOT / "schemas" / "hla_associations_v.sql"
LOAD_DATA = REPO_ROOT / "scripts" / "load_data.py"

# base column -> name the view exposes it as. Every other base column must pass through
# unrenamed. Pinned here so that dropping the source half of `mlogp AS mlog10p` (which
# leaves a file that still mentions mlog10p) cannot look like a pass.
RENAMES = {
    "mlogp": "mlog10p",
    "sebeta": "se",
    "af_alt": "af",
    "af_alt_cases": "af_cases",
    "af_alt_controls": "af_controls",
}

# select-list entries that are computed rather than read from the base table
DERIVED_OUTPUTS = {"resource"}

_PLAIN = re.compile(r"^([A-Za-z_]\w*)$")
_ALIASED = re.compile(r"^([A-Za-z_]\w*)\s+AS\s+([A-Za-z_]\w*)$", re.IGNORECASE)
_TRAILING_ALIAS = re.compile(r"\bAS\s+([A-Za-z_]\w*)$", re.IGNORECASE | re.DOTALL)


def _strip_comments(sql: str) -> str:
    """Drop `--` and `/* */` comments without touching either inside string literals."""
    out = []
    quote = None
    i = 0
    while i < len(sql):
        ch = sql[i]
        if quote:
            out.append(ch)
            if ch == quote:
                quote = None
        elif ch in "'\"`":
            quote = ch
            out.append(ch)
        elif ch == "-" and sql[i + 1 : i + 2] == "-":
            while i < len(sql) and sql[i] != "\n":
                i += 1
            continue
        elif ch == "/" and sql[i + 1 : i + 2] == "*":
            end = sql.find("*/", i + 2)
            if end == -1:
                # unterminated: hand the rest back verbatim so the caller's check trips
                out.append(sql[i:])
                break
            # a comment separates tokens, so it cannot vanish without a trace
            out.append(" ")
            i = end + 2
            continue
        else:
            out.append(ch)
        i += 1
    return "".join(out)


def _select_items(sql: str) -> list[str]:
    """Split the top-level SELECT list of a single-SELECT statement."""
    body = _strip_comments(sql)
    if "/*" in body or "*/" in body:
        pytest.fail(f"{VIEW_SQL}: block comment delimiter survived stripping; cannot parse")
    # anchor on the view's own SELECT: the first SELECT in the file may belong to text
    # that is not the definition at all
    match = re.search(
        r"\bCREATE\s+(?:OR\s+REPLACE\s+)?VIEW\b[^;]*?\bAS\s+SELECT\b",
        body,
        re.IGNORECASE | re.DOTALL,
    )
    if match is None:
        pytest.fail(f"{VIEW_SQL}: no `CREATE ... VIEW ... AS SELECT` found")
    i = match.end()
    depth = 0
    quote = None
    items = []
    current = []
    while i < len(body):
        ch = body[i]
        if quote:
            current.append(ch)
            if ch == quote:
                quote = None
        elif ch in "'\"`":
            quote = ch
            current.append(ch)
        elif ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif depth == 0 and ch == ",":
            items.append("".join(current))
            current = []
        elif (
            depth == 0
            # `_` is a word character in SQL identifiers, so `dataset_from` is one token
            and not (current and (current[-1].isalnum() or current[-1] == "_"))
            and re.match(r"FROM(?!\w)", body[i:], re.IGNORECASE)
        ):
            items.append("".join(current))
            current = None
            break
        else:
            current.append(ch)
        i += 1
    if current is not None:
        pytest.fail(f"{VIEW_SQL}: SELECT list has no top-level FROM; cannot parse")
    return [" ".join(item.split()) for item in items if item.strip()]


def _parse_view() -> tuple[dict[str, str], set[str]]:
    """Return ({base column: exposed name}, {derived output names})."""
    items = _select_items(VIEW_SQL.read_text())
    if not items:
        pytest.fail(f"{VIEW_SQL}: empty SELECT list")
    selected: dict[str, str] = {}
    derived: set[str] = set()
    for item in items:
        if item.startswith("*") or item.endswith(".*"):
            pytest.fail(f"{VIEW_SQL}: SELECT * cannot carry the required renames")
        plain = _PLAIN.match(item)
        aliased = _ALIASED.match(item)
        if plain:
            source = exposed = plain.group(1)
        elif aliased:
            source, exposed = aliased.group(1), aliased.group(2)
        else:
            trailing = _TRAILING_ALIAS.search(item)
            if trailing is None:
                pytest.fail(f"{VIEW_SQL}: cannot classify select item {item!r}")
            derived.add(trailing.group(1))
            continue
        if source in selected:
            pytest.fail(f"{VIEW_SQL}: {source!r} selected twice")
        selected[source] = exposed
    return selected, derived


def _base_columns() -> list[str]:
    spec = importlib.util.spec_from_file_location("_hla_load_data", LOAD_DATA)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    fields = module.SCHEMAS["hla_associations"]
    assert fields, "SCHEMAS['hla_associations'] is empty"
    return [field.name for field in fields]


@pytest.fixture(scope="module")
def view():
    return _parse_view()


@pytest.fixture(scope="module")
def base_columns():
    return _base_columns()


def test_view_selects_every_base_column(view, base_columns):
    """The regression this file exists for: a column added to hla_associations that
    nobody added to the view."""
    selected, _ = view
    missing = [column for column in base_columns if column not in selected]
    assert not missing, (
        f"{VIEW_SQL} does not select {missing} from hla_associations. "
        "Add each column to the view's select list (renaming it if the suite spells "
        "it differently), or remove it from SCHEMAS['hla_associations']."
    )


def test_view_selects_nothing_the_base_table_lacks(view, base_columns):
    selected, _ = view
    unknown = sorted(set(selected) - set(base_columns))
    assert not unknown, f"{VIEW_SQL} selects columns absent from the base table: {unknown}"


def test_renamed_columns_use_the_house_spelling(view):
    selected, _ = view
    actual = {source: exposed for source, exposed in selected.items() if source != exposed}
    assert actual == RENAMES


def test_unrenamed_columns_pass_through_unaliased(view, base_columns):
    selected, _ = view
    for column in base_columns:
        if column not in RENAMES:
            assert selected.get(column) == column


def test_derived_outputs_are_exactly_the_expected_ones(view):
    """Fails closed: a new computed column forces this file to be revisited."""
    _, derived = view
    assert derived == DERIVED_OUTPUTS


def test_exposed_names_are_unique(view):
    selected, derived = view
    exposed = list(selected.values()) + sorted(derived)
    assert len(exposed) == len(set(exposed)), f"duplicate output column names: {exposed}"
