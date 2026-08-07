#!/usr/bin/env python3
"""Generate the SQL that collects every `dataset` value the results views actually contain.

scripts/load_phenotypes.sh feeds the result to build_phenotypes.py --validate-against, which
cross-checks it against BQ_DATASETS_BY_DATASET_ID - a registry key -> results-view `dataset`
mapping that exists in no config and is hand-maintained.

Why this is generated rather than written out: the previous version of the cross-check listed
nine table names by hand. A brand-new table therefore contributed nothing to the live set, so
its `dataset` value could not be missing from the mapping, so the check passed clean - it
failed OPEN for precisely the case where drift is most likely. That is not hypothetical:
hla_associations landed with dataset = 'finngen_hla' and the loader ran clean while the
`datasets` table had zero rows for it.

The scope is therefore derived from the same VIEWS list api/main.py exposes. Anything the API
lets an agent query is in scope, so a new view puts its `dataset` values in front of the
cross-check the moment it is exposed, and an unmapped value fails the build.

Views are excluded only by an entry in EXCLUDED_VIEWS carrying a reason. A view that is not
excluded and has no dataset-bearing column FAILS - being skipped for lack of a column is the
same fail-open trap one level down.
"""

import argparse
import ast
import csv
import io
import sys

# views exposed by api/main.py that contribute no `dataset` values to the cross-check.
# An entry here is a decision with a reason attached; a view merely forgotten is an error.
EXCLUDED_VIEWS = {
    "gene_annotations_v": "gene coordinate reference (Ensembl/GENCODE), has no `dataset` column",
    "variant_annotation_v": "variant annotation reference, has no `dataset` column; its own "
                            "view SQL notes it carries no dataset discriminator",
    "phenotypes_v": "built by build_phenotypes.py from the same mapping this check validates - "
                    "including it would make the check circular and self-confirming",
    "datasets_v": "built by build_phenotypes.py from the same mapping this check validates - "
                  "including it would make the check circular and self-confirming",
}

# columns that hold a results-view `dataset` value. colocalization pairs two datasets per row
# and has no bare `dataset` column, so both sides must be collected or half its datasets are
# invisible to the check.
DATASET_COLUMNS = ("dataset", "dataset1", "dataset2")

_VIEWS_NAME = "VIEWS"


def _top_level_binding(module):
    """Return the (target node, value node) of a top-level `VIEWS = ...` / `VIEWS: t = ...`."""
    found = None
    for node in module.body:
        if isinstance(node, ast.Assign):
            targets = [t for t in node.targets
                       if isinstance(t, ast.Name) and t.id == _VIEWS_NAME]
        elif (isinstance(node, ast.AnnAssign) and node.value is not None
                and isinstance(node.target, ast.Name) and node.target.id == _VIEWS_NAME):
            targets = [node.target]
        else:
            continue
        if not targets:
            continue
        if found is not None:
            raise ValueError(
                f"`{_VIEWS_NAME}` is assigned more than once at module level (line "
                f"{targets[0].lineno}) - which assignment wins is not something this parser "
                "may guess. Bind it once as a list literal.")
        found = (targets[0], node.value)
    return found


def parse_views(source):
    """Extract the VIEWS list literal from api/main.py source text.

    Parsed rather than imported: importing api.main constructs a BigQuery client and loads
    datasets.yaml, neither of which the loader should need in order to know the view names.

    Parsed with ast rather than a regex because a regex over the literal returns a SHORT list
    for `VIEWS = [...] + EXTRA`, `VIEWS.append(...)` and `VIEWS += [...]` - and a short list is
    not a crash. It yields valid SQL over fewer views, the loader runs clean, and the omitted
    views' `dataset` values are invisible to the cross-check: the same fail-open this module
    exists to close, reintroduced one level up. So anything that makes the literal an
    incomplete account of VIEWS is an error here, not a truncation.
    """
    try:
        module = ast.parse(source)
    except SyntaxError as error:
        raise ValueError(f"could not parse the VIEWS source file: {error}") from error

    binding = _top_level_binding(module)
    if binding is None:
        raise ValueError(
            f"could not find a top-level `{_VIEWS_NAME} = [...]` list - if api/main.py now "
            "builds VIEWS some other way, update parse_views() rather than hardcoding view "
            "names")
    target, value = binding

    mutators = sorted({node.func.attr for node in ast.walk(module)
                       if isinstance(node, ast.Call)
                       and isinstance(node.func, ast.Attribute)
                       and isinstance(node.func.value, ast.Name)
                       and node.func.value.id == _VIEWS_NAME})
    if mutators:
        raise ValueError(
            f"`{_VIEWS_NAME}` is used as the receiver of "
            + ", ".join(f"{_VIEWS_NAME}.{name}()" for name in mutators)
            + f", so the list literal is not the full set of exposed views. Build {_VIEWS_NAME}"
            " as one literal, or teach parse_views() how to resolve it - a short list here "
            "silently narrows the cross-check instead of failing it.")

    rebinds = [node for node in ast.walk(module)
               if isinstance(node, ast.Name) and node.id == _VIEWS_NAME
               and isinstance(node.ctx, (ast.Store, ast.Del)) and node is not target]
    if rebinds:
        raise ValueError(
            f"`{_VIEWS_NAME}` is rebound or extended at line(s) "
            + ", ".join(str(node.lineno) for node in rebinds)
            + " (`+=`, a loop target, a `del`, …), so the list literal is not the full set of "
            "exposed views. Bind it once as a literal, or update parse_views().")

    if not isinstance(value, ast.List) or not all(
            isinstance(item, ast.Constant) and isinstance(item.value, str)
            for item in value.elts):
        raise ValueError(
            f"top-level `{_VIEWS_NAME}` is not a plain list of string literals (found "
            f"{type(value).__name__}) - a computed VIEWS cannot be read statically. Keep it a "
            "literal, or update parse_views() rather than hardcoding view names.")

    views = [item.value for item in value.elts]
    if not views:
        raise ValueError(f"parsed {_VIEWS_NAME} is an empty list of names")
    return views


def parse_columns_csv(text):
    """Read `table_name,column_name` rows (an INFORMATION_SCHEMA.COLUMNS dump) into a dict."""
    columns_by_view = {}
    reader = csv.reader(io.StringIO(text))
    for row in reader:
        if len(row) < 2:
            continue
        table, column = row[0].strip(), row[1].strip()
        if (table, column) == ("table_name", "column_name"):
            continue  # bq --format=csv header
        columns_by_view.setdefault(table, set()).add(column)
    return columns_by_view


def dataset_columns_for(view, columns_by_view):
    return [c for c in DATASET_COLUMNS if c in columns_by_view.get(view, set())]


def build_union_sql(views, columns_by_view, project_id, dataset_id):
    """Return SQL producing one comma-joined string of every live `dataset` value.

    Raises when an in-scope view has no dataset-bearing column: either it belongs in
    EXCLUDED_VIEWS with a reason, or the column dump is incomplete (bad auth, wrong dataset),
    and silently narrowing the scope is exactly the failure this module exists to remove.
    """
    selects = []
    missing = []
    for view in views:
        if view in EXCLUDED_VIEWS:
            continue
        columns = dataset_columns_for(view, columns_by_view)
        if not columns:
            missing.append(view)
            continue
        for column in columns:
            selects.append(
                f"SELECT {column} AS d FROM `{project_id}.{dataset_id}.{view}` GROUP BY 1")
    if missing:
        raise ValueError(
            "no dataset-bearing column found for: " + ", ".join(sorted(missing)) +
            f". Expected one of {', '.join(DATASET_COLUMNS)}. Either the column dump is "
            "incomplete, or the view genuinely has no `dataset` column - in which case add "
            "it to EXCLUDED_VIEWS in scripts/live_dataset_scope.py WITH A REASON.")
    if not selects:
        raise ValueError("no in-scope views left - the cross-check would have nothing to do")
    return ("SELECT STRING_AGG(DISTINCT d, ',') FROM (\n    "
            + "\n    UNION ALL ".join(selects) + "\n  )")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--views-file", required=True,
                        help="Path to api/main.py, the source of the exposed VIEWS list")
    parser.add_argument("--columns-csv", default="-",
                        help="table_name,column_name CSV from INFORMATION_SCHEMA.COLUMNS "
                             "('-' reads stdin)")
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--dataset-id", required=True)
    args = parser.parse_args()

    with open(args.views_file) as handle:
        views = parse_views(handle.read())
    text = sys.stdin.read() if args.columns_csv == "-" else open(args.columns_csv).read()

    try:
        sql = build_union_sql(views, parse_columns_csv(text), args.project_id, args.dataset_id)
    except ValueError as error:
        sys.exit(f"ERROR: {error}")
    print(sql)


if __name__ == "__main__":
    main()
