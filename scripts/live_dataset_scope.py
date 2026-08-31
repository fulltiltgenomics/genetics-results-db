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

The scope is therefore derived from configs/datasets.yaml, through the same load_views() the
API derives VIEWS from: the scope follows EXPOSURE, not mere documentation. A documented but
unexposed table is unreachable, and it need not even exist in BigQuery yet, so demanding a
dataset-bearing column from it would fail the build for a table nobody can query. Exposing it
is what puts its `dataset` values in front of the cross-check, and an unmapped value then
fails the build.

Views are excluded only by an entry in EXCLUDED_VIEWS carrying a reason. A view that is not
excluded and has no dataset-bearing column FAILS - being skipped for lack of a column is the
same fail-open trap one level down.
"""

import argparse
import csv
import io
import os
import sys

import yaml

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from api.yaml_loader import load_views  # noqa: E402

# exposed views that contribute no `dataset` values to the cross-check.
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


def views_in_scope(datasets_yaml_path):
    """Read the exposed view names out of datasets.yaml.

    Read through the API's own loader rather than from api/main.py's source text, so both ends
    apply one definition of "exposed" to one registry and neither can be parsed short. They
    still read separate copies of the file (see docs/project-spec.md), so being in step is a
    property of the sync, not of this call.

    An unreadable or malformed config is raised as a ValueError so main() reports it as a
    diagnosis rather than a traceback the caller's own error message then misattributes.
    """
    try:
        with open(datasets_yaml_path) as handle:
            config = yaml.safe_load(handle)
    except (OSError, yaml.YAMLError) as error:
        raise ValueError(f"cannot read {datasets_yaml_path}: {error}") from error
    views = load_views(config or {})
    if not views:
        raise ValueError(
            f"no views marked `exposed: true` in {datasets_yaml_path} - the cross-check scope "
            "would be empty, which narrows the check instead of failing it")
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
    parser.add_argument("--datasets-yaml", required=True,
                        help="Path to configs/datasets.yaml, the registry of exposed views")
    parser.add_argument("--columns-csv", default="-",
                        help="table_name,column_name CSV from INFORMATION_SCHEMA.COLUMNS "
                             "('-' reads stdin)")
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--dataset-id", required=True)
    args = parser.parse_args()

    text = sys.stdin.read() if args.columns_csv == "-" else open(args.columns_csv).read()

    try:
        views = views_in_scope(args.datasets_yaml)
        sql = build_union_sql(views, parse_columns_csv(text), args.project_id, args.dataset_id)
    except ValueError as error:
        sys.exit(f"ERROR: {error}")
    print(sql)


if __name__ == "__main__":
    main()
