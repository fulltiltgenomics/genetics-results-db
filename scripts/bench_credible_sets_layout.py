#!/usr/bin/env python3
"""A/B the credible_sets clustering layouts against real logged traffic.

The clustering swap's acceptance criterion is a weighted scan-size total over the
commonest logged query shapes. Nothing reproduced that number, so this rebuilds the
measurement from the two surviving inputs: BigQuery job history, and the two layouts
sitting side by side in the dev dataset.

Three subcommands:

  shapes  extract and normalise the query population -- metadata only, no scan cost
  run     execute the top-N shapes against both layouts and report the weighted totals
  verify  re-measure the runbook's own hand-picked shapes on the same rig, so a reader
          can tell a disagreement in shape *selection* from one in measurement

`run` needs both layouts present in one dataset. The old arm is a view carrying the
pre-swap body (variant and resource derived in the view) over the pre-swap table; this
script creates it, because a hand-created one drifts from the resource rules in
datasets.yaml the moment those change.

Exit codes follow the suite convention: 0 pass, 1 assertion failed (--min-reduction
unmet), 2 could not run (no credentials, no job history, a layout missing).
"""

import argparse
import json
import os
import re
import subprocess
import sys
from collections import defaultdict

EXIT_OK, EXIT_ASSERT, EXIT_CANNOT_RUN = 0, 1, 2

HERE = os.path.dirname(os.path.abspath(__file__))


# --------------------------------------------------------------------------- shapes

def strip_comments(q):
    q = re.sub(r"--[^\n]*", " ", q)
    return re.sub(r"/\*.*?\*/", " ", q, flags=re.S)


def normalise(q):
    """Collapse a query to its shape: same structure, arguments replaced.

    Rules, in order. Each is listed in the report so a reader can re-derive the shape
    set rather than trust it.

      1. drop -- and /* */ comments
      2. string literals -> 'S'   (variant ids, gene symbols, dataset names, traits)
      3. numeric literals -> N    (chr, pos, thresholds)
      4. runs of identical placeholders -> one   ('S','S','S' -> 'S'), so an inlined
         200-gene UNNEST list and a 5-gene one are the same shape
      5. whitespace -> single space, then lowercase

    Rule 4 is the only aggressive one. Without it every inlined gene panel is its own
    shape; with it, queries that differ solely in how many arguments they carry collapse.
    It does not merge queries whose structure differs, which is why the collapse ratio
    stays close to 1 on this traffic.
    """
    q = strip_comments(q)
    q = re.sub(r"'(?:[^']|'')*'", "'S'", q)
    q = re.sub(r"\b\d+\.\d+(?:[eE][-+]?\d+)?\b", "N", q)
    q = re.sub(r"\b\d+\b", "N", q)
    q = re.sub(r"(?:'S'\s*,\s*)+'S'", "'S'", q)
    q = re.sub(r"(?:N\s*,\s*)+N", "N", q)
    return re.sub(r"\s+", " ", q).strip().lower()


NORMALISATION_RULES = [
    "drop -- and /* */ comments",
    "string literals -> 'S'",
    "numeric literals -> N",
    "runs of identical placeholders collapse to one ('S','S','S' -> 'S')",
    "whitespace collapsed to single space, then lowercased",
]


def fetch_jobs(client, args):
    table_list = ", ".join(f'"{t}"' for t in args.table_ids)
    caller_pred = ""
    params = []
    if args.caller:
        caller_pred = "AND user_email IN UNNEST(@callers)"
        params.append(_array_param("callers", args.caller))
    sql = f"""
    SELECT job_id, creation_time, user_email, query,
           total_bytes_processed, total_bytes_billed, cache_hit,
           (SELECT STRING_AGG(DISTINCT CONCAT(t.dataset_id, ".", t.table_id)
                              ORDER BY CONCAT(t.dataset_id, ".", t.table_id))
            FROM UNNEST(referenced_tables) t) AS refs
    FROM `{args.region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
    WHERE job_type = "QUERY"
      AND statement_type = "SELECT"
      AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
      {caller_pred}
      AND EXISTS (SELECT 1 FROM UNNEST(referenced_tables) t
                  WHERE t.table_id IN ({table_list}))
    ORDER BY creation_time
    """
    from google.cloud import bigquery

    params.append(bigquery.ScalarQueryParameter("days", "INT64", args.days))
    cfg = bigquery.QueryJobConfig(query_parameters=params, use_query_cache=False)
    return [dict(r) for r in client.query(sql, job_config=cfg).result()]


def _array_param(name, values):
    from google.cloud import bigquery

    return bigquery.ArrayQueryParameter(name, "STRING", values)


def group_shapes(jobs):
    shapes = defaultdict(list)
    for j in jobs:
        shapes[normalise(j["query"])].append(j)
    return shapes


def pick_representative(instances):
    """The instance whose scan size is the median of its shape group.

    A real logged query, verbatim -- no literal is invented. The median rather than the
    first or the largest, because shapes whose argument count varies (an inlined gene
    panel) have scan sizes spread over an order of magnitude, and either extreme would
    misweight the shape.
    """
    ranked = sorted(instances, key=lambda j: (j["total_bytes_processed"] or 0, j["job_id"]))
    return ranked[len(ranked) // 2]


# ------------------------------------------------------------------------- rewriting

def build_rewriter(src_dataset, src_view, dst_project, dst_dataset):
    """Repoint the credible_sets reference at one arm, leaving every other table alone.

    Queries that also read gene_annotations_v or mpra_v keep reading them from the
    logged dataset: that contribution is identical in both arms, so it cancels out of
    the delta while staying in the absolute totals the runbook's claim is stated over.
    """
    ref = rf"(?:[\w-]+\.)?{re.escape(src_dataset)}\.{re.escape(src_view)}"
    pattern = re.compile(rf"`{ref}`|\b{ref}\b", re.I)

    def rewrite(query, arm_view):
        dst = f"`{dst_project}.{dst_dataset}.{arm_view}`"
        out, n = pattern.subn(dst, query)
        return out, n

    return rewrite


# ------------------------------------------------------------------------- arm setup

def generated_resource_case():
    """The pre-swap view derived `resource` with this CASE; take it from the generator
    so the old arm cannot drift from datasets.yaml behind our back."""
    gen = os.path.join(HERE, "generate_resource_sql.py")
    out = subprocess.run(
        [sys.executable, gen, "generate", "credible_sets_v"],
        capture_output=True, text=True,
    )
    if out.returncode != 0:
        raise RuntimeError(f"generate_resource_sql.py failed:\n{out.stderr}")
    return out.stdout.rstrip("\n")


def ensure_old_arm_view(client, args):
    """Created even under --dry-run: a dry run cannot estimate against a view that does
    not exist, and creating a view scans nothing."""
    case_sql = generated_resource_case()
    ddl = f"""CREATE OR REPLACE VIEW `{args.project}.{args.ab_dataset}.{args.old_view}` AS
SELECT
  *,
  CONCAT(chr, ':', pos, ':', ref, ':', alt) AS variant,
  LEAST(aaf, 1 - aaf) AS maf,
{case_sql}
FROM `{args.project}.{args.ab_dataset}.{args.old_table}`"""
    client.query(ddl).result()
    return ddl


def check_layouts(client, args):
    """Refuse to compare two things that are not the same data in two shapes."""
    from google.cloud import bigquery

    sql = f"""
    SELECT table_name, COUNT(*) AS ncols,
           STRING_AGG(IF(clustering_ordinal_position IS NULL, NULL, column_name),
                      "," ORDER BY clustering_ordinal_position) AS clustering
    FROM `{args.project}.{args.ab_dataset}`.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name IN (@new_t, @old_t)
    GROUP BY table_name
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("new_t", "STRING", args.new_table),
        bigquery.ScalarQueryParameter("old_t", "STRING", args.old_table),
    ])
    found = {r["table_name"]: dict(r) for r in client.query(sql, job_config=cfg).result()}
    missing = [t for t in (args.new_table, args.old_table) if t not in found]
    if missing:
        raise LookupError(f"missing in {args.ab_dataset}: {', '.join(missing)}")

    rows = {}
    for t in (args.new_table, args.old_table):
        tbl = client.get_table(f"{args.project}.{args.ab_dataset}.{t}")
        rows[t] = tbl.num_rows
        found[t]["size_bytes"] = tbl.num_bytes
    return found, rows


# ---------------------------------------------------------------------------- runner

def measure(client, sql, dry_run):
    from google.cloud import bigquery

    cfg = bigquery.QueryJobConfig(use_query_cache=False, dry_run=dry_run)
    job = client.query(sql, job_config=cfg)
    if dry_run:
        return {"bytes": job.total_bytes_processed, "billed": None, "job_id": None}
    job.result()
    return {"bytes": job.total_bytes_processed,
            "billed": job.total_bytes_billed,
            "job_id": job.job_id}


def fmt(n):
    return "n/a" if n is None else f"{n:,}"


def gb(n):
    return None if n is None else n / 1e9


# ------------------------------------------------------------------------------ main

def cmd_shapes(client, args):
    jobs = fetch_jobs(client, args)
    if not jobs:
        print("no matching jobs in the window -- nothing to measure", file=sys.stderr)
        return EXIT_CANNOT_RUN, None
    report_shapes(jobs, args)
    return EXIT_OK, jobs


def report_shapes(jobs, args):
    shapes = group_shapes(jobs)
    total_bytes = sum(j["total_bytes_processed"] or 0 for j in jobs)
    print("=" * 78)
    print("QUERY POPULATION")
    print("=" * 78)
    print(f"window            : last {args.days} days")
    print(f"callers           : {', '.join(args.caller) if args.caller else '(all)'}")
    print(f"tables matched    : {', '.join(args.table_ids)}")
    print(f"jobs              : {len(jobs):,}")
    print(f"distinct shapes   : {len(shapes):,}")
    print(f"collapse ratio    : {len(jobs) / len(shapes):.2f} queries per shape")
    singles = sum(1 for v in shapes.values() if len(v) == 1)
    print(f"singleton shapes  : {singles:,} "
          f"({singles / len(shapes):.1%} of shapes, {singles / len(jobs):.1%} of jobs)")
    print(f"logged bytes      : {total_bytes:,} ({total_bytes / 1e9:.2f} GB)")
    print()
    print("normalisation rules:")
    for i, r in enumerate(NORMALISATION_RULES, 1):
        print(f"  {i}. {r}")
    print()

    byfreq = sorted(shapes.items(), key=lambda kv: (-len(kv[1]), kv[0]))
    for n in (5, args.top_n, 20, 50):
        if n > len(byfreq):
            continue
        top = byfreq[:n]
        jq = sum(len(v) for _, v in top)
        by = sum(j["total_bytes_processed"] or 0 for _, v in top for j in v)
        print(f"top {n:>3} shapes cover {jq:>5,} jobs ({jq / len(jobs):5.1%})  and "
              f"{by / 1e9:9.2f} GB ({by / total_bytes:5.1%} of logged bytes)")
    print()
    print("=" * 78)
    print(f"TOP {args.top_n} SHAPES BY FREQUENCY")
    print("=" * 78)
    for i, (s, v) in enumerate(byfreq[:args.top_n], 1):
        b = sum(j["total_bytes_processed"] or 0 for j in v)
        rep = pick_representative(v)
        print(f"\n[{i:>2}] n={len(v):<4} logged_sum={b / 1e9:8.2f} GB  "
              f"representative_job={rep['job_id']} "
              f"({(rep['total_bytes_processed'] or 0) / 1e9:.3f} GB)")
        print(f"     {s[:args.shape_chars]}")
    return byfreq


# The shapes the runbook's scan-size table is stated over, with its published figures.
# They are hand-picked, not traffic-derived -- that is the point of measuring them
# separately. The projection is fixed because absolute numbers do not compare across
# projections.
PROJ = "SELECT dataset, trait, pip, mlog10p FROM {t}"
RUNBOOK_SHAPES = [
    ("variant equality, no chr filter",
     PROJ + " WHERE variant = '12:111446804:T:C'", 7_606_541_126, 253_707_488),
    ("variant equality + chr filter",
     PROJ + " WHERE chr = 12 AND variant = '12:111446804:T:C'", 367_715_852, 288_326_272),
    ("resource = 'finngen'",
     PROJ + " WHERE resource = 'finngen'", 5_035_212_695, 3_110_953_252),
    ("unfiltered", PROJ, None, None),
    ("dataset = ... (approved regression)",
     PROJ + " WHERE dataset = 'FinnGen_R14'", None, None),
    ("gene_most_severe = ... (approved regression)",
     PROJ + " WHERE gene_most_severe = 'APOE'", None, None),
]


def cmd_verify(client, args):
    try:
        layouts, rows = check_layouts(client, args)
    except LookupError as e:
        print(f"cannot run the A/B: {e}", file=sys.stderr)
        return EXIT_CANNOT_RUN
    ensure_old_arm_view(client, args)

    print("=" * 94)
    print("RUNBOOK SHAPES RE-MEASURED ON THIS RIG"
          + ("  (DRY RUN)" if args.dry_run else ""))
    print("=" * 94)
    hdr = (f"{'shape':<38} {'old bytes':>15} {'new bytes':>15} {'change':>8} "
           f"{'runbook old':>13}")
    print(hdr)
    print("-" * len(hdr))
    for name, tmpl, pub_old, _pub_new in RUNBOOK_SHAPES:
        got = {}
        for arm, view in (("old", args.old_view), ("new", args.new_view)):
            t = f"`{args.project}.{args.ab_dataset}.{view}`"
            got[arm] = measure(client, tmpl.format(t=t), args.dry_run)["bytes"] or 0
        chg = "n/a" if not got["old"] else f"{(got['new'] - got['old']) / got['old']:+7.1%}"
        print(f"{name:<38} {got['old']:>15,} {got['new']:>15,} {chg:>8} "
              f"{(fmt(pub_old)):>13}")
    print("\nthe runbook column is its published pre-swap figure, for the three shapes it "
          "quotes;\na close match here means the rig agrees with it and any disagreement "
          "in the\nheadline total is about which shapes were weighted, not about "
          "measurement.")
    return EXIT_OK


def cmd_run(client, args):
    jobs = fetch_jobs(client, args)
    if not jobs:
        print("no matching jobs in the window -- nothing to measure", file=sys.stderr)
        return EXIT_CANNOT_RUN

    byfreq = report_shapes(jobs, args)

    try:
        layouts, rows = check_layouts(client, args)
    except LookupError as e:
        print(f"cannot run the A/B: {e}", file=sys.stderr)
        return EXIT_CANNOT_RUN

    print()
    print("=" * 78)
    print("LAYOUTS UNDER TEST")
    print("=" * 78)
    for t in (args.old_table, args.new_table):
        d = layouts[t]
        arm = "old" if t == args.old_table else "new"
        print(f"{arm:>3} {t:<28} rows={rows[t]:>12,}  "
              f"size={d['size_bytes'] / 1e9:7.2f} GB  clustered on: {d['clustering']}")
    if len(set(rows.values())) != 1:
        print("\nrow counts differ between the layouts -- the comparison is not valid",
              file=sys.stderr)
        return EXIT_CANNOT_RUN

    ensure_old_arm_view(client, args)

    rewrite = build_rewriter(args.src_dataset, args.src_view, args.project, args.ab_dataset)
    arms = [("old", args.old_view), ("new", args.new_view)]

    results = []
    for i, (shape, instances) in enumerate(byfreq[:args.top_n], 1):
        rep = pick_representative(instances)
        row = {"rank": i, "weight": len(instances), "shape": shape,
               "representative_job": rep["job_id"],
               "logged_bytes": rep["total_bytes_processed"],
               "refs": rep.get("refs"), "arms": {}}
        for arm, view in arms:
            sql, n = rewrite(rep["query"], view)
            if n == 0:
                print(f"shape {i}: no {args.src_view} reference to rewrite -- skipping",
                      file=sys.stderr)
                row["error"] = "no reference rewritten"
                break
            row[f"sql_{arm}"] = sql
            row["arms"][arm] = measure(client, sql, args.dry_run)
        results.append(row)

    print()
    print("=" * 78)
    print("PER-SHAPE SCAN SIZES" + ("  (DRY RUN -- estimates)" if args.dry_run else ""))
    print("=" * 78)
    hdr = (f"{'#':>2} {'weight':>6} {'old bytes':>16} {'new bytes':>16} "
           f"{'change':>8} {'logged(prod,old)':>17}")
    print(hdr)
    print("-" * len(hdr))
    wt_old = wt_new = 0
    for r in results:
        if "error" in r:
            print(f"{r['rank']:>2} {r['weight']:>6} {'ERROR: ' + r['error']:>16}")
            continue
        o = r["arms"]["old"]["bytes"] or 0
        n = r["arms"]["new"]["bytes"] or 0
        wt_old += o * r["weight"]
        wt_new += n * r["weight"]
        chg = "n/a" if o == 0 else f"{(n - o) / o:+7.1%}"
        print(f"{r['rank']:>2} {r['weight']:>6} {o:>16,} {n:>16,} {chg:>8} "
              f"{(r['logged_bytes'] or 0):>17,}")

    print()
    print("=" * 78)
    print("WEIGHTED TOTALS" + ("  (DRY RUN -- estimates)" if args.dry_run else ""))
    print("=" * 78)
    print(f"weighting         : observed frequency of each shape in the window")
    print(f"shapes included   : top {args.top_n} of {len(byfreq):,}")
    print(f"old layout        : {wt_old:>18,} B  ({wt_old / 1e9:9.2f} GB)")
    print(f"new layout        : {wt_new:>18,} B  ({wt_new / 1e9:9.2f} GB)")
    if wt_old:
        print(f"change            : {(wt_new - wt_old) / wt_old:+.1%}  "
              f"(ratio {wt_new / wt_old:.4f})")

    billed = sum((r["arms"].get(a, {}).get("billed") or 0)
                 for r in results for a in ("old", "new"))
    if not args.dry_run:
        print(f"\nbytes billed by this measurement run: {billed:,} ({billed / 1e9:.2f} GB)")

    if args.out:
        payload = {
            "window_days": args.days, "callers": args.caller,
            "jobs": len(jobs), "shapes": len(byfreq), "top_n": args.top_n,
            "normalisation_rules": NORMALISATION_RULES,
            "dry_run": args.dry_run,
            "weighted_old_bytes": wt_old, "weighted_new_bytes": wt_new,
            "measurement_bytes_billed": billed,
            "layouts": {k: {"clustering": v["clustering"], "size_bytes": v["size_bytes"],
                            "rows": rows[k]} for k, v in layouts.items()},
            "per_shape": results,
        }
        with open(args.out, "w") as fh:
            json.dump(payload, fh, indent=2, default=str)
        print(f"\nwrote {args.out}")

    if args.min_reduction is not None and wt_old:
        got = (wt_old - wt_new) / wt_old
        if got < args.min_reduction:
            print(f"\nFAIL: reduction {got:.1%} < required {args.min_reduction:.1%}",
                  file=sys.stderr)
            return EXIT_ASSERT
        print(f"\nPASS: reduction {got:.1%} >= required {args.min_reduction:.1%}")
    return EXIT_OK


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=("shapes", "run", "verify"))
    p.add_argument("--project", default=os.environ.get("BQ_PROJECT", "daly-finngenie"))
    p.add_argument("--region", default="region-us-central1")
    p.add_argument("--days", type=int, default=180)
    p.add_argument("--caller", action="append", default=None,
                   help="user_email to include; repeatable. Default: the production "
                        "service account only, so ad-hoc human and agent queries "
                        "(including this harness's own) stay out of the population.")
    p.add_argument("--table-ids", nargs="+", default=["credible_sets", "credible_sets_v"])
    p.add_argument("--top-n", type=int, default=10)
    p.add_argument("--shape-chars", type=int, default=400)

    p.add_argument("--src-dataset", default="genetics_results",
                   help="dataset the logged queries name")
    p.add_argument("--src-view", default="credible_sets_v")
    p.add_argument("--ab-dataset", default="genetics_results_dev",
                   help="dataset holding both layouts")
    p.add_argument("--new-table", default="credible_sets")
    p.add_argument("--new-view", default="credible_sets_v")
    p.add_argument("--old-table", default="credible_sets_pre_swap")
    p.add_argument("--old-view", default="credible_sets_ab_preswap_v")

    p.add_argument("--dry-run", action="store_true",
                   help="estimate with dry-run jobs instead of executing")
    p.add_argument("--min-reduction", type=float, default=None,
                   help="fail (exit 1) if the weighted reduction is below this "
                        "fraction, e.g. 0.5")
    p.add_argument("--out", default=None, help="write full results as JSON here")
    args = p.parse_args()

    if args.caller is None:
        args.caller = [f"genetics-suite-gke@{args.project}.iam.gserviceaccount.com"]
    if args.caller == ["all"]:
        args.caller = None

    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=args.project)
    except Exception as e:
        print(f"cannot reach BigQuery: {e}", file=sys.stderr)
        return EXIT_CANNOT_RUN

    if args.command == "shapes":
        rc, _ = cmd_shapes(client, args)
        return rc
    if args.command == "verify":
        return cmd_verify(client, args)
    return cmd_run(client, args)


if __name__ == "__main__":
    sys.exit(main())
