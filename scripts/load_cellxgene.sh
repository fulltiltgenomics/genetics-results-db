#!/bin/bash
# Load one CELLxGENE release into BigQuery, replacing the previous one all-or-none.
#
#   scripts/load_cellxgene.sh gs://<bucket>/<root>/releases/<release>
#
# The release is what genetics-results-munge scripts/cellxgene/publish.py wrote: the bq/ files
# and manifest.json. Every cellxgene_* table is replaced in ONE transaction (load_data.py
# --transaction), because their dataset_key/group_key are release-local: a reader joining a
# table from the old release to one from the new would get wrong labels, not an error.
# Each file's staged row count must equal the manifest's, or nothing is touched.
#
# The release name goes into cellxgene_datasets.release in the same transaction, so
#   SELECT DISTINCT release FROM `<project>.<dataset>.cellxgene_datasets`
# is the release BigQuery serves; it is the `release` field of the bucket's releases/CURRENT.
#
# The munge driver calls this through scripts/cellxgene/load_cellxgene.sh there, after publish
# and before it writes releases/CURRENT: exit 0 lets CURRENT flip, anything else stops it.
#
# Tables must exist first (scripts/setup_bigquery.sh). Env: PROJECT_ID (default: the
# project of the application-default credentials, which on a Batch VM is the VM's own
# project from the metadata server), DATASET_ID (default genetics_results).
#
# Python clients only, no gcloud/bq: this runs inside the cellxgene image, which has neither.
# `--help` exits 0 once the interpreter can import google-cloud-bigquery.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/lib/common.sh"

usage() { echo "usage: $0 gs://<bucket>/<root>/releases/<release>"; }
if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
  usage
  exit 0
fi
if [ "$#" -ne 1 ] || [[ "$1" != gs://*/releases/* ]]; then
  usage >&2
  exit 2
fi
RELEASE_URI="${1%/}"
RELEASE="${RELEASE_URI##*/}"
PROJECT_ID="${PROJECT_ID:-$("$PY" -c 'import google.auth; print(google.auth.default()[1] or "")' 2>/dev/null || true)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
if [ -z "${PROJECT_ID}" ]; then
  echo "PROJECT_ID is not set and the application-default credentials name no project" >&2
  exit 2
fi

ts "Loading CELLxGENE release ${RELEASE} into ${PROJECT_ID}.${DATASET_ID}"

# table <- file(s), and the manifest row count each table's staged files must match
SPEC="$("$PY" - "${RELEASE_URI}" "${RELEASE}" <<'PYEOF'
import json
import sys
from urllib.parse import quote

import google.auth
from google.auth.transport.requests import AuthorizedSession

uri, release = sys.argv[1:]
bucket, prefix = uri.removeprefix("gs://").split("/", 1)
creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/devstorage.read_only"])
r = AuthorizedSession(creds).get(
    f"https://storage.googleapis.com/storage/v1/b/{bucket}/o/{quote(prefix + '/manifest.json', safe='')}",
    params={"alt": "media"}, timeout=60)
r.raise_for_status()
m = r.json()
if m.get("genes_mode"):
    sys.exit(f"{uri} is a --genes release (a gene panel); loading it would replace every gene")
# summary and per_dataset are one shard per chromosome (BigQuery refuses a gzip over 4 GB);
# every shard the manifest lists is staged, and their rows must add up to the release total
shards = m.get("bq_shards", {})
tables = {
    "cellxgene_summary": (list(shards.get("summary", {})), "summary"),
    "cellxgene_expression": (list(shards.get("per_dataset", {})), "per_dataset"),
    "cellxgene_groups": (["bq/groups.tsv.gz"], "groups"),
    "cellxgene_datasets": (["bq/datasets.tsv"], "datasets"),
    "cellxgene_measured_genes": (["bq/measured_genes.tsv.gz"], "measured_genes"),
}
missing = [k for fs, k in tables.values()
           if not fs or k not in m.get("rows", {}) or any(f not in m.get("files", {}) for f in fs)]
if missing:
    sys.exit(f"{uri}/manifest.json lists no file, shard or row count for {missing}; publish.py too old?")
for kind in ("summary", "per_dataset"):
    if sum(shards[kind].values()) != m["rows"][kind]:
        sys.exit(f"{uri}/manifest.json: {kind} shards hold {sum(shards[kind].values())} rows, "
                 f"rows.{kind} says {m['rows'][kind]}")
spec = {t: {"table": t, "gcs_uri": [f"{uri}/{f}" for f in fs], "expect_rows": m["rows"][k]}
        for t, (fs, k) in tables.items()}
th = m["thresholds"]
spec["cellxgene_datasets"]["const_columns"] = {
    "release": release, "min_cells": th["min_cells"], "min_donors": th["min_donors"]}
json.dump(list(spec.values()), sys.stdout, indent=1)
PYEOF
)"

# exec, so a SIGTERM to this script reaches load_data.py, which cancels its BigQuery jobs and
# drops its staging tables; a shell left in between would die and leave python loading
exec "$PY" "${SCRIPT_DIR}/load_data.py" \
  --project "${PROJECT_ID}" \
  --dataset "${DATASET_ID}" \
  --transaction - <<<"${SPEC}"
