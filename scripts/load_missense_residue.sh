#!/bin/bash
# Load the variant-to-residue map of one protein structure release into missense_residue,
# replacing the previous release all-or-none.
#
#   scripts/load_missense_residue.sh --dataset <dataset> gs://<bucket>/<root>/releases/<release>
#   scripts/load_missense_residue.sh --dataset <dataset> gs://<bucket>/<root>/releases/CURRENT
#
# The file loaded is <release>/variants.tsv.gz, the object genetics-results-api serves, read
# in place: no copy, no shards (BigQuery refuses a compressed CSV above 4 GB, and would
# refuse this one if it grew past that). Given the pointer, the release is the one it names
# and the manifest must hash to the pointer's manifest_sha256. Either way the object's size
# must equal the manifest's, the header must be the loader's staged columns in order
# (load_data.py SCHEMAS, not the manifest), and the staged row count must equal the
# manifest's, or the table's rows are left as they were (load_data.py --transaction).
#
# The release name goes into missense_residue.release in the same transaction, so
#   SELECT DISTINCT release FROM `<project>.<dataset>.missense_residue`
# is the release BigQuery serves.
#
# The DDL runs before the file is staged, because load_data.py refuses a target that does
# not exist: once the pointer and manifest checks pass, a missing table is created (empty)
# and the view is replaced from schemas/, even when the header or row-count check then
# refuses the file. So a failed first run leaves an empty table and its view behind, and an
# existing table is never altered: a changed schema file needs a deliberate rebuild.
#
# The dataset comes from --dataset only. Env: PROJECT_ID (default: the project of the
# application-default credentials).
#
# Python clients only, no gcloud/bq, as load_cellxgene.sh.

set -euo pipefail

TABLE="missense_residue"

usage() {
  echo "usage: $0 --dataset <dataset> gs://<bucket>/<root>/releases/<release>|gs://<bucket>/<root>/releases/CURRENT"
}

refuse() {
  echo "$0: $1" >&2
  usage >&2
  exit 2
}

# not read from DATASET_ID: the other loaders default that variable to the served dataset,
# so a shell that exported it for one of them would aim this whole-table replace there
DATASET=""
SOURCE_URI=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -h | --help)
      usage
      exit 0
      ;;
    --dataset)
      [ "$#" -ge 2 ] && [ -n "$2" ] || refuse "--dataset needs a value"
      DATASET="$2"
      shift
      ;;
    gs://*/releases/?*)
      [ -z "${SOURCE_URI}" ] || refuse "more than one release URI: ${SOURCE_URI} and $1"
      SOURCE_URI="${1%/}"
      ;;
    gs://*/releases/ | gs://*/releases)
      refuse "$1 names no release (want .../releases/<release> or .../releases/CURRENT)"
      ;;
    *)
      refuse "unexpected argument: $1"
      ;;
  esac
  shift
done
[ -n "${SOURCE_URI}" ] || refuse "no release URI"
[ -n "${DATASET}" ] || refuse "no dataset: pass --dataset (there is no default, and DATASET_ID is not read)"
DATASET_ID="${DATASET}"

# sourced after argument handling: it exits unless a Python with google-cloud-bigquery is found
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/lib/common.sh"
PROJECT_ID="${PROJECT_ID:-$("$PY" -c 'import google.auth; print(google.auth.default()[1] or "")' 2>/dev/null || true)}"
if [ -z "${PROJECT_ID}" ]; then
  echo "PROJECT_ID is not set and the application-default credentials name no project" >&2
  exit 2
fi

SPEC="$("$PY" - "${SOURCE_URI}" "${TABLE}" <<'PYEOF'
import hashlib
import json
import sys
from urllib.parse import quote

import google.auth
from google.auth.transport.requests import AuthorizedSession

source, table = sys.argv[1:]
creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/devstorage.read_only"])
session = AuthorizedSession(creds)


def gcs(uri, media=True):
    bucket, name = uri.removeprefix("gs://").split("/", 1)
    r = session.get(
        f"https://storage.googleapis.com/storage/v1/b/{bucket}/o/{quote(name, safe='')}",
        params={"alt": "media"} if media else {}, timeout=60)
    r.raise_for_status()
    return r.content if media else r.json()


uri, pointer = source, None
if source.endswith("/releases/CURRENT"):
    pointer = json.loads(gcs(source))
    uri = pointer["uri"].rstrip("/")
    if uri.rsplit("/", 1)[-1] != pointer["release"]:
        sys.exit(f"{source} names release {pointer['release']!r} but points at {uri}")
release = uri.rsplit("/", 1)[-1]

raw = gcs(f"{uri}/manifest.json")
if pointer and hashlib.sha256(raw).hexdigest() != pointer["manifest_sha256"]:
    sys.exit(f"{uri}/manifest.json does not hash to the manifest_sha256 in {source}")
m = json.loads(raw)
if m.get("release") != release:
    sys.exit(f"{uri}/manifest.json is for release {m.get('release')!r}")
try:
    rows = m["counts"]["variants"]["variant_rows"]
    size = m["objects"]["variants.tsv.gz"]["bytes"]
except KeyError as e:
    sys.exit(f"{uri}/manifest.json has no {e} for variants.tsv.gz")
got = int(gcs(f"{uri}/variants.tsv.gz", media=False)["size"])
if got != size:
    sys.exit(f"{uri}/variants.tsv.gz is {got} bytes, its manifest says {size}")
json.dump([{"table": table, "gcs_uri": f"{uri}/variants.tsv.gz", "expect_rows": rows,
            "const_columns": {"release": release}}], sys.stdout, indent=1)
PYEOF
)"

ts "Loading $("$PY" -c 'import json, sys; print(json.load(sys.stdin)[0]["gcs_uri"])' <<<"${SPEC}") into ${PROJECT_ID}.${DATASET_ID}.${TABLE}"

# the substitution setup_bigquery.sh applies, here so this one table can be added to a
# dataset without running every schema file against it
"$PY" - "${PROJECT_ID}" "${DATASET_ID}" "${SCRIPT_DIR}/../schemas/${TABLE}.sql" "${SCRIPT_DIR}/../schemas/${TABLE}_v.sql" <<'PYEOF'
import sys

from google.cloud import bigquery

project, dataset, *paths = sys.argv[1:]
client = bigquery.Client(project=project)
for path in paths:
    with open(path) as fh:
        client.query(fh.read().replace("genetics_results", f"{project}.{dataset}")).result()
PYEOF

# exec, so a SIGTERM to this script reaches load_data.py, which cancels its BigQuery jobs and
# drops its staging table
exec "$PY" "${SCRIPT_DIR}/load_data.py" \
  --project "${PROJECT_ID}" \
  --dataset "${DATASET_ID}" \
  --transaction - <<<"${SPEC}"
