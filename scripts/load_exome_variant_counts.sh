#!/bin/bash
# Load one case-control exome study's per-variant allele counts into exome_variant_counts,
# the table the ASC release's per-variant counts share. The munge
# (genetics-results-munge/scripts/munge_exome_variant_counts.py) stages
#   gs://<bucket>/<prefix>exome_results/schema/SCHEMA2_variant_counts.munged.tsv.gz
#   gs://<bucket>/<prefix>exome_results/bipex/BipEx2_variant_counts.munged.tsv.gz
#
# Deletes the dataset's own rows and appends, so a rerun is idempotent and independent of
# load_asc.sh and of the other dataset. The DELETE runs before the load, so a failed load
# leaves the table without that dataset's rows; rerunning is the recovery.
#
# The view is created by setup_bigquery.sh, not here.
#
# Run once per dataset and profile by setting the env vars, e.g.:
#   finngen:   PROJECT_ID=<finngen-project> GCS_BUCKET=finngen-commons \
#              GCS_PREFIX=results_api_data/ scripts/load_exome_variant_counts.sh SCHEMA2

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/lib/common.sh"

case "${1:-}" in
  SCHEMA2) SUBDIR="schema" ;;
  BipEx2)  SUBDIR="bipex" ;;
  *) echo "usage: $0 <SCHEMA2|BipEx2>" >&2; exit 2 ;;
esac
EXOME_DATASET="$1"

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
GCS_BUCKET="${GCS_BUCKET:-bucket-name}"
GCS_PREFIX="$(resolve_gcs_prefix unset-or-empty "")"

TABLE="exome_variant_counts"
GCS_URI="gs://${GCS_BUCKET}/${GCS_PREFIX}exome_results/${SUBDIR}/${EXOME_DATASET}_variant_counts.munged.tsv.gz"

ts "Loading ${EXOME_DATASET} into ${PROJECT_ID}.${DATASET_ID}.${TABLE}"

if ! gsutil -q stat "${GCS_URI}" 2>/dev/null; then
  ts "ERROR: ${GCS_URI} not found"
  exit 1
fi

ts "=== ${TABLE}: deleting existing ${EXOME_DATASET} rows ==="
bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false \
  "DELETE FROM \`${PROJECT_ID}.${DATASET_ID}.${TABLE}\` WHERE dataset = '${EXOME_DATASET}'"
ts "=== ${TABLE}: loading ${GCS_URI} ==="
"$PY" "${SCRIPT_DIR}/load_data.py" \
  --project "${PROJECT_ID}" \
  --dataset "${DATASET_ID}" \
  --table "${TABLE}" \
  --gcs-uri "${GCS_URI}" \
  --write-disposition WRITE_APPEND

echo ""
ts "=== Loading complete ==="
report_row_counts "${TABLE}"
