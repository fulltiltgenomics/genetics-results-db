#!/bin/bash
# Load the SCHEMA2 per-variant allele counts into exome_variant_counts, the table the ASC
# release's per-variant counts share. The munge
# (genetics-results-munge/scripts/munge_schema2_variant_counts.py) stages
#   gs://<bucket>/<prefix>exome_results/schema/SCHEMA2_variant_counts.munged.tsv.gz
#
# Deletes its own rows (dataset = 'SCHEMA2') and appends, so a rerun is idempotent and
# independent of load_asc.sh. The DELETE runs before the load, so a failed load leaves the
# table without SCHEMA2 rows; rerunning is the recovery.
#
# The view is created by setup_bigquery.sh, not here.
#
# Run once per profile by setting the env vars, e.g.:
#   finngen:   PROJECT_ID=<finngen-project> GCS_BUCKET=finngen-commons \
#              GCS_PREFIX=results_api_data/ scripts/load_schema2_variant_counts.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/lib/common.sh"

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
GCS_BUCKET="${GCS_BUCKET:-bucket-name}"
GCS_PREFIX="$(resolve_gcs_prefix unset-or-empty "")"

SCHEMA2_DATASET="SCHEMA2"
TABLE="exome_variant_counts"
GCS_URI="gs://${GCS_BUCKET}/${GCS_PREFIX}exome_results/schema/SCHEMA2_variant_counts.munged.tsv.gz"

ts "Loading ${SCHEMA2_DATASET} into ${PROJECT_ID}.${DATASET_ID}.${TABLE}"

if ! gsutil -q stat "${GCS_URI}" 2>/dev/null; then
  ts "ERROR: ${GCS_URI} not found"
  exit 1
fi

ts "=== ${TABLE}: deleting existing ${SCHEMA2_DATASET} rows ==="
bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false \
  "DELETE FROM \`${PROJECT_ID}.${DATASET_ID}.${TABLE}\` WHERE dataset = '${SCHEMA2_DATASET}'"
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
