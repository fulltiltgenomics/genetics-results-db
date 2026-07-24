#!/bin/bash
# Load FinnGen (R14) per-variant functional annotations from GCS into BigQuery.
# Source is the same tabix file the genetics-results-api serves at
# /variant_annotation/finngen: R14_annotated_variants_v0.small.gz. The file is a
# bgzip-compressed TSV (gzip-compatible, so BigQuery decompresses it directly) and
# already encodes chr numerically (X=23), so no chr-string conversion is needed.
# Loaded with WRITE_TRUNCATE (single source, full refresh).

set -euo pipefail

ts() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
GCS_BUCKET="${GCS_BUCKET:-finngen-commons}"
GCS_PREFIX="${GCS_PREFIX-results_api_data/}"
VA_FILE="${VA_FILE:-variant_annotations/R14_annotated_variants_v0.small.gz}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

gcs_uri="gs://${GCS_BUCKET}/${GCS_PREFIX}${VA_FILE}"

ts "Loading variant annotations into ${PROJECT_ID}.${DATASET_ID}"

if ! gsutil -q stat "${gcs_uri}" 2>/dev/null; then
  ts "ERROR: ${gcs_uri} not found"
  exit 1
fi

ts "=== Loading variant annotations from ${gcs_uri} ==="
python3 "${SCRIPT_DIR}/load_data.py" \
  --project "${PROJECT_ID}" \
  --dataset "${DATASET_ID}" \
  --table variant_annotation \
  --gcs-uri "${gcs_uri}" \
  --write-disposition WRITE_TRUNCATE

echo ""
ts "=== variant annotation loading complete ==="

echo ""
ts "Table row count:"
count=$(bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --format=csv \
  "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET_ID}.variant_annotation\`" 2>/dev/null | tail -1) || count="error"
ts "  variant_annotation: ${count} rows"
