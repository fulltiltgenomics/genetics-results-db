#!/bin/bash
# Load predicted variant-effect data from GCS into BigQuery (Product B).
# The canonical/tabix TSVs already carry the 18-column layout including the
# `dataset` column, so no --const-column injection is needed. The chr-prefixed
# `chrom` string is converted to INT64 `chr` by load_data.py (CHR_STRING_TABLES).

set -euo pipefail

ts() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
GCS_BUCKET="${GCS_BUCKET:-bucket-name}"
GCS_PREFIX="${GCS_PREFIX:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ts "Loading variant-effect data into ${PROJECT_ID}.${DATASET_ID}"

# Each entry: "<dataset-id>|<resource>". The GCS layout is per-resource/per-dataset:
# gs://<bucket>/<prefix>variant_effect/<resource>/<dataset-id>.tsv.gz
# Both marderstein datasets (chrombpnet, flare) resolve to resource marderstein.
VARIANT_EFFECT_FILES=(
  "marderstein_chrombpnet|marderstein"
  "marderstein_flare|marderstein"
)

echo ""
ts "=== Loading variant-effect results ==="
first_variant_effect=true
for entry in "${VARIANT_EFFECT_FILES[@]}"; do
  dataset_id="${entry%%|*}"
  resource="${entry#*|}"
  gcs_uri="gs://${GCS_BUCKET}/${GCS_PREFIX}variant_effect/${resource}/${dataset_id}.tsv.gz"
  if ! gsutil -q stat "${gcs_uri}" 2>/dev/null; then
    ts "ERROR: ${gcs_uri} not found"
    exit 1
  fi
  ts "Loading ${gcs_uri} (dataset=${dataset_id}, resource=${resource})..."
  if [ "$first_variant_effect" = true ]; then
    disposition="WRITE_TRUNCATE"
    first_variant_effect=false
  else
    disposition="WRITE_APPEND"
  fi
  python3 "${SCRIPT_DIR}/load_data.py" \
    --project "${PROJECT_ID}" \
    --dataset "${DATASET_ID}" \
    --table variant_effect \
    --gcs-uri "${gcs_uri}" \
    --write-disposition "${disposition}"
done

echo ""
ts "=== variant-effect data loading complete ==="

echo ""
ts "Table row counts:"
count=$(bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --format=csv \
  "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET_ID}.variant_effect\`" 2>/dev/null | tail -1) || count="error"
ts "  variant_effect: ${count} rows"
