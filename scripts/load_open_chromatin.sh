#!/bin/bash
# Load open-chromatin atlas data from GCS into BigQuery (Product A).
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

ts "Loading open-chromatin data into ${PROJECT_ID}.${DATASET_ID}"

# Each entry: "<dataset-id>|<resource>". The GCS layout is per-resource/per-dataset:
# gs://<bucket>/<prefix>open_chromatin/<resource>/<dataset-id>.tsv.gz
OPEN_CHROMATIN_FILES=(
  "marderstein_open_chromatin|marderstein"
  "li_brain_open_chromatin|li_brain_atac"
  "catlas_open_chromatin|catlas"
  "epimap_open_chromatin|epimap"
  "calderon_open_chromatin|calderon_immune"
  "rosmap_open_chromatin|rosmap_brain"
)

echo ""
ts "=== Loading open-chromatin results ==="
first_open_chromatin=true
for entry in "${OPEN_CHROMATIN_FILES[@]}"; do
  dataset_id="${entry%%|*}"
  resource="${entry#*|}"
  gcs_uri="gs://${GCS_BUCKET}/${GCS_PREFIX}open_chromatin/${resource}/${dataset_id}.tsv.gz"
  if ! gsutil -q stat "${gcs_uri}" 2>/dev/null; then
    ts "ERROR: ${gcs_uri} not found"
    exit 1
  fi
  ts "Loading ${gcs_uri} (dataset=${dataset_id}, resource=${resource})..."
  if [ "$first_open_chromatin" = true ]; then
    disposition="WRITE_TRUNCATE"
    first_open_chromatin=false
  else
    disposition="WRITE_APPEND"
  fi
  python3 "${SCRIPT_DIR}/load_data.py" \
    --project "${PROJECT_ID}" \
    --dataset "${DATASET_ID}" \
    --table open_chromatin \
    --gcs-uri "${gcs_uri}" \
    --write-disposition "${disposition}"
done

echo ""
ts "=== open-chromatin data loading complete ==="

echo ""
ts "Table row counts:"
count=$(bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --format=csv \
  "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET_ID}.open_chromatin\`" 2>/dev/null | tail -1) || count="error"
ts "  open_chromatin: ${count} rows"
