#!/bin/bash
# Append additional exome variant results (IBD, SCHEMA2) to exome_variant_results.
# Run after load_genebass_variants.sh, which loads GeneBASS and truncates the table.

set -euo pipefail

ts() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
GCS_BUCKET="${GCS_BUCKET:-bucket-name}"
GCS_PREFIX="${GCS_PREFIX:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ts "Appending exome variant results into ${PROJECT_ID}.${DATASET_ID}.exome_variant_results"

EXOME_VARIANT_FILES=(
  "gs://${GCS_BUCKET}/${GCS_PREFIX}exome_results4/ibd/IBD_exome_IBD_variant_results.munged.mlog10p_gt4.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}exome_results4/ibd/IBD_exome_UC_variant_results.munged.mlog10p_gt4.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}exome_results4/ibd/IBD_exome_CD_variant_results.munged.mlog10p_gt4.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}exome_results4/schema/SCHEMA2_variant_results.munged.mlog10p_gt4.tsv.gz"
)

echo ""
ts "=== Appending exome variant results ==="
for gcs_uri in "${EXOME_VARIANT_FILES[@]}"; do
  if ! gsutil -q stat "${gcs_uri}" 2>/dev/null; then
    ts "ERROR: ${gcs_uri} not found"
    exit 1
  fi
  ts "Loading ${gcs_uri}..."
  python3 "${SCRIPT_DIR}/load_data.py" \
    --project "${PROJECT_ID}" \
    --dataset "${DATASET_ID}" \
    --table exome_variant_results \
    --gcs-uri "${gcs_uri}" \
    --write-disposition WRITE_APPEND
done

echo ""
ts "=== Loading complete ==="

count=$(bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --format=csv \
  "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET_ID}.exome_variant_results\`" 2>/dev/null | tail -1) || count="error"
ts "  exome_variant_results: ${count} rows"
