#!/bin/bash
# Load ASM-QTL data from GCS into BigQuery (deCODE CpG + MDS methylation)

set -euo pipefail

ts() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
GCS_BUCKET="${GCS_BUCKET:-bucket-name}"
GCS_PREFIX="${GCS_PREFIX:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ts "Loading ASM-QTL data into ${PROJECT_ID}.${DATASET_ID}"

# Each entry: "<dataset-value>|<gcs-uri>". The munged TSVs don't include a `dataset`
# column, so it is injected at load time via --const-column.
ASM_QTL_FILES=(
  "deCODE_asmQTL_CpG|gs://${GCS_BUCKET}/${GCS_PREFIX}asm_qtl/deCODE_asmQTL_CpG.munged.tsv.gz"
  "deCODE_asmQTL_MDS|gs://${GCS_BUCKET}/${GCS_PREFIX}asm_qtl/deCODE_asmQTL_MDS.munged.tsv.gz"
)

echo ""
ts "=== Loading ASM-QTL results ==="
first_asm_qtl=true
for entry in "${ASM_QTL_FILES[@]}"; do
  dataset_value="${entry%%|*}"
  gcs_uri="${entry#*|}"
  if ! gsutil -q stat "${gcs_uri}" 2>/dev/null; then
    ts "ERROR: ${gcs_uri} not found"
    exit 1
  fi
  ts "Loading ${gcs_uri} as dataset=${dataset_value}..."
  if [ "$first_asm_qtl" = true ]; then
    disposition="WRITE_TRUNCATE"
    first_asm_qtl=false
  else
    disposition="WRITE_APPEND"
  fi
  python3 "${SCRIPT_DIR}/load_data.py" \
    --project "${PROJECT_ID}" \
    --dataset "${DATASET_ID}" \
    --table asm_qtl \
    --gcs-uri "${gcs_uri}" \
    --write-disposition "${disposition}" \
    --const-column "dataset=${dataset_value}"
done

echo ""
ts "=== ASM-QTL data loading complete ==="

echo ""
ts "Table row counts:"
count=$(bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --format=csv \
  "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET_ID}.asm_qtl\`" 2>/dev/null | tail -1) || count="error"
ts "  asm_qtl: ${count} rows"
