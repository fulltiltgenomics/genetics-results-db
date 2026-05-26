#!/bin/bash
# Append additional gene burden test results (BipEx, IBD, SCHEMA2) to gene_burden_results.
# Run after load_exome_data.sh, which loads Genebass and truncates the table.

set -euo pipefail

ts() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
GCS_BUCKET="${GCS_BUCKET:-bucket-name}"
GCS_PREFIX="${GCS_PREFIX:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ts "Appending gene burden results into ${PROJECT_ID}.${DATASET_ID}.gene_burden_results"

GENE_BURDEN_FILES=(
  "gs://${GCS_BUCKET}/${GCS_PREFIX}exome_results4/bipex/BipEx2_gene_results.munged.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}exome_results4/ibd/IBD_exome_CD_gene_results.munged.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}exome_results4/ibd/IBD_exome_IBD_gene_results.munged.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}exome_results4/ibd/IBD_exome_UC_gene_results.munged.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}exome_results4/schema/SCHEMA2_gene_results.munged.tsv.gz"
)

echo ""
ts "=== Appending gene burden results ==="
for gcs_uri in "${GENE_BURDEN_FILES[@]}"; do
  if ! gsutil -q stat "${gcs_uri}" 2>/dev/null; then
    ts "ERROR: ${gcs_uri} not found"
    exit 1
  fi
  ts "Loading ${gcs_uri}..."
  python3 "${SCRIPT_DIR}/load_data.py" \
    --project "${PROJECT_ID}" \
    --dataset "${DATASET_ID}" \
    --table gene_burden_results \
    --gcs-uri "${gcs_uri}" \
    --write-disposition WRITE_APPEND
done

echo ""
ts "=== Loading complete ==="

count=$(bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --format=csv \
  "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET_ID}.gene_burden_results\`" 2>/dev/null | tail -1) || count="error"
ts "  gene_burden_results: ${count} rows"
