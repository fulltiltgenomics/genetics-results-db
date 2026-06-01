#!/bin/bash
# Load GeneBASS gene burden results into gene_burden_results (WRITE_TRUNCATE).
# Truncates the table — run before load_gene_burden_extra.sh.

set -euo pipefail

ts() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
GCS_BUCKET="${GCS_BUCKET:-bucket-name}"
GCS_PREFIX="${GCS_PREFIX:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ts "Loading GeneBASS gene burden results into ${PROJECT_ID}.${DATASET_ID}.gene_burden_results"

GENEBASS_GENE_FILES=(
  "gs://${GCS_BUCKET}/${GCS_PREFIX}exome_results/genebass/gene_burden_results.tsv.gz"
)

echo ""
ts "=== Loading GeneBASS gene burden results ==="
# Reset via TRUNCATE TABLE (DML) rather than a WRITE_TRUNCATE load: the latter
# replaces the table schema and drops column descriptions the API depends on.
# Requires the table to exist (run setup_bigquery.sh first).
ts "Truncating gene_burden_results (preserves schema/descriptions)..."
bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false \
  "TRUNCATE TABLE \`${PROJECT_ID}.${DATASET_ID}.gene_burden_results\`"
for gcs_uri in "${GENEBASS_GENE_FILES[@]}"; do
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
