#!/bin/bash
# Load exome results data from GCS into BigQuery (genebass + IBD + SCHEMA2)

set -euo pipefail

ts() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
GCS_BUCKET="${GCS_BUCKET:-bucket-name}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ts "Loading exome data into ${PROJECT_ID}.${DATASET_ID}"

# all exome variant results files share the same schema
EXOME_VARIANT_FILES=(
  "gs://${GCS_BUCKET}/results_api_data/exome_results4/genebass/genebass_variant_results.munged.mlog10p_gt4.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/exome_results4/ibd/IBD_exome_IBD_variant_results.munged.mlog10p_gt4.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/exome_results4/ibd/IBD_exome_UC_variant_results.munged.mlog10p_gt4.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/exome_results4/ibd/IBD_exome_CD_variant_results.munged.mlog10p_gt4.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/exome_results4/schema/SCHEMA2_variant_results.munged.mlog10p_gt4.tsv.gz"
)

echo ""
ts "=== Loading exome variant results ==="
first_exome_variant=true

for gcs_uri in "${EXOME_VARIANT_FILES[@]}"; do
  if ! gsutil -q stat "${gcs_uri}" 2>/dev/null; then
    ts "ERROR: ${gcs_uri} not found"
    exit 1
  fi
  ts "Loading ${gcs_uri}..."
  if [ "$first_exome_variant" = true ]; then
    disposition="WRITE_TRUNCATE"
    first_exome_variant=false
  else
    disposition="WRITE_APPEND"
  fi
  python3 "${SCRIPT_DIR}/load_data.py" \
    --project "${PROJECT_ID}" \
    --dataset "${DATASET_ID}" \
    --table exome_variant_results \
    --gcs-uri "${gcs_uri}" \
    --write-disposition "${disposition}"
done

# gene burden results files
GENE_BURDEN_FILES=(
  "gs://${GCS_BUCKET}/results_api_data/exome_results4/genebass/genebass_gene_results.munged.tsv.gz"
)

echo ""
ts "=== Loading gene burden results ==="
first_gene_burden=true
for gcs_uri in "${GENE_BURDEN_FILES[@]}"; do
  if ! gsutil -q stat "${gcs_uri}" 2>/dev/null; then
    ts "ERROR: ${gcs_uri} not found"
    exit 1
  fi
  ts "Loading ${gcs_uri}..."
  if [ "$first_gene_burden" = true ]; then
    disposition="WRITE_TRUNCATE"
    first_gene_burden=false
  else
    disposition="WRITE_APPEND"
  fi
  python3 "${SCRIPT_DIR}/load_data.py" \
    --project "${PROJECT_ID}" \
    --dataset "${DATASET_ID}" \
    --table gene_burden_results \
    --gcs-uri "${gcs_uri}" \
    --write-disposition "${disposition}"
done

echo ""
ts "=== Exome data loading complete ==="

# show table row counts
echo ""
ts "Table row counts:"
for table in exome_variant_results gene_burden_results; do
  count=$(bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --format=csv \
    "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET_ID}.${table}\`" 2>/dev/null | tail -1) || count="error"
  ts "  ${table}: ${count} rows"
done
