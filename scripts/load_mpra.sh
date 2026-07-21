#!/bin/bash
# Load Siraj et al. MPRA functional-annotation data from GCS into BigQuery.
# The munge stages a SINGLE LONG file (one row per variant x cell_line) to
# gs://<bucket>/<prefix>mpra/siraj_mpra/siraj_mpra.tsv.gz.
#
# Unlike load_open_chromatin.sh / load_variant_effect.sh — whose canonical TSVs
# already carry the `dataset` column in-file — the MPRA LONG file has NO
# `dataset` column, so it is injected at load time via --const-column
# dataset=siraj_mpra (same pattern as load_asm_qtl.sh). The numeric `chrom`
# string is converted to INT64 `chr` by load_data.py (CHR_STRING_TABLES).
#
# Run once per profile by setting the env vars, e.g.:
#   finngen: PROJECT_ID=<finngen-project> GCS_BUCKET=finngen-commons \
#            GCS_PREFIX=results_api_data/ scripts/load_mpra.sh
#   daly:    PROJECT_ID=<daly-project> GCS_BUCKET=daly-genetics-results \
#            GCS_PREFIX= scripts/load_mpra.sh

set -euo pipefail

ts() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
GCS_BUCKET="${GCS_BUCKET:-bucket-name}"
GCS_PREFIX="${GCS_PREFIX:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ts "Loading MPRA data into ${PROJECT_ID}.${DATASET_ID}"

# Each entry: "<dataset-id>|<resource>". A single Siraj MPRA dataset/resource;
# the array keeps the structure parallel to load_variant_effect.sh. The GCS
# layout is per-resource/per-dataset:
# gs://<bucket>/<prefix>mpra/<resource>/<dataset-id>.tsv.gz
MPRA_FILES=(
  "siraj_mpra|siraj_mpra"
)

echo ""
ts "=== Loading MPRA results ==="
first_mpra=true
for entry in "${MPRA_FILES[@]}"; do
  dataset_id="${entry%%|*}"
  resource="${entry#*|}"
  gcs_uri="gs://${GCS_BUCKET}/${GCS_PREFIX}mpra/${resource}/${dataset_id}.tsv.gz"
  if ! gsutil -q stat "${gcs_uri}" 2>/dev/null; then
    ts "ERROR: ${gcs_uri} not found"
    exit 1
  fi
  ts "Loading ${gcs_uri} (dataset=${dataset_id}, resource=${resource})..."
  if [ "$first_mpra" = true ]; then
    disposition="WRITE_TRUNCATE"
    first_mpra=false
  else
    disposition="WRITE_APPEND"
  fi
  # dataset column is absent from the file — inject it here (see header note)
  python3 "${SCRIPT_DIR}/load_data.py" \
    --project "${PROJECT_ID}" \
    --dataset "${DATASET_ID}" \
    --table mpra \
    --gcs-uri "${gcs_uri}" \
    --write-disposition "${disposition}" \
    --const-column "dataset=${dataset_id}"
done

echo ""
ts "=== MPRA data loading complete ==="

echo ""
ts "Table row counts:"
count=$(bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --format=csv \
  "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET_ID}.mpra\`" 2>/dev/null | tail -1) || count="error"
ts "  mpra: ${count} rows"
