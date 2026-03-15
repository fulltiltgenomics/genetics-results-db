#!/bin/bash
# Load pseudo credible set data from GCS into BigQuery
# these are FinnGen+UKBB and FinnGen+MVP+UKBB meta-analysis pseudo credible sets

set -euo pipefail

ts() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
GCS_BUCKET="${GCS_BUCKET:-finngen-commons}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ts "Loading pseudo credible sets into ${PROJECT_ID}.${DATASET_ID}"

PSEUDO_CREDSET_FILES=(
  "gs://${GCS_BUCKET}/results_api_data/credible_sets/finngen_ukbb_pseudo/r13/FinnGen_R13_UKBB_pseudo_credible_sets.mlog10p_2.r2_0.6.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/credible_sets/finngen_ukbb_labs_pseudo/r13/FinnGen_R13_UKBB_labs_pseudo_credible_sets.mlog10p_2.r2_0.6.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/credible_sets/finngen_mvp_ukbb_pseudo/r13/FinnGen_R13_MVP_UKBB_pseudo_credible_sets.mlog10p_2.r2_0.6.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/credible_sets/finngen_mvp_ukbb_labs_pseudo/r13/FinnGen_R13_MVP_UKBB_labs_pseudo_credible_sets.mlog10p_2.r2_0.6.tsv.gz"
)

echo ""
ts "=== Loading pseudo credible sets ==="
for gcs_uri in "${PSEUDO_CREDSET_FILES[@]}"; do
  if gsutil -q stat "${gcs_uri}" 2>/dev/null; then
    ts "Loading ${gcs_uri}..."
    python3 "${SCRIPT_DIR}/load_data.py" \
      --project "${PROJECT_ID}" \
      --dataset "${DATASET_ID}" \
      --table credible_sets \
      --gcs-uri "${gcs_uri}" \
      --write-disposition "WRITE_APPEND"
  else
    ts "Skipping ${gcs_uri} (not found)"
  fi
done

echo ""
ts "=== Pseudo credible set loading complete ==="

echo ""
ts "Credible sets row count:"
count=$(bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --format=csv \
  "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET_ID}.credible_sets\`" 2>/dev/null | tail -1) || count="error"
ts "  credible_sets: ${count} rows"
