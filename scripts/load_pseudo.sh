#!/bin/bash
# Load pseudo credible set data from GCS into BigQuery
# these are FinnGen+MVP+UKBB+AoU (R14) meta-analysis pseudo
# credible sets, a shared external file bundling COVID-19 HGI, PGC (SCZ/BIP), GP2 (PD) and
# IIBDGC (IBD/UC/CD), and the deCODE 2021 plasma pQTL sets (their own file)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/lib/common.sh"

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
GCS_BUCKET="${GCS_BUCKET:-finngen-commons}"
GCS_PREFIX="$(resolve_gcs_prefix unset-only "results_api_data/")"

ts "Loading pseudo credible sets into ${PROJECT_ID}.${DATASET_ID}"

PSEUDO_CREDSET_FILES=(
  "gs://${GCS_BUCKET}/${GCS_PREFIX}credible_sets/ext_pseudo/EXT_20260610_pseudo_credible_sets.mlog10p_2.r2_0.6.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}credible_sets/decode_pseudo/deCODE_pQTL_2021_pseudo_credible_sets.mlog10p_2.r2_0.6.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}credible_sets/finngen_mvp_ukbb_aou_pseudo/r14/FinnGen_R14_MVP_UKBB_AOU_pseudo_credible_sets.mlog10p_2.r2_0.6.tsv.gz"
)

# the delete below is a single statement over every listed dataset, so a file missing later
# in the list would leave the earlier datasets deleted and unloaded. stderr is left open so
# an auth failure reads differently from a missing object.
missing=()
for gcs_uri in "${PSEUDO_CREDSET_FILES[@]}"; do
  gsutil -q stat "${gcs_uri}" || missing+=("${gcs_uri}")
done
if [ "${#missing[@]}" -gt 0 ]; then
  ts "ERROR: not found or not readable, nothing deleted:"
  printf '  %s\n' "${missing[@]}"
  exit 1
fi

echo ""
ts "=== Deleting existing pseudo credible set rows ==="
bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false \
  "DELETE FROM \`${PROJECT_ID}.${DATASET_ID}.credible_sets\`
   WHERE dataset IN ('FinnGen_R14_MVP_UKBB_AOU',
                     'COVID19_HGI', 'PGC', 'GP2', 'IIBDGC', 'deCODE_pQTL_2021')"
ts "Done"

echo ""
ts "=== Loading pseudo credible sets ==="
for gcs_uri in "${PSEUDO_CREDSET_FILES[@]}"; do
  ts "Loading ${gcs_uri}..."
  "$PY" "${SCRIPT_DIR}/load_data.py" \
    --project "${PROJECT_ID}" \
    --dataset "${DATASET_ID}" \
    --table credible_sets \
    --gcs-uri "${gcs_uri}" \
    --write-disposition "WRITE_APPEND"
done

echo ""
ts "=== Pseudo credible set loading complete ==="

echo ""
ts "Credible sets row count:"
report_row_counts credible_sets
