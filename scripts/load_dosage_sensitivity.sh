#!/bin/bash
# Load the Collins et al. 2022 gene dosage-sensitivity scores from GCS into BigQuery.
# The munge (genetics-results-munge/scripts/munge_rcnv.sh --product scores) stages ONE
# gzipped TSV per bucket:
#   gs://<bucket>/<prefix>rcnv/collins_rcnv_2022/collins_rcnv_2022_dosage_sensitivity.tsv.gz
#
# Whole-table refresh only (WRITE_TRUNCATE): the source is a single frozen Zenodo
# release, so there is nothing to append to.
#
# The file has no `dataset` column and the table has none either — the scores are one
# published product, and `dosage_sensitivity_v` appends the constant 'rcnv' AS resource
# the way gene_annotations_v does.
#
# Run once per profile by setting the env vars, e.g.:
#   finngen: PROJECT_ID=<finngen-project> GCS_BUCKET=finngen-commons \
#            GCS_PREFIX=results_api_data/ scripts/load_dosage_sensitivity.sh
#   daly:    PROJECT_ID=<daly-project> GCS_BUCKET=daly-genetics-results \
#            GCS_PREFIX= scripts/load_dosage_sensitivity.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/lib/common.sh"

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
GCS_BUCKET="${GCS_BUCKET:-bucket-name}"
# with an empty default both rules agree, so nothing here proves unset-or-empty is the
# right one; give this a non-empty default and the documented daly `GCS_PREFIX=` needs it
GCS_PREFIX="$(resolve_gcs_prefix unset-or-empty "")"

DATASET_NAME="${DATASET_NAME:-collins_rcnv_2022}"
GCS_URI="gs://${GCS_BUCKET}/${GCS_PREFIX}rcnv/${DATASET_NAME}/${DATASET_NAME}_dosage_sensitivity.tsv.gz"

ts "Loading dosage sensitivity scores into ${PROJECT_ID}.${DATASET_ID}"

if ! gsutil -q stat "${GCS_URI}" 2>/dev/null; then
  ts "ERROR: ${GCS_URI} not found"
  exit 1
fi

ts "Loading ${GCS_URI}..."
python3 "${SCRIPT_DIR}/load_data.py" \
  --project "${PROJECT_ID}" \
  --dataset "${DATASET_ID}" \
  --table dosage_sensitivity \
  --gcs-uri "${GCS_URI}" \
  --write-disposition WRITE_TRUNCATE

# CREATE OR REPLACE is idempotent; the backend reads the table through this view.
ts "Creating/updating view ${PROJECT_ID}.${DATASET_ID}.dosage_sensitivity_v"
sed "s/genetics_results/${PROJECT_ID}.${DATASET_ID}/g" "${SCRIPT_DIR}/../schemas/dosage_sensitivity_v.sql" | \
  bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --nouse_cache

echo ""
ts "=== dosage_sensitivity load complete ==="
report_row_counts dosage_sensitivity
