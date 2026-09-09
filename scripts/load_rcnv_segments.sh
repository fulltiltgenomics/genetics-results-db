#!/bin/bash
# Load the Collins et al. 2022 disease-associated rare-CNV segments (Table S3) from GCS
# into BigQuery. The munge (genetics-results-munge/scripts/munge_rcnv.sh --product segments)
# stages ONE gzipped TSV per bucket:
#   gs://<bucket>/<prefix>rcnv/collins_rcnv_2022/collins_rcnv_2022_segments.tsv.gz
#
# Whole-table refresh only (WRITE_TRUNCATE): the source is a single frozen Zenodo release,
# so there is nothing to append to.
#
# The file carries a `dataset` column ('Collins_rCNV_2022' on every row), so no
# --const-column is needed; that column is what rcnv_segments_v's CASE switches on.
#
# Run once per profile by setting the env vars, e.g.:
#   finngen: PROJECT_ID=<finngen-project> GCS_BUCKET=finngen-commons \
#            GCS_PREFIX=results_api_data/ scripts/load_rcnv_segments.sh
#   daly:    PROJECT_ID=<daly-project> GCS_BUCKET=daly-genetics-results \
#            GCS_PREFIX= scripts/load_rcnv_segments.sh

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
GCS_URI="gs://${GCS_BUCKET}/${GCS_PREFIX}rcnv/${DATASET_NAME}/${DATASET_NAME}_segments.tsv.gz"

ts "Loading rCNV segments into ${PROJECT_ID}.${DATASET_ID}"

if ! gsutil -q stat "${GCS_URI}" 2>/dev/null; then
  ts "ERROR: ${GCS_URI} not found"
  exit 1
fi

ts "Loading ${GCS_URI}..."
python3 "${SCRIPT_DIR}/load_data.py" \
  --project "${PROJECT_ID}" \
  --dataset "${DATASET_ID}" \
  --table rcnv_segments \
  --gcs-uri "${GCS_URI}" \
  --write-disposition WRITE_TRUNCATE

# CREATE OR REPLACE is idempotent; the backend reads the table through this view
ts "Creating/updating view ${PROJECT_ID}.${DATASET_ID}.rcnv_segments_v"
sed "s/genetics_results/${PROJECT_ID}.${DATASET_ID}/g" "${SCRIPT_DIR}/../schemas/rcnv_segments_v.sql" | \
  bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --nouse_cache

echo ""
ts "=== rcnv_segments load complete ==="
report_row_counts rcnv_segments
