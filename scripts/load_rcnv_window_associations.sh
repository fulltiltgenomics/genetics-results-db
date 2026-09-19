#!/bin/bash
# Load the Collins et al. 2022 sliding-window rare-CNV association statistics from GCS into
# BigQuery. The munge (genetics-results-munge/scripts/munge_rcnv.sh --product windows) stages
# ONE gzipped TSV per bucket:
#   gs://<bucket>/<prefix>rcnv/collins_rcnv_2022/collins_rcnv_2022_window_associations.tsv.gz
#
# Whole-table refresh only (WRITE_TRUNCATE): the source is a single frozen Zenodo release,
# so there is nothing to append to.
#
# 11.2M rows, ~236 MB gzipped — by far the largest table of the rCNV product. The load runs
# entirely inside BigQuery (a load job reading the GCS object); nothing is decompressed on
# the machine running this script.
#
# The file carries its own `dataset` column ('Collins_rCNV_2022' on every row), so no
# --const-column is needed, and `chr` is already a bare integer so no chr-string staging is
# needed either: this takes load_data.py's direct-load path.
#
# Run once per profile by setting the env vars, e.g.:
#   finngen: PROJECT_ID=<finngen-project> GCS_BUCKET=finngen-commons \
#            GCS_PREFIX=results_api_data/ scripts/load_rcnv_window_associations.sh
#   daly:    PROJECT_ID=<daly-project> GCS_BUCKET=daly-genetics-results \
#            GCS_PREFIX= scripts/load_rcnv_window_associations.sh

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
GCS_URI="gs://${GCS_BUCKET}/${GCS_PREFIX}rcnv/${DATASET_NAME}/${DATASET_NAME}_window_associations.tsv.gz"

ts "Loading rCNV sliding-window associations into ${PROJECT_ID}.${DATASET_ID}"

if ! gsutil -q stat "${GCS_URI}" 2>/dev/null; then
  ts "ERROR: ${GCS_URI} not found"
  exit 1
fi

ts "Loading ${GCS_URI}..."
"$PY" "${SCRIPT_DIR}/load_data.py" \
  --project "${PROJECT_ID}" \
  --dataset "${DATASET_ID}" \
  --table rcnv_window_associations \
  --gcs-uri "${GCS_URI}" \
  --write-disposition WRITE_TRUNCATE

# CREATE OR REPLACE is idempotent; the backend reads the table through this view. Unlike
# rcnv_gene_associations_v the view joins nothing, so no other table has to exist first.
ts "Creating/updating view ${PROJECT_ID}.${DATASET_ID}.rcnv_window_associations_v"
sed "s/genetics_results/${PROJECT_ID}.${DATASET_ID}/g" "${SCRIPT_DIR}/../schemas/rcnv_window_associations_v.sql" | \
  bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --nouse_cache

echo ""
ts "=== rcnv_window_associations load complete ==="
report_row_counts rcnv_window_associations
