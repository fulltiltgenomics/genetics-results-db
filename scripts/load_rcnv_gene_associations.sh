#!/bin/bash
# Load the Collins et al. 2022 gene-level rare-CNV association statistics from GCS into
# BigQuery. The munge (genetics-results-munge/scripts/munge_rcnv.sh --product genes) stages
# ONE gzipped TSV per bucket:
#   gs://<bucket>/<prefix>rcnv/collins_rcnv_2022/collins_rcnv_2022_gene_associations.tsv.gz
#
# Whole-table refresh only (WRITE_TRUNCATE): the source is a single frozen Zenodo release,
# so there is nothing to append to.
#
# The file DOES carry a `dataset` column ('Collins_rCNV_2022' on every row), unlike the
# dosage-sensitivity scores next to it, so no --const-column is needed. That column is what
# rcnv_gene_associations_v's CASE switches on and what the registry cross-check in
# load_phenotypes.sh matches against BQ_DATASETS_BY_DATASET_ID['collins_rcnv_2022'] —
# which means this table must be loaded BEFORE load_phenotypes.sh runs, or that check
# fails for the whole profile.
#
# Run once per profile by setting the env vars, e.g.:
#   finngen: PROJECT_ID=<finngen-project> GCS_BUCKET=finngen-commons \
#            GCS_PREFIX=results_api_data/ scripts/load_rcnv_gene_associations.sh
#   daly:    PROJECT_ID=<daly-project> GCS_BUCKET=daly-genetics-results \
#            GCS_PREFIX= scripts/load_rcnv_gene_associations.sh

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
GCS_URI="gs://${GCS_BUCKET}/${GCS_PREFIX}rcnv/${DATASET_NAME}/${DATASET_NAME}_gene_associations.tsv.gz"

ts "Loading rCNV gene associations into ${PROJECT_ID}.${DATASET_ID}"

if ! gsutil -q stat "${GCS_URI}" 2>/dev/null; then
  ts "ERROR: ${GCS_URI} not found"
  exit 1
fi

ts "Loading ${GCS_URI}..."
"$PY" "${SCRIPT_DIR}/load_data.py" \
  --project "${PROJECT_ID}" \
  --dataset "${DATASET_ID}" \
  --table rcnv_gene_associations \
  --gcs-uri "${GCS_URI}" \
  --write-disposition WRITE_TRUNCATE

# CREATE OR REPLACE is idempotent; the backend reads the table through this view. The view
# LEFT JOINs dosage_sensitivity, so that table has to exist first (load_dosage_sensitivity.sh).
ts "Creating/updating view ${PROJECT_ID}.${DATASET_ID}.rcnv_gene_associations_v"
sed "s/genetics_results/${PROJECT_ID}.${DATASET_ID}/g" "${SCRIPT_DIR}/../schemas/rcnv_gene_associations_v.sql" | \
  bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --nouse_cache

echo ""
ts "=== rcnv_gene_associations load complete ==="
report_row_counts rcnv_gene_associations
