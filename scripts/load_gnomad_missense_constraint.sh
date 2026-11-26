#!/bin/bash
# Load the gnomAD v4.1.1 MPC scores and missense constraint regions from GCS into BigQuery.
# The munge (genetics-results-munge/scripts/munge_gnomad_missense_constraint.py) stages
#   gs://<bucket>/<prefix>gnomad/missense_constraint/gnomad_v4.1.1_mpc.munged.tsv.gz
#   gs://<bucket>/<prefix>gnomad/missense_constraint/gnomad_v4.1.1_missense_constraint_regions.munged.tsv.gz
#
# Whole-table refresh (WRITE_TRUNCATE): each file is one frozen release. Creates the tables
# and views itself, so they can be added to a dataset without running setup_bigquery.sh.
#
# Run once per profile by setting the env vars, e.g.:
#   daly:    PROJECT_ID=<daly-project> GCS_BUCKET=daly-genetics-results \
#            GCS_PREFIX= scripts/load_gnomad_missense_constraint.sh
#   finngen: PROJECT_ID=<finngen-project> GCS_BUCKET=finngen-commons \
#            GCS_PREFIX=results_api_data/ scripts/load_gnomad_missense_constraint.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/lib/common.sh"

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
GCS_BUCKET="${GCS_BUCKET:-bucket-name}"
GCS_PREFIX="$(resolve_gcs_prefix unset-or-empty "")"
GCS_DIR="gs://${GCS_BUCKET}/${GCS_PREFIX}gnomad/missense_constraint"

TABLES="gnomad_mpc:gnomad_v4.1.1_mpc gnomad_missense_constraint_regions:gnomad_v4.1.1_missense_constraint_regions"

ts "Loading gnomAD missense constraint into ${PROJECT_ID}.${DATASET_ID}"

# every file is checked before any table is truncated
for entry in ${TABLES}; do
  uri="${GCS_DIR}/${entry#*:}.munged.tsv.gz"
  if ! gsutil -q stat "${uri}" 2>/dev/null; then
    ts "ERROR: ${uri} not found"
    exit 1
  fi
done

for entry in ${TABLES}; do
  table="${entry%%:*}"
  uri="${GCS_DIR}/${entry#*:}.munged.tsv.gz"
  for name in "${table}" "${table}_v"; do
    sed "s/genetics_results/${PROJECT_ID}.${DATASET_ID}/g" "${SCRIPT_DIR}/../schemas/${name}.sql" \
      | bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --nouse_cache
  done
  ts "=== ${table}: loading ${uri} ==="
  "$PY" "${SCRIPT_DIR}/load_data.py" \
    --project "${PROJECT_ID}" \
    --dataset "${DATASET_ID}" \
    --table "${table}" \
    --gcs-uri "${uri}" \
    --write-disposition WRITE_TRUNCATE
done

echo ""
ts "=== Loading complete ==="
report_row_counts gnomad_mpc gnomad_missense_constraint_regions
