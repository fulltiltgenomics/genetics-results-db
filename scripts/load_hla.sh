#!/bin/bash
# Load FinnGen classical HLA allele associations from GCS into BigQuery.
# The munge (genetics-results-munge/scripts/munge_hla.sh) stages TWO artifacts per
# bucket: the per-phenotype tabix files results-api reads, and ONE combined TSV
# holding every (phenotype, allele) row — that combined file is what loads here:
# gs://<bucket>/<prefix>hla/finngen_hla/finngen_hla.tsv.gz
#
# BigQuery is the only place the cross-phenotype question is answerable ("which
# traits is B*27:05 associated with?"): the per-phenotype files can only go the
# other way.
#
# Like load_mpra.sh, the combined file has NO `dataset` column, so it is injected at
# load time via --const-column dataset=finngen_hla. The numeric `chrom` string
# (always "6") is converted to INT64 `chr` by load_data.py (CHR_STRING_TABLES).
#
# Run once per profile by setting the env vars, e.g.:
#   finngen: PROJECT_ID=<finngen-project> GCS_BUCKET=finngen-commons \
#            GCS_PREFIX=results_api_data/ scripts/load_hla.sh
#   daly:    PROJECT_ID=<daly-project> GCS_BUCKET=daly-genetics-results \
#            GCS_PREFIX= scripts/load_hla.sh

set -euo pipefail

ts() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
GCS_BUCKET="${GCS_BUCKET:-bucket-name}"
GCS_PREFIX="${GCS_PREFIX:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ts "Loading HLA allele associations into ${PROJECT_ID}.${DATASET_ID}"

# Each entry: "<dataset-id>". A single FinnGen HLA dataset today; the array keeps the
# structure parallel to load_mpra.sh so a second release drops in without restructuring.
HLA_FILES=(
  "finngen_hla"
)

echo ""
ts "=== Loading HLA results ==="
first_hla=true
for dataset_id in "${HLA_FILES[@]}"; do
  gcs_uri="gs://${GCS_BUCKET}/${GCS_PREFIX}hla/${dataset_id}/${dataset_id}.tsv.gz"
  if ! gsutil -q stat "${gcs_uri}" 2>/dev/null; then
    ts "ERROR: ${gcs_uri} not found"
    exit 1
  fi
  ts "Loading ${gcs_uri} (dataset=${dataset_id})..."
  if [ "$first_hla" = true ]; then
    disposition="WRITE_TRUNCATE"
    first_hla=false
  else
    disposition="WRITE_APPEND"
  fi
  # dataset column is absent from the file — inject it here (see header note)
  python3 "${SCRIPT_DIR}/load_data.py" \
    --project "${PROJECT_ID}" \
    --dataset "${DATASET_ID}" \
    --table hla_associations \
    --gcs-uri "${gcs_uri}" \
    --write-disposition "${disposition}" \
    --const-column "dataset=${dataset_id}"
done

echo ""
ts "=== HLA data loading complete ==="

echo ""
ts "Table row counts:"
count=$(bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --format=csv \
  "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET_ID}.hla_associations\`" 2>/dev/null | tail -1) || count="error"
ts "  hla_associations: ${count} rows"
