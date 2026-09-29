#!/bin/bash
# Load the Autism Sequencing Consortium 2026 exome release (dataset ASC2) into its three
# count-based tables: exome_gene_counts, exome_gene_bayes_results, exome_variant_counts.
# The munge (genetics-results-munge/scripts/munge_asc.py) stages one bgzipped TSV per table:
#   gs://<bucket>/<prefix>exome_results/asc/ASC2_gene_counts.munged.tsv.gz
#   gs://<bucket>/<prefix>exome_results/asc/ASC2_gene_bayes_results.munged.tsv.gz
#   gs://<bucket>/<prefix>exome_results/asc/ASC2_variant_counts.munged.tsv.gz
#
# Deletes its own rows (dataset = 'ASC2') from each table and appends, so a rerun is
# idempotent and the tables stay shared for the next count-based release. The DELETE runs
# before each load, so a failed load leaves that table without ASC2 rows; rerunning is the
# recovery.
#
# The three views are created by setup_bigquery.sh, not here: none of them joins another
# table, so there is no dependency to sequence. The tables must be loaded BEFORE
# load_phenotypes.sh runs, or the registry cross-check fails for the profile on the
# asc_gene_based / asc_exome entries' `dataset` value.
#
# Run once per profile by setting the env vars, e.g.:
#   finngen:   PROJECT_ID=<finngen-project> GCS_BUCKET=finngen-commons \
#              GCS_PREFIX=results_api_data/ scripts/load_asc.sh
#   rehearsal: the same with DATASET_ID=<clone built by genetics-results-suite/scripts/bq-dev-dataset.sh>

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/lib/common.sh"

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
GCS_BUCKET="${GCS_BUCKET:-bucket-name}"
GCS_PREFIX="$(resolve_gcs_prefix unset-or-empty "")"

ASC_DATASET="ASC2"
ASC_DIR="gs://${GCS_BUCKET}/${GCS_PREFIX}exome_results/asc"

# table -> staged file
TABLES=(
  "exome_gene_counts:${ASC_DIR}/ASC2_gene_counts.munged.tsv.gz"
  "exome_gene_bayes_results:${ASC_DIR}/ASC2_gene_bayes_results.munged.tsv.gz"
  "exome_variant_counts:${ASC_DIR}/ASC2_variant_counts.munged.tsv.gz"
)

ts "Loading ${ASC_DATASET} into ${PROJECT_ID}.${DATASET_ID}"

for entry in "${TABLES[@]}"; do
  table="${entry%%:*}"
  gcs_uri="${entry#*:}"
  if ! gsutil -q stat "${gcs_uri}" 2>/dev/null; then
    ts "ERROR: ${gcs_uri} not found"
    exit 1
  fi
done

for entry in "${TABLES[@]}"; do
  table="${entry%%:*}"
  gcs_uri="${entry#*:}"
  echo ""
  ts "=== ${table}: deleting existing ${ASC_DATASET} rows ==="
  bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false \
    "DELETE FROM \`${PROJECT_ID}.${DATASET_ID}.${table}\` WHERE dataset = '${ASC_DATASET}'"
  ts "=== ${table}: loading ${gcs_uri} ==="
  "$PY" "${SCRIPT_DIR}/load_data.py" \
    --project "${PROJECT_ID}" \
    --dataset "${DATASET_ID}" \
    --table "${table}" \
    --gcs-uri "${gcs_uri}" \
    --write-disposition WRITE_APPEND
done

echo ""
ts "=== Loading complete ==="
report_row_counts exome_gene_counts exome_gene_bayes_results exome_variant_counts
