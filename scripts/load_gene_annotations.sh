#!/bin/bash
# Build and load the gene_annotations reference table (WRITE_TRUNCATE full rebuild).
#
# Joins HGNC complete-set + GENCODE coordinates + HGNC gene-group hierarchy into a
# single NEWLINE_DELIMITED_JSON file (required for the gene_group_ids/names ARRAY
# columns) and loads it into BigQuery.
#
# NOTE: the three HGNC gene-group files (hgnc_gene_has_family.csv,
# hgnc_hierarchy_closure.csv, hgnc_family.csv -- HGNC publishes these as CSV)
# must already be uploaded to GCS.

set -euo pipefail

ts() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
GCS_BUCKET="${GCS_BUCKET:-finngen-commons}"
GCS_PREFIX="${GCS_PREFIX:-results_api_data/mapping_files/}"

GENCODE_VERSION="${GENCODE_VERSION:-49}"
HGNC_VERSION="${HGNC_VERSION:?set HGNC_VERSION (e.g. 2026-06-01)}"
# GCS path used to stage the generated NDJSON before the BigQuery load
STAGING_URI="${STAGING_URI:-gs://${GCS_BUCKET}/${GCS_PREFIX}gene_annotations.ndjson}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE="gs://${GCS_BUCKET}/${GCS_PREFIX}"

ts "Building gene_annotations NDJSON -> ${STAGING_URI}"
python3 "${SCRIPT_DIR}/build_gene_annotations.py" \
  --hgnc "${BASE}hgnc_complete_set.txt" \
  --gencode "${BASE}gencode.v${GENCODE_VERSION}.annotation.genes.tsv" \
  --gene-has-family "${BASE}hgnc_gene_has_family.csv" \
  --hierarchy-closure "${BASE}hgnc_hierarchy_closure.csv" \
  --family "${BASE}hgnc_family.csv" \
  --out "${STAGING_URI}" \
  --gencode-version "${GENCODE_VERSION}" \
  --hgnc-version "${HGNC_VERSION}"

ts "Loading ${STAGING_URI} into ${PROJECT_ID}.${DATASET_ID}.gene_annotations (WRITE_TRUNCATE)"
python3 "${SCRIPT_DIR}/load_data.py" \
  --project "${PROJECT_ID}" \
  --dataset "${DATASET_ID}" \
  --table gene_annotations \
  --gcs-uri "${STAGING_URI}" \
  --write-disposition WRITE_TRUNCATE

ts "=== gene_annotations load complete ==="
count=$(bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --format=csv \
  "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET_ID}.gene_annotations\`" 2>/dev/null | tail -1) || count="error"
ts "  gene_annotations: ${count} rows"
