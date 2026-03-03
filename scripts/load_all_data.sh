#!/bin/bash
# Load all genetics results data from GCS into BigQuery

set -euo pipefail

ts() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
GCS_BUCKET="${GCS_BUCKET:-bucket-name}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ts "Loading data into ${PROJECT_ID}.${DATASET_ID}"

# credible sets files
CREDSET_FILES=(
  "gs://${GCS_BUCKET}/results_api_data/credible_sets/finngen_core/r13_20251024/FinnGen_R13_credible_sets.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/credible_sets/finngen_kanta/r12_20251024/FinnGen_R12kanta_credible_sets.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/credible_sets/finngen_drugs/r12_20251024/FinnGen_R12drugs_credible_sets.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/credible_sets/finngen_olink/20251024/FinnGen_Olink_1-4_credible_sets.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/credible_sets/ukb_ppp/20251024/UKB_PPP_credible_sets.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/credible_sets/finngen_snrnaseq/20251024/FinnGen_snRNAseq_202509_credible_sets.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/credible_sets/finngen_atacseq/20251118/FinnGen_ATACseq_202509_credible_sets.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/credible_sets/eqtl_catalogue/r7/eQTL_Catalogue_R7.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/credible_sets/open_targets/202512/Open_Targets_25.12_credible_sets.tsv.gz"
)

echo ""
ts "=== Loading credible sets ==="
first_credset=true
for gcs_uri in "${CREDSET_FILES[@]}"; do
  if gsutil -q stat "${gcs_uri}" 2>/dev/null; then
    ts "Loading ${gcs_uri}..."
    # truncate on first file, append on subsequent
    if [ "$first_credset" = true ]; then
      disposition="WRITE_TRUNCATE"
      first_credset=false
    else
      disposition="WRITE_APPEND"
    fi
    python3 "${SCRIPT_DIR}/load_data.py" \
      --project "${PROJECT_ID}" \
      --dataset "${DATASET_ID}" \
      --table credible_sets \
      --gcs-uri "${gcs_uri}" \
      --write-disposition "${disposition}"
  else
    ts "Skipping ${gcs_uri} (not found)"
  fi
done

# colocalization files
COLOC_FILES=(
  "gs://${GCS_BUCKET}/results_api_data/coloc/colocQC.munged.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/coloc/FinnGen-R12.eQTL.colocQC.munged.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/coloc/FinnGen-KANTA.eQTL.colocQC.munged.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/coloc/FinnGen-R12.caQTL.colocQC.munged.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/coloc/FinnGen-KANTA.caQTL.colocQC.munged.tsv.gz"
)

echo ""
ts "=== Loading colocalization results ==="
first_coloc=true
for gcs_uri in "${COLOC_FILES[@]}"; do
  if gsutil -q stat "${gcs_uri}" 2>/dev/null; then
    ts "Loading ${gcs_uri}..."
    if [ "$first_coloc" = true ]; then
      disposition="WRITE_TRUNCATE"
      first_coloc=false
    else
      disposition="WRITE_APPEND"
    fi
    python3 "${SCRIPT_DIR}/load_data.py" \
      --project "${PROJECT_ID}" \
      --dataset "${DATASET_ID}" \
      --table colocalization \
      --gcs-uri "${gcs_uri}" \
      --write-disposition "${disposition}"
  else
    ts "Skipping ${gcs_uri} (not found)"
  fi
done

# coloc credsets files
COLOC_CREDSET_FILES=(
  "gs://${GCS_BUCKET}/results_api_data/coloc/coloc.credsets.munged.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/coloc/FinnGen-R12.eQTL.coloc.credsets.munged.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/coloc/FinnGen-KANTA.eQTL.coloc.credsets.munged.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/coloc/FinnGen-R12.caQTL.coloc.credsets.munged.tsv.gz"
  "gs://${GCS_BUCKET}/results_api_data/coloc/FinnGen-KANTA.caQTL.coloc.credsets.munged.tsv.gz"
)

echo ""
ts "=== Loading coloc credsets ==="
first_coloc_credset=true
for gcs_uri in "${COLOC_CREDSET_FILES[@]}"; do
  if gsutil -q stat "${gcs_uri}" 2>/dev/null; then
    ts "Loading ${gcs_uri}..."
    if [ "$first_coloc_credset" = true ]; then
      disposition="WRITE_TRUNCATE"
      first_coloc_credset=false
    else
      disposition="WRITE_APPEND"
    fi
    python3 "${SCRIPT_DIR}/load_data.py" \
      --project "${PROJECT_ID}" \
      --dataset "${DATASET_ID}" \
      --table coloc_credsets \
      --gcs-uri "${gcs_uri}" \
      --write-disposition "${disposition}"
  else
    ts "Skipping ${gcs_uri} (not found)"
  fi
done

echo ""
ts "=== Data loading complete ==="

# show table row counts
echo ""
ts "Table row counts:"
for table in credible_sets colocalization coloc_credsets; do
  count=$(bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --format=csv \
    "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET_ID}.${table}\`" 2>/dev/null | tail -1) || count="error"
  ts "  ${table}: ${count} rows"
done
