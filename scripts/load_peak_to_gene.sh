#!/bin/bash
# Load Open4Gene peak-to-gene links from GCS into BigQuery (truncates peak_to_gene).
#
# Source is the same canonical TSV the tabix API serves at /peak_to_genes and
# /gene_to_peaks, staged at gs://<bucket>/<prefix>atacseq/open4gene.all.results.sig.tsv.gz.
#
# The file has no `dataset` column, so it is injected at load time via --const-column
# dataset=FinnGen_ATACseq — the same value credible_sets uses for this study, so the two
# tables join directly. load_data.py also converts the "chr1".."chrX" chrom strings to
# INT64 `chr` (CHR_STRING_TABLES) and strips the "predicted.celltype." prefix from
# cell_type (CELL_TYPE_PREFIX_TABLES) so cell_type joins credible_sets.cell_type.
#
# Run once per profile by setting the env vars, e.g.:
#   finngen: PROJECT_ID=<finngen-project> GCS_BUCKET=finngen-commons \
#            GCS_PREFIX=results_api_data/ scripts/load_peak_to_gene.sh
#   daly:    PROJECT_ID=<daly-project> GCS_BUCKET=daly-genetics-results \
#            GCS_PREFIX= scripts/load_peak_to_gene.sh

set -euo pipefail

ts() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
GCS_BUCKET="${GCS_BUCKET:-finngen-commons}"
GCS_PREFIX="${GCS_PREFIX:-results_api_data/}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ts "Loading peak-to-gene links into ${PROJECT_ID}.${DATASET_ID}"

dataset_id="FinnGen_ATACseq"
gcs_uri="gs://${GCS_BUCKET}/${GCS_PREFIX}atacseq/open4gene.all.results.sig.tsv.gz"

if ! gsutil -q stat "${gcs_uri}" 2>/dev/null; then
  ts "ERROR: ${gcs_uri} not found"
  exit 1
fi

echo ""
ts "=== Loading peak-to-gene links ==="
ts "Loading ${gcs_uri} (dataset=${dataset_id})..."
python3 "${SCRIPT_DIR}/load_data.py" \
  --project "${PROJECT_ID}" \
  --dataset "${DATASET_ID}" \
  --table peak_to_gene \
  --gcs-uri "${gcs_uri}" \
  --write-disposition WRITE_TRUNCATE \
  --const-column "dataset=${dataset_id}"

echo ""
ts "=== Peak-to-gene loading complete ==="

echo ""
ts "Table row counts:"
count=$(bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --format=csv \
  "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET_ID}.peak_to_gene\`" 2>/dev/null | tail -1) || count="error"
ts "  peak_to_gene: ${count} rows"
