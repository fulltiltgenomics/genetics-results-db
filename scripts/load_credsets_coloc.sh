#!/bin/bash
# Load credible sets and colocalization data from GCS into BigQuery
#
# Loads within a table run in parallel. This is safe because concurrent
# BigQuery load jobs that WRITE_APPEND to the same table don't conflict
# (BigQuery serializes the final metadata commits). The only ordering that
# matters is that a table's wipe (DELETE for credible_sets, the first
# WRITE_TRUNCATE load for the coloc tables) must COMPLETE before that table's
# parallel appends start, otherwise a truncate could race and drop appended
# rows. The three tables are independent; credible_sets is loaded first, then
# colocalization and coloc_credsets are loaded concurrently.

set -euo pipefail

ts() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
GCS_BUCKET="${GCS_BUCKET:-bucket-name}"
GCS_PREFIX="${GCS_PREFIX:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ts "Loading data into ${PROJECT_ID}.${DATASET_ID}"

# Load a single GCS file into a table with the given write disposition.
load_one() {
  local table="$1" gcs_uri="$2" disposition="$3"
  python3 "${SCRIPT_DIR}/load_data.py" \
    --project "${PROJECT_ID}" \
    --dataset "${DATASET_ID}" \
    --table "${table}" \
    --gcs-uri "${gcs_uri}" \
    --write-disposition "${disposition}"
}

# Verify every URI exists up front; returns non-zero if any is missing.
check_files() {
  local missing=0 gcs_uri
  for gcs_uri in "$@"; do
    if ! gsutil -q stat "${gcs_uri}" 2>/dev/null; then
      ts "ERROR: ${gcs_uri} not found"
      missing=1
    fi
  done
  return "${missing}"
}

# Append all given URIs into a table concurrently, then wait for all of them.
# Returns non-zero if any load failed.
parallel_append() {
  local table="$1"; shift
  local pids=() pid gcs_uri rc=0
  for gcs_uri in "$@"; do
    ts "Appending ${gcs_uri} -> ${table}..."
    load_one "${table}" "${gcs_uri}" "WRITE_APPEND" &
    pids+=("$!")
  done
  for pid in "${pids[@]}"; do
    wait "${pid}" || rc=1
  done
  return "${rc}"
}

# Load a coloc-style table whose whole contents this script owns: wipe it by
# truncate-loading the first file (awaited), then append the rest in parallel.
load_coloc_group() {
  local table="$1"; shift
  local first="$1"; shift
  ts "Truncate-loading ${first} -> ${table}..."
  load_one "${table}" "${first}" "WRITE_TRUNCATE"
  if [ "$#" -gt 0 ]; then
    parallel_append "${table}" "$@"
  fi
}

# credible sets files
CREDSET_FILES=(
  "gs://${GCS_BUCKET}/${GCS_PREFIX}credible_sets/finngen_core/r14/FinnGen_R14_credible_sets.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}credible_sets/finngen_kanta/r14/FinnGen_R14kanta_credible_sets.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}credible_sets/finngen_drugs/r12_20251024/FinnGen_R12drugs_credible_sets.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}credible_sets/finngen_olink/20251024/FinnGen_Olink_1-4_credible_sets.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}credible_sets/finngen_olink_5k/FinnGen_Olink_5K_credible_sets.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}credible_sets/ukb_ppp/20251024/UKB_PPP_credible_sets.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}credible_sets/finngen_snrnaseq/20251024/FinnGen_snRNAseq_202509_credible_sets.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}credible_sets/finngen_atacseq/20251118/FinnGen_ATACseq_202509_credible_sets.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}credible_sets/eqtl_catalogue/r8/eQTL_Catalogue_R8.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}credible_sets/open_targets/202512/Open_Targets_25.12_credible_sets.tsv.gz"
)

# colocalization files
COLOC_FILES=(
  "gs://${GCS_BUCKET}/${GCS_PREFIX}coloc/finngen_r14_colocQC.munged.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}coloc/FinnGen-R12.eQTL.colocQC.munged.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}coloc/FinnGen-KANTA.eQTL.colocQC.munged.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}coloc/FinnGen-R12.caQTL.colocQC.munged.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}coloc/FinnGen-KANTA.caQTL.colocQC.munged.tsv.gz"
)

# coloc credsets files
COLOC_CREDSET_FILES=(
  "gs://${GCS_BUCKET}/${GCS_PREFIX}coloc/finngen_r14_coloc.credsets.munged.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}coloc/FinnGen-R12.eQTL.coloc.credsets.munged.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}coloc/FinnGen-KANTA.eQTL.coloc.credsets.munged.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}coloc/FinnGen-R12.caQTL.coloc.credsets.munged.tsv.gz"
  "gs://${GCS_BUCKET}/${GCS_PREFIX}coloc/FinnGen-KANTA.caQTL.coloc.credsets.munged.tsv.gz"
)

echo ""
ts "=== Verifying all source files exist ==="
check_files "${CREDSET_FILES[@]}" "${COLOC_FILES[@]}" "${COLOC_CREDSET_FILES[@]}"
ts "All files present"

echo ""
ts "=== Deleting existing credible_sets rows owned by this script ==="
# Surgical DELETE keeps rows loaded by other scripts (e.g. load_pseudo.sh's
# pseudo CS datasets). When adding a dataset to CREDSET_FILES above, add the
# matching dataset value here too.
bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false \
  "DELETE FROM \`${PROJECT_ID}.${DATASET_ID}.credible_sets\`
   WHERE dataset IN ('FinnGen_R14', 'FinnGen_kanta', 'FinnGen_drugs',
                     'FinnGen_Olink', 'FinnGen_Olink_5K', 'UKB_PPP',
                     'FinnGen_snRNAseq', 'FinnGen_ATACseq', 'Open_Targets_25.12')
      OR dataset LIKE 'QTD%'"
ts "Done"

echo ""
ts "=== Loading credible sets (parallel) ==="
parallel_append credible_sets "${CREDSET_FILES[@]}"
ts "Credible sets done"

echo ""
ts "=== Loading colocalization + coloc credsets (parallel) ==="
# Independent tables: run both groups concurrently. Each group truncates first
# (awaited inside the group) before its parallel appends, so no truncate race.
load_coloc_group colocalization "${COLOC_FILES[@]}" &
coloc_pid=$!
load_coloc_group coloc_credsets "${COLOC_CREDSET_FILES[@]}" &
coloc_credset_pid=$!

coloc_rc=0
wait "${coloc_pid}" || coloc_rc=1
wait "${coloc_credset_pid}" || coloc_rc=1
if [ "${coloc_rc}" -ne 0 ]; then
  ts "ERROR: one or more coloc loads failed"
  exit 1
fi
ts "Colocalization + coloc credsets done"

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
