#!/bin/bash
# Build and load the phenotypes + datasets metadata tables (WRITE_TRUNCATE full rebuild).
#
# Both tables are derived entirely from datasets.yaml and the per-dataset metadata_file
# JSON/TSVs it points at, so a full rebuild is always cheap (~33k + ~900 rows) and there is
# no incremental path: re-run this whenever datasets.yaml or a metadata file changes.
#
# datasets.yaml is the SYNCED copy in this repo's configs/ (see CLAUDE.md - the canonical
# file lives in genetics-results-suite). The metadata_file URIs it carries are already
# profile-specific, so PROFILE selects both the dataset registry and the GCS bucket the
# metadata is read from; there is no GCS_BUCKET/GCS_PREFIX here for that reason.
#
# The generated NDJSON is staged on GCS because BigQuery load jobs read from GCS, so
# GCS_BUCKET/GCS_PREFIX still control WHERE the staging files are written.
#
# Run once per profile, e.g.:
#   finngen: PROJECT_ID=<finngen-project> PROFILE=finngen GCS_BUCKET=finngen-commons \
#            GCS_PREFIX=results_api_data/mapping_files/ scripts/load_phenotypes.sh
#   daly:    PROJECT_ID=<daly-project> PROFILE=daly GCS_BUCKET=daly-genetics-results \
#            GCS_PREFIX=mapping_files/ scripts/load_phenotypes.sh

set -euo pipefail

ts() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
PROFILE="${PROFILE:-finngen}"
GCS_BUCKET="${GCS_BUCKET:-finngen-commons}"
# no colon: only an UNSET prefix takes the default, so an explicitly empty GCS_PREFIX=""
# (a bucket-root layout) is honored
GCS_PREFIX="${GCS_PREFIX-results_api_data/mapping_files/}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATASETS_YAML="${DATASETS_YAML:-${SCRIPT_DIR}/../configs/datasets.yaml}"

PHENOTYPES_URI="${PHENOTYPES_URI:-gs://${GCS_BUCKET}/${GCS_PREFIX}phenotypes.ndjson}"
DATASETS_URI="${DATASETS_URI:-gs://${GCS_BUCKET}/${GCS_PREFIX}datasets.ndjson}"

if [ ! -f "${DATASETS_YAML}" ]; then
  ts "ERROR: ${DATASETS_YAML} not found - run ../genetics-results-suite/scripts/sync-datasets.sh"
  exit 1
fi

ts "Loading phenotype/dataset metadata into ${PROJECT_ID}.${DATASET_ID} (profile=${PROFILE})"

# the registry key -> results-view `dataset` mapping lives in build_phenotypes.py and cannot
# be derived from config, so cross-check it against what the results tables actually contain;
# the builder FAILS on any mismatch it has not been told to expect
ts "Collecting live results-view dataset names for cross-check..."
live_datasets=$(bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --format=csv \
  --max_rows=100000 "
  SELECT STRING_AGG(DISTINCT d, ',') FROM (
    SELECT dataset AS d FROM \`${PROJECT_ID}.${DATASET_ID}.credible_sets\` GROUP BY d
    UNION ALL SELECT dataset1 FROM \`${PROJECT_ID}.${DATASET_ID}.colocalization\` GROUP BY 1
    UNION ALL SELECT dataset2 FROM \`${PROJECT_ID}.${DATASET_ID}.colocalization\` GROUP BY 1
    UNION ALL SELECT dataset FROM \`${PROJECT_ID}.${DATASET_ID}.coloc_credsets\` GROUP BY 1
    UNION ALL SELECT dataset FROM \`${PROJECT_ID}.${DATASET_ID}.exome_variant_results\` GROUP BY 1
    UNION ALL SELECT dataset FROM \`${PROJECT_ID}.${DATASET_ID}.gene_burden_results\` GROUP BY 1
    UNION ALL SELECT dataset FROM \`${PROJECT_ID}.${DATASET_ID}.asm_qtl\` GROUP BY 1
    UNION ALL SELECT dataset FROM \`${PROJECT_ID}.${DATASET_ID}.open_chromatin\` GROUP BY 1
    UNION ALL SELECT dataset FROM \`${PROJECT_ID}.${DATASET_ID}.variant_effect\` GROUP BY 1
    UNION ALL SELECT dataset FROM \`${PROJECT_ID}.${DATASET_ID}.mpra\` GROUP BY 1
  )" 2>/dev/null | tail -1 | tr -d '"') || live_datasets=""
# peak_to_gene and variant_annotation are deliberately absent: neither carries a meaningful
# `dataset` column (they are coordinate/annotation reference tables), so unioning them would
# only add non-registry noise to the cross-check.
# STRING_AGG produces one comma-bearing CSV field, which bq quotes; the quotes are stripped
# above so the first and last names are not mangled into "X and Y"

# an empty result means the cross-check could not run - bad auth, quota, a renamed table.
# build_phenotypes.py treats an empty --validate-against as "nothing to check" and would
# load happily, i.e. the one guard over a mapping that exists in no config would switch
# itself off exactly when the environment is broken. Refuse instead.
if [ -z "${live_datasets}" ]; then
  ts "ERROR: could not read live results-view dataset names - the registry cross-check cannot run."
  ts "       Check credentials, quota and that the results tables exist in ${PROJECT_ID}.${DATASET_ID}."
  if [ "${ALLOW_UNVALIDATED:-0}" != "1" ]; then
    ts "       Refusing to load unvalidated. Set ALLOW_UNVALIDATED=1 to override."
    exit 1
  fi
  ts "       ALLOW_UNVALIDATED=1 set - proceeding WITHOUT the registry cross-check."
fi

ts "Building NDJSON -> ${PHENOTYPES_URI}, ${DATASETS_URI}"
build_args=()
if [ "${ALLOW_UNVALIDATED:-0}" = "1" ]; then
  build_args+=(--allow-unvalidated)
fi
python3 "${SCRIPT_DIR}/build_phenotypes.py" \
  --datasets-yaml "${DATASETS_YAML}" \
  --profile "${PROFILE}" \
  --phenotypes-out "${PHENOTYPES_URI}" \
  --datasets-out "${DATASETS_URI}" \
  --validate-against "${live_datasets}" \
  "${build_args[@]+"${build_args[@]}"}"

for table in phenotypes datasets; do
  case "${table}" in
    phenotypes) uri="${PHENOTYPES_URI}" ;;
    datasets) uri="${DATASETS_URI}" ;;
  esac
  echo ""
  ts "=== Loading ${table} ==="
  python3 "${SCRIPT_DIR}/load_data.py" \
    --project "${PROJECT_ID}" \
    --dataset "${DATASET_ID}" \
    --table "${table}" \
    --gcs-uri "${uri}" \
    --write-disposition WRITE_TRUNCATE
done

echo ""
ts "=== Metadata loading complete ==="

echo ""
ts "Table row counts:"
for table in phenotypes datasets; do
  count=$(bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --format=csv \
    "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET_ID}.${table}\`" 2>/dev/null | tail -1) || count="error"
  ts "  ${table}: ${count} rows"
done
