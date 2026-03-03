#!/bin/bash
# Setup BigQuery dataset and tables for genetics fine-mapping, colocalization and exome results

set -euo pipefail

usage() {
  echo "Usage: $0 [--recreate]"
  echo ""
  echo "Options:"
  echo "  --recreate    Drop and recreate existing tables (WARNING: deletes all data)"
  echo ""
  echo "Environment variables:"
  echo "  PROJECT_ID    GCP project ID (default: gcloud config)"
  echo "  DATASET_ID    BigQuery dataset name (default: genetics_results)"
  echo "  LOCATION      Dataset location (default: europe-west1)"
  exit 1
}

RECREATE=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --recreate)
      RECREATE=true
      shift
      ;;
    -h|--help)
      usage
      ;;
    *)
      echo "Unknown option: $1"
      usage
      ;;
  esac
done

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
LOCATION="${LOCATION:-europe-west1}"

echo "Setting up BigQuery dataset in project ${PROJECT_ID} in location ${LOCATION}"
if [ "$RECREATE" = true ]; then
  echo "WARNING: --recreate flag set, existing tables will be dropped"
fi

# create dataset if it doesn't exist
if ! bq show --project_id="${PROJECT_ID}" "${DATASET_ID}" &>/dev/null; then
  echo "Creating dataset ${DATASET_ID}..."
  bq mk \
    --project_id="${PROJECT_ID}" \
    --dataset \
    --location="${LOCATION}" \
    --description="Genetics fine-mapping, colocalization and exome results" \
    "${DATASET_ID}"
else
  echo "Dataset ${DATASET_ID} already exists"
fi

# create tables from schema files (exclude view definitions)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEMA_DIR="${SCRIPT_DIR}/../schemas"

for schema_file in "${SCHEMA_DIR}"/*.sql; do
  # skip view definitions (handled in second pass)
  [[ "$(basename "${schema_file}")" == *_v.sql ]] && continue

  table_name=$(basename "${schema_file}" .sql)
  full_table="${PROJECT_ID}:${DATASET_ID}.${table_name}"

  # drop table first if --recreate flag is set
  if [ "$RECREATE" = true ]; then
    if bq show "${full_table}" &>/dev/null; then
      echo "Dropping table ${table_name}..."
      bq rm -f -t "${full_table}"
    fi
  fi

  echo "Creating table ${table_name}..."

  # replace placeholder with actual project.dataset, remove IF NOT EXISTS when recreating
  if [ "$RECREATE" = true ]; then
    sed -e "s/genetics_results/${PROJECT_ID}.${DATASET_ID}/g" \
        -e "s/CREATE TABLE IF NOT EXISTS/CREATE TABLE/g" \
        "${schema_file}" | \
      bq query \
        --project_id="${PROJECT_ID}" \
        --use_legacy_sql=false \
        --nouse_cache
  else
    sed "s/genetics_results/${PROJECT_ID}.${DATASET_ID}/g" "${schema_file}" | \
      bq query \
        --project_id="${PROJECT_ID}" \
        --use_legacy_sql=false \
        --nouse_cache
  fi
done

# create views (idempotent, always uses CREATE OR REPLACE)
for view_file in "${SCHEMA_DIR}"/*_v.sql; do
  [ -f "${view_file}" ] || continue
  view_name=$(basename "${view_file}" .sql)
  echo "Creating view ${view_name}..."
  sed "s/genetics_results/${PROJECT_ID}.${DATASET_ID}/g" "${view_file}" | \
    bq query \
      --project_id="${PROJECT_ID}" \
      --use_legacy_sql=false \
      --nouse_cache
done

echo ""
echo "BigQuery setup complete"
bq ls --project_id="${PROJECT_ID}" "${DATASET_ID}"
