#!/bin/bash
# Append BRaVa gene burden results to gene_burden_results.
#
# The unfiltered per-trait files munge_brava.py stages, one object per trait, matching the
# unfiltered genebass and _extra loads: BigQuery holds every gene x annotation x trait so a
# caller can ask for one gene in one trait regardless of significance.
#
# Unlike load_gene_burden_extra.sh this does NOT depend on load_genebass_gene.sh having
# truncated the table first: it deletes its own rows (dataset = 'BRaVa') and appends, so a
# rerun is idempotent and a dataset holding no Genebass rows — the rehearsal dataset — is a
# valid target.
#
# Object names carry the ancestry stratum as part of the phenocode ("AFib|EUR.tsv.gz"), so
# they contain '|'. The wildcard URI below does not: the '|' falls inside the span `*`
# matches, and BigQuery expands the wildcard against names Cloud Storage returns rather than
# re-parsing each as a URI. The trait-count check after the load establishes that every
# object contributed at least one distinct trait_original, not that any object was read
# completely; if it ever reports fewer traits than objects, replace the wildcard with a
# per-object loop over `gcloud storage ls`.
#
# The DELETE runs before the load, so a failed load leaves the table with no BRaVa rows and
# a failed trait-count assertion below leaves a partial set; rerunning is the recovery either
# way, since the DELETE at the top makes the script idempotent.
#
# Run once per profile by setting the env vars, e.g.:
#   daly:      PROJECT_ID=<daly-project> GCS_BUCKET=daly-genetics-results \
#              GCS_PREFIX= scripts/load_brava_gene.sh
#   rehearsal: PROJECT_ID=<daly-project> GCS_BUCKET=daly-genetics-results GCS_PREFIX= \
#              DATASET_ID=genetics_results_brava_dev scripts/load_brava_gene.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/lib/common.sh"

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
GCS_BUCKET="${GCS_BUCKET:-bucket-name}"
GCS_PREFIX="$(resolve_gcs_prefix unset-or-empty "")"

BRAVA_DATASET="BRaVa"
BRAVA_GENE_URI="gs://${GCS_BUCKET}/${GCS_PREFIX}exome_results/brava/gene_burden_per_trait/*.tsv.gz"

ts "Appending BRaVa gene burden results into ${PROJECT_ID}.${DATASET_ID}.gene_burden_results"

echo ""
ts "=== Verifying source objects exist ==="
# gsutil stat takes no wildcard, so match the way BigQuery will
if ! objects=$(gcloud storage ls "${BRAVA_GENE_URI}" 2>/dev/null) || [ -z "${objects}" ]; then
  ts "ERROR: ${BRAVA_GENE_URI} matches no objects"
  exit 1
fi
object_count=$(printf '%s\n' "${objects}" | wc -l | tr -d ' ')
ts "${object_count} per-trait objects"

echo ""
ts "=== Deleting existing gene_burden_results rows owned by this script ==="
# Surgical DELETE rather than a truncate: Genebass and the _extra datasets share this table.
bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false \
  "DELETE FROM \`${PROJECT_ID}.${DATASET_ID}.gene_burden_results\`
   WHERE dataset = '${BRAVA_DATASET}'"
ts "Done"

echo ""
ts "=== Appending BRaVa gene burden results ==="
ts "Loading ${BRAVA_GENE_URI}..."
python3 "${SCRIPT_DIR}/load_data.py" \
  --project "${PROJECT_ID}" \
  --dataset "${DATASET_ID}" \
  --table gene_burden_results \
  --gcs-uri "${BRAVA_GENE_URI}" \
  --write-disposition WRITE_APPEND

echo ""
ts "=== Verifying every per-trait object was read ==="
# one object per trait_original by construction (munge_brava.py names each file after the
# phenocode it holds), so a shortfall means the wildcard skipped objects
loaded_traits=$(bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --format=csv \
  "SELECT COUNT(DISTINCT trait_original) FROM \`${PROJECT_ID}.${DATASET_ID}.gene_burden_results\`
   WHERE dataset = '${BRAVA_DATASET}'" | tail -1)
loaded_rows=$(bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --format=csv \
  "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET_ID}.gene_burden_results\`
   WHERE dataset = '${BRAVA_DATASET}'" | tail -1)
ts "  ${loaded_traits} distinct traits, ${loaded_rows} rows loaded from ${object_count} objects"
if [ "${loaded_traits}" != "${object_count}" ]; then
  ts "ERROR: trait count does not match the object count — the wildcard did not read every"
  ts "       object (phenocodes with '|' are the suspects). Load them by literal URI instead."
  exit 1
fi

echo ""
ts "=== Loading complete ==="

report_row_counts gene_burden_results
