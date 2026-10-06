#!/bin/bash
# Load the served gnomAD sites file (bgzip + tabix TSV, one row per variant) into
# gnomad_variant_annotation, one chromosome at a time.
#
# Two stages, either of which can be run alone:
#
#   shard : tabix one chromosome out of the LOCAL file, cut it into gzip shards and
#           upload them under SHARD_URI, followed by a row-count manifest. Runs when
#           GNOMAD_FILE is set.
#   load  : load every chromosome that has a manifest under SHARD_URI. Runs unless
#           SKIP_LOAD=1. With GNOMAD_FILE unset this is the whole run, which is how
#           shards copied to another bucket are loaded into another project.
#
# BigQuery cannot split a gzip file and refuses a compressed CSV above 4 GB
# (https://cloud.google.com/bigquery/quotas#load_jobs), so the file cannot be loaded
# whole. Shards are cut by UNCOMPRESSED size, which bounds the compressed size without
# measuring it; they are also what lets one load job read a chromosome in parallel.
#
# A chromosome is replaced, never appended to: load_data.py --replace-chr deletes its
# rows before inserting, and --expect-rows refuses a staged row count that differs from
# the manifest before the table is touched. A rerun therefore cannot duplicate rows and
# a partly uploaded chromosome cannot be loaded short.
#
# Environment:
#   SHARD_URI     gs:// prefix holding the shards (required)
#   GNOMAD_FILE   local served file with its .tbi beside it; unset = shards already exist
#   WORK_DIR      scratch directory for one chromosome's shards (required with
#                 GNOMAD_FILE; no default, because the usual default is a small root disk)
#   PROJECT_ID    GCP project (default: gcloud config)
#   DATASET_ID    BigQuery dataset (default: genetics_results)
#   CHROMS        space-separated chromosome codes to restrict the run to (default: all)
#   SHARD_BYTES   uncompressed bytes per shard, in split(1) syntax (default: 2G)
#   THREADS       bgzip compression threads (default: 2)
#   SKIP_LOAD=1   shard and upload only
#   DRY_RUN=1     print what would run; reads the tabix index and the manifests only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/lib/common.sh"

TABLE="gnomad_variant_annotation"
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
DATASET_ID="${DATASET_ID:-genetics_results}"
SHARD_URI="${SHARD_URI:?set SHARD_URI to the gs:// prefix that holds (or will hold) the shards}"
SHARD_URI="${SHARD_URI%/}"
GNOMAD_FILE="${GNOMAD_FILE:-}"
CHROMS="${CHROMS:-}"
SHARD_BYTES="${SHARD_BYTES:-2G}"
THREADS="${THREADS:-2}"
DRY_RUN="${DRY_RUN:-0}"
SKIP_LOAD="${SKIP_LOAD:-0}"

run() {
  if [ "${DRY_RUN}" = 1 ]; then
    echo "+ $*"
  else
    "$@"
  fi
}

wanted() {
  [ -z "${CHROMS}" ] && return 0
  case " ${CHROMS} " in
    *" $1 "*) return 0 ;;
    *) return 1 ;;
  esac
}

shard_chromosome() {
  local chr="$1" dir="${WORK_DIR}/chr${1}" rows
  if [ "${DRY_RUN}" = 1 ]; then
    echo "+ tabix ${GNOMAD_FILE} ${chr} | split -C ${SHARD_BYTES} | bgzip -> ${SHARD_URI}/chr${chr}.part-*.tsv.gz + chr${chr}.rows"
    return
  fi

  # the manifest goes first and comes back last: its presence is what tells the load
  # stage that every shard of this chromosome is in place
  gcloud storage rm "${SHARD_URI}/chr${chr}.rows" 2>/dev/null || true
  gcloud storage rm "${SHARD_URI}/chr${chr}.part-*" 2>/dev/null || true

  rm -rf "${dir}"
  mkdir -p "${dir}"
  # tabix prints no header for a region, so the shards carry data rows only. The copy on
  # fd 3 counts the rows in the same pass, under the same pipefail as the shard writer.
  rows=$(
    {
      tabix "${GNOMAD_FILE}" "${chr}" \
        | tee /dev/fd/3 \
        | split -C "${SHARD_BYTES}" -a 4 \
            --filter="bgzip -@ ${THREADS} -c > \"\$FILE.tsv.gz\"" \
            - "${dir}/chr${chr}.part-" >&2
    } 3>&1 | wc -l
  )
  if [ "${rows}" -eq 0 ]; then
    ts "ERROR: no rows for chromosome ${chr} in ${GNOMAD_FILE}"
    exit 1
  fi
  echo "${rows}" > "${dir}/chr${chr}.rows"

  gcloud storage cp "${dir}"/chr"${chr}".part-*.tsv.gz "${SHARD_URI}/"
  gcloud storage cp "${dir}/chr${chr}.rows" "${SHARD_URI}/"
  rm -rf "${dir}"
  ts "  chr ${chr}: ${rows} rows sharded"
}

load_chromosome() {
  local chr="$1" rows
  rows=$(gcloud storage cat "${SHARD_URI}/chr${chr}.rows")
  ts "=== chr ${chr}: loading ${rows} rows ==="
  run "$PY" "${SCRIPT_DIR}/load_data.py" \
    --project "${PROJECT_ID}" \
    --dataset "${DATASET_ID}" \
    --table "${TABLE}" \
    --gcs-uri "${SHARD_URI}/chr${chr}.part-*.tsv.gz" \
    --skip-rows 0 \
    --replace-chr "${chr}" \
    --expect-rows "${rows}"
}

create_table_and_view() {
  local name
  for name in "${TABLE}" "${TABLE}_v"; do
    if [ "${DRY_RUN}" = 1 ]; then
      echo "+ bq query < schemas/${name}.sql (as ${PROJECT_ID}.${DATASET_ID})"
      continue
    fi
    # same substitution setup_bigquery.sh applies; done here so this one table can be
    # added to a dataset without running every schema file against it
    sed "s/genetics_results/${PROJECT_ID}.${DATASET_ID}/g" "${SCRIPT_DIR}/../schemas/${name}.sql" \
      | bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --nouse_cache
  done
}

ts "gnomAD variant annotation -> ${PROJECT_ID}.${DATASET_ID}.${TABLE} via ${SHARD_URI}"

if [ -n "${GNOMAD_FILE}" ]; then
  WORK_DIR="${WORK_DIR:?set WORK_DIR to a scratch directory on a disk with room for one chromosome}"
  if [ ! -f "${GNOMAD_FILE}" ] || [ ! -f "${GNOMAD_FILE}.tbi" ]; then
    ts "ERROR: ${GNOMAD_FILE} or its .tbi not found"
    exit 1
  fi
  ts "=== Sharding ${GNOMAD_FILE} ==="
  for chr in $(tabix -l "${GNOMAD_FILE}"); do
    wanted "${chr}" || continue
    shard_chromosome "${chr}"
  done
fi

if [ "${SKIP_LOAD}" = 1 ]; then
  ts "SKIP_LOAD=1: not loading"
  exit 0
fi

manifests=$(gcloud storage ls "${SHARD_URI}/chr*.rows" 2>/dev/null || true)
if [ -z "${manifests}" ] && [ "${DRY_RUN}" != 1 ]; then
  ts "ERROR: no chr*.rows manifest under ${SHARD_URI}"
  exit 1
fi

create_table_and_view

loaded=0
for manifest in ${manifests}; do
  chr="${manifest##*/chr}"
  chr="${chr%.rows}"
  wanted "${chr}" || continue
  load_chromosome "${chr}"
  loaded=$((loaded + 1))
done
if [ "${loaded}" -eq 0 ] && [ "${DRY_RUN}" != 1 ]; then
  ts "ERROR: no manifest under ${SHARD_URI} matches CHROMS='${CHROMS}'"
  exit 1
fi

if [ "${DRY_RUN}" != 1 ]; then
  echo ""
  ts "Table row count:"
  report_row_counts "${TABLE}"
fi
