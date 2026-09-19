# shellcheck shell=bash
#
# Helpers shared by scripts/load_*.sh. Source it after resolving SCRIPT_DIR:
#
#   SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   . "${SCRIPT_DIR}/lib/common.sh"
#
# SCRIPT_DIR is deliberately not set here: inside a sourced file ${BASH_SOURCE[0]} names
# this file rather than the caller, and the caller needs the value before it can find us.

# Interpreter for the Python helpers under scripts/. Only the repo's .venv is guaranteed to
# carry google-cloud-bigquery, so a bare `python3` is whatever the machine happens to have.
PY="${PYTHON:-}"
if [ -z "${PY}" ]; then
  if [ -x "${SCRIPT_DIR}/../.venv/bin/python" ]; then
    PY="${SCRIPT_DIR}/../.venv/bin/python"
  else
    PY="python3"
  fi
fi

# Probed here, at source time, rather than left to the first python call: several loaders
# mutate the table before that point (load_brava_gene.sh DELETEs its dataset's rows,
# load_genebass_gene.sh TRUNCATEs, load_phenotypes.sh rebuilds), so an unusable interpreter
# discovered late destroys data and then fails.
if ! "${PY}" -c 'import google.cloud.bigquery' >/dev/null 2>&1; then
  echo "common.sh: ${PY} cannot import google.cloud.bigquery." >&2
  echo "  Run 'uv sync' in $(cd "${SCRIPT_DIR}/.." && pwd) to create the .venv," >&2
  echo "  or set PYTHON=/path/to/python to an interpreter that has it." >&2
  exit 1
fi

ts() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

# Resolve GCS_PREFIX against a per-script default, with the substitution rule named instead
# of spelled: `unset-only` is ${GCS_PREFIX-default}, `unset-or-empty` is ${GCS_PREFIX:-default}.
#
# The two differ only for an explicitly EMPTY prefix, and that case is real: the daly bucket
# keeps some products at the bucket root, so GCS_PREFIX="" has to survive rather than be
# replaced by the finngen default. Which rule a loader wants depends on where its own data
# sits in each deployment's bucket, so it is a per-script argument and there is no default.
#
# GCS_PREFIX is read from the environment rather than taken as an argument because
# unset-versus-empty is precisely the distinction an argument cannot carry.
resolve_gcs_prefix() {
  if [ "$#" -ne 2 ]; then
    echo "resolve_gcs_prefix: usage: resolve_gcs_prefix <unset-only|unset-or-empty> <default>" >&2
    exit 1
  fi
  case "$1" in
    unset-only) printf '%s' "${GCS_PREFIX-$2}" ;;
    unset-or-empty) printf '%s' "${GCS_PREFIX:-$2}" ;;
    *)
      echo "resolve_gcs_prefix: unknown rule '$1' (want unset-only or unset-or-empty)" >&2
      exit 1
      ;;
  esac
}

# Print "  <table>: <n> rows" for each named table, reading PROJECT_ID and DATASET_ID from
# the caller. Callers print their own heading line first, so the wording stays theirs.
#
# A failed count reports "error" rather than aborting: this runs after the load has already
# committed, and killing the script here would report a failure for work that succeeded.
# The assignment is separate from `local` on purpose — `local count=$(...)` would make the
# exit status that of `local`, which always succeeds, so the fallback could never fire.
report_row_counts() {
  local table count
  for table in "$@"; do
    count=$(bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --format=csv \
      "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET_ID}.${table}\`" 2>/dev/null | tail -1) || count="error"
    ts "  ${table}: ${count} rows"
  done
}
