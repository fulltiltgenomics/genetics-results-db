#!/usr/bin/env sh
# Warns when a commit changes something the docs describe but leaves the doc
# untouched. Mappings mirror the "Documentation ownership" table in CLAUDE.md.
#
# This never blocks. A warning that is occasionally ignored beats a gate that
# gets bypassed with --no-verify, because a bypassed gate is both absent and
# assumed present.

set -u

root=$(git rev-parse --show-toplevel 2>/dev/null) || exit 0
cd "$root" || exit 0

staged=$(git diff --cached --name-only --diff-filter=ACMRD)
[ -n "$staged" ] || exit 0

hit() {
    printf '%s\n' "$staged" | grep -qE "$1"
}

found=0
check() {
    if hit "$1" && ! hit "$2"; then
        if [ "$found" -eq 0 ]; then
            printf '\ndoc-drift warning — this commit changes code the docs describe:\n\n' >&2
            found=1
        fi
        printf '  %s\n' "$3" >&2
    fi
}

DOCS_SPEC='^(docs/project-spec\.md|README\.md)$'

check '^schemas/' '^docs/project-spec\.md$' \
    'schemas/ -> docs/project-spec.md (data model column tables, partition/cluster clauses, view columns)'

check '^api/' "$DOCS_SPEC" \
    'api/ -> docs/project-spec.md + README.md (endpoint table, query params, env vars, auth)'

check '^scripts/(load_[a-z_]*|setup_bigquery)\.sh$' "$DOCS_SPEC" \
    'loader/setup scripts -> README.md + docs/project-spec.md (loader list, setup steps, GCS defaults)'

check '^configs/datasets\.yaml$' '^docs/project-spec\.md$' \
    'configs/datasets.yaml -> docs/project-spec.md (dataset/resource config; this copy is GENERATED — genetics-results-suite is canonical, update its docs too)'

if [ "$found" -eq 1 ]; then
    printf '\n  Update the doc in this commit, or note why it does not apply.\n' >&2
    printf '  Not blocking. Mappings live in CLAUDE.md > Documentation ownership.\n\n' >&2
fi

exit 0
