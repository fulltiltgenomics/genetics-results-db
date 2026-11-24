#!/usr/bin/env sh
# Warns when a commit changes something the docs describe but leaves the doc
# untouched. Mappings mirror the "Documentation ownership" table in CLAUDE.md.
#
# The rules are this repo's own; `check` and the rest of the mechanism live in
# scripts/doc-drift-lib.sh, which is the same file in every repo of the suite.

. "$(dirname "$0")/doc-drift-lib.sh"

DOCS_SPEC='^(docs/project-spec\.md|README\.md)$'

check '^schemas/' '^docs/project-spec\.md$' \
    'schemas/ -> docs/project-spec.md (data model column tables, partition/cluster clauses, view columns)'

check '^api/' "$DOCS_SPEC" \
    'api/ -> docs/project-spec.md + README.md (endpoint table, query params, env vars, auth)'

check '^scripts/(load_[a-z_]*\.sh$|setup_bigquery\.sh$|lib/)' "$DOCS_SPEC" \
    'loader/setup scripts + scripts/lib -> README.md + docs/project-spec.md (loader list, setup steps, GCS defaults, shared prefix rule)'

check '^configs/datasets\.yaml$' '^docs/project-spec\.md$' \
    'configs/datasets.yaml -> docs/project-spec.md (dataset/resource config; this copy is GENERATED — genetics-results-suite is canonical, update its docs too)'

check '^(scripts/(lint-staged|install-git-hooks)\.sh|pyproject\.toml)$' "$DOCS_SPEC" \
    'lint gate (scripts/lint-staged.sh, install-git-hooks.sh, pyproject.toml) -> README.md + docs/project-spec.md (which commits it blocks, the ruff rule set and its per-file ignores, how ruff is resolved in a worktree)'

doc_drift_finish
