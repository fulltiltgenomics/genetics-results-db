#!/usr/bin/env python3
"""Generate CASE/WHEN SQL fragments from shared datasets.yaml mapping rules.

Usage:
  python scripts/generate_resource_sql.py generate credible_sets_v
  python scripts/generate_resource_sql.py generate colocalization_v
  python scripts/generate_resource_sql.py lint
"""

import argparse
import os
import re
import sys

import yaml

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
DEFAULT_YAML = os.path.join(REPO_ROOT, "..", "genetics-results-suite", "configs", "datasets.yaml")
SCHEMAS_DIR = os.path.join(REPO_ROOT, "schemas")

ALL_VIEWS = [
    "credible_sets_v",
    "colocalization_v",
    "coloc_credsets_v",
    "exome_variant_results_v",
    "gene_burden_results_v",
    "asm_qtl_v",
    "open_chromatin_v",
    "variant_effect_v",
    "mpra_v",
]

# colocalization_v maps dataset1->resource1 and dataset2->resource2
COLOC_PAIRS = [("dataset1", "resource1"), ("dataset2", "resource2")]


def load_rules(yaml_path):
    with open(yaml_path) as f:
        data = yaml.safe_load(f)
    return data["dataset_to_resource_rules"]


def rules_for_view(rules, view_name):
    """Filter rules to those applicable to a given view."""
    result = []
    for rule in rules:
        applies_to = rule.get("applies_to")
        if applies_to is None or view_name in applies_to:
            result.append(rule)
    return result


def is_like_pattern(pattern):
    """True if the pattern uses SQL LIKE wildcards.

    Only `%` is treated as a wildcard marker. Bare `_` is a literal here —
    using LIKE would turn it into a 1-char wildcard and broaden the match
    (e.g. `LIKE 'IBD_exome_2026'` would also match `IBDXexomeX2026`).
    Authors who need a 1-char wildcard should write `%` instead.
    """
    return "%" in pattern


def generate_case_fragment(rules, dataset_col="dataset", indent=2):
    """Build a CASE/WHEN SQL fragment from mapping rules.

    Returns a list of lines (without trailing newline).
    """
    prefix = " " * indent
    lines = [f"{prefix}CASE"]

    for rule in rules:
        pattern = rule["pattern"]
        resource = rule.get("resource")
        transform = rule.get("transform")

        if pattern == "*":
            # wildcard fallback
            if transform == "lowercase":
                lines.append(f"{prefix}  ELSE LOWER({dataset_col})")
            elif resource is not None:
                lines.append(f"{prefix}  ELSE '{resource}'")
            else:
                lines.append(f"{prefix}  ELSE LOWER({dataset_col})")
            continue

        # Match case-insensitively: dataset values can be loaded with different
        # casing than the rule pattern (e.g. deCODE% vs decode_*), and a
        # case-sensitive comparison would silently fall through to the lowercase
        # fallback, mislabeling the resource.
        if is_like_pattern(pattern):
            condition = f"LOWER({dataset_col}) LIKE '{pattern.lower()}'"
        else:
            condition = f"LOWER({dataset_col}) = '{pattern.lower()}'"

        if resource is not None:
            lines.append(f"{prefix}  WHEN {condition} THEN '{resource}'")
        elif transform == "lowercase":
            lines.append(f"{prefix}  WHEN {condition} THEN LOWER({dataset_col})")

    lines.append(f"{prefix}END")
    return lines


def generate_for_view(rules, view_name):
    """Return the CASE/WHEN fragment(s) for a view as a single string."""
    view_rules = rules_for_view(rules, view_name)
    if view_name == "colocalization_v":
        fragments = []
        for dataset_col, resource_col in COLOC_PAIRS:
            case_lines = generate_case_fragment(view_rules, dataset_col=dataset_col)
            case_lines[-1] += f" AS {resource_col}"
            fragments.append("\n".join(case_lines))
        return ",\n".join(fragments)
    else:
        case_lines = generate_case_fragment(view_rules, dataset_col="dataset")
        case_lines[-1] += " AS resource"
        return "\n".join(case_lines)


def extract_case_blocks(sql_text):
    """Extract all CASE...END AS <alias> blocks from SQL text.

    Returns a list of (alias, block_text) tuples where block_text is the
    CASE...END AS alias fragment with original indentation.
    """
    # match multi-line CASE blocks ending with END AS <identifier>
    pattern = re.compile(
        r"^([ \t]*CASE\b.*?END)\s+AS\s+(\w+)",
        re.MULTILINE | re.DOTALL,
    )
    results = []
    for m in pattern.finditer(sql_text):
        block = m.group(1).rstrip()
        alias = m.group(2)
        full = f"{block} AS {alias}"
        results.append((alias, full))
    return results


def lint_view(rules, view_name, schemas_dir):
    """Compare generated fragment against existing SQL file.

    Returns (ok, message).
    """
    sql_path = os.path.join(schemas_dir, f"{view_name}.sql")
    if not os.path.exists(sql_path):
        return False, f"  {view_name}: MISSING file {sql_path}"

    with open(sql_path) as f:
        sql_text = f.read()

    existing_blocks = extract_case_blocks(sql_text)
    generated = generate_for_view(rules, view_name)
    generated_blocks = extract_case_blocks(generated)

    if not existing_blocks:
        return False, f"  {view_name}: no CASE blocks found in SQL file"

    if len(existing_blocks) != len(generated_blocks):
        return False, (
            f"  {view_name}: block count mismatch — "
            f"file has {len(existing_blocks)}, generated {len(generated_blocks)}"
        )

    mismatches = []
    for (ex_alias, ex_block), (gen_alias, gen_block) in zip(existing_blocks, generated_blocks):
        if normalize(ex_block) != normalize(gen_block):
            mismatches.append(
                f"  {view_name} ({ex_alias}):\n"
                f"    existing:\n{indent_block(ex_block, 6)}\n"
                f"    generated:\n{indent_block(gen_block, 6)}"
            )

    if mismatches:
        return False, "\n".join(mismatches)
    return True, f"  {view_name}: OK"


def normalize(text):
    """Normalize whitespace for comparison."""
    return re.sub(r"\s+", " ", text.strip())


def indent_block(text, spaces):
    pad = " " * spaces
    return "\n".join(pad + line for line in text.splitlines())


def cmd_generate(args):
    rules = load_rules(args.yaml)
    print(generate_for_view(rules, args.view))


def cmd_lint(args):
    rules = load_rules(args.yaml)
    schemas = args.schemas_dir or SCHEMAS_DIR
    all_ok = True
    for view in ALL_VIEWS:
        ok, msg = lint_view(rules, view, schemas)
        print(msg)
        if not ok:
            all_ok = False
    if all_ok:
        print("\nAll views match.")
    else:
        print("\nMismatches found.", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Generate or lint CASE/WHEN SQL from datasets.yaml"
    )
    parser.add_argument(
        "--yaml", default=os.environ.get("DATASETS_YAML", DEFAULT_YAML),
        help="Path to datasets.yaml (default: sibling repo or DATASETS_YAML env var)",
    )
    parser.add_argument(
        "--schemas-dir", default=None,
        help="Path to schemas directory (default: <repo>/schemas/)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="Output CASE/WHEN fragment for a view")
    gen.add_argument("view", choices=ALL_VIEWS, help="View name")
    gen.set_defaults(func=cmd_generate)

    lint = sub.add_parser("lint", help="Check all SQL views match YAML rules")
    lint.set_defaults(func=cmd_lint)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
