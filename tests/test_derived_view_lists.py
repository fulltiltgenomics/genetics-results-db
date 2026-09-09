"""The view lists db-api's build scripts derive from datasets.yaml, pinned by name.

Three different questions are asked of the same registry, and the answers are only
accidentally nested: which views have a `resource` generated from a `dataset` discriminator
(the lint scope), which of those store it at load time instead of in the view SQL, and which
views contribute no `dataset` values to the registry cross-check. Deriving either side of an
assertion from the other would prove nothing, so the expected names are written out here — a
config edit that moves a view between them fails loudly rather than silently narrowing a
check.
"""

import os
import sys

import pytest
import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from api.yaml_loader import (  # noqa: E402
    load_dataset_cross_check_exclusions,
    load_resource_derivation,
)
from generate_resource_sql import resource_derivation  # noqa: E402
from live_dataset_scope import excluded_views  # noqa: E402

DATASETS_YAML = os.path.join(os.path.dirname(__file__), "..", "configs", "datasets.yaml")

LINTED_VIEWS = {
    "asm_qtl_v",
    "coloc_credsets_v",
    "colocalization_v",
    "credible_sets_v",
    "exome_variant_results_v",
    "gene_burden_results_v",
    "hla_associations_v",
    "mpra_v",
    "open_chromatin_v",
    "peak_to_gene_v",
    "rcnv_gene_associations_v",
    "rcnv_segments_v",
    "variant_effect_v",
}

MATERIALIZED_VIEWS = {"credible_sets_v"}

CROSS_CHECK_EXCLUDED = {
    "datasets_v",
    "dosage_sensitivity_v",
    "gene_annotations_v",
    "phenotypes_v",
    "variant_annotation_v",
}


def _config():
    with open(DATASETS_YAML) as handle:
        return yaml.safe_load(handle)


def test_lint_scope_matches_the_pinned_names():
    lintable, _ = resource_derivation(DATASETS_YAML)
    assert set(lintable) == LINTED_VIEWS


def test_materialized_views_are_a_subset_of_the_lint_scope():
    """A stored `resource` is still generated from the rules; it just is not in the view SQL,
    so lint must reach it to assert the CASE stayed out."""
    lintable, materialized = resource_derivation(DATASETS_YAML)
    assert materialized == MATERIALIZED_VIEWS
    assert materialized <= set(lintable)


def test_cross_check_exclusions_match_the_pinned_names_and_carry_reasons():
    excluded = excluded_views(DATASETS_YAML)
    assert set(excluded) == CROSS_CHECK_EXCLUDED
    assert all(reason.strip() for reason in excluded.values())


def test_the_two_exclusions_are_separate_decisions():
    """The memberships coincide today by coincidence, so nothing here asserts they match: a
    view can carry the `dataset` column the cross-check needs while its `resource` is a
    constant. What must hold is that each side states its own reason — the two are answers to
    different questions, and identical prose would mean one had been derived from the other."""
    modes = load_resource_derivation(_config())
    not_derived = {name for name, mode in modes.items() if mode == "none"}
    assert {"phenotypes_v", "datasets_v"} <= not_derived
    reasons = load_dataset_cross_check_exclusions(_config())
    assert "self-confirming" in reasons["phenotypes_v"]
    assert "registry" in _config()["tables"]["phenotypes_v"]["resource_derivation"]["reason"]


def test_every_table_declares_how_its_resource_is_produced():
    modes = load_resource_derivation(_config())
    assert set(modes) == set(_config()["tables"])


def test_a_table_with_no_declared_derivation_fails():
    """The failure has to be loud: a view that drops out of the lint scope silently is a CASE
    nobody compares against the rules."""
    with pytest.raises(ValueError, match="resource_derivation.mode"):
        load_resource_derivation({"tables": {"future_v": {"exposed": True}}})


def test_an_opt_out_without_a_reason_fails():
    with pytest.raises(ValueError, match="reason"):
        load_resource_derivation(
            {"tables": {"future_v": {"resource_derivation": {"mode": "none"}}}})
    with pytest.raises(ValueError, match="excluded_reason"):
        load_dataset_cross_check_exclusions(
            {"tables": {"future_v": {"dataset_cross_check": {"excluded_reason": "  "}}}})


def test_an_empty_cross_check_block_fails():
    """Writing the key at all is the opt-out; an empty one must not read as 'not excluded'."""
    for block in (None, {}):
        with pytest.raises(ValueError, match="excluded_reason"):
            load_dataset_cross_check_exclusions(
                {"tables": {"future_v": {"dataset_cross_check": block}}})
