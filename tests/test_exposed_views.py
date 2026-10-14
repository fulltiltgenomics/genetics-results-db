"""What the API exposes is an allow-list, so it is pinned here by name.

VIEWS decides reachability for /query, /schema, /stats and /tables/{name}/sample, and it is
derived from the `exposed` flags in configs/datasets.yaml's `tables` block rather than written
out in api/main.py. A derivation can widen a set as easily as it can narrow one, so the
expected names are written out HERE, in the one place whose job is to disagree with the code.
Deriving both sides of this assertion would prove nothing.

Adding a view is meant to be `exposed: true` in the config plus a line here; a view carrying
`profiles: [...]` goes in GATED_VIEWS under each profile it lists instead.
"""

import os
import sys

import pytest
import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from test_sandbox_token_auth import _reload  # noqa: E402

from api.yaml_loader import load_views  # noqa: E402

EXPOSED_VIEWS = {
    "asm_qtl_v",
    "coloc_credsets_v",
    "colocalization_v",
    "credible_sets_v",
    "datasets_v",
    "decode_pqtl_signals_v",
    "dosage_sensitivity_v",
    "exome_gene_bayes_results_v",
    "exome_gene_counts_v",
    "exome_variant_counts_v",
    "exome_variant_results_v",
    "gene_annotations_v",
    "gene_burden_results_v",
    "gnomad_variant_annotation_v",
    "hla_associations_v",
    "mpra_v",
    "open_chromatin_v",
    "peak_to_gene_v",
    "phenotypes_v",
    "rcnv_gene_associations_v",
    "rcnv_segments_v",
    "rcnv_window_associations_v",
    "variant_annotation_v",
    "variant_effect_v",
}

GATED_VIEWS = {
    "daly": {
        "cellxgene_datasets_v",
        "cellxgene_expression_v",
        "cellxgene_groups_v",
        "cellxgene_measured_genes_v",
        "cellxgene_summary_v",
    },
}

DATASETS_YAML = os.path.join(os.path.dirname(__file__), "..", "configs", "datasets.yaml")


# None stands for an unset CONFIG_PROFILE, which must hide every gated view
PROFILES = ["daly", "finngen", None]


def _expected(profile):
    return EXPOSED_VIEWS | GATED_VIEWS.get(profile, set())


@pytest.fixture(scope="module", params=PROFILES, ids=lambda p: p or "unset")
def main(request):
    patcher = pytest.MonkeyPatch()
    request.addfinalizer(patcher.undo)
    patcher.delenv("CONFIG_PROFILE", raising=False)
    module = _reload(patcher, INTERNAL_API_SECRET="test-secret", CONFIG_PROFILE=request.param)
    module.expected_views = _expected(request.param)
    return module


@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p or "unset")
def test_config_exposes_exactly_these_views(profile):
    with open(DATASETS_YAML) as handle:
        views = load_views(yaml.safe_load(handle), profile)
    assert len(views) == len(set(views))
    assert set(views) == _expected(profile)


def test_views_is_exactly_the_exposed_set(main):
    assert len(main.VIEWS) == len(set(main.VIEWS))
    assert set(main.VIEWS) == main.expected_views


def test_base_table_aliases_still_derive_from_views(main):
    assert set(main._BASE_TABLES) == {v.removesuffix("_v") for v in main.expected_views}
    assert set(main._BASE_TABLES.values()) == main.expected_views


def test_query_allow_list_is_neither_wider_nor_narrower(main):
    """_ALLOWED_TABLE_IDS is what /query checks the dry run's referenced tables against."""
    prefix = f"{main.PROJECT_ID}.{main.DATASET_ID}."
    expected = {prefix + name for name in main.expected_views}
    expected |= {prefix + name.removesuffix("_v") for name in main.expected_views}
    assert set(main._ALLOWED_TABLE_IDS) == expected
