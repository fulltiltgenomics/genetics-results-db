"""What the API exposes is an allow-list, so it is pinned here by name.

VIEWS decides reachability for /query, /schema, /stats and /tables/{name}/sample, and it is
derived from the `exposed` flags in configs/datasets.yaml's `tables` block rather than written
out in api/main.py. A derivation can widen a set as easily as it can narrow one, so the
expected names are written out HERE, in the one place whose job is to disagree with the code.
Deriving both sides of this assertion would prove nothing.

Adding a view is meant to be `exposed: true` in the config plus a line here.
"""

import os
import sys

import pytest
import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.yaml_loader import load_views  # noqa: E402
from test_sandbox_token_auth import _reload  # noqa: E402

EXPOSED_VIEWS = {
    "asm_qtl_v",
    "coloc_credsets_v",
    "colocalization_v",
    "credible_sets_v",
    "datasets_v",
    "dosage_sensitivity_v",
    "exome_variant_results_v",
    "gene_annotations_v",
    "gene_burden_results_v",
    "hla_associations_v",
    "mpra_v",
    "open_chromatin_v",
    "peak_to_gene_v",
    "phenotypes_v",
    "rcnv_gene_associations_v",
    "rcnv_segments_v",
    "variant_annotation_v",
    "variant_effect_v",
}

DATASETS_YAML = os.path.join(os.path.dirname(__file__), "..", "configs", "datasets.yaml")


@pytest.fixture(scope="module")
def main(request):
    patcher = pytest.MonkeyPatch()
    request.addfinalizer(patcher.undo)
    return _reload(patcher, INTERNAL_API_SECRET="test-secret")


def test_config_exposes_exactly_these_views():
    with open(DATASETS_YAML) as handle:
        views = load_views(yaml.safe_load(handle))
    assert len(views) == len(set(views))
    assert set(views) == EXPOSED_VIEWS


def test_views_is_exactly_the_exposed_set(main):
    assert len(main.VIEWS) == len(set(main.VIEWS))
    assert set(main.VIEWS) == EXPOSED_VIEWS


def test_base_table_aliases_still_derive_from_views(main):
    assert set(main._BASE_TABLES) == {v.removesuffix("_v") for v in EXPOSED_VIEWS}
    assert set(main._BASE_TABLES.values()) == EXPOSED_VIEWS


def test_query_allow_list_is_neither_wider_nor_narrower(main):
    """_ALLOWED_TABLE_IDS is what /query checks the dry run's referenced tables against."""
    prefix = f"{main.PROJECT_ID}.{main.DATASET_ID}."
    expected = {prefix + name for name in EXPOSED_VIEWS}
    expected |= {prefix + name.removesuffix("_v") for name in EXPOSED_VIEWS}
    assert set(main._ALLOWED_TABLE_IDS) == expected
