"""Unit tests for the phenotypes/datasets build helpers.

Uses tiny in-repo fixtures; does not touch GCS or BigQuery.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from build_phenotypes import (  # noqa: E402
    ABSENT_FROM_RESULTS,
    BQ_DATASETS_BY_DATASET_ID,
    COLOC_PARTNER_ONLY_DATASET_IDS,
    build_datasets,
    build_phenotypes,
    parse_date,
    safe_int,
    validate,
)


RESOURCES = {
    "finngen": {"label": "FinnGen", "aliases": ["FinnGen"]},
    "pgc": {"label": "PGC", "aliases": ["psychiatric genomics"]},
    "eqtl_catalogue": {"label": "eQTL Catalogue", "aliases": []},
}

REGISTRY = {
    "finngen_gwas": {
        "resource": "finngen", "version": "R14", "description": "core GWAS",
        "author": "FinnGen Consortium", "publication_date": "2026-05-13",
        "data_type": "gwas", "trait_type": "binary",
        "metadata_file": "gs://x/r14.json", "metadata_harmonizer": "finngen_r13",
    },
    "finngen_kanta": {
        "resource": "finngen", "version": "R14", "description": "kanta labs",
        "author": "FinnGen Consortium", "publication_date": "2026-05-29",
        "data_type": "gwas", "trait_type": "mixed",
        "metadata_file": "gs://x/kanta_r14.json", "metadata_harmonizer": "finngen_kanta",
    },
    "finngen_kanta_r12": {
        "resource": "finngen", "version": "R12", "description": "kanta labs R12",
        "author": "FinnGen Consortium", "publication_date": "2025-03-10",
        "data_type": "gwas", "trait_type": "mixed",
        "metadata_file": "gs://x/kanta_r12.json", "metadata_harmonizer": "finngen_kanta",
    },
    "pgc_scz": {
        "resource": "pgc", "version": "2022", "description": "SCZ",
        "author": "PGC", "publication_date": "2022-04-08", "data_type": "gwas",
        "metadata_file": None, "metadata_harmonizer": None,
    },
    "pgc_bip": {
        "resource": "pgc", "version": "2021", "description": "BIP",
        "author": "PGC", "publication_date": "2021-05-17", "data_type": "gwas",
        "metadata_file": None, "metadata_harmonizer": None,
    },
    "eqtl_catalogue": {
        "resource": "eqtl_catalogue", "version": "R8", "description": "collection",
        "author": "eQTL Catalogue", "publication_date": "2026-01-01",
        "data_type": "mixed", "trait_type": "quantitative",
        "metadata_file": "gs://x/eqtl.tsv", "metadata_harmonizer": "eqtl_catalogue",
        "collection": True, "subdataset_id_field": "phenotype_code",
    },
    # registry entry with no BigQuery presence
    "gtex_expression": {
        "resource": "gtex", "version": "v8", "description": "expression",
        "author": "GTEx", "publication_date": "2020-09-11", "data_type": "expression",
        "metadata_file": None, "metadata_harmonizer": None,
    },
}

METADATA = {
    "finngen_gwas": [
        {"phenocode": "HEIGHT_IRN", "phenostring": "Height, inverse-rank normalized",
         "category": "Quantitative endpoints", "num_cases": 480000, "num_controls": 0},
        {"phenocode": "T2D", "phenostring": "Type 2 diabetes",
         "category": "Endocrine", "num_cases": 95803, "num_controls": 300000},
    ],
    # R14 Kanta codes are prefixed; R12 Kanta codes are bare OMOP ids, so the two
    # registry entries share the results-view dataset without colliding
    "finngen_kanta": [
        {"phenocode": "median_3000963_age_adjusted_IRN", "phenostring": "Hemoglobin",
         "category": "Quantitative", "num_cases": 457704, "num_controls": 0,
         "num_total": 457704},
    ],
    "finngen_kanta_r12": [
        {"phenocode": "3000963", "phenostring": "Hemoglobin",
         "category": "Quantitative", "num_cases": 447271, "num_controls": 0,
         "num_total": 447271},
    ],
    "eqtl_catalogue": [
        {"dataset_id": "QTD000001", "study_label": "Alasoo_2018",
         "sample_group": "macrophage_naive", "tissue_label": "macrophage",
         "condition_label": "naive", "sample_size": "84", "quant_method": "ge"},
        {"dataset_id": "QTD000002", "study_label": "Alasoo_2018",
         "sample_group": "macrophage_IFNg", "tissue_label": "macrophage",
         "condition_label": "IFNg", "sample_size": "84", "quant_method": "txrev"},
    ],
}


def _phenotypes():
    return build_phenotypes(REGISTRY, METADATA)


def test_key_column_is_the_phenocode_not_the_display_name():
    """The results views store the phenocode in trait_original; joining on the display
    string returns nothing, so the built key must be the code."""
    rows, _ = _phenotypes()
    by_key = {(r["dataset"], r["trait_original"]): r for r in rows}
    assert ("FinnGen_R14", "HEIGHT_IRN") in by_key
    assert by_key[("FinnGen_R14", "HEIGHT_IRN")]["trait_name"] == "Height, inverse-rank normalized"


def test_key_is_unique():
    rows, _ = _phenotypes()
    keys = [(r["dataset"], r["trait_original"]) for r in rows]
    assert len(keys) == len(set(keys))


def test_kanta_releases_share_a_dataset_without_colliding():
    """finngen_kanta and finngen_kanta_r12 both map to results-view dataset
    'FinnGen_kanta'; their phenocode spaces are disjoint so both survive."""
    rows, dropped = _phenotypes()
    kanta = {r["trait_original"]: r for r in rows if r["dataset"] == "FinnGen_kanta"}
    assert set(kanta) == {"median_3000963_age_adjusted_IRN", "3000963"}
    assert kanta["3000963"]["coloc_partner_only"] is True
    assert kanta["median_3000963_age_adjusted_IRN"]["coloc_partner_only"] is False
    assert dropped == []


def test_trait_type_derived_from_counts_not_hardcoded_binary():
    """results-api's finngen_r13 harmonizer calls every trait binary; a GWAS with zero
    controls is quantitative."""
    rows, _ = _phenotypes()
    by_code = {r["trait_original"]: r for r in rows if r["dataset"] == "FinnGen_R14"}
    assert by_code["HEIGHT_IRN"]["trait_type"] == "quantitative"
    assert by_code["T2D"]["trait_type"] == "binary"
    assert by_code["T2D"]["n_samples"] == 95803 + 300000


def test_eqtl_catalogue_produces_no_phenotype_rows():
    """QTD ids are sub-STUDIES (results-view `dataset` values), not traits."""
    rows, _ = _phenotypes()
    assert not [r for r in rows if r["dataset"].startswith("QTD")]


def test_datasets_unique_on_dataset_and_merges_shared_entries():
    rows = build_datasets(REGISTRY, RESOURCES, METADATA)
    named = [r["dataset"] for r in rows if r["dataset"]]
    assert len(named) == len(set(named))
    pgc = next(r for r in rows if r["dataset"] == "PGC")
    assert sorted(pgc["dataset_ids"]) == ["pgc_bip", "pgc_scz"]
    assert "SCZ" in pgc["description"] and "BIP" in pgc["description"]


def test_registry_entries_without_bigquery_presence_get_a_null_dataset():
    rows = build_datasets(REGISTRY, RESOURCES, METADATA)
    gtex = next(r for r in rows if r["dataset_id"] == "gtex_expression")
    assert gtex["dataset"] is None


def test_collection_subdatasets_become_their_own_rows():
    rows = build_datasets(REGISTRY, RESOURCES, METADATA)
    qtd = {r["dataset"]: r for r in rows if (r["dataset"] or "").startswith("QTD")}
    assert set(qtd) == {"QTD000001", "QTD000002"}
    assert qtd["QTD000001"]["subdataset_of"] == "eqtl_catalogue"
    assert qtd["QTD000001"]["data_type"] == "eQTL"
    assert qtd["QTD000002"]["data_type"] == "sQTL"
    assert qtd["QTD000001"]["description"] == "macrophage_naive - macrophage - naive"
    assert qtd["QTD000001"]["n_samples"] == 84
    collection = next(r for r in rows if r["dataset_id"] == "eqtl_catalogue")
    assert collection["collection"] is True
    assert collection["dataset"] is None


def test_resource_metadata_is_attached():
    rows = build_datasets(REGISTRY, RESOURCES, METADATA)
    r14 = next(r for r in rows if r["dataset"] == "FinnGen_R14")
    assert r14["resource_label"] == "FinnGen"
    assert r14["resource_aliases"] == ["FinnGen"]


def test_validate_reports_both_directions():
    rows = build_datasets(REGISTRY, RESOURCES, METADATA)
    phenotype_rows, _ = _phenotypes()
    problems = validate(rows, phenotype_rows, {"FinnGen_R14", "SomethingNew"})
    assert any("SomethingNew" in p for p in problems)
    assert any("FinnGen_kanta" in p for p in problems)


def test_validate_reports_a_registry_claim_that_resolves_to_nothing():
    """The direction that used to be missing: mapped - live. Registry entries claiming a
    dataset no results table holds send agents to an empty result."""
    rows = build_datasets(REGISTRY, RESOURCES, METADATA)
    phenotype_rows, _ = _phenotypes()
    problems = validate(rows, phenotype_rows, {"FinnGen_R14"})
    assert any("FinnGen_kanta" in p and "no results table contains it" in p for p in problems)


def test_validate_flags_a_stale_absent_from_results_entry():
    """A suppressed name that came back to life must un-suppress, not stay hidden."""
    name = next(iter(ABSENT_FROM_RESULTS))
    problems = validate([], [], {name})
    assert any(name in p and "IS live now" in p for p in problems)


def test_absent_datasets_are_emitted_with_a_null_dataset_and_no_phenotypes():
    rows = build_datasets(REGISTRY, RESOURCES, METADATA)
    assert all(r["dataset"] not in ABSENT_FROM_RESULTS for r in rows)
    phenotype_rows, _ = _phenotypes()
    assert all(r["dataset"] not in ABSENT_FROM_RESULTS for r in phenotype_rows)


def test_every_coloc_partner_only_dataset_is_mapped():
    """A coloc partner with no results-view dataset would produce unresolvable
    colocalization rows, which is the whole reason these entries are kept."""
    for dataset_id in COLOC_PARTNER_ONLY_DATASET_IDS:
        assert BQ_DATASETS_BY_DATASET_ID.get(dataset_id)


def test_parse_date_drops_partial_dates_rather_than_inventing_them():
    assert parse_date("2026-05-13") == "2026-05-13"
    assert parse_date("2021") is None
    assert parse_date("") is None
    assert parse_date(None) is None


def test_safe_int_treats_na_sentinels_as_missing():
    assert safe_int("NA") is None
    assert safe_int("") is None
    assert safe_int("-1") is None
    assert safe_int("0") == 0
    assert safe_int("84") == 84
