"""Unit tests for the gene_annotations build helpers (lineage closure + chrom encoding).

Uses tiny in-repo polars fixtures; does not touch GCS or BigQuery.
"""

import os
import sys

import polars as pl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from build_gene_annotations import (  # noqa: E402
    assemble_genes,
    build_closure_map,
    canonical_hgnc_id,
    chrom_to_int,
    full_lineage_groups,
)


# family hierarchy: root(1) -> mid(2) -> leaf(3); plus a standalone group(9)
HIERARCHY_CLOSURE = pl.DataFrame(
    {
        # already transitive: leaf 3 lists BOTH parents 2 and 1
        "parent_fam_id": ["1", "2", "1"],
        "child_fam_id": ["2", "3", "3"],
        "distance": ["1", "1", "2"],
    }
)

FAMILY = pl.DataFrame(
    {
        "id": ["1", "2", "3", "9"],
        "abbreviation": ["ROOT", "MID", "LEAF", "STAND"],
        "name": ["Root family", "Mid family", "Leaf family", "Standalone family"],
    }
)

# GENE_A leaf is the deep child (3); GENE_B is in standalone group (9).
# NOTE: hgnc_gene_has_family.csv uses BARE numeric hgnc ids ("1"), whereas
# hgnc_complete_set.txt (HGNC below) uses the prefixed "HGNC:1" form. The build
# must canonicalize both or the join yields empty group arrays.
GENE_HAS_FAMILY = pl.DataFrame(
    {
        "hgnc_id": ["1", "2"],
        "family_id": ["3", "9"],
    }
)

HGNC = pl.DataFrame(
    {
        "hgnc_id": ["HGNC:1", "HGNC:2"],
        "symbol": ["GENE_A", "GENE_B"],
        "name": ["Gene A", "Gene B"],
        "ensembl_gene_id": ["ENSG00000000001.5", "ENSG00000000002.1"],
        "alias_symbol": ['"AltA1|AltA2"', None],
        "prev_symbol": ["OldA", None],
        "locus_type": ["gene with protein product", "gene with protein product"],
        "entrez_id": ["111", "222"],
    }
)

GENCODE = pl.DataFrame(
    {
        "gene_id": ["ENSG00000000001.5", "ENSG00000000002.9"],
        "chrom": ["X", "7"],
        "gene_start": ["1000", "2000"],
        "gene_end": ["1500", "2500"],
        "gene_strand": ["+", "-"],
    }
)


def test_chrom_to_int_string_encoding():
    assert chrom_to_int("X") == 23
    assert chrom_to_int("chrX") == 23
    assert chrom_to_int("Y") == 24
    assert chrom_to_int("M") == 25
    assert chrom_to_int("MT") == 25
    assert chrom_to_int("7") == 7
    assert chrom_to_int("GL000.1") is None


def test_chrom_to_int_integer_encoded_passthrough():
    # already-integer-encoded files: 23=X, 24=Y, 25/26=M/MT pass through unchanged
    assert chrom_to_int("23") == 23
    assert chrom_to_int("24") == 24
    assert chrom_to_int("25") == 25
    assert chrom_to_int("26") == 26
    assert chrom_to_int(7) == 7
    assert chrom_to_int("chr1") == 1
    assert chrom_to_int("0") is None
    assert chrom_to_int("27") is None


def test_canonical_hgnc_id_normalizes_both_formats():
    # bare numeric (gene_has_family form) gets the prefix
    assert canonical_hgnc_id("5") == "HGNC:5"
    # already-prefixed (complete_set form) is unchanged
    assert canonical_hgnc_id("HGNC:5") == "HGNC:5"
    # case-insensitive prefix + stray whitespace
    assert canonical_hgnc_id(" hgnc:5 ") == "HGNC:5"
    # empty / missing
    assert canonical_hgnc_id("") is None
    assert canonical_hgnc_id(None) is None


def test_closure_map_inverts_to_ancestors():
    ancestors = build_closure_map(
        HIERARCHY_CLOSURE.select(
            pl.col("parent_fam_id").cast(pl.Int64),
            pl.col("child_fam_id").cast(pl.Int64),
        )
    )
    # leaf 3 must see both mid(2) and root(1)
    assert ancestors[3] == {1, 2}
    assert ancestors[2] == {1}


def test_full_lineage_includes_ancestors_and_names():
    ancestors = build_closure_map(
        HIERARCHY_CLOSURE.select(
            pl.col("parent_fam_id").cast(pl.Int64),
            pl.col("child_fam_id").cast(pl.Int64),
        )
    )
    names = {1: "Root family", 2: "Mid family", 3: "Leaf family"}
    ids, group_names = full_lineage_groups([3], ancestors, names)
    assert ids == [1, 2, 3]
    assert group_names == ["Root family", "Mid family", "Leaf family"]


def test_assemble_genes_full_lineage_and_coords():
    records = assemble_genes(
        HGNC,
        GENCODE,
        GENE_HAS_FAMILY,
        HIERARCHY_CLOSURE,
        FAMILY,
        gencode_version="49",
        hgnc_version="2026-06-01",
        download_date="2026-06-02",
    )
    by_symbol = {r["symbol"]: r for r in records}

    a = by_symbol["GENE_A"]
    # (a) deep-child leaf also receives ancestor + root ids AND names
    assert a["gene_group_ids"] == [1, 2, 3]
    assert a["gene_group_names"] == ["Root family", "Mid family", "Leaf family"]
    # (b) chr X -> 23, strand and coords carried through, ensembl version stripped
    assert a["chr"] == 23
    assert a["strand"] == "+"
    assert a["gene_start"] == 1000
    assert a["gene_end"] == 1500
    assert a["ensembl_gene_id"] == "ENSG00000000001"
    assert a["ncbi_gene_id"] == "111"
    # quoted pipe field cleaned but pipes preserved
    assert a["alias_symbols"] == "AltA1|AltA2"
    assert a["gencode_version"] == "49"

    b = by_symbol["GENE_B"]
    # standalone leaf with no ancestors -> just itself
    assert b["gene_group_ids"] == [9]
    assert b["gene_group_names"] == ["Standalone family"]
    assert b["chr"] == 7
    assert b["strand"] == "-"
