#!/usr/bin/env python3
"""Build the gene_annotations NDJSON load file from HGNC + GENCODE sources.

One row per HGNC gene: core HGNC fields, GENCODE GRCh38 coordinates joined by
version-stripped Ensembl id, and FULL-lineage HGNC gene-group arrays (leaf
group plus all ancestors).

gene_group_ids/gene_group_names are ARRAYs, so the output is
NEWLINE_DELIMITED_JSON (CSV/TSV cannot carry REPEATED columns into BigQuery).

The lineage-closure and assembly logic live in importable pure functions so they
can be unit-tested without touching GCS/BigQuery.

Source files (HGNC gene-group headers verified against
https://www.genenames.org/download/gene-groups/):
- hgnc_complete_set.txt (TAB-separated): hgnc_id, symbol, name, ensembl_gene_id,
  alias_symbol, prev_symbol (pipe-delimited), locus_type, entrez_id (NCBI/Entrez)
- gencode.v49.annotation.genes.tsv (TAB-separated): gene_id (versioned ENSG),
  chrom, gene_start, gene_end, gene_strand
- hgnc_gene_has_family.csv (CSV, HGNC publishes these comma-separated): hgnc_id, family_id
- hgnc_hierarchy_closure.csv (CSV): parent_fam_id, child_fam_id, distance (transitive)
- hgnc_family.csv (CSV): id, abbreviation, name, ... (only id + name used)
"""

import argparse
import datetime
import json
import sys

import polars as pl


def chrom_to_int(chrom: str | None) -> int | None:
    """Convert a GENCODE chromosome value to its BigQuery integer encoding.

    The exact encoding of the GENCODE file cannot be verified in this
    environment, so this handles BOTH known encodings:
      - STRING form: '1'..'22', 'X', 'Y', 'M'/'MT' (optionally 'chr'-prefixed)
      - already-INTEGER-encoded form: '1'..'22', 23=X, 24=Y, 25=M, 26=MT

    Convention: 23=X, matching the integer chromosome encoding used by the other
    BigQuery views (Y/M/MT extend the same scheme). Any already-integer input in
    range 1..26 -- including 23/24/25/26 -- passes through unchanged.

    Scaffolds / unmapped contigs return None so they don't pollute the
    integer-typed column.
    """
    if chrom is None:
        return None
    c = str(chrom).strip()
    if c.lower().startswith("chr"):
        c = c[3:]
    c = c.upper()
    if c.isdigit():
        n = int(c)
        return n if 1 <= n <= 26 else None
    return {"X": 23, "Y": 24, "M": 25, "MT": 25}.get(c)


def strip_ensembl_version(ensembl_id: str | None) -> str | None:
    """Drop the '.N' version suffix from an Ensembl gene id (ENSG00000123.4 -> ENSG00000123)."""
    if not ensembl_id:
        return None
    return ensembl_id.split(".")[0]


def canonical_hgnc_id(value: str | None) -> str | None:
    """Normalize an HGNC id to its canonical 'HGNC:NNNN' form.

    The HGNC sources disagree on format: hgnc_complete_set.txt uses the prefixed
    'HGNC:5' form, while the gene-group files (hgnc_gene_has_family.csv) use bare
    numeric ids ('5'). Both must be normalized to the same key or the gene->family
    join silently produces empty gene_group arrays for every gene.
    """
    if value is None:
        return None
    v = str(value).strip()
    if not v:
        return None
    if v.upper().startswith("HGNC:"):
        return "HGNC:" + v[5:].strip()
    if v.isdigit():
        return f"HGNC:{v}"
    return v


def build_closure_map(closure: pl.DataFrame) -> dict[int, set[int]]:
    """Map each child family id to the set of all its ancestor (parent) family ids.

    hierarchy_closure is already transitive, so every (parent, child) pair is
    listed directly; we only invert it into child -> {parents}. distance==0
    self-rows (if any) are ignored since the leaf is unioned in separately.
    """
    ancestors: dict[int, set[int]] = {}
    for row in closure.iter_rows(named=True):
        parent = row["parent_fam_id"]
        child = row["child_fam_id"]
        if parent is None or child is None or parent == child:
            continue
        ancestors.setdefault(int(child), set()).add(int(parent))
    return ancestors


def full_lineage_groups(
    leaf_family_ids: list[int],
    ancestors: dict[int, set[int]],
    family_names: dict[int, str],
) -> tuple[list[int], list[str]]:
    """Expand leaf family ids to leaf + all ancestors, mapped to names.

    Returns (ids, names) as parallel sorted lists. Unknown ids (no name) are
    dropped from both lists so they stay aligned.
    """
    full: set[int] = set()
    for leaf in leaf_family_ids:
        full.add(int(leaf))
        full |= ancestors.get(int(leaf), set())

    ids = sorted(fid for fid in full if fid in family_names)
    names = [family_names[fid] for fid in ids]
    return ids, names


def assemble_genes(
    hgnc: pl.DataFrame,
    gencode: pl.DataFrame,
    gene_has_family: pl.DataFrame,
    hierarchy_closure: pl.DataFrame,
    family: pl.DataFrame,
    *,
    gencode_version: str,
    hgnc_version: str,
    download_date: str,
) -> list[dict]:
    """Join all sources into one record per HGNC gene with full-lineage group arrays.

    Inputs are raw polars frames with the documented source columns (all read as
    strings). Returns a list of plain dicts ready for NDJSON serialization.
    """
    ancestors = build_closure_map(
        hierarchy_closure.select(
            pl.col("parent_fam_id").cast(pl.Int64, strict=False),
            pl.col("child_fam_id").cast(pl.Int64, strict=False),
        )
    )

    family_names = {
        int(row["id"]): row["name"]
        for row in family.select(
            pl.col("id").cast(pl.Int64, strict=False), pl.col("name")
        ).iter_rows(named=True)
        if row["id"] is not None
    }

    # hgnc_id -> list of leaf family ids
    leaves: dict[str, list[int]] = {}
    for row in gene_has_family.select(
        pl.col("hgnc_id"), pl.col("family_id").cast(pl.Int64, strict=False)
    ).iter_rows(named=True):
        key = canonical_hgnc_id(row["hgnc_id"])
        if key and row["family_id"] is not None:
            leaves.setdefault(key, []).append(int(row["family_id"]))

    # gencode coords keyed by version-stripped ENSG
    coords: dict[str, dict] = {}
    for row in gencode.iter_rows(named=True):
        ensg = strip_ensembl_version(row.get("gene_id"))
        if not ensg:
            continue
        coords[ensg] = {
            "chr": chrom_to_int(row.get("chrom")),
            "gene_start": _to_int(row.get("gene_start")),
            "gene_end": _to_int(row.get("gene_end")),
            "strand": row.get("gene_strand"),
        }

    records = []
    for row in hgnc.iter_rows(named=True):
        hgnc_id = row.get("hgnc_id")
        symbol = row.get("symbol")
        if not symbol:
            continue

        ensembl_id = strip_ensembl_version(row.get("ensembl_gene_id"))
        coord = coords.get(ensembl_id) if ensembl_id else None

        group_ids, group_names = full_lineage_groups(
            leaves.get(canonical_hgnc_id(hgnc_id), []), ancestors, family_names
        )

        records.append(
            {
                "hgnc_id": hgnc_id,
                "symbol": symbol,
                "name": row.get("name"),
                "prev_symbols": _clean_pipe(row.get("prev_symbol")),
                "alias_symbols": _clean_pipe(row.get("alias_symbol")),
                "ensembl_gene_id": ensembl_id,
                "ncbi_gene_id": row.get("entrez_id"),
                "chr": coord["chr"] if coord else None,
                "gene_start": coord["gene_start"] if coord else None,
                "gene_end": coord["gene_end"] if coord else None,
                "strand": coord["strand"] if coord else None,
                "locus_type": row.get("locus_type"),
                "gene_group_ids": group_ids,
                "gene_group_names": group_names,
                "gencode_version": gencode_version,
                "hgnc_version": hgnc_version,
                "download_date": download_date,
            }
        )
    return records


def _to_int(value) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clean_pipe(value: str | None) -> str | None:
    """HGNC pipe-delimited fields are sometimes quoted; strip quotes, keep '|'."""
    if not value:
        return None
    return value.replace('"', "")


def _read_tsv(path: str) -> pl.DataFrame:
    # infer_schema_length=0 -> all columns as strings, avoiding pipe-field parse errors
    return pl.read_csv(path, separator="\t", infer_schema_length=0, null_values=[""])


def _read_csv(path: str) -> pl.DataFrame:
    # HGNC gene-group DB files are comma-separated with quoted fields; quote-aware
    # parsing (polars default) handles commas inside quoted family names
    return pl.read_csv(path, separator=",", infer_schema_length=0, null_values=[""])


def main():
    parser = argparse.ArgumentParser(description="Build gene_annotations NDJSON from HGNC + GENCODE")
    parser.add_argument("--hgnc", required=True, help="hgnc_complete_set.txt (TSV)")
    parser.add_argument("--gencode", required=True, help="gencode.vNN.annotation.genes.tsv")
    parser.add_argument("--gene-has-family", required=True, help="hgnc_gene_has_family.csv")
    parser.add_argument("--hierarchy-closure", required=True, help="hgnc_hierarchy_closure.csv")
    parser.add_argument("--family", required=True, help="hgnc_family.csv")
    parser.add_argument("--out", required=True, help="output NDJSON path (local or gs://)")
    parser.add_argument("--gencode-version", default="49")
    parser.add_argument("--hgnc-version", required=True, help="HGNC release/date used")
    parser.add_argument(
        "--download-date",
        default=datetime.date.today().isoformat(),
        help="ISO date the sources were downloaded (default: today)",
    )
    args = parser.parse_args()

    records = assemble_genes(
        _read_tsv(args.hgnc),
        _read_tsv(args.gencode),
        _read_csv(args.gene_has_family),
        _read_csv(args.hierarchy_closure),
        _read_csv(args.family),
        gencode_version=args.gencode_version,
        hgnc_version=args.hgnc_version,
        download_date=args.download_date,
    )

    # fsspec.open supports both local paths and gs:// URIs
    import fsspec

    with fsspec.open(args.out, "wt") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")

    print(f"Wrote {len(records)} gene_annotations records to {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
