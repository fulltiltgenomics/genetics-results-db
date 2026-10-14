#!/usr/bin/env python3
"""
Load genetics results data from GCS into BigQuery.
Handles TSV.gz files with appropriate schema mapping.

By default a TSV file must contain every column declared in the target schema,
in the same order. If the file omits a column that the schema requires (e.g. a
`dataset` discriminator that distinguishes per-source rows in a shared table),
pass `--const-column NAME=VALUE` to inject the value at load time. The data is
loaded into a temporary staging table (without those columns), then projected
into the target table with the constants filled in. The flag is repeatable.

The same staging indirection also materialises DERIVED_COLUMNS: columns that are
computed from other columns of the same row and are likewise absent from the source
TSV (see `credible_sets.variant` / `credible_sets.resource`).

`--transaction SPEC.json` replaces several tables all-or-none: every file is staged and
counted first, then one multi-statement transaction DELETEs and re-INSERTs every target.
"""

import argparse
import datetime
import json
import os
import signal
import sys
import uuid

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, SourceFormat, WriteDisposition

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


# schema definitions matching the SQL table definitions
SCHEMAS = {
    # `resource` and `variant` are NOT in the source TSV — they are computed on
    # projection from the staging table (see DERIVED_COLUMNS). They sit here in the
    # target table's column order, which the projection must match; the staging
    # schema drops them, so the positional TSV layout is unchanged.
    "credible_sets": [
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("resource", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("data_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait_original", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cell_type", "STRING"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ref", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("alt", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("variant", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("mlog10p", "FLOAT64"),
        # nullable: see the comment on the same column in schemas/credible_sets.sql
        bigquery.SchemaField("beta", "FLOAT64"),
        bigquery.SchemaField("se", "FLOAT64"),
        bigquery.SchemaField("pip", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("cs_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cs_size", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("cs_min_r2", "FLOAT64"),
        bigquery.SchemaField("aaf", "FLOAT64"),
        bigquery.SchemaField("most_severe", "STRING"),
        bigquery.SchemaField("gene_most_severe", "STRING"),
    ],
    # column order must match TSV file exactly
    "colocalization": [
        bigquery.SchemaField("dataset1", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("dataset2", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("data_type1", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("data_type2", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait1", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait1_original", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait2", "STRING"),
        bigquery.SchemaField("trait2_original", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cell_type1", "STRING"),
        bigquery.SchemaField("cell_type2", "STRING"),
        bigquery.SchemaField("cs1_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cs2_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("hit1", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("hit2", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("hit1_beta", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("hit1_mlog10p", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("hit2_beta", "FLOAT64"),
        bigquery.SchemaField("hit2_mlog10p", "FLOAT64"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("region_start_min", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("region_end_max", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("PP_H0_abf", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("PP_H1_abf", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("PP_H2_abf", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("PP_H3_abf", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("PP_H4_abf", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("nsnps", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("nsnps1", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("nsnps2", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("cs1_log10bf", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("cs2_log10bf", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("clpp", "FLOAT64"),
        bigquery.SchemaField("clpa", "FLOAT64"),
        bigquery.SchemaField("cs1_size", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("cs2_size", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("cs_overlap", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("topInOverlap", "STRING", mode="REQUIRED"),
    ],
    # column order must match TSV file exactly
    "coloc_credsets": [
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("data_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait_original", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cell_type", "STRING"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ref", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("alt", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("mlog10p", "FLOAT64"),
        bigquery.SchemaField("beta", "FLOAT64"),
        bigquery.SchemaField("se", "FLOAT64"),
        bigquery.SchemaField("pip", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("cs_id", "STRING", mode="REQUIRED"),
    ],
    # column order must match TSV file exactly
    "asm_qtl": [
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ref", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("alt", "STRING"),
        bigquery.SchemaField("rsid", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("beta", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("se", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("mlog10p", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("af", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("most_severe", "STRING"),
        bigquery.SchemaField("gene_most_severe", "STRING"),
        bigquery.SchemaField("target_start", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("target_end", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ref_methylrate", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("alt_methylrate", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("n_haplotypes", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("variant_rank", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ld_count", "INT64"),
        bigquery.SchemaField("vartype", "STRING", mode="REQUIRED"),
    ],
    # column order must match TSV file exactly
    "exome_variant_results": [
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ref", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("alt", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gene", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("annotation", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("mlog10p", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("beta", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("se", "FLOAT64"),
        bigquery.SchemaField("af_overall", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("af_cases", "FLOAT64"),
        bigquery.SchemaField("af_controls", "FLOAT64"),
        bigquery.SchemaField("ac", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("an", "INT64"),
        bigquery.SchemaField("n_cases", "INT64"),
        bigquery.SchemaField("n_controls", "INT64"),
        bigquery.SchemaField("trait", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait_original", "STRING", mode="REQUIRED"),
    ],
    # column order must match TSV file exactly
    "gene_burden_results": [
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gene", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gene_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("gene_start_pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("gene_end_pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("annotation", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("mlog10p_burden", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("beta", "FLOAT64", mode="REQUIRED"),
        # NULL where the burden test returned no estimate (beta 0, p 1) — only
        # visible now that the unfiltered results are loaded
        bigquery.SchemaField("se", "FLOAT64"),
        bigquery.SchemaField("total_variants", "INT64"),
        bigquery.SchemaField("total_variants_pheno", "INT64"),
        bigquery.SchemaField("n_cases", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("n_controls", "INT64"),
        bigquery.SchemaField("trait_original", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("flags", "STRING"),
    ],
    # field order is irrelevant here: loaded from NEWLINE_DELIMITED_JSON (named fields),
    # not positional CSV. gene_group_ids/names are REPEATED arrays, which CSV/TSV
    # cannot carry — see JSON_SCHEMAS below.
    # modes mirror schemas/gene_annotations.sql: HGNC core identity fields and
    # build-stamped provenance columns are REQUIRED (never NA); coordinate/
    # cross-reference columns are NULLABLE (not every gene maps to GENCODE/
    # Ensembl/NCBI); the gene-group arrays are REPEATED.
    "gene_annotations": [
        bigquery.SchemaField("hgnc_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("prev_symbols", "STRING"),
        bigquery.SchemaField("alias_symbols", "STRING"),
        bigquery.SchemaField("ensembl_gene_id", "STRING"),
        bigquery.SchemaField("ncbi_gene_id", "STRING"),
        bigquery.SchemaField("chr", "INT64"),
        bigquery.SchemaField("gene_start", "INT64"),
        bigquery.SchemaField("gene_end", "INT64"),
        bigquery.SchemaField("strand", "STRING"),
        bigquery.SchemaField("locus_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gene_group_ids", "INT64", mode="REPEATED"),
        bigquery.SchemaField("gene_group_names", "STRING", mode="REPEATED"),
        bigquery.SchemaField("gencode_version", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("hgnc_version", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("download_date", "DATE", mode="REQUIRED"),
    ],
    # column order must match TSV file exactly (canonical open_chromatin layout:
    # chrom, start, end, peak_id, dataset, cell_type, tissue, life_stage, condition,
    # assay, score, score_type, n_cells, cell_ontology_id, uberon_id, target_gene,
    # target_gene_id, version). The source chrom is a numeric string (X=23,Y=24,
    # M=25); it is loaded to the INT64 `chr` column via CHR_STRING_TABLES staging.
    "open_chromatin": [
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("peak_start", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("peak_end", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("peak_id", "STRING"),
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cell_type", "STRING"),
        bigquery.SchemaField("tissue", "STRING"),
        bigquery.SchemaField("life_stage", "STRING"),
        bigquery.SchemaField("condition", "STRING"),
        bigquery.SchemaField("assay", "STRING"),
        bigquery.SchemaField("score", "FLOAT64"),
        bigquery.SchemaField("score_type", "STRING"),
        bigquery.SchemaField("n_cells", "INT64"),
        bigquery.SchemaField("cell_ontology_id", "STRING"),
        bigquery.SchemaField("uberon_id", "STRING"),
        bigquery.SchemaField("target_gene", "STRING"),
        bigquery.SchemaField("target_gene_id", "STRING"),
        bigquery.SchemaField("version", "STRING"),
    ],
    # column order must match TSV file exactly (canonical variant_effect layout:
    # chrom, pos, ref, alt, variant, rsid, dataset, model, cell_type, tissue,
    # life_stage, score, score_type, mlog10p, predicted_direction, quantile_rank,
    # is_significant, version). The source chrom is a numeric string (X=23,Y=24,
    # M=25); it is loaded to the INT64 `chr` column via CHR_STRING_TABLES staging.
    "variant_effect": [
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ref", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("alt", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("variant", "STRING"),
        bigquery.SchemaField("rsid", "STRING"),
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("model", "STRING"),
        bigquery.SchemaField("cell_type", "STRING"),
        bigquery.SchemaField("tissue", "STRING"),
        bigquery.SchemaField("life_stage", "STRING"),
        bigquery.SchemaField("score", "FLOAT64"),
        bigquery.SchemaField("score_type", "STRING"),
        bigquery.SchemaField("mlog10p", "FLOAT64"),
        bigquery.SchemaField("predicted_direction", "STRING"),
        bigquery.SchemaField("quantile_rank", "FLOAT64"),
        bigquery.SchemaField("is_significant", "BOOL"),
        bigquery.SchemaField("version", "STRING"),
    ],
    # column order must match TSV file exactly (canonical mpra LONG layout:
    # chrom, pos, variant, ref, alt, cohort, cell_line, emVar, active, log2Skew,
    # log2Skew_se, log2Skew_mlog10p, log2FC, log2FC_mlog10p, mean_RNA_ref,
    # mean_RNA_alt). The source chrom is a numeric string (X=23,Y=24,M=25); it is
    # loaded to the INT64 `chr` column via CHR_STRING_TABLES staging. `dataset` is
    # not in the file — it is injected at load with --const-column dataset=siraj_mpra.
    "mpra": [
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("variant", "STRING"),
        bigquery.SchemaField("ref", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("alt", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cohort", "STRING"),
        bigquery.SchemaField("cell_line", "STRING"),
        bigquery.SchemaField("emVar", "BOOL"),
        bigquery.SchemaField("active", "BOOL"),
        bigquery.SchemaField("log2Skew", "FLOAT64"),
        bigquery.SchemaField("log2Skew_se", "FLOAT64"),
        bigquery.SchemaField("log2Skew_mlog10p", "FLOAT64"),
        bigquery.SchemaField("log2FC", "FLOAT64"),
        bigquery.SchemaField("log2FC_mlog10p", "FLOAT64"),
        bigquery.SchemaField("mean_RNA_ref", "FLOAT64"),
        bigquery.SchemaField("mean_RNA_alt", "FLOAT64"),
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
    ],
    # column order must match TSV file exactly (canonical HLA combined layout:
    # chrom, pos, gene, allele, phenotype, pval, mlogp, beta, sebeta, af_alt,
    # af_alt_cases, af_alt_controls, info). The source chrom is a numeric string
    # (always "6"); it is loaded to the INT64 `chr` column via CHR_STRING_TABLES
    # staging. `dataset` is not in the file — it is injected at load with
    # --const-column dataset=finngen_hla.
    "hla_associations": [
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("gene", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("allele", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("phenotype", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pval", "FLOAT64"),
        bigquery.SchemaField("mlogp", "FLOAT64"),
        bigquery.SchemaField("beta", "FLOAT64"),
        bigquery.SchemaField("sebeta", "FLOAT64"),
        bigquery.SchemaField("af_alt", "FLOAT64"),
        bigquery.SchemaField("af_alt_cases", "FLOAT64"),
        bigquery.SchemaField("af_alt_controls", "FLOAT64"),
        bigquery.SchemaField("info", "FLOAT64"),
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
    ],
    # column order must match TSV file exactly (canonical FinnGen R14 annotation
    # layout from R14_annotated_variants_v0.small.gz, the same file the API serves
    # at /variant_annotation/finngen): variant, chr, pos, ref, alt, INFO, AF,
    # AC_Het, AC_Hom, most_severe, gene_most_severe, rsid, EXOME_enrichment_nfe,
    # GENOME_enrichment_nfe, index. chr is already numeric in the source (X=23),
    # so no CHR_STRING_TABLES staging is needed.
    "variant_annotation": [
        bigquery.SchemaField("variant", "STRING"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ref", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("alt", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("INFO", "FLOAT64"),
        bigquery.SchemaField("AF", "FLOAT64"),
        bigquery.SchemaField("AC_Het", "INT64"),
        bigquery.SchemaField("AC_Hom", "INT64"),
        bigquery.SchemaField("most_severe", "STRING"),
        bigquery.SchemaField("gene_most_severe", "STRING"),
        bigquery.SchemaField("rsid", "STRING"),
        bigquery.SchemaField("EXOME_enrichment_nfe", "FLOAT64"),
        bigquery.SchemaField("GENOME_enrichment_nfe", "FLOAT64"),
        bigquery.SchemaField("index", "INT64"),
    ],
    # `variant` is not in the source file; it is computed on projection (DERIVED_COLUMNS).
    # The other columns follow the served gnomAD sites file's header order.
    "gnomad_variant_annotation": [
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ref", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("alt", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("variant", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("rsids", "STRING"),
        bigquery.SchemaField("filters", "STRING"),
        bigquery.SchemaField("AN", "INT64"),
        bigquery.SchemaField("AF", "FLOAT64"),
        bigquery.SchemaField("AF_afr", "FLOAT64"),
        bigquery.SchemaField("AF_amr", "FLOAT64"),
        bigquery.SchemaField("AF_asj", "FLOAT64"),
        bigquery.SchemaField("AF_eas", "FLOAT64"),
        bigquery.SchemaField("AF_fin", "FLOAT64"),
        bigquery.SchemaField("AF_mid", "FLOAT64"),
        bigquery.SchemaField("AF_nfe", "FLOAT64"),
        bigquery.SchemaField("AF_remaining", "FLOAT64"),
        bigquery.SchemaField("AF_sas", "FLOAT64"),
        bigquery.SchemaField("most_severe", "STRING"),
        bigquery.SchemaField("gene_most_severe", "STRING"),
        # a JSON array string in the source; staged as STRING and parsed on projection
        # (JSON_ARRAY_COLUMNS). The sub-fields are the JSON's keys, in the file's order.
        bigquery.SchemaField(
            "consequences",
            "RECORD",
            mode="REPEATED",
            fields=(
                bigquery.SchemaField("gene_symbol", "STRING"),
                bigquery.SchemaField("gene_id", "STRING"),
                bigquery.SchemaField("consequences", "STRING", mode="REPEATED"),
                bigquery.SchemaField("gene_symbol_source", "STRING"),
                bigquery.SchemaField("canonical", "INT64"),
                bigquery.SchemaField("biotype", "STRING"),
            ),
        ),
        bigquery.SchemaField("genome_or_exome", "STRING", mode="REQUIRED"),
    ],
    # column order must match TSV file exactly (the Open4Gene results the tabix API
    # serves at /peak_to_genes: chrom, start, end, peak_id, gene_id, symbol, cell_type,
    # total_cell_num, expr_cell_num, open_cell_num, hurdle_*). The source chrom is
    # "chr1".."chrX", loaded to the INT64 `chr` column via CHR_STRING_TABLES staging,
    # and cell_type is prefixed ("predicted.celltype.l1.B"), stripped via
    # CELL_TYPE_PREFIX_TABLES so it joins credible_sets.cell_type. `dataset` is not in
    # the file — injected at load with --const-column dataset=FinnGen_ATACseq.
    "peak_to_gene": [
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("peak_start", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("peak_end", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("peak_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gene_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("cell_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("total_cell_num", "INT64"),
        bigquery.SchemaField("expr_cell_num", "INT64"),
        bigquery.SchemaField("open_cell_num", "INT64"),
        bigquery.SchemaField("hurdle_zero_beta", "FLOAT64"),
        bigquery.SchemaField("hurdle_zero_se", "FLOAT64"),
        bigquery.SchemaField("hurdle_zero_z", "FLOAT64"),
        bigquery.SchemaField("hurdle_zero_nlog10p", "FLOAT64"),
        bigquery.SchemaField("hurdle_count_beta", "FLOAT64"),
        bigquery.SchemaField("hurdle_count_se", "FLOAT64"),
        bigquery.SchemaField("hurdle_count_z", "FLOAT64"),
        bigquery.SchemaField("hurdle_count_nlog10p", "FLOAT64"),
        bigquery.SchemaField("hurdle_aic", "FLOAT64"),
        bigquery.SchemaField("hurdle_bic", "FLOAT64"),
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
    ],
    # modes mirror schemas/phenotypes.sql: only the join key, its provenance and the
    # coloc-partner flag are REQUIRED. Names, trait types and sample sizes are genuinely
    # missing for some source rows and must stay NULLABLE rather than be defaulted.
    "phenotypes": [
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait_original", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait_name", "STRING"),
        bigquery.SchemaField("trait_type", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("n_samples", "INT64"),
        bigquery.SchemaField("n_cases", "INT64"),
        bigquery.SchemaField("n_controls", "INT64"),
        bigquery.SchemaField("dataset_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("resource", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("author", "STRING"),
        bigquery.SchemaField("publication_date", "DATE"),
        bigquery.SchemaField("version", "STRING"),
        bigquery.SchemaField("coloc_partner_only", "BOOL", mode="REQUIRED"),
    ],
    # `dataset` is NULLABLE on purpose: a registry entry with no BigQuery presence still
    # gets a row. dataset_ids/resource_aliases are REPEATED, so this table is JSON-loaded.
    "datasets": [
        bigquery.SchemaField("dataset", "STRING"),
        bigquery.SchemaField("dataset_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("dataset_ids", "STRING", mode="REPEATED"),
        bigquery.SchemaField("resource", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("resource_label", "STRING"),
        bigquery.SchemaField("resource_aliases", "STRING", mode="REPEATED"),
        bigquery.SchemaField("version", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("author", "STRING"),
        bigquery.SchemaField("publication_date", "DATE"),
        bigquery.SchemaField("data_type", "STRING"),
        bigquery.SchemaField("trait_type", "STRING"),
        bigquery.SchemaField("n_samples", "INT64"),
        bigquery.SchemaField("pseudo_credible_sets", "BOOL", mode="REQUIRED"),
        bigquery.SchemaField("coloc_partner_only", "BOOL", mode="REQUIRED"),
        bigquery.SchemaField("collection", "BOOL", mode="REQUIRED"),
        bigquery.SchemaField("subdataset_of", "STRING"),
    ],
    # column order must match TSV file exactly (as staged by
    # genetics-results-munge/scripts/munge_rcnv.sh --product scores: symbol,
    # symbol_gencode_v19, ensembl_gene_id, phaplo, ptriplo, haploinsufficient,
    # triplosensitive). All REQUIRED — the staged file carries no NA, so a NULL is a
    # broken munge and must fail the load. The two BOOL columns are the lowercase
    # true/false the CSV loader parses natively; no staging conversion is needed.
    # the direct CSV path is positional, and the two adjacent same-typed pairs below
    # (symbol/symbol_gencode_v19, haploinsufficient/triplosensitive) mean a munge-side
    # column reorder would load silently wrong instead of failing on type.
    "dosage_sensitivity": [
        bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("symbol_gencode_v19", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ensembl_gene_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("phaplo", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("ptriplo", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("haploinsufficient", "BOOL", mode="REQUIRED"),
        bigquery.SchemaField("triplosensitive", "BOOL", mode="REQUIRED"),
    ],
    # column order must match TSV file exactly (as staged by
    # genetics-results-munge/scripts/munge_rcnv.sh --product genes). NA is the null
    # marker: `cohorts_excluded` is NA where no cohort was dropped, and everything from
    # `beta` on is NA for the 65% of rows where the gene was tested but the meta-analysis
    # produced no estimate — those rows load with NULL stats on purpose, so "tested, no
    # estimate" stays distinguishable from "not tested".
    # column order must match the TSVs genetics-results-munge/scripts/munge_asc.py writes
    "exome_gene_counts": [
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gene", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gene_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("gene_start_pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("gene_end_pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("variant_class", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("inheritance_mode", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("n_affected", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("n_unaffected", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("trait_original", "STRING", mode="REQUIRED"),
    ],
    "exome_gene_bayes_results": [
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gene", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gene_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("gene_start_pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("gene_end_pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("bayes_factor", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("fdr", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("qc_flagged", "BOOL", mode="REQUIRED"),
        bigquery.SchemaField("trait_original", "STRING", mode="REQUIRED"),
    ],
    "exome_variant_counts": [
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ref", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("alt", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gene", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gene_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("transcript_id", "STRING"),
        bigquery.SchemaField("consequence", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("variant_class", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("hgvsp", "STRING"),
        bigquery.SchemaField("mpc", "FLOAT64"),
        bigquery.SchemaField("alpha_missense", "FLOAT64"),
        bigquery.SchemaField("is_other_splice", "BOOL", mode="REQUIRED"),
        bigquery.SchemaField("gnomad_af", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("de_novo_ac_proband", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("de_novo_ac_sibling", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("transmitted_ac_proband", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("untransmitted_ac_proband", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ac_case", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ac_ctrl", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("trait", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait_original", "STRING", mode="REQUIRED"),
    ],
    "rcnv_gene_associations": [
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("phenotype", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cnv_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("symbol_gencode_v19", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ensembl_gene_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("n_nominal_cohorts", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("top_cohort", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cohorts_excluded", "STRING"),
        bigquery.SchemaField("case_freq", "FLOAT64"),
        bigquery.SchemaField("control_freq", "FLOAT64"),
        bigquery.SchemaField("beta", "FLOAT64"),
        bigquery.SchemaField("beta_lower", "FLOAT64"),
        bigquery.SchemaField("beta_upper", "FLOAT64"),
        bigquery.SchemaField("z", "FLOAT64"),
        bigquery.SchemaField("mlog10p", "FLOAT64"),
        bigquery.SchemaField("mlog10_fdr_q", "FLOAT64"),
        bigquery.SchemaField("beta_secondary", "FLOAT64"),
        bigquery.SchemaField("beta_lower_secondary", "FLOAT64"),
        bigquery.SchemaField("beta_upper_secondary", "FLOAT64"),
        bigquery.SchemaField("z_secondary", "FLOAT64"),
        bigquery.SchemaField("mlog10p_secondary", "FLOAT64"),
        bigquery.SchemaField("mlog10_fdr_q_secondary", "FLOAT64"),
    ],
    # column order must match TSV file exactly (as staged by
    # genetics-results-munge/scripts/munge_rcnv.sh --product segments). the source TSV's
    # bare `start`/`end` are loaded as `segment_start`/`segment_end` (a table queried by
    # model-written SQL should not need a reserved keyword backticked). NA is the null
    # marker: segment_start/segment_end are NA for the segments with no GRCh38 lift at all,
    # and `credints` is NA where every credible interval of a segment failed to lift. The six
    # ';'-joined list columns load as written and are SPLIT into ARRAY<STRING> by
    # rcnv_segments_v — DERIVED_COLUMNS only materialises columns the TSV does not carry.
    "rcnv_segments": [
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("segment_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cnv_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("segment_start", "INT64"),
        bigquery.SchemaField("segment_end", "INT64"),
        bigquery.SchemaField("segment_start_grch37", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("segment_end_grch37", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("cytoband", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("best_significance", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("control_freq", "FLOAT64"),
        bigquery.SchemaField("case_freq", "FLOAT64"),
        bigquery.SchemaField("beta", "FLOAT64"),
        bigquery.SchemaField("beta_lower", "FLOAT64"),
        bigquery.SchemaField("beta_upper", "FLOAT64"),
        bigquery.SchemaField("beta_min", "FLOAT64"),
        bigquery.SchemaField("beta_max", "FLOAT64"),
        bigquery.SchemaField("n_hpos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("associated_hpos", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("n_credints", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("credints", "STRING"),
        bigquery.SchemaField("credints_grch37", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("credint_size", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("n_genes", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("genes", "STRING"),
        bigquery.SchemaField("genes_gencode_v19", "STRING"),
        bigquery.SchemaField("gene_ensembl_ids", "STRING"),
    ],
    # column order must match TSV file exactly (as staged by
    # genetics-results-munge/scripts/munge_rcnv.sh --product windows). the source `chr` is
    # already a bare integer (autosomes only), so no CHR_STRING_TABLES staging is needed.
    # NA is the null marker, but unlike the gene product it only ever appears in
    # `cohorts_excluded` and the *_secondary columns: rows whose primary statistics are NA
    # are dropped at munge, so every loaded row carries a beta.
    "rcnv_window_associations": [
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("phenotype", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cnv_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("window_start", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("window_end", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("window_start_grch37", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("window_end_grch37", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("n_nominal_cohorts", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("top_cohort", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cohorts_excluded", "STRING"),
        bigquery.SchemaField("case_freq", "FLOAT64"),
        bigquery.SchemaField("control_freq", "FLOAT64"),
        bigquery.SchemaField("beta", "FLOAT64"),
        bigquery.SchemaField("beta_lower", "FLOAT64"),
        bigquery.SchemaField("beta_upper", "FLOAT64"),
        bigquery.SchemaField("z", "FLOAT64"),
        bigquery.SchemaField("mlog10p", "FLOAT64"),
        bigquery.SchemaField("mlog10_fdr_q", "FLOAT64"),
        bigquery.SchemaField("beta_secondary", "FLOAT64"),
        bigquery.SchemaField("beta_lower_secondary", "FLOAT64"),
        bigquery.SchemaField("beta_upper_secondary", "FLOAT64"),
        bigquery.SchemaField("z_secondary", "FLOAT64"),
        bigquery.SchemaField("mlog10p_secondary", "FLOAT64"),
        bigquery.SchemaField("mlog10_fdr_q_secondary", "FLOAT64"),
    ],
    # CELLxGENE release tables, one per file under <release>/bq/ as written by
    # genetics-results-munge scripts/cellxgene/publish.py, whose *_COLUMNS constants are the
    # positional layout. The keys are release-local, so these tables are only ever replaced
    # together (scripts/load_cellxgene.sh, --transaction).
    "cellxgene_summary": [
        bigquery.SchemaField("gene_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("gene_start", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("gene_end", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("gene_name", "STRING"),
        bigquery.SchemaField("source_gene_ids", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cell_type_rollup_ontology_term_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cell_type_rollup", "STRING"),
        bigquery.SchemaField("tissue_general_ontology_term_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tissue_general", "STRING"),
        bigquery.SchemaField("n_cells", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("n_donors", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("n_datasets", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("mean_log1p_cp10k", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("frac_expressing", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("donor_pseudobulk_mean_cp10k", "FLOAT64"),
        bigquery.SchemaField("donor_pseudobulk_n_donors", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("tau", "FLOAT64"),
    ],
    "cellxgene_expression": [
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("gene_start", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("gene_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gene_name", "STRING"),
        bigquery.SchemaField("dataset_key", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("group_key", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("mean_log1p_cp10k", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("frac_expressing", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("donor_pseudobulk_mean_cp10k", "FLOAT64"),
    ],
    "cellxgene_groups": [
        bigquery.SchemaField("group_key", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("dataset_key", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("dataset_version_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("dataset_id", "STRING"),
        bigquery.SchemaField("tissue_ontology_term_id", "STRING"),
        bigquery.SchemaField("tissue", "STRING"),
        bigquery.SchemaField("tissue_type", "STRING"),
        bigquery.SchemaField("tissue_general_ontology_term_id", "STRING"),
        bigquery.SchemaField("tissue_general", "STRING"),
        bigquery.SchemaField("cell_type_ontology_term_id", "STRING"),
        bigquery.SchemaField("cell_type", "STRING"),
        bigquery.SchemaField("cell_type_rollup_ontology_term_id", "STRING"),
        bigquery.SchemaField("cell_type_rollup", "STRING"),
        bigquery.SchemaField("disease_ontology_term_id", "STRING"),
        bigquery.SchemaField("disease", "STRING"),
        bigquery.SchemaField("assay_ontology_term_id", "STRING"),
        bigquery.SchemaField("assay", "STRING"),
        bigquery.SchemaField("suspension_type", "STRING"),
        bigquery.SchemaField("spatial", "BOOL"),
        bigquery.SchemaField("n_cells", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("n_donors", "INT64", mode="REQUIRED"),
    ],
    # `release`, `min_cells` and `min_donors` are not in the file: scripts/load_cellxgene.sh
    # injects them from the manifest as const columns, so they commit with the rows
    "cellxgene_datasets": [
        bigquery.SchemaField("dataset_key", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("dataset_id", "STRING"),
        bigquery.SchemaField("dataset_version_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("collection_id", "STRING"),
        bigquery.SchemaField("collection_name", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("collection_doi", "STRING"),
        bigquery.SchemaField("citation", "STRING"),
        bigquery.SchemaField("cells_kept", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("n_groups", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("n_genes", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("n_genes_measured", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("n_genes_mapped", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("release", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("min_cells", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("min_donors", "INT64", mode="REQUIRED"),
    ],
    "cellxgene_measured_genes": [
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("gene_start", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("gene_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gene_name", "STRING"),
        bigquery.SchemaField("dataset_key", "INT64", mode="REQUIRED"),
    ],
}

# tables whose source writes NULL as an empty field rather than NA. A custom null marker
# makes BigQuery refuse an empty non-STRING field, so these keep the CSV default instead.
EMPTY_NULL_TABLES = {
    "cellxgene_summary",
    "cellxgene_expression",
    "cellxgene_groups",
    "cellxgene_datasets",
    "cellxgene_measured_genes",
}

# tables loaded from NEWLINE_DELIMITED_JSON instead of CSV/TSV (required for
# REPEATED/ARRAY columns, which the CSV loader cannot populate, and for the nullable
# INT64/DATE columns of the metadata tables, which CSV would coerce)
JSON_SCHEMAS = {"gene_annotations", "phenotypes", "datasets"}

# tables whose source TSV encodes the `chr` column as a string but whose BigQuery
# column is INT64. The same file is served to the tabix API, whose seqnames are now
# numeric ("1".."25", X=23/Y=24/M=25), so the source chrom is already numeric. These
# tables are always routed through the staging path: `chr` is loaded as STRING, then
# converted to INT64 on projection. The conversion still tolerates a legacy "chr"
# prefix and X/Y/M spellings so mixed inputs load consistently.
CHR_STRING_TABLES = {
    "open_chromatin",
    "variant_effect",
    "mpra",
    "peak_to_gene",
    "hla_associations",
}

# tables whose source `cell_type` carries the Open4Gene "predicted.celltype." prefix while
# every other table (credible_sets in particular) stores the bare form ("l1.B"). The prefix
# is stripped on projection so peak_to_gene joins credible_sets on cell_type by equality —
# the tabix API strips the same prefix at read time. Routed through the staging path.
CELL_TYPE_PREFIX_TABLES = {"peak_to_gene"}

# tables whose source carries raw JSON in a field. The CSV loader's default quote
# character is the double quote, which JSON is full of, so quoting is switched off and
# every field is taken verbatim. That also means a field cannot contain a tab or newline.
UNQUOTED_TABLES = {"gnomad_variant_annotation"}

CELL_TYPE_STRIP_PREFIX_SQL = "REGEXP_REPLACE({col}, r'^predicted\\.celltype\\.', '')"

# SQL to normalize a chrom string to the INT64 encoding used across the tables:
# X=23, Y=24, M/MT=25 (mirrors chrom_to_int() in build_gene_annotations.py).
# Numeric input (e.g. "23") SAFE_CASTs straight through; any leading "chr" is
# stripped first. Non-matching values become NULL and fail the REQUIRED chr
# constraint, surfacing malformed input rather than dropping it.
CHR_STRING_TO_INT_SQL = (
    "CASE UPPER(REGEXP_REPLACE({col}, r'(?i)^chr', ''))"
    " WHEN 'X' THEN 23"
    " WHEN 'Y' THEN 24"
    " WHEN 'M' THEN 25"
    " WHEN 'MT' THEN 25"
    " ELSE SAFE_CAST(REGEXP_REPLACE({col}, r'(?i)^chr', '') AS INT64)"
    " END"
)


def resource_case_sql(view_name: str) -> str:
    """CASE expression mapping `dataset` -> `resource`, generated from datasets.yaml.

    The mapping used to be a CASE inside the `*_v` view. `credible_sets.resource` is a
    clustering key now, and a view-derived column prunes nothing, so it is materialised
    here instead. Generating the SQL from the shared rules rather than hardcoding it
    keeps datasets.yaml the single place the mapping is written down — a hardcoded copy
    would drift silently and mislabel rows for a whole load.
    """
    import generate_resource_sql as g

    yaml_path = os.environ.get("DATASETS_YAML")
    if not yaml_path:
        local = os.path.join(os.path.dirname(SCRIPT_DIR), "configs", "datasets.yaml")
        yaml_path = local if os.path.exists(local) else g.DEFAULT_YAML
    fragment = g.generate_for_view(g.load_rules(yaml_path), view_name)
    return fragment.removesuffix(" AS resource").strip()


# columns computed at load time from other columns of the same row. Like const columns
# they are absent from the source TSV, are dropped from the staging schema and are filled
# in on projection — so materialising them requires no change to the source files.
# Values are SQL expressions over the STAGING table's columns, or a zero-arg callable
# returning one (used where the expression itself is generated from datasets.yaml).
DERIVED_COLUMNS = {
    "credible_sets": {
        "variant": "CONCAT(`chr`, ':', `pos`, ':', `ref`, ':', `alt`)",
        "resource": lambda: resource_case_sql("credible_sets_v"),
    },
    "gnomad_variant_annotation": {
        "variant": "CONCAT(`chr`, ':', `pos`, ':', `ref`, ':', `alt`)",
    },
}


# ARRAY<STRUCT> columns whose source field is a JSON array of objects. The CSV loader
# cannot populate a REPEATED column, so the field is staged as the raw STRING and parsed
# into the typed array on projection; the SQL is generated from the SCHEMAS sub-fields so
# the keys are written down once.
JSON_ARRAY_COLUMNS = {"gnomad_variant_annotation": {"consequences"}}


def json_array_projection_sql(field: bigquery.SchemaField) -> str:
    """Expression turning the staged JSON string `field.name` into its typed array.

    A NULL source (the file's NA) yields an empty array, which is also all BigQuery can
    store for it. The explicit offset ordering is what keeps the file's element order:
    an ARRAY subquery without ORDER BY promises none.
    """
    parts = []
    for sub in field.fields:
        path = f"'$.{sub.name}'"
        if sub.mode == "REPEATED" and sub.field_type == "STRING":
            expr = f"JSON_VALUE_ARRAY(e, {path})"
        elif sub.mode != "REPEATED" and sub.field_type == "STRING":
            expr = f"JSON_VALUE(e, {path})"
        elif sub.mode != "REPEATED" and sub.field_type in ("INT64", "INTEGER"):
            expr = f"SAFE_CAST(JSON_VALUE(e, {path}) AS INT64)"
        else:
            raise ValueError(
                f"no JSON projection for {field.name}.{sub.name} ({sub.mode} {sub.field_type})"
            )
        parts.append(f"{expr} AS `{sub.name}`")
    return (
        f"ARRAY(SELECT AS STRUCT {', '.join(parts)}"
        f" FROM UNNEST(JSON_QUERY_ARRAY(`{field.name}`)) AS e WITH OFFSET AS o ORDER BY o)"
    )


def json_array_mismatch_sql(field: bigquery.SchemaField) -> str:
    """Predicate true for a staged value the typed array does not reproduce.

    The JSON functions answer NULL rather than fail on a value they cannot read, so a
    malformed string, an unexpected key, a missing key or a wrongly typed value would
    otherwise load as an empty or thinner array. Comparing the parsed source with the
    typed result, both as canonical JSON, catches all of them without listing them. One
    difference passes by design: a null inner list equals the empty list it is stored as.
    """
    return (
        f"`{field.name}` IS NOT NULL AND IFNULL("
        f"TO_JSON_STRING(SAFE.PARSE_JSON(`{field.name}`))"
        f" != TO_JSON_STRING(TO_JSON({json_array_projection_sql(field)})), TRUE)"
    )


def _coerce_const(value: str, bq_type: str):
    """Coerce a string CLI argument to the Python type matching the schema field."""
    t = bq_type.upper()
    if t in ("INT64", "INTEGER"):
        return int(value)
    if t in ("FLOAT64", "FLOAT", "NUMERIC", "BIGNUMERIC"):
        return float(value)
    if t in ("BOOL", "BOOLEAN"):
        return value.strip().lower() in ("true", "1", "yes", "y", "t")
    return value


def _csv_load_config(table_type: str, schema, skip_leading_rows: int, write_disposition):
    config = LoadJobConfig(
        schema=schema,
        source_format=SourceFormat.CSV,
        field_delimiter="\t",
        skip_leading_rows=skip_leading_rows,
        write_disposition=write_disposition,
        allow_quoted_newlines=True,
    )
    if table_type not in EMPTY_NULL_TABLES:
        config.null_marker = "NA"
    if table_type in UNQUOTED_TABLES:
        config.quote_character = ""
        config.allow_quoted_newlines = False
    return config


def _staging_schema(table_type: str, const_columns: dict) -> list:
    """The target schema as the source file lays it out: injected const and derived columns
    dropped, chr-string and JSON-array columns staged as the raw STRING."""
    derived_columns = DERIVED_COLUMNS.get(table_type, {})
    json_array_columns = JSON_ARRAY_COLUMNS.get(table_type, set())
    staging_schema = []
    for f in SCHEMAS[table_type]:
        if f.name in const_columns or f.name in derived_columns:
            continue
        if table_type in CHR_STRING_TABLES and f.name == "chr":
            staging_schema.append(bigquery.SchemaField("chr", "STRING"))
        elif f.name in json_array_columns:
            staging_schema.append(bigquery.SchemaField(f.name, "STRING"))
        else:
            staging_schema.append(f)
    return staging_schema


def _insert_sql(table_type: str, table_id: str, staging_id: str, const_columns: dict,
                param_prefix: str = "const_"):
    """INSERT projecting the staging table into the target, constants filled in.

    DML into the existing table, not a query job with destination=table_id: a
    destination-table WRITE_TRUNCATE replaces the table's schema with the query's
    result schema, silently dropping every NOT NULL mode and column description.
    An INSERT keeps the committed DDL and has BigQuery enforce NOT NULL on the
    loaded rows. `param_prefix` keeps parameter names apart when several of these
    share one script.
    """
    full_schema = SCHEMAS[table_type]
    derived_columns = DERIVED_COLUMNS.get(table_type, {})
    json_array_columns = JSON_ARRAY_COLUMNS.get(table_type, set())
    col_exprs = []
    params = []
    for f in full_schema:
        if f.name in const_columns:
            pname = f"{param_prefix}{f.name}"
            py_value = _coerce_const(const_columns[f.name], f.field_type)
            col_exprs.append(f"@{pname} AS `{f.name}`")
            params.append(bigquery.ScalarQueryParameter(pname, f.field_type, py_value))
        elif f.name in derived_columns:
            expr = derived_columns[f.name]
            if callable(expr):
                expr = expr()
            col_exprs.append(f"({expr}) AS `{f.name}`")
        elif table_type in CHR_STRING_TABLES and f.name == "chr":
            col_exprs.append(CHR_STRING_TO_INT_SQL.format(col="`chr`") + " AS `chr`")
        elif f.name in json_array_columns:
            col_exprs.append(f"{json_array_projection_sql(f)} AS `{f.name}`")
        elif table_type in CELL_TYPE_PREFIX_TABLES and f.name == "cell_type":
            col_exprs.append(
                CELL_TYPE_STRIP_PREFIX_SQL.format(col="`cell_type`") + " AS `cell_type`"
            )
        else:
            col_exprs.append(f"`{f.name}`")
    columns = ", ".join(f"`{f.name}`" for f in full_schema)
    sql = (
        f"INSERT INTO `{table_id}` ({columns})\n"
        f"SELECT {', '.join(col_exprs)} FROM `{staging_id}`"
    )
    return sql, params


def load_table(
    client: bigquery.Client,
    gcs_uri: str,
    table_id: str,
    table_type: str,
    write_disposition: str = "WRITE_APPEND",
    skip_leading_rows: int = 1,
    const_columns: dict | None = None,
    replace_chr: int | None = None,
    expect_rows: int | None = None,
):
    """Load a GCS file into a BigQuery table.

    When `const_columns` is empty/None: loads directly with the full schema
    (original behavior). When provided: the source file is expected to omit
    those columns, and the values are injected via a staging-table indirection.

    Returns (job, rows_written) for the job that produced rows in the target
    table. `rows_written` is None on the direct path, where the job has not been
    awaited yet and the caller reads it off the completed LoadJob; the staging
    path has necessarily already awaited its jobs and returns the INSERT's
    `num_dml_affected_rows`.

    `replace_chr` makes a per-chromosome load rerunnable: the target's rows for that
    chromosome are deleted before the INSERT, after the staged rows have been checked
    to hold no other chromosome. A JSON_ARRAY_COLUMNS value that does not parse into
    its typed array is refused at the same point. `expect_rows` refuses a staged row count other than
    the one given, before the target is touched. Both need the staging table, so they
    are refused on the direct path. The DELETE and the INSERT are separate statements:
    a failure between them leaves the chromosome empty until the load is run again.

    The staging path never creates the target: it inserts into the table
    `scripts/setup_bigquery.sh` made from `schemas/*.sql`, which is where the
    NOT NULL modes and column descriptions come from.
    """

    if table_type not in SCHEMAS:
        raise ValueError(f"Unknown table type: {table_type}. Must be one of {list(SCHEMAS.keys())}")

    full_schema = SCHEMAS[table_type]
    const_columns = const_columns or {}
    convert_chr = table_type in CHR_STRING_TABLES
    strip_cell_type_prefix = table_type in CELL_TYPE_PREFIX_TABLES
    derived_columns = DERIVED_COLUMNS.get(table_type, {})
    json_array_columns = JSON_ARRAY_COLUMNS.get(table_type, set())

    if derived_columns and convert_chr:
        # derived expressions read the STAGING columns, where `chr` is still the raw
        # source string for these tables — a `variant` built from it would be wrong
        raise ValueError(
            f"table '{table_type}' has derived columns and chr-string staging; the "
            f"derived expressions would see the unconverted `chr`"
        )

    overlap = sorted(set(const_columns) & set(derived_columns))
    if overlap:
        raise ValueError(
            f"--const-column cannot override computed columns of '{table_type}': {overlap}"
        )

    if table_type in JSON_SCHEMAS and (
        const_columns
        or convert_chr
        or strip_cell_type_prefix
        or derived_columns
        or json_array_columns
    ):
        # const-column injection / column rewrites rely on a CSV staging table;
        # they are not wired up for the JSON load path (no current JSON table needs it)
        raise ValueError(
            f"staging-path load (const-column / column rewrite) is not supported "
            f"for JSON-loaded table '{table_type}'"
        )

    needs_staging = (
        bool(const_columns)
        or convert_chr
        or strip_cell_type_prefix
        or bool(derived_columns)
        or bool(json_array_columns)
    )

    if (replace_chr is not None or expect_rows is not None) and not needs_staging:
        raise ValueError(
            f"replace_chr / expect_rows need the staging path, which '{table_type}' does not take"
        )
    if replace_chr is not None and convert_chr:
        # the single-chromosome check reads the staged `chr`, a raw string for these tables
        raise ValueError(f"replace_chr is not supported for chr-string table '{table_type}'")
    if replace_chr is not None and write_disposition != "WRITE_APPEND":
        raise ValueError("replace_chr replaces one chromosome and requires WRITE_APPEND")

    if not needs_staging:
        # direct-load path: no constant columns to inject, no chr conversion
        if table_type in JSON_SCHEMAS:
            # ARRAY/REPEATED columns require JSON; CSV/TSV cannot carry them
            job_config = LoadJobConfig(
                schema=full_schema,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=getattr(WriteDisposition, write_disposition),
            )
        else:
            job_config = _csv_load_config(
                table_type,
                full_schema,
                skip_leading_rows,
                getattr(WriteDisposition, write_disposition),
            )
        print(f"Loading {gcs_uri} into {table_id}...")
        # left un-awaited so the caller's error handling can report job.errors
        return client.load_table_from_uri(gcs_uri, table_id, job_config=job_config), None

    schema_by_name = {f.name: f for f in full_schema}
    unknown = sorted(set(const_columns) - set(schema_by_name))
    if unknown:
        raise ValueError(
            f"--const-column references columns not in schema '{table_type}': {unknown}"
        )

    staging_schema = _staging_schema(table_type, const_columns)
    staging_id = f"{table_id}__staging_{uuid.uuid4().hex[:8]}"

    try:
        target = client.get_table(table_id)
    except NotFound:
        raise RuntimeError(
            f"target table {table_id} does not exist: create it with "
            f"scripts/setup_bigquery.sh first (a table the loader created would "
            f"carry neither NOT NULL modes nor column descriptions)"
        ) from None
    if write_disposition not in ("WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"):
        raise ValueError(f"unknown write disposition {write_disposition!r}")
    if write_disposition == "WRITE_EMPTY" and target.num_rows:
        raise RuntimeError(f"WRITE_EMPTY: {table_id} already holds {target.num_rows} rows")

    load_config = _csv_load_config(
        table_type, staging_schema, skip_leading_rows, WriteDisposition.WRITE_TRUNCATE
    )
    print(f"Loading {gcs_uri} into staging {staging_id}...")
    load_job = client.load_table_from_uri(gcs_uri, staging_id, job_config=load_config)
    load_job.result()
    staged_rows = load_job.output_rows
    print(f"  staged {staged_rows} rows")

    try:
        if expect_rows is not None and staged_rows != expect_rows:
            raise RuntimeError(
                f"staged {staged_rows} rows from {gcs_uri}, expected {expect_rows}: "
                f"{table_id} left untouched"
            )
        # every refusal is counted in one pass over the staging table, and before the
        # target is modified: after the DELETE below there is nothing left to protect
        refusals = {}
        if replace_chr is not None:
            # a stray chromosome would be appended again on every rerun, because the
            # DELETE below only clears the one named
            refusals[f"are not on chr {replace_chr}"] = (
                f"`chr` IS DISTINCT FROM {int(replace_chr)}"
            )
        for f in full_schema:
            if f.name in json_array_columns:
                refusals[f"carry a `{f.name}` value the typed array does not reproduce"] = (
                    json_array_mismatch_sql(f)
                )
        if refusals:
            counts = next(
                iter(
                    client.query(
                        f"SELECT {', '.join(f'COUNTIF({sql})' for sql in refusals.values())}"
                        f" FROM `{staging_id}`"
                    ).result()
                )
            )
            for reason, count in zip(refusals, counts):
                if count:
                    raise RuntimeError(
                        f"{count} staged rows from {gcs_uri} {reason}: {table_id} left untouched"
                    )

        sql, params = _insert_sql(table_type, table_id, staging_id, const_columns)
        query_config = bigquery.QueryJobConfig(query_parameters=params)
        annotations = [f"{k}={v!r}" for k, v in const_columns.items()]
        annotations += [f"computed {name}" for name in derived_columns]
        annotations += [f"parsed {name}" for name in sorted(json_array_columns)]
        if write_disposition == "WRITE_TRUNCATE":
            print(f"Truncating {table_id}...")
            client.query(f"TRUNCATE TABLE `{table_id}`").result()
        if replace_chr is not None:
            print(f"Deleting chr {replace_chr} from {table_id}...")
            client.query(
                f"DELETE FROM `{table_id}` WHERE `chr` = {int(replace_chr)}"
            ).result()
        print(f"Projecting staging -> {table_id} ({', '.join(annotations)})...")
        query_job = client.query(sql, job_config=query_config)
        query_job.result()
        return query_job, query_job.num_dml_affected_rows
    finally:
        client.delete_table(staging_id, not_found_ok=True)
        print(f"  dropped staging {staging_id}")


# a staging table outlives a run only when the process is killed outright (SIGKILL, a lost VM);
# the expiry bounds what that leaves behind
STAGING_EXPIRY = datetime.timedelta(hours=24)

# source header spellings that differ from the column name, for the header check
HEADER_ALIASES = {
    "cellxgene_summary": {"chr": "chrom"},
    "cellxgene_expression": {"chr": "chrom"},
    "cellxgene_measured_genes": {"chr": "chrom"},
}


def read_gcs_header(uri: str) -> list[str]:
    """The first line of a gs:// file, split on tabs; gzip is detected by its magic bytes.
    Reads a 64 KiB prefix, not the file."""
    import zlib
    from urllib.parse import quote

    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    bucket, name = uri.removeprefix("gs://").split("/", 1)
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/devstorage.read_only"])
    r = AuthorizedSession(creds).get(
        f"https://storage.googleapis.com/storage/v1/b/{bucket}/o/{quote(name, safe='')}",
        params={"alt": "media"}, headers={"Range": "bytes=0-65535"}, timeout=60)
    r.raise_for_status()
    data = r.content
    if data[:2] == b"\x1f\x8b":
        data = zlib.decompressobj(16 + zlib.MAX_WBITS).decompress(data)
    return data.split(b"\n", 1)[0].decode().rstrip("\r").split("\t")


def load_tables_transactional(client: bigquery.Client, loads: list[dict], skip_leading_rows: int = 1,
                              read_header=None):
    """Replace every target in `loads` with its file(s), all or none, so a reader never sees a
    half-loaded release or one table from the old release joined to another from the new.

    Each load is {"table_id", "table_type", "gcs_uri" (one URI or a list of shards),
    "expect_rows"?, "const_columns"?}. Before anything is staged, every file's header row must
    equal the staged columns in order: the load itself maps columns by position, so a swapped
    or extra column would otherwise load silently. Then every file is staged, and a staging
    failure or a row count other than `expect_rows` stops the run with no target touched.
    Last, one multi-statement transaction DELETEs each target and INSERTs its staged rows; it
    re-counts each target before COMMIT, and any error rolls the whole transaction back.
    On any exit, in-flight jobs are cancelled (best effort) and staging tables dropped.
    Returns {table_id: rows}.
    """
    read_header = read_header or read_gcs_header
    ids = [ld["table_id"] for ld in loads]
    if not loads or len(set(ids)) != len(ids):
        raise ValueError(f"a transaction needs distinct target tables, got {ids}")
    for ld in loads:
        t = ld["table_type"]
        if t not in SCHEMAS:
            raise ValueError(f"Unknown table type: {t}. Must be one of {list(SCHEMAS.keys())}")
        if t in JSON_SCHEMAS or t in JSON_ARRAY_COLUMNS:
            # their refusals and JSON staging are wired into load_table only
            raise ValueError(f"table '{t}' cannot be loaded in a transaction")
        unknown = sorted(set(ld.get("const_columns") or {}) - {f.name for f in SCHEMAS[t]})
        if unknown:
            raise ValueError(f"--const-column references columns not in schema '{t}': {unknown}")
        if isinstance(ld["gcs_uri"], str):
            ld["gcs_uri"] = [ld["gcs_uri"]]
        try:
            client.get_table(ld["table_id"])
        except NotFound:
            raise RuntimeError(
                f"target table {ld['table_id']} does not exist: create it with "
                f"scripts/setup_bigquery.sh first"
            ) from None

    if skip_leading_rows == 1:
        for ld in loads:
            t = ld["table_type"]
            aliases = HEADER_ALIASES.get(t, {})
            want = [aliases.get(f.name, f.name) for f in _staging_schema(t, ld.get("const_columns") or {})]
            for uri in ld["gcs_uri"]:
                got = read_header(uri)
                if got != want:
                    raise RuntimeError(
                        f"{uri} header {got} is not {want} (columns load by position): "
                        f"no table touched"
                    )

    staged, inflight = [], []

    def run(job):
        inflight.append(job)
        job.result()
        inflight.remove(job)
        return job

    try:
        expires = datetime.datetime.now(datetime.timezone.utc) + STAGING_EXPIRY
        for ld in loads:
            t, consts = ld["table_type"], ld.get("const_columns") or {}
            schema = _staging_schema(t, consts)
            staging = bigquery.Table(f"{ld['table_id']}__staging_{uuid.uuid4().hex[:8]}", schema=schema)
            staging.expires = expires
            client.create_table(staging)
            staged.append(staging.reference)
            config = _csv_load_config(t, schema, skip_leading_rows, WriteDisposition.WRITE_APPEND)
            print(f"Loading {len(ld['gcs_uri'])} file(s) from {ld['gcs_uri'][0]} into staging {staging.table_id}...")
            job = run(client.load_table_from_uri(ld["gcs_uri"], staging.reference, job_config=config))
            ld["staged_rows"] = job.output_rows
            print(f"  staged {job.output_rows} rows")
            if ld.get("expect_rows") is not None and job.output_rows != ld["expect_rows"]:
                raise RuntimeError(
                    f"staged {job.output_rows} rows from {ld['gcs_uri'][0]}..., expected "
                    f"{ld['expect_rows']}: no table touched"
                )

        statements, params = [], []
        for i, (ld, ref) in enumerate(zip(loads, staged)):
            sql, p = _insert_sql(
                ld["table_type"], ld["table_id"], f"{ref.project}.{ref.dataset_id}.{ref.table_id}",
                ld.get("const_columns") or {}, param_prefix=f"t{i}_",
            )
            params += p
            statements += [f"DELETE FROM `{ld['table_id']}` WHERE TRUE", sql]
        for ld in loads:
            # inside the transaction a COUNT sees its own writes, so a short INSERT rolls
            # everything back instead of being reported after the commit
            statements.append(
                f"IF (SELECT COUNT(*) FROM `{ld['table_id']}`) != {int(ld['staged_rows'])} THEN "
                f"RAISE USING MESSAGE = '{ld['table_id']} does not hold the "
                f"{int(ld['staged_rows'])} staged rows'; END IF"
            )
        body = ";\n  ".join(statements)
        script = (
            "BEGIN\n"
            "  BEGIN TRANSACTION;\n"
            f"  {body};\n"
            "  COMMIT TRANSACTION;\n"
            "EXCEPTION WHEN ERROR THEN\n"
            "  ROLLBACK TRANSACTION;\n"
            "  RAISE USING MESSAGE = @@error.message;\n"
            "END;"
        )
        print(f"Replacing {', '.join(ids)} in one transaction...")
        # once submitted, the script runs server-side: a kill here cancels it best effort, but
        # if the COMMIT has already gone through the tables hold the new release while the
        # caller saw a failure (no CURRENT flip), so BigQuery can be ahead of CURRENT until
        # the next successful run
        run(client.query(script, job_config=bigquery.QueryJobConfig(query_parameters=params)))
        return {ld["table_id"]: ld["staged_rows"] for ld in loads}
    finally:
        for job in inflight:
            try:
                job.cancel()
                print(f"  cancelled {job.job_id}")
            except Exception as e:  # noqa: BLE001  best effort on the way out
                print(f"  could not cancel {getattr(job, 'job_id', job)}: {e}")
        for ref in staged:
            client.delete_table(ref, not_found_ok=True)
            print(f"  dropped staging {ref.table_id}")


def main():
    parser = argparse.ArgumentParser(description="Load genetics data from GCS into BigQuery")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--dataset", default="genetics_results", help="BigQuery dataset ID")
    parser.add_argument("--table", help="Target table")
    parser.add_argument("--schema", choices=list(SCHEMAS.keys()), help="Schema key (defaults to --table value)")
    parser.add_argument("--gcs-uri", help="GCS URI (gs://bucket/path/*.tsv.gz)")
    parser.add_argument(
        "--transaction",
        metavar="SPEC.json",
        help=(
            "Replace several tables all-or-none instead of loading one: a JSON list of "
            '{"table", "gcs_uri", "schema"?, "expect_rows"?, "const_columns"?: {NAME: VALUE}}. '
            "Every file is staged and counted first, then each target's rows are DELETEd and "
            "the staged rows INSERTed in one multi-statement transaction. `-` reads stdin; "
            "`gcs_uri` may be a list of shards. Excludes --table, "
            "--gcs-uri and the single-table options."
        ),
    )
    parser.add_argument(
        "--write-disposition",
        default="WRITE_APPEND",
        choices=["WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"],
        help="How to handle existing data",
    )
    parser.add_argument("--skip-rows", type=int, default=1, help="Header rows to skip")
    parser.add_argument(
        "--const-column",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help=(
            "Inject a constant value for a schema column that is absent from the source "
            "file. Repeatable. Triggers a staging-table load: data is first loaded into a "
            "temporary table with the truncated schema, then projected into the target with "
            "the constants filled in. Example: --const-column dataset=deCODE_asmQTL_CpG"
        ),
    )

    parser.add_argument(
        "--replace-chr",
        type=int,
        metavar="N",
        help=(
            "Delete the target's rows for chromosome N before inserting, so a "
            "per-chromosome load can be rerun without duplicating rows. The source "
            "must hold that chromosome only. Staging-path tables, WRITE_APPEND only."
        ),
    )
    parser.add_argument(
        "--expect-rows",
        type=int,
        metavar="N",
        help="Fail before touching the target unless exactly N rows were staged.",
    )

    args = parser.parse_args()

    if args.transaction:
        if args.table or args.gcs_uri or args.schema or args.const_column or args.replace_chr is not None \
                or args.expect_rows is not None or args.write_disposition != "WRITE_APPEND":
            parser.error("--transaction takes every table from its spec; drop the single-table options")
        if args.transaction == "-":
            spec = json.load(sys.stdin)
        else:
            with open(args.transaction) as fh:
                spec = json.load(fh)
        loads = [
            {
                "table_id": f"{args.project}.{args.dataset}.{e['table']}",
                "table_type": e.get("schema", e["table"]),
                "gcs_uri": e["gcs_uri"],
                "expect_rows": e.get("expect_rows"),
                "const_columns": {k: str(v) for k, v in (e.get("const_columns") or {}).items()},
            }
            for e in spec
        ]
        client = bigquery.Client(project=args.project)
        # SIGTERM (the driver's own SIGTERM, a cancelled Batch job) unwinds through the
        # finally that cancels in-flight jobs and drops the staging tables
        signal.signal(signal.SIGTERM, lambda signum, frame: sys.exit(128 + signum))
        try:
            rows = load_tables_transactional(client, loads, args.skip_rows)
        except Exception as e:
            print(f"Error loading data: {e}")
            sys.exit(1)
        for table_id, n in rows.items():
            print(f"Loaded {n} rows into {table_id}")
        return

    if not args.table or not args.gcs_uri:
        parser.error("--table and --gcs-uri are required (or --transaction)")

    const_columns: dict[str, str] = {}
    for spec in args.const_column:
        if "=" not in spec:
            parser.error(f"--const-column expects NAME=VALUE, got: {spec!r}")
        name, _, value = spec.partition("=")
        name = name.strip()
        if not name:
            parser.error(f"--const-column has empty NAME: {spec!r}")
        const_columns[name] = value

    schema_key = args.schema or args.table
    if schema_key not in SCHEMAS:
        parser.error(f"Unknown schema '{schema_key}'. Must be one of {list(SCHEMAS.keys())}")

    client = bigquery.Client(project=args.project)
    table_id = f"{args.project}.{args.dataset}.{args.table}"

    job, rows = load_table(
        client,
        args.gcs_uri,
        table_id,
        schema_key,
        args.write_disposition,
        args.skip_rows,
        const_columns,
        args.replace_chr,
        args.expect_rows,
    )

    # wait for job to complete (a no-op when load_table already awaited)
    try:
        job.result()
        if rows is None:
            rows = job.output_rows
        print(f"Loaded {rows} rows into {table_id}")
    except Exception as e:
        print(f"Error loading data: {e}")
        if job.errors:
            for error in job.errors:
                print(f"  - {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
