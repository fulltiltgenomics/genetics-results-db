# genetics-results-db - Project specification

## Introduction

genetics-results-db is a BigQuery-based database solution for storing and querying genetics fine-mapping, colocalization, and exome sequencing results. It provides a REST API for AI agents and applications to query credible set variants, colocalization analysis results, exome variant associations, gene burden tests, and related genetics data across multiple datasets including FinnGen, Open Targets, eQTL Catalogue, and GeneBASS.

## Purpose and Goals

- Provide a scalable, serverless database for hundreds of millions of rows of genetics results data
- Enable SQL-based querying of fine-mapped credible sets and colocalization results
- Support AI agent workflows with a simple REST API
- Minimize operational overhead and cost through BigQuery's pay-per-query model
- Keep infrastructure simple and reproducible with shell scripts and standard GCP tooling

## Key Features

- BigQuery tables with partitioning by chromosome and clustering by dataset/data_type for typical queries
- REST API (FastAPI) with human/agent usable endpoints for SQL queries, schema discovery, and statistics
- Shared-secret authentication on every endpoint except `/health`
- Query authorization via a BigQuery dry run: only single `SELECT` statements over the exposed views are executed (though read-only IAM is recommended in any case)
- Cost controls via configurable bytes-billed limits and dry-run support
- Direct loading of tsv.gz files from GCS with schema validation
- Unqualified table names resolved by BigQuery via the job config's `default_dataset`, so callers may write `FROM credible_sets_v` (base table names are exposed too)

## Architecture

```
GCS (tsv.gz files)
      ↓ (one-time load via bq load)
BigQuery Dataset
  ├── credible_sets (partitioned by chr, clustered by data_type, resource, variant, pos)
  │   └── credible_sets_v (view: adds maf; variant and resource are stored columns)
  ├── colocalization (partitioned by chr, clustered by dataset pairs)
  │   └── colocalization_v (view: adds resource columns)
  ├── coloc_credsets (partitioned by chr, clustered by dataset, data_type)
  │   └── coloc_credsets_v (view: adds variant, resource columns)
  ├── exome_variant_results (partitioned by chr, clustered by dataset, gene, trait)
  │   └── exome_variant_results_v (view: adds variant, resource columns)
  ├── gene_burden_results (partitioned by chr, clustered by dataset, gene, trait)
  │   └── gene_burden_results_v (view: adds resource column)
  ├── asm_qtl (partitioned by chr, clustered by dataset, gene_most_severe, most_severe)
  │   └── asm_qtl_v (view: adds variant, maf, resource columns)
  ├── gene_annotations (unpartitioned reference table, clustered by symbol)
  │   └── gene_annotations_v (view: adds resource column)
  ├── open_chromatin (partitioned by chr, clustered by dataset, tissue, cell_type)
  │   └── open_chromatin_v (view: adds resource column)
  ├── variant_effect (partitioned by chr, clustered by dataset, tissue, model)
  │   └── variant_effect_v (view: adds resource column)
  ├── mpra (partitioned by chr, clustered by dataset, cell_line)
  │   └── mpra_v (view: adds resource column)
  ├── variant_annotation (partitioned by chr, clustered by most_severe, gene_most_severe)
  │   └── variant_annotation_v (view: adds constant resource='finngen')
  ├── peak_to_gene (unpartitioned link table, clustered by symbol, cell_type, peak_id)
  │   └── peak_to_gene_v (view: adds resource column)
  ├── hla_associations (unpartitioned, clustered by phenotype, gene, allele)
  │   └── hla_associations_v (view: adds resource column, mapped to 'finngen')
  ├── dosage_sensitivity (unpartitioned reference table, clustered by symbol)
  │   └── dosage_sensitivity_v (view: adds constant resource='rcnv')
  ├── rcnv_gene_associations (unpartitioned, clustered by phenotype, cnv_type, ensembl_gene_id)
  │   └── rcnv_gene_associations_v (view: adds resource column mapped to 'rcnv', LEFT JOINs dosage_sensitivity)
  ├── rcnv_segments (unpartitioned, clustered by chr, segment_start)
  │   └── rcnv_segments_v (view: adds resource column mapped to 'rcnv', SPLITs the six ';'-joined lists into ARRAY<STRING>)
  ├── rcnv_window_associations (partitioned by chr, clustered by phenotype, cnv_type, window_start)
  │   └── rcnv_window_associations_v (view: adds resource column mapped to 'rcnv'; no join — a window has no gene)
  ├── phenotypes (unpartitioned metadata table, clustered by dataset, trait_original)
  │   └── phenotypes_v (view: pass-through — resource is already a registry column)
  └── datasets (unpartitioned metadata table, clustered by dataset, resource)
      └── datasets_v (view: pass-through — resource is already a registry column)
      ↓
API (FastAPI) — exposes only views, not underlying tables
      ↓
AI Agents / Applications
```

## Data Model

### credible_sets

Fine-mapped credible set variants from multiple genetics datasets.

`resource` and `variant` are **stored** columns here, unlike in the other product tables
where `resource` is a view-derived `CASE`. They are the clustering keys, and a view-derived
column prunes nothing — before the change, `WHERE resource = 'finngen'` on the view scanned
*more* than an unfiltered scan, because the `CASE` forced an extra read of `dataset`.
Clustering on the two columns callers actually filter by cut the weighted cost of the
commonest logged query shapes by 87%. Consequences: `resource` is materialised by
`scripts/load_data.py` (`DERIVED_COLUMNS`) from the `datasets.yaml` rules, so a mapping-rule
change needs a reload or backfill rather than just re-creating the view; and filtering by
`dataset`, `gene_most_severe` or `most_severe` is now slower than filtering by `resource`.
See [credible-sets-clustering-swap.md](credible-sets-clustering-swap.md).

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| dataset | STRING | Yes | Source dataset (FinnGen_R14, Open_Targets_26.06, etc.) |
| resource | STRING | Yes | Resource identifier (lowercase), derived from `dataset` at load time. Clustering key — filter on this, not `dataset` |
| data_type | STRING | Yes | GWAS, eQTL, pQTL, sQTL, caQTL |
| trait | STRING | Yes | Phenotype/trait name. For `caQTL` rows this is a chromatin peak id (`chr-start-end`), never a gene — reach genes via [peak_to_gene](#peak_to_gene) |
| trait_original | STRING | Yes | Original trait name |
| cell_type | STRING | No | Cell/tissue type (null for GWAS) |
| chr | INT64 | Yes | Chromosome |
| pos | INT64 | Yes | Position |
| ref | STRING | Yes | Reference allele |
| alt | STRING | Yes | Alternate allele |
| variant | STRING | Yes | Variant identifier (chr:pos:ref:alt), computed at load time. Clustering key |
| mlog10p | FLOAT64 | No | -log10(p-value) |
| beta | FLOAT64 | No | Effect size |
| se | FLOAT64 | No | Standard error |
| pip | FLOAT64 | Yes | Posterior inclusion probability |
| cs_id | STRING | Yes | Credible set ID |
| cs_size | INT64 | Yes | Credible set size |
| cs_min_r2 | FLOAT64 | No | Minimum R² between variants in credible set |
| aaf | FLOAT64 | No | Alternate allele frequency |
| maf | FLOAT64 | No | Minor allele frequency (view only, derived as LEAST(aaf, 1-aaf)) |
| most_severe | STRING | No | Most severe variant consequence |
| gene_most_severe | STRING | No | Gene with most severe consequence |

### colocalization

Colocalization analysis results between associations from different datasets.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| dataset1 | STRING | Yes | First dataset name |
| dataset2 | STRING | Yes | Second dataset name |
| data_type1 | STRING | Yes | First dataset type |
| data_type2 | STRING | Yes | Second dataset type |
| trait1 | STRING | Yes | First trait name |
| trait1_original | STRING | Yes | First original trait name |
| trait2 | STRING | No | Second trait name |
| trait2_original | STRING | Yes | Second original trait name |
| cell_type1 | STRING | No | First cell/tissue type |
| cell_type2 | STRING | No | Second cell/tissue type |
| cs1_id | STRING | Yes | First credible set ID |
| cs2_id | STRING | Yes | Second credible set ID |
| hit1 | STRING | Yes | Lead variant in first credible set |
| hit2 | STRING | Yes | Lead variant in second credible set |
| hit1_beta | FLOAT64 | Yes | Effect size of lead variant in first set |
| hit1_mlog10p | FLOAT64 | Yes | -log10(p-value) of lead variant in first set |
| hit2_beta | FLOAT64 | No | Effect size of lead variant in second set |
| hit2_mlog10p | FLOAT64 | No | -log10(p-value) of lead variant in second set |
| chr | INT64 | Yes | Chromosome |
| region_start_min | INT64 | Yes | Region start position |
| region_end_max | INT64 | Yes | Region end position |
| PP_H0_abf | FLOAT64 | Yes | Posterior probability H0: no association in either |
| PP_H1_abf | FLOAT64 | Yes | Posterior probability H1: association in dataset 1 only |
| PP_H2_abf | FLOAT64 | Yes | Posterior probability H2: association in dataset 2 only |
| PP_H3_abf | FLOAT64 | Yes | Posterior probability H3: both associated, different variants |
| PP_H4_abf | FLOAT64 | Yes | Posterior probability H4: both associated, shared variant |
| nsnps | INT64 | Yes | Number of SNPs in region |
| nsnps1 | INT64 | Yes | Number of SNPs in first trait region |
| nsnps2 | INT64 | Yes | Number of SNPs in second trait region |
| cs1_log10bf | FLOAT64 | Yes | Log10 Bayes factor for first credible set |
| cs2_log10bf | FLOAT64 | Yes | Log10 Bayes factor for second credible set |
| clpp | FLOAT64 | No | Causal posterior probability |
| clpa | FLOAT64 | No | Causal posterior agreement |
| cs1_size | INT64 | Yes | First credible set size |
| cs2_size | INT64 | Yes | Second credible set size |
| cs_overlap | INT64 | Yes | Number of overlapping variants in the credible sets |
| topInOverlap | STRING | Yes | Whether the top variant is in the credible set overlap |

### coloc_credsets

Variants belonging to colocalized credible sets.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| dataset | STRING | Yes | Source dataset |
| data_type | STRING | Yes | Data type |
| trait | STRING | Yes | Trait name |
| trait_original | STRING | Yes | Original trait name |
| cell_type | STRING | No | Cell/tissue type |
| chr | INT64 | Yes | Chromosome |
| pos | INT64 | Yes | Position |
| ref | STRING | Yes | Reference allele |
| alt | STRING | Yes | Alternate allele |
| mlog10p | FLOAT64 | No | -log10(p-value) |
| beta | FLOAT64 | No | Effect size |
| se | FLOAT64 | No | Standard error |
| pip | FLOAT64 | Yes | Posterior inclusion probability |
| cs_id | STRING | Yes | Credible set ID |

### exome_variant_results

Variant-level association results from exome sequencing studies (GeneBASS, IBD exome). All filtered to mlog10p > 4. Data files are in `exome_results/` on GCS.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| dataset | STRING | Yes | Source dataset (genebass, IBD_exome) |
| chr | INT64 | Yes | Chromosome |
| pos | INT64 | Yes | Position |
| ref | STRING | Yes | Reference allele |
| alt | STRING | Yes | Alternate allele |
| gene | STRING | Yes | Gene symbol |
| annotation | STRING | Yes | Variant annotation (pLoF, missense, synonymous, splice_region_variant, etc.) |
| mlog10p | FLOAT64 | Yes | -log10(p-value) |
| beta | FLOAT64 | Yes | Effect size |
| se | FLOAT64 | No | Standard error |
| af_overall | FLOAT64 | Yes | Allele frequency overall |
| af_cases | FLOAT64 | No | Allele frequency in cases |
| af_controls | FLOAT64 | No | Allele frequency in controls |
| ac | INT64 | Yes | Allele count |
| an | INT64 | No | Allele number |
| n_cases | INT64 | No | Number of cases (may be NA) |
| n_controls | INT64 | No | Number of controls (may be NA) |
| trait | STRING | Yes | Trait identifier |
| trait_original | STRING | Yes | Original trait name in the respective dataset |

### gene_burden_results

Gene-level burden test results from exome sequencing studies (GeneBASS, BipEx2, IBD exome, SCHEMA2, BRaVa). **Unfiltered** — every gene x annotation x trait combination is here, so a query can ask for one gene in one trait regardless of significance. GeneBASS alone is ~343M rows, loaded from the ~4.5k per-trait files in `exome_results/genebass/gene_burden_per_trait/`; BRaVa likewise comes from per-trait files in `exome_results/brava/gene_burden_per_trait/`, one per phenocode with the ancestry stratum in the name (`AFib|EUR`); the other datasets come from their full `.munged.tsv.gz`. Clustering on `dataset, gene, trait` keeps single-gene lookups cheap despite the size.

The tabix API is filtered differently: `/gene_based/{gene}` reads a combined mlog10p_burden > 4 file for GeneBASS (returning every trait of one gene unfiltered would be ~18k rows), while `/gene_based_results_by_phenotype/{resource}/{trait}` serves the same unfiltered per-trait files this table is loaded from. Data files are in `exome_results/` on GCS.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| dataset | STRING | Yes | Source dataset (genebass, BipEx2, IBD_exome, SCHEMA2) — mirrors `schemas/gene_burden_results.sql`'s column description, left stale by design rather than ALTERed against a live 343M-row table; BRaVa is a fifth value, see `gene_burden_results_v`'s `resource` CASE and `configs/datasets.yaml` for the live list |
| trait | STRING | Yes | Trait identifier |
| gene | STRING | Yes | Gene symbol |
| gene_id | STRING | Yes | Ensembl gene ID |
| chr | INT64 | Yes | Chromosome |
| gene_start_pos | INT64 | Yes | Gene start position |
| gene_end_pos | INT64 | Yes | Gene end position |
| annotation | STRING | Yes | Annotation category (pLoF, nonsynonymous, etc.) |
| mlog10p_burden | FLOAT64 | Yes | -log10(p-value) for burden test |
| beta | FLOAT64 | Yes | Effect size |
| se | FLOAT64 | No | Standard error. NULL where the burden test returned no estimate (beta 0, p 1) |
| total_variants | INT64 | No | Number of variants in gene |
| total_variants_pheno | INT64 | No | Number of variants in gene for this trait |
| n_cases | INT64 | Yes | Number of cases, or number of samples for quantitative traits |
| n_controls | INT64 | No | Number of controls (NULL for quantitative traits) |
| trait_original | STRING | Yes | Original trait name in the respective dataset |
| flags | STRING | No | Quality or analysis flags (NA if none) |

### asm_qtl

Allele-specific methylation QTL results from deCODE (Stefansson et al. 2024), from Oxford Nanopore whole-genome sequencing of 7,179 Icelandic samples. Associations between sequence variants and CpG methylation rates (`deCODE_asmQTL_CpG`) or methylation-depleted-sequence rates (`deCODE_asmQTL_MDS`). Only primary and secondary signals are released (see `variant_rank`); the source is already filtered to Bonferroni significance (~1e-12 CpG, ~1e-10 MDS), MAF > 1e-4, INFO > 0.9, and variant within 100 kb of the methylation target. The `dataset` column is not in the source TSVs and is injected at load time.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| dataset | STRING | Yes | Source dataset (deCODE_asmQTL_CpG, deCODE_asmQTL_MDS) |
| chr | INT64 | Yes | Chromosome |
| pos | INT64 | Yes | Position |
| ref | STRING | Yes | Reference allele |
| alt | STRING | No | Alternate allele |
| rsid | STRING | Yes | dbSNP rsID |
| beta | FLOAT64 | Yes | Effect size |
| se | FLOAT64 | Yes | Standard error |
| mlog10p | FLOAT64 | Yes | -log10(p-value) |
| af | FLOAT64 | Yes | Allele frequency |
| maf | FLOAT64 | No | Minor allele frequency (view only, derived as LEAST(af, 1-af)) |
| most_severe | STRING | No | Most severe variant consequence |
| gene_most_severe | STRING | No | Gene with most severe consequence |
| target_start | INT64 | Yes | Methylation target region start position |
| target_end | INT64 | Yes | Methylation target region end position |
| ref_methylrate | FLOAT64 | Yes | Methylation rate on reference haplotype |
| alt_methylrate | FLOAT64 | Yes | Methylation rate on alternate haplotype |
| n_haplotypes | INT64 | Yes | Number of haplotypes used in analysis |
| variant_rank | STRING | Yes | Variant rank: primary or secondary |
| ld_count | INT64 | No | Number of variants in LD with this variant |
| vartype | STRING | Yes | Variant type: SNV, SV, etc. |

### gene_annotations

Whole-universe gene reference table: one row per HGNC gene, covering the full gene universe (not filtered to results). Built from the HGNC complete-set joined to GENCODE v49 GRCh38 coordinates, with full-lineage HGNC gene-group arrays. Coordinates use GRCh38 with chromosome X encoded as 23 (Y as 24, M as 25), matching the integer chromosome convention of the other views.

This table is a `query_bigquery` surface only. Its primary purpose is enabling cis/trans QTL filtering, where gene coordinates are JOINed against `colocalization_v` (or `credible_sets_v`) inside BigQuery — a join that cannot be done through the specialized API tools — and any-group enumeration via the gene-group arrays. The mcp-server specialized tools (e.g. `get_gene_group_members`, `normalize_gene_symbols`) do NOT read this table; they call the genetics-results-api. The table is fed from the same HGNC source as that API, so the two stay consistent.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| hgnc_id | STRING | Yes | HGNC ID (e.g. HGNC:5) |
| symbol | STRING | Yes | HGNC approved gene symbol |
| name | STRING | Yes | HGNC approved gene name |
| prev_symbols | STRING | No | Previous HGNC symbols, pipe-delimited |
| alias_symbols | STRING | No | Alias symbols, pipe-delimited |
| ensembl_gene_id | STRING | No | Ensembl gene ID |
| ncbi_gene_id | STRING | No | NCBI (Entrez) gene ID |
| chr | INT64 | No | Chromosome (GRCh38; X encoded as 23) |
| gene_start | INT64 | No | Gene start position (GRCh38, GENCODE) |
| gene_end | INT64 | No | Gene end position (GRCh38, GENCODE) |
| strand | STRING | No | Strand (+ or -) |
| locus_type | STRING | Yes | HGNC locus type (e.g. gene with protein product) |
| gene_group_ids | ARRAY\<INT64\> | REPEATED | Full-lineage HGNC gene-group IDs (leaf group plus all ancestors) |
| gene_group_names | ARRAY\<STRING\> | REPEATED | Full-lineage HGNC gene-group names (leaf group plus all ancestors) |
| gencode_version | STRING | Yes | GENCODE release used for coordinates (provenance) |
| hgnc_version | STRING | Yes | HGNC complete-set version/date used (provenance) |
| download_date | DATE | Yes | Date the source data was downloaded/built (provenance) |

Required columns are NOT NULL in the DDL because they never contain NA: HGNC core identity fields (`hgnc_id`, `symbol`, `name`, `locus_type` — always present in the complete set; `symbol` is also filtered during the build) and the build-stamped provenance columns. Coordinate/cross-reference columns (`chr`, `gene_start`, `gene_end`, `strand`, `ensembl_gene_id`, `ncbi_gene_id`) are NULLABLE because not every HGNC gene maps to GENCODE/Ensembl/NCBI. The gene-group arrays are REPEATED (an empty array, never NULL).

**Gene-group lineage.** `gene_group_ids` and `gene_group_names` are full-lineage arrays: each gene's leaf group(s) plus all ancestor groups in the HGNC hierarchy. The arrays are built from three HGNC-native CSV files — `hgnc_gene_has_family.csv` (gene → leaf group), `hgnc_hierarchy_closure.csv` (which is already transitive, expanding each child group to all of its ancestors), and `hgnc_family.csv` (group ID → name). Because the lineage is precomputed, membership in *any* group (leaf or ancestor) is queryable directly with `<group_id> IN UNNEST(gene_group_ids)`, with no recursive join needed. **HGNC id format**: `hgnc_gene_has_family.csv` keys genes by BARE numeric id (`3023`) while `hgnc_complete_set.txt` uses the prefixed `HGNC:3023` form; the build canonicalizes both to `HGNC:NNNN` (via `canonical_hgnc_id`) before joining. Without this the gene→family join silently misses for every gene, leaving all `gene_group_*` arrays empty — so after any rebuild, sanity-check that `COUNTIF(ARRAY_LENGTH(gene_group_ids) > 0) > 0`. For GPCR-type analyses, exclude olfactory receptors (which dominate the GPCR group by count) with `NOT ('Olfactory receptors' IN UNNEST(gene_group_names))` and restrict to `locus_type = 'gene with protein product'`.

### open_chromatin

Atlas of accessible/active chromatin regions labeled by cell type, tissue and condition. One row per peak per context (LONG layout), region-indexed with no p-values: a row means the interval is open/active chromatin in that context, so queries overlap a position or region against `peak_start`/`peak_end`. Six source datasets (marderstein, li_brain_atac, catlas, epimap, calderon_immune, rosmap_brain) — filter on `resource`, not `dataset`. `start`/`end` are named `peak_start`/`peak_end` because `end` is a reserved word, and the source `chrom` string is converted to the INT64 `chr` encoding on load (`CHR_STRING_TABLES`). Scores are never unit-harmonized across datasets; `score_type` says what a score means.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| chr | INT64 | Yes | Chromosome (X=23, Y=24, M/MT=25) |
| peak_start | INT64 | Yes | Peak/region start position (0-based BED start) |
| peak_end | INT64 | Yes | Peak/region end position |
| peak_id | STRING | No | Source peak/element identifier |
| dataset | STRING | Yes | Source dataset (e.g. marderstein_open_chromatin, catlas_open_chromatin) |
| cell_type | STRING | No | Free-text source cell-type label (provenance only, not a join key) |
| tissue | STRING | No | Harmonized tissue axis (e.g. brain, heart, immune) |
| life_stage | STRING | No | Harmonized life stage (e.g. fetal, adult) |
| condition | STRING | No | Harmonized condition (e.g. resting, stimulated, AD, control) |
| assay | STRING | No | Assay type: scATAC, snATAC, bulk_ATAC, chromHMM |
| score | FLOAT64 | No | Peak score/signal; NULL for presence-only baselines |
| score_type | STRING | No | Categorical score type (e.g. presence, chromhmm_18state) |
| n_cells | INT64 | No | Number of cells/nuclei supporting the peak, when available |
| cell_ontology_id | STRING | No | Cell Ontology (CL) identifier, when available |
| uberon_id | STRING | No | UBERON tissue identifier, when available |
| target_gene | STRING | No | Linked target gene symbol (enhancer/cCRE-to-gene link), when available |
| target_gene_id | STRING | No | Linked target gene Ensembl id, when available |
| version | STRING | No | Dataset version/build stamp |

### variant_effect

In-silico *predicted* effects of variants on chromatin accessibility from deep-learning models (ChromBPNet, FLARE) — model predictions, not measured associations, and distinct from the measured reporter activity in [mpra](#mpra). One row per variant per model per context (LONG layout). The `model` column keeps the table model-generic. `variant` is a stored column rather than view-derived because the canonical TSV shared with the API already carries it, which keeps the positional CSV load aligned. Both current datasets map to `resource = 'marderstein'`, so filter on `resource`, not `dataset`.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| chr | INT64 | Yes | Chromosome (X=23, Y=24, M/MT=25) |
| pos | INT64 | Yes | Variant position (1-based) |
| ref | STRING | Yes | Reference allele |
| alt | STRING | Yes | Alternate allele |
| variant | STRING | No | Variant identifier (chr:pos:ref:alt) |
| rsid | STRING | No | dbSNP rsID, when available |
| dataset | STRING | Yes | Source dataset (marderstein_chrombpnet, marderstein_flare) |
| model | STRING | No | Prediction model: chrombpnet, flare |
| cell_type | STRING | No | Free-text source cell-type label (provenance only, not a join key) |
| tissue | STRING | No | Harmonized tissue axis (e.g. brain, heart, immune) |
| life_stage | STRING | No | Harmonized life stage (e.g. fetal, adult) |
| score | FLOAT64 | No | Predicted effect score; interpretation depends on score_type/model |
| score_type | STRING | No | Categorical score type (e.g. chrombpnet_logfc, flare_score) |
| mlog10p | FLOAT64 | No | -log10(p-value) for the predicted effect, when available |
| predicted_direction | STRING | No | Predicted direction of effect (e.g. gain, loss), when available |
| quantile_rank | FLOAT64 | No | Quantile rank of the score within the model's distribution |
| is_significant | BOOL | No | Whether the predicted effect passes the model's significance threshold |
| version | STRING | No | Dataset version/build stamp |

### mpra

*Measured* cis-regulatory allelic activity from a massively parallel reporter assay (Siraj et al. 2026). One row per variant per `cell_line` (LONG layout), where `cell_line` is either `meta` (cross-cell-line meta-analysis) or one of the five tested lines (K562, HEPG2, SKNSH, HCT116, A549). Reports whether an allele modulates reporter expression (`emVar` / `log2Skew`) and whether the element drives expression above background (`active` / `log2FC`). Distinct from both the in-silico [variant_effect](#variant_effect) predictions and endogenous eQTL/caQTL. Several columns are populated only for one row flavour, so the `cell_line` filter matters: `log2Skew_se` only on `meta` rows, `mean_RNA_ref`/`mean_RNA_alt` only on per-cell-line rows. The p-value columns are also not comparable across flavours (raw for `meta`, adjusted per cell line). `dataset` is constant and injected at load time; `variant` is stored for the same reason as in `variant_effect`.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| chr | INT64 | Yes | Chromosome (X=23, Y=24, M/MT=25) |
| pos | INT64 | Yes | Variant position (1-based) |
| variant | STRING | No | Variant identifier (chr:pos:ref:alt) |
| ref | STRING | Yes | Reference allele |
| alt | STRING | Yes | Alternate (tested) allele |
| cohort | STRING | No | Fine-mapping cohort the variant was drawn from (GTEx, UKBB, BBJ, control); NULL if no meta-analysis row |
| cell_line | STRING | No | MPRA context: `meta` or one of K562, HEPG2, SKNSH, HCT116, A549 |
| emVar | BOOL | No | Whether the allele modulates reporter expression in this context (allelic skew significant) |
| active | BOOL | No | Whether the element drives reporter expression above background in this context |
| log2Skew | FLOAT64 | No | Signed allelic effect, log2(alt/ref) of reporter activity |
| log2Skew_se | FLOAT64 | No | Standard error of log2Skew; only on `cell_line='meta'` rows |
| log2Skew_mlog10p | FLOAT64 | No | -log10 p for allelic skew (raw for meta rows, adjusted for per-cell-line rows) |
| log2FC | FLOAT64 | No | Element activity vs background, log2 fold change |
| log2FC_mlog10p | FLOAT64 | No | -log10 p for element activity (raw for meta rows, Bonferroni-adjusted for per-cell-line rows) |
| mean_RNA_ref | FLOAT64 | No | Mean reporter RNA level for the ref allele; only on per-cell-line rows |
| mean_RNA_alt | FLOAT64 | No | Mean reporter RNA level for the alt allele; only on per-cell-line rows |
| dataset | STRING | Yes | Source dataset (constant `siraj_mpra`) |

### hla_associations

Classical HLA allele associations from FinnGen R14: every imputed HLA allele tested against every core R14 endpoint. One row per (phenotype, allele) — 2,712 phenotypes x 187 alleles across 10 genes, ~507k rows.

**The unit is an allele, not a variant.** There are deliberately no `ref`/`alt` columns: the source models the test as `ref='<absent>'` / `alt='<allele name>'`, and the munge rewrites that into explicit `gene`/`allele`. These rows therefore cannot be joined to `credible_sets`/`variant_annotation` on chr/pos/ref/alt, and `pos` is the HLA *gene's* single anchor position shared by all of its alleles rather than the allele's own location (HLA-DRB3/DRB4/DRB5 share the placeholder 32500000). `pos` is kept because it is what the tabix index the API reads is built on.

**Why the table exists.** results-api serves the same data from per-phenotype tabix files, which answers "all alleles for a trait". The reverse — "all traits for an allele", the PheWAS view that makes MHC pleiotropy visible — spans all 2,712 files and is only answerable here. Clustering is `phenotype, gene, allele` to serve both directions.

Two columns need care when querying. `pval` **underflows to 0** for the strongest signals (coeliac `DQB1*02:01` is mlogp 1596), so rank and threshold on `mlogp` (`mlog10p` when querying the view — see the rename below). `info` is the allele's imputation quality (constant per allele across phenotypes) and filtering on it is not optional in practice: rare alleles imputed below ~0.5 produce enormous unstable betas that read as spectacular associations but are artifacts.

The table is unpartitioned — every row is chr 6, so a `RANGE_BUCKET(chr, …)` partition would put the whole table in one partition anyway. `dataset` is constant and injected at load time.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| chr | INT64 | Yes | Chromosome (always 6, the MHC) |
| pos | INT64 | Yes | Anchor position of the allele's HLA gene, shared by all its alleles; does not locate the allele |
| gene | STRING | Yes | HLA gene symbol (HLA-A, -B, -C, -DPB1, -DQA1, -DQB1, -DRB1, -DRB3, -DRB4, -DRB5) |
| allele | STRING | Yes | Imputed classical allele at 4-digit (two-field) resolution, e.g. `B*27:05`; not gene-prefixed |
| phenotype | STRING | Yes | FinnGen R14 endpoint code |
| pval | FLOAT64 | No | Association p-value; underflows to 0 for the strongest signals — rank on `mlogp` |
| mlogp | FLOAT64 | No | -log10 p-value (genome-wide significance 7.3) |
| beta | FLOAT64 | No | Effect size per copy of the allele (log OR for binary endpoints) |
| sebeta | FLOAT64 | No | Standard error of beta |
| af_alt | FLOAT64 | No | Allele frequency in the full cohort |
| af_alt_cases | FLOAT64 | No | Allele frequency in cases; NULL for quantitative endpoints |
| af_alt_controls | FLOAT64 | No | Allele frequency in controls; NULL for quantitative endpoints |
| info | FLOAT64 | No | Imputation INFO for the allele; below ~0.5 the effect estimates are artifacts |
| dataset | STRING | Yes | Source dataset (constant `finngen_hla`) |

The `hla_associations_v` view maps `dataset` to `resource = 'finngen'` explicitly rather than via the lowercase fallback the other product views use, since that would yield `finngen_hla` — these results belong to the same resource as the FinnGen GWAS they were run alongside.

It also **renames the statistic columns** to the suite's house spelling, which is what every consumer sees and what the column tables in `../genetics-results-suite/configs/datasets.yaml` document:

| table column | view column |
|---|---|
| mlogp | mlog10p |
| sebeta | se |
| af_alt | af |
| af_alt_cases | af_cases |
| af_alt_controls | af_controls |

The values are unchanged — the rename exists because results-api serves the same quantities from the per-phenotype tabix files under `mlog10p`/`se`/`af`/`af_cases`/`af_controls`, and the SDK's `hla()` returns results from both stores (`genetics-results-suite-5wm`). The table keeps FinnGen's native spelling so the loader stays a straight copy of the staged file. Because of the rename the view lists its columns explicitly instead of `SELECT *`: a new column on `hla_associations` must be named in the view too, or it will not surface. `tests/test_hla_view_columns.py` is what catches the omission — it parses the select list of `schemas/hla_associations_v.sql` offline and asserts its source identifiers (the left side of `mlogp AS mlog10p`) cover every field of `SCHEMAS["hla_associations"]` in `scripts/load_data.py`.

Replacing this view is not covered by the suite's `deploy.sh` — it is applied by `scripts/setup_bigquery.sh` — and it is not compatible with the previously deployed mcp-server. See "HLA column rename rollout" in `../genetics-results-suite/docs/project-spec.md` for the ordering.

### dosage_sensitivity

Gene-level dosage-sensitivity scores from Collins et al. 2022, *A cross-disorder dosage sensitivity map of the human genome* (Cell 185:3041, doi 10.1016/j.cell.2022.06.036, Zenodo record 6347673, CC-BY 4.0). One row per autosomal protein-coding gene: `phaplo` is the probability that losing one copy is deleterious, `ptriplo` the probability that gaining one is, both estimated from rare CNVs in ~1M individuals.

**No coordinates, by construction.** The scores are gene-keyed, so nothing here depends on the GRCh37 the source was called on and no liftover is involved. Join `gene_annotations_v` on `ensembl_gene_id` when coordinates are wanted.

**The key is `ensembl_gene_id`, not `symbol`.** GENCODE gives a handful of gene pairs the same name, so a symbol join can legitimately return two rows; `symbol_gencode_v19` keeps the source's own spelling for genes whose symbol has since been renamed (and for the residue the mapping could not resolve, where the two columns are equal).

`haploinsufficient` and `triplosensitive` materialise the paper's published cutoffs (`phaplo >= 0.86`, `ptriplo >= 0.94`) so a caller need not remember them and cannot silently substitute another; the raw scores remain available for anyone who wants a different threshold. The table is small and unpartitioned, clustered by `symbol`.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| symbol | STRING | Yes | Current HGNC gene symbol, mapped from the source GENCODE v19 symbol via ENSG |
| symbol_gencode_v19 | STRING | Yes | Gene symbol as published (GENCODE v19 spelling) |
| ensembl_gene_id | STRING | Yes | Ensembl gene ID (unversioned); unique — the table's key |
| phaplo | FLOAT64 | Yes | pHaplo: probability the gene is haploinsufficient (0-1) |
| ptriplo | FLOAT64 | Yes | pTriplo: probability the gene is triplosensitive (0-1) |
| haploinsufficient | BOOL | Yes | `phaplo >= 0.86` |
| triplosensitive | BOOL | Yes | `ptriplo >= 0.94` |

`dosage_sensitivity_v` appends a constant `'rcnv' AS resource` on the `gene_annotations_v` pattern: the table is a single published product with no `dataset` column for a CASE to switch on.

### rcnv_gene_associations

The per-phenotype half of the same Collins et al. 2022 release: one row per (phenotype, cnv_type, gene) over 54 HPO phenotype groups x {DEL, DUP} x 17,263 autosomal protein-coding genes (1,864,404 rows). Where `dosage_sensitivity` says whether a gene is dosage sensitive at all, this says which phenotype a deletion or duplication of it is associated with.

**No coordinates, by construction** — same reason as `dosage_sensitivity`, but the stakes are higher here because the source BEDs *do* carry GRCh37 intervals: storing them would make a silently wrong join against any GRCh38 view possible. Coordinates come from `gene_annotations_v` on `ensembl_gene_id`.

**65% of rows carry NULL from `beta` onward.** Every gene appears for every (phenotype, cnv_type) pair, including the 1,214,820 rows where the gene was tested but the meta-analysis produced no estimate. Keeping them is what makes "tested, no estimate" distinguishable from "not tested", matching `gene_burden_results_v`'s unfiltered contract; a caller wanting estimated associations filters `beta IS NOT NULL`. `n_nominal_cohorts` is not a proxy for that — the NULL rows are not the `n_nominal_cohorts = 0` rows.

**Significance has two tiers, each gated by secondary evidence.** A gene is exome-wide significant if `mlog10p > -LOG10(2.90e-6)` (P <= 2.90e-6), or FDR significant if `mlog10_fdr_q > -LOG10(0.01)` (FDR < 1%); either tier additionally requires `n_nominal_cohorts >= 2` OR `mlog10p_secondary > -LOG10(0.05)`. Checked against dev: the full rule returns 5,680 rows over 739 distinct genes, matching the paper's published totals. `beta` is ln(OR). The `*_secondary` columns are a leave-`top_cohort`-out sensitivity re-analysis, NULL for 86% of rows.

Unpartitioned — there is no chromosome column to `RANGE_BUCKET` on — and clustered by `phenotype, cnv_type, ensembl_gene_id`. Clustering is a sort prefix, so a phenotype-scoped scan prunes and a gene-only PheWAS across all 54 groups does not; `symbol` is deliberately not a fourth clustering column, being functionally determined by the third.

Column list: `schemas/rcnv_gene_associations.sql` (23 columns; the loader's `SCHEMAS["rcnv_gene_associations"]` in `scripts/load_data.py` must match the staged TSV's column order, and the null marker is `NA`).

`rcnv_gene_associations_v` derives `resource` from the `Collins_rCNV%` rule the way the other product views do, and LEFT JOINs `dosage_sensitivity` on `ensembl_gene_id` so `phaplo`/`ptriplo`/`haploinsufficient`/`triplosensitive` ride on every association row. It is the one view whose base table must be loaded *before* `scripts/load_phenotypes.sh` runs: `build_phenotypes.BQ_DATASETS_BY_DATASET_ID['collins_rcnv_2022']` names `Collins_rCNV_2022`, and the registry cross-check fails for the whole profile while no results view carries that value.

### rcnv_segments

The region-level product of the same Collins et al. 2022 release (Table S3): the 163 disease-associated rare-CNV segments, one row each — 69 DEL and 94 DUP, 88 genome-wide significant and 75 FDR significant. Where `rcnv_gene_associations` is gene-keyed, this is interval-keyed, and it is the view that answers "what is known about this locus".

**Coordinates are dual, and the GRCh38 pair is incomplete.** `segment_start_grch37`/`segment_end_grch37` are the published intervals and are always present; `segment_start`/`segment_end` are the GRCh38 lift and are NULL for the segments that lift neither as a whole interval nor from the two published 200 kb sliding windows sitting on their boundaries (six of the 163 in the current load, among them the 22q11.21 duplication) — recurrent genomic disorders over segmental-duplication-flanked regions are exactly the sequence the two builds rearranged. Which segments those are is a property of the liftOver chain the munge ran, not of the table, so a reload can change the set; `genetics-results-munge`'s `docs/rcnv-dosage-sensitivity.md` names the current one. A `BETWEEN segment_start AND segment_end` predicate silently drops them, so queries state `segment_start IS NOT NULL` and fall back to the GRCh37 pair when the region matters more than the build. The source TSV's bare `start`/`end` are loaded under these names rather than `end` staying a reserved GoogleSQL keyword that a caller must remember to backtick — this table is queried by model-written SQL, and the rename removes the hazard instead of documenting it.

**Six list columns are stored joined and split in the view.** `associated_hpos`, `credints`, `credints_grch37`, `genes`, `genes_gencode_v19` and `gene_ensembl_ids` load as the source's `';'`-joined strings and `rcnv_segments_v` exposes them as `ARRAY<STRING>`. The split is not done at load time because `DERIVED_COLUMNS` in `scripts/load_data.py` materialises columns the TSV does *not* carry — it drops them from the staging schema — while these six are columns of the file; converting them in place would mean new staging machinery on the `CHR_STRING_TABLES` model for a 163-row table. Two need more than a bare `SPLIT`: `credints` is NULL where every one of a segment's intervals failed to lift (the whole field was the `NA` null marker), and the view restores the literal so it stays positionally aligned with `credints_grch37`; the gene lists are empty for the 12 gene-poor segments, and `SPLIT('')` returns a one-element array of the empty string, which would make `ARRAY_LENGTH` disagree with `n_genes`.

Unpartitioned, clustered by `chr, segment_start`. The repo partitions results tables by `RANGE_BUCKET(chr, ...)`, but 163 rows over 22 chromosomes is ~7 rows a partition — the metadata costs more than the pruning saves, the same reasoning that left `dosage_sensitivity` unpartitioned.

Column list: `schemas/rcnv_segments.sql` (the loader's `SCHEMAS["rcnv_segments"]` in `scripts/load_data.py` must match the staged TSV's column order, and the null marker is `NA`).

### rcnv_window_associations

The position-keyed product of the same Collins et al. 2022 release: the genome-wide sliding-window DEL/DUP meta-analysis, one row per (phenotype, cnv_type, window). 11,198,315 rows over 259,795 windows — the largest table of the rCNV product. A row is the association of the CNVs *overlapping* an interval; nothing attributes it to a gene, so there is no gene column and no join to `dosage_sensitivity`.

**Coordinates are dual, and only the GRCh37 pair is a grid.** The published windows are 200 kb wide with a 10 kb step in GRCh37, and `window_start_grch37`/`window_end_grch37` are those values. `window_start`/`window_end` are the GRCh38 lift (UCSC liftOver, whole interval, same chromosome, 180-220 kb) and are what callers query, since every other view in the suite is GRCh38 — but the lifted set is not a grid: widths run 190,000-219,265 and adjacent pairs can reorder — see `genetics-results-munge`'s `docs/rcnv-sliding-windows.md` for the width distribution. Distinct windows are therefore counted on `(chr, window_start_grch37, window_end_grch37)`.

**Rows are dropped, not nulled.** 4,880 of the 267,237 published windows (1.83%) fail to lift and are absent, clustered on chr9 (7.7%), chr21 (4.4%), chr22 (3.6%) and chr1 (3.2%); a further 2,562 lift but carry NA statistics in every phenotype x CNV group. This is the opposite of `rcnv_gene_associations`, which keeps its 65% of NULL-stat rows so "tested, no estimate" stays visible: a window with no estimate is not a fact about an entity anyone can ask about, so the munge drops it. Every loaded row carries a `beta`, and rows per (phenotype, cnv_type) group range from 17,114 to 257,726.

Partitioned by `RANGE_BUCKET(chr, GENERATE_ARRAY(1, 23, 1))` exactly as `credible_sets` is — unlike the two small rCNV tables, this one has the row count to pay for it — and clustered by `phenotype, cnv_type, window_start`, so a phenotype-and-region question prunes and a coordinate range stays contiguous inside a phenotype.

Column list: `schemas/rcnv_window_associations.sql` (the loader's `SCHEMAS["rcnv_window_associations"]` in `scripts/load_data.py` must match the staged TSV's column order; the null marker is `NA` and reaches only `cohorts_excluded` and the `*_secondary` columns). The source `chr` is already a bare integer, so this takes the direct-load path rather than `CHR_STRING_TABLES` staging.

### peak_to_gene

Open4Gene peak-to-gene links from the FinnGen ATAC-seq study: which genes a chromatin peak's accessibility is associated with, in which cell type. One row per (peak, gene, cell type), ~1.07M rows over 112,032 peaks and 12,445 genes across 33 cell types. Only significant links are published, so a missing row means no significant link was found, not evidence against one.

**Why the table exists.** caQTL rows in `credible_sets` carry a PEAK id in `trait` (e.g. `chr5-35482826-35484273`), not a gene, so no gene-based caQTL question is answerable from `credible_sets` alone. This table is the join that makes it one:

```sql
SELECT l.symbol, cs.cell_type, cs.trait AS peak_id, cs.cs_id, cs.pos, cs.pip
FROM credible_sets_v cs
JOIN peak_to_gene_v l ON l.peak_id = cs.trait AND l.cell_type = cs.cell_type
WHERE cs.data_type = 'caQTL' AND l.symbol = 'IL7R'
```

Dropping the `cell_type` predicate gives the cell-type-agnostic, peak-level answer. Approximating the link by comparing peak and gene coordinates does not work: linked peaks sit up to ~1 Mb from the gene, and most peaks near a gene are not linked to it (for IL7R, a ±500 kb window contains 84 peaks with credible sets, of which 25 are actually linked).

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| chr | INT64 | Yes | Chromosome (GRCh38; X encoded as 23) |
| peak_start | INT64 | Yes | Peak start position (0-based BED start) |
| peak_end | INT64 | Yes | Peak end position |
| peak_id | STRING | Yes | Peak identifier as chr-start-end; joins `credible_sets.trait` for caQTL rows |
| gene_id | STRING | Yes | Linked gene Ensembl ID (no version suffix) |
| symbol | STRING | No | Linked gene symbol |
| cell_type | STRING | Yes | Cell type the link was found in (e.g. `l1.CD4_T`); joins `credible_sets.cell_type` |
| total_cell_num | INT64 | No | Total number of cells in the analysis |
| expr_cell_num | INT64 | No | Number of cells expressing the gene |
| open_cell_num | INT64 | No | Number of cells with the peak accessible |
| hurdle_zero_beta / _se / _z / _nlog10p | FLOAT64 | No | Zero (detection) component of the hurdle model |
| hurdle_count_beta / _se / _z / _nlog10p | FLOAT64 | No | Count (expression-level) component of the hurdle model |
| hurdle_aic | FLOAT64 | No | Akaike information criterion of the fitted model |
| hurdle_bic | FLOAT64 | No | Bayesian information criterion of the fitted model |
| dataset | STRING | Yes | Source dataset (constant `FinnGen_ATACseq`, matching `credible_sets.dataset`) |

**Two normalizations happen at load time**, both so the join is a plain equality rather than a rewrite at query time: the source chrom is `chr1`..`chrX` and is converted to INT64 (`CHR_STRING_TABLES`), and the source `cell_type` carries a `predicted.celltype.` prefix that `credible_sets` does not, which is stripped (`CELL_TYPE_PREFIX_TABLES`). The tabix API strips the same prefix at read time. Unlike the other tables this one is unpartitioned (it is small) and clustered by `symbol` first, since gene-keyed lookup is the dominant access pattern. When the gene's chromosome is known, adding a literal `cs.chr = <n>` to the join prunes `credible_sets` partitions and cuts the scan by an order of magnitude.

The same links are served by the genetics-results-api as `/peak_to_genes/{peak_id}` and `/gene_to_peaks/{gene}`, from the same source file.

### variant_annotation

Per-variant functional annotations for FinnGen (R14). This is the same data the genetics-results-api serves at `/variant_annotation/finngen`, loaded from the identical tabix source (`R14_annotated_variants_v0.small.gz`) so BigQuery agents can filter variants by functional consequence (`most_severe`, `gene_most_severe`) without going through the API. One row per variant. `chr` is already numeric in the source (X=23), so no chr-string conversion is applied on load. The `variant_annotation_v` view adds a constant `resource = 'finngen'` (single-source table, so no dataset-derived CASE and not part of the datasets.yaml resource linting).

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| variant | STRING | No | Variant identifier (chr:pos:ref:alt, GRCh38) |
| chr | INT64 | Yes | Chromosome (X=23) |
| pos | INT64 | Yes | Position (1-based, GRCh38) |
| ref | STRING | Yes | Reference allele |
| alt | STRING | Yes | Alternate allele |
| INFO | FLOAT64 | No | Imputation INFO score |
| AF | FLOAT64 | No | Alternate allele frequency in FinnGen |
| AC_Het | INT64 | No | Heterozygous genotype count in FinnGen |
| AC_Hom | INT64 | No | Homozygous (alt) genotype count in FinnGen |
| most_severe | STRING | No | Most severe variant consequence (VEP) |
| gene_most_severe | STRING | No | Gene of the most severe consequence |
| rsid | STRING | No | dbSNP rsID |
| EXOME_enrichment_nfe | FLOAT64 | No | Finnish vs non-Finnish European (NFE) enrichment, gnomAD exomes |
| GENOME_enrichment_nfe | FLOAT64 | No | Finnish vs non-Finnish European (NFE) enrichment, gnomAD genomes |
| index | INT64 | No | Row index in the source annotation file |

### phenotypes

Trait/phenotype metadata: the human-readable name, trait type, category and sample sizes behind the opaque phenotype codes the results tables store. One row per `(dataset, trait_original)`, 32,611 rows. Built from the per-dataset `metadata_file` JSON/TSVs referenced by `datasets.yaml`, which were previously reachable only through genetics-results-api — so resolving a trait code cost a separate round trip.

**Join on `trait_original`, never on `trait`.** In `credible_sets`, `colocalization`, `coloc_credsets`, `exome_variant_results` and `gene_burden_results`, `trait_original` is the phenotype code and `trait` is a display form that differs for most rows: FinnGen R14 stores `trait='Height,_inverse-rank_normalized'` with `trait_original='HEIGHT_IRN'`, Genebass stores `trait='Mean corpuscular volume'` with `trait_original='continuous_30040_both_sexes__irnt'`, and QTL rows store a gene symbol in `trait` with the Ensembl id in `trait_original`. Joining on `trait` returns zero rows silently.

```sql
SELECT cs.dataset, cs.trait_original, p.trait_name, p.n_cases, cs.pip
FROM credible_sets_v cs
LEFT JOIN phenotypes_v p USING (dataset, trait_original)
WHERE cs.chr = 6 AND cs.pos BETWEEN 32000000 AND 33000000
```

**Coverage is partial by design.** Only datasets that ship a phenotype metadata file have rows — FinnGen R14/R12/Kanta/drugs, the FinnGen+UKBB and FinnGen+MVP+UKBB meta-analyses, Open Targets, Genebass, BRaVa, COVID-19 HGI, IIBDGC, the Collins rCNV map and the EstBB-UKBB NMR traits. QTL datasets have none (their traits are genes, proteins and peaks, resolved via `gene_annotations` and `peak_to_gene`), and neither do datasets whose codes are already readable (PGC, GP2, BipEx2, SCHEMA2, IBD_exome). Use a `LEFT JOIN` when the dataset is not known in advance. Ranked fuzzy phenotype *search* stays on results-api; this table serves exact resolution and SQL-expressible filtering.

| Column | Type | Required | Description |
|---|---|---|---|
| dataset | STRING | Yes | Results-view dataset name; joins the `dataset` column of every results view |
| trait_original | STRING | Yes | Phenotype code exactly as the results views store it — the join key |
| trait_name | STRING | No | Human-readable trait name |
| trait_type | STRING | No | `binary` or `quantitative`; NULL when the source states neither |
| category | STRING | No | Source grouping (FinnGen ICD chapter, Kanta class, Open Targets project id, Genebass trait_type) |
| n_samples | INT64 | No | Total analysed sample size; NULL (not 0) when unreported |
| n_cases | INT64 | No | Number of cases |
| n_controls | INT64 | No | Number of controls |
| dataset_id | STRING | Yes | ONE contributing `datasets.yaml` registry key — provenance, **not** a join key: merged `datasets` rows keep only their first contributor, so `finngen_kanta_r12` and `genebass_gene_based` match no `datasets` row. Join on `dataset`, or on `dataset_id IN UNNEST(datasets.dataset_ids)` |
| resource | STRING | Yes | Resource the dataset belongs to |
| author | STRING | No | Study author; per-study for Open Targets |
| publication_date | DATE | No | NULL rather than invented when the source gives only a year |
| version | STRING | No | Dataset version label |
| coloc_partner_only | BOOL | Yes | TRUE for traits whose dataset exists only as a colocalization partner (FinnGen R12 core and R12 Kanta) |

### datasets

Dataset registry: what every `dataset` value appearing in the results views actually is. 888 rows, of which 841 are eQTL Catalogue QTD sub-studies. Unique on `dataset`, so `JOIN datasets_v USING (dataset)` never fans results out — where several registry entries share one results-view dataset (`pgc_scz` + `pgc_bip` inside `PGC`, the two Genebass products inside `genebass`, the two IBD exome products inside `IBD_exome`) they are merged into one row and `dataset_ids` lists every contributor.

`dataset` is NULL for the seven registry entries with no BigQuery presence — summary-statistics-only products, expression, chromatin peaks and gene-disease sets that only results-api serves. They are kept so the table answers "what data exists at all"; filter `dataset IS NOT NULL` for queryable datasets only.

| Column | Type | Required | Description |
|---|---|---|---|
| dataset | STRING | No | Results-view dataset name; NULL when the dataset has no BigQuery presence |
| dataset_id | STRING | Yes | Primary `datasets.yaml` registry key |
| dataset_ids | ARRAY&lt;STRING&gt; | REPEATED | Every registry key merged into this row |
| resource | STRING | Yes | Resource name; matches the derived `resource` column of the results views |
| resource_label | STRING | No | Display label for the resource |
| resource_aliases | ARRAY&lt;STRING&gt; | REPEATED | Alternative names users write for the resource |
| version | STRING | No | Dataset version label |
| description | STRING | No | What the dataset is, its cohort and caveats; merged entries joined with ` \| ` |
| author | STRING | No | Producing consortium; for QTD sub-studies the source study label |
| publication_date | DATE | No | Release/publication date |
| data_type | STRING | No | Lower-case registry vocabulary (`gwas`, `eQTL`, `pqtl`, …), NOT the upper-case `data_type` of the results views |
| trait_type | STRING | No | Dataset-level `binary`/`quantitative`/`mixed` |
| n_samples | INT64 | No | Dataset-level sample size where the registry states one |
| pseudo_credible_sets | BOOL | Yes | TRUE when this dataset's credible sets are LD-clumped proxies, not SuSiE fine-mapping — check before interpreting `pip`/`cs_size` |
| coloc_partner_only | BOOL | Yes | TRUE when the dataset ships no independently queryable product |
| collection | BOOL | Yes | TRUE for a collection whose sub-studies carry `subdataset_of` = its `dataset_id` |
| subdataset_of | STRING | No | Parent collection's `dataset_id` for sub-studies (QTD ids under `eqtl_catalogue`) |

## Technical Implementation

### BigQuery Configuration

- **Partitioning**: a table is partitioned by chromosome (`PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 23, 1))`) exactly when a chromosome filter can eliminate work — i.e. its rows span chromosomes *and* it is accessed positionally. Everything else is unpartitioned: the small reference/link/metadata tables, whose access is gene- or key-keyed and a full scan of which is cheap, and `hla_associations`, whose every row is chr 6 so a chromosome partition would hold the whole table. **Do not read the membership of either set out of this paragraph** — it has already gone stale once by omitting `hla_associations`. Re-derive it: `grep -L 'PARTITION BY' schemas/*.sql | grep -v _v.sql` lists the unpartitioned base tables.
- **Clustering**: Tables clustered by frequently filtered columns (dataset, data_type, most_severe; `symbol` first for the gene-keyed tables). `credible_sets` is the exception and the model to copy for high-traffic tables: it clusters on `data_type, resource, variant, pos`, the columns callers are actually told to filter by, which required storing `resource` and `variant` instead of deriving them in the view — clustering cannot use a view-derived expression. Clustering and partitioning cannot be changed in place; see [credible-sets-clustering-swap.md](credible-sets-clustering-swap.md) for the rebuild pattern and why `setup_bigquery.sh` cannot do it.

### API Service

- **Framework**: FastAPI with uvicorn

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/schema` | GET | Get table schemas with column descriptions and allowed values for categorical columns; optional `table` query param limits the response to one view |
| `/stats` | GET | Get database statistics and row counts |
| `/tables/{name}/sample` | GET | Get sample rows from a table (`limit`, default 10, capped at 100) |
| `/query` | POST | Execute SQL query |
| `/docs`, `/redoc`, `/openapi.json` | GET | Interactive API docs and OpenAPI schema, re-declared as ordinary authenticated routes |

`/schema` and `/tables/{name}/sample` accept either a view name (`credible_sets_v`) or the base
table name it wraps (`credible_sets`), and 404 on anything else.

### Query Endpoint Parameters

```json
{
  "sql": "SELECT * FROM credible_sets LIMIT 10",
  "max_rows": 1000,
  "dry_run": false
}
```

- `sql` (required): SQL query to execute
- `max_rows` (default 1000, max 100000): Maximum rows to return
- `dry_run` (default false): Estimate query cost without executing

Every query is authorized before it runs (see Security → Query authorization). Rejections are 400 for a query BigQuery refuses under the caller's per-query byte cap (`bytesBilledLimitExceeded`, raised at execution rather than by the dry run): the detail gives the estimate and the cap and, per base table the dry run reported, the partition column a literal predicate must name — read once per table from BigQuery and cached in process — because the cap is checked against the partition-pruned estimate, which no clustering-key or derived-column filter lowers; 400 for a statement the dry run parses as something other than a single `SELECT` (a script, `EXECUTE IMMEDIATE`, `CREATE TEMP TABLE`), a syntax error or a name that resolves to nothing; 403 for a `SELECT` that references a table outside the exposed views or that BigQuery denies (denial text logged rather than returned) — DML and DDL against an exposed table arrive as this 403 rather than as 400, because the read-only service account makes the dry run itself refuse them before the statement type is inspected; and 503 for a BigQuery 403 whose reason is anything other than `accessDenied` — a quota, billing or block failure, and today also the `policyViolation` a table in a VPC-Service-Controls-protected project returns.

### Query Response Format

```json
{
  "columns": ["col1", "col2"],
  "rows": [["val1", "val2"], ...],
  "total_rows": 100,
  "bytes_processed": 1048576,
  "truncated": false,
  "max_rows_applied": 1000
}
```

Cell values are JSON scalars; a STRUCT column comes back as a nested object and an ARRAY as
an array (`_serialize_value` recurses), so `ARRAY_AGG(STRUCT(...))[OFFSET(0)]` reaches the
sandbox SDK as a polars struct rather than its Python-repr string. Anything else non-scalar
(Decimal, date, bytes) is its string form.

`max_rows_applied` is the row ceiling this request actually ran under — `min(max_rows, the
per-credential cap)`, so 25 000 is the most a sandbox execution can see and 100 000 the most a
caller verified against `INTERNAL_API_SECRET` can. It exists because `truncated` says the rows
are a positional prefix without saying **where** the cut fell, and the two candidate ceilings
differ by 4x, so a caller could not tell whether raising `max_rows` would help; mcp-server's SDK
quotes this number in the error it raises rather than hardcoding one
(`genetics-results-suite-4h6.32`). It is additive — no existing field changed name or meaning,
because chat-backend and mcp-server both parse this response.

### Schema Response Format

`/schema` returns each view's columns with type/mode/description plus, for low-cardinality categorical columns, the actual allowed values discovered from the data. Column `mode` (NULLABLE/REQUIRED) and `row_count` are read from the underlying base table, since a BigQuery view reports every scalar column as NULLABLE. A REPEATED column keeps the view's own mode: it is the only signal that the column is an ARRAY, and `rcnv_segments_v` SPLITs scalar STRING base columns into arrays, so the base table's mode would erase it. View-only derived columns are declared explicitly: `variant` and `resource`/`resource1`/`resource2` are REQUIRED (non-null transforms of REQUIRED base columns), while `maf` is NULLABLE (`LEAST(aaf, 1-aaf)` with nullable `aaf`). For `credible_sets_v` the base table now answers for `variant`/`resource` directly — they are stored `NOT NULL` columns there, which is why the schema file declares them `NOT NULL` rather than following the nullable stored `variant` of `variant_effect`/`mpra`/`variant_annotation`: it keeps `/schema` reporting REQUIRED as before. Two shapes:

- `allowed_values`: flat list of valid values (e.g. `resource`, `dataset`, `most_severe`).
- `allowed_values_by_<col>`: mapping from a parent column's value to the values valid for that parent. Used when a column's valid set depends on another (e.g. `data_type` depends on `resource`, `annotation` depends on `resource`).

Example:
```json
{
  "name": "data_type",
  "type": "STRING",
  "allowed_values_by_resource": {
    "open_targets": ["GWAS", "eQTL", "pQTL", "sQTL", "caQTL"],
    "finngen": ["GWAS"],
    "ukbb": ["GWAS"]
  }
}
```

Values are computed by querying `SELECT DISTINCT` on each view and cached in-process for one hour. New datasets show up automatically after the cache expires. Whatever the column's BigQuery type, the values (and the parent keys of `allowed_values_by_<col>`) are always rendered as strings in BigQuery's own spelling — a BOOL column enumerates as `["false", "true"]` — so a consumer reads every list the same way, though the column's own type still says whether a value belongs quoted in SQL.

### Logging

Structured JSON logging to stdout, compatible with GCP Cloud Logging. Each endpoint (except `/health`) emits one log line per request with:

- `message`: endpoint name (query, schema, sample, stats)
- `log_type`: "endpoint_access"
- `service`: the constant `"db-api"` — the **service discriminator** in the shared sink (see below)
- `log_source`: `LOG_SOURCE`, default `genetics_db_api_prod` — which *environment* wrote the row, not which service
- `endpoint_path`: the route template (`/query`, `/schema`, `/tables/{table_name}/sample`, `/stats`)
- `http_method`
- `principal`: which credential authorized the call — `internal` (the shared secret), `sandbox` (a validated execution token), or `unauthenticated` (the fail-open branch, `INTERNAL_API_SECRET` unset)
- `duration_ms`: request duration in milliseconds
- Endpoint-specific fields: `sql`, `dry_run`, `total_rows`, `rows_returned`, `bytes_processed`, `estimated_cost_usd` (for `/query`); `table`, `tables_returned` (for `/schema`); `table`, `rows_returned` (for `/sample`)

**No `user_email`, deliberately.** db-api sits behind results-api and the internal secret rather
than in front of users, so its caller is a *service*, not a person; `principal` names the
credential, which is the only principal that exists here. Do not read the absence as "the user
was not resolved" — there is no user to resolve.

**Why `service` exists, and why it is not `log_source`.** db-api's rows land in
`phewas-development.genetics_api_logs.stdout` together with results-api's, because a Cloud
Logging → BigQuery sink names its table after the log ID (`stdout`), not after the service.
Something in the payload therefore has to say which service wrote the row, and the two things
that previously did the job both move:

- `endpoint_path IS NULL` identified db-api only while db-api emitted no path — the *absence* of
  a field, which stopped meaning "db-api" the moment db-api started emitting `endpoint_path`.
- `log_source` is derived from the environment, carries no service name, is asymmetric between
  the two services (`genetics_db_api_prod` vs results-api's `finngenie_prod`), and has already
  been renamed once in production (`genetics-results-api-prod` → `finngenie_prod`, 2026-06-03).
  A query keyed on a renamed value returns **nothing and no error**.

`service` is a module constant (`api/main.py`, `SERVICE = "db-api"`), not read from the
environment, so only an edit to that line can move it. `log_source` is kept as the *environment*
axis. The sink's BigQuery schema auto-evolves, so `service` gets its own column on the first row
written after this ships — no migration. Rows written **before** it do not have one; see
`genetics-results-suite/docs/project-spec.md` → "Log sinks" for the three eras a historical query
has to span.

BigQuery cost is estimated at $6.25 per TiB (on-demand pricing). Noisy loggers (uvicorn.access, google, urllib3, asyncio) are suppressed to WARNING level.

### Security

#### Authentication

Every endpoint except `/health` requires `Authorization: Bearer $INTERNAL_API_SECRET` — the same shared secret chat-backend and mcp-server already send on every call, so no client change was needed. The comparison is constant-time (`hmac.compare_digest`) and runs on the **bytes** of both sides: `compare_digest` raises `TypeError` when handed a `str` containing non-ASCII, which turned a bad credential into a 500 instead of a 401 (`genetics-results-suite-zyi`). **The two sides use different codecs on purpose** (`genetics-results-suite-ctq`): the presented token is re-encoded **latin-1**, which undoes exactly how starlette decoded the raw header bytes (verified on the pinned starlette 1.6.0) — UTF-8 re-encoded the mojibake instead (`b"s\xc3\xa9cret"` came back out as `b"s\xc3\x83\xc2\xa9cret"`). `INTERNAL_API_SECRET` stays **UTF-8**. This is *not* justified by "callers transmit it UTF-8-encoded": measured off a real socket, the clients disagree with each other — node fetch/undici and python-requests put latin-1 on the wire, aiohttp puts UTF-8, and httpx 0.28 (which is what mcp-server and chat-backend use) refuses to send a non-ASCII header value at all. Byte-exactness across all callers is therefore unachievable, and under a hypothetical non-ASCII secret this pairing would authenticate the aiohttp-shaped caller and 401 the others — the reverse of the old UTF-8/UTF-8 pairing. What makes the comparison well defined is the **ASCII invariant**, now enforced rather than assumed: `_require_ascii_secret` refuses a non-ASCII `INTERNAL_API_SECRET`, staying silent when it is absent or empty (the dev/test shape). The invariant holds at both times, and **the two times behave differently on purpose**, because `require_auth` obtains the secret only through `_internal_api_secret()` (`genetics-results-suite-xi6`, below) and validating only the import-time snapshot would leave the comparison running on bytes nothing had checked. **At import** `_require_ascii_secret` **raises**: the failure mode is deliberately a startup crash — the pod never passes readiness and the rollout stalls with the old pods still serving, rather than every internal call 401ing at request time with nothing local saying why. **At request time** the accessor **fails closed instead — 401, never an exception.** Raising there would propagate out of a FastAPI dependency as a **500 for every call, including one presenting the correct credential**, on a pod kubelet keeps Ready because `/health` returns before the read — the inverse of the failure mode above. A non-ASCII value can only reach the request-time read through an in-process mutation of `os.environ`, which nothing in `api/` does and no pod environment permits, so refusing is the right answer there and crashing is the right answer at startup. Every codec coincides on ASCII, which is what every deployment has, so nothing observable changed. There is no `try/except UnicodeEncodeError` around the re-encode as there is in results-api, because `require_auth` takes a starlette `Request` and can only see a str starlette itself latin-1-decoded; the comment there says so. `tests/test_api_auth.py` pins the ASCII behaviour, the startup guard, and — with a hand-built ASGI scope — which raw wire bytes authenticate under a non-ASCII secret. That last one cannot be written with TestClient: `starlette/testclient.py` re-encodes httpx's decoded header str as UTF-8, so latin-1 wire bytes never reach the app. The sandbox branch does not shield this — it declines a non-ASCII bearer as not `alg: HS256`-shaped, which is precisely what lets it reach the comparison.

`/health` is exempt because kubelet probes and the monitor CronJob poll it without credentials. FastAPI mounts `/docs`, `/redoc` and `/openapi.json` with `add_route`, which bypasses app-level dependencies, so those three are re-declared as ordinary routes and are authenticated too.

**Fails open when `INTERNAL_API_SECRET` is unset**, logging a startup warning, so local development works unchanged and a cluster mid-rollout doesn't hard-fail. In the deployment the env var comes from the `genetics-secrets/internal-api-secret` key, which `create-secrets.sh` always populates.

**The secret is read per request, not snapshotted at import** (`genetics-results-suite-xi6`). It used to be a module global assigned at import, which made *when the module was first imported* decide whether authentication ran at all: pytest imports every test module at collection time, before any fixture sets the variable, so a shuffled run (`--randomly-seed=2662673150`) froze it to `""`, `require_auth` took its fail-open branch, and the nine tests in `test_api_auth.py` that assert 401 got 200 and **failed** — 9 failed, 87 passed. **The defect was a seed-dependent red suite, not a silently green one**; an earlier version of this paragraph and of the bead note claimed the reverse ("a green suite that had never exercised the auth path"), which is backwards, since a test asserting 401 that receives 200 fails loudly. The justification is the order-dependence itself: the auth path's behaviour depended on collection order, so a shuffled run could go red for reasons unrelated to the change under test, and any future module-scope `import api.main` in a test file would freeze the secret and break an unrelated file.

`require_auth` now calls `_internal_api_secret()`, which reads the environment and ASCII-validates in one place; the import-time read remains, purely so a bad secret still fails fast at startup. **The accessor latches, deliberately asymmetrically**: a runtime change may *enable* authentication (empty → set, which is what the ordering tests need) but may never *disable* it — once the process has observed a non-empty secret, a later empty or deleted one is a fail-**closed** condition (401), not a licence to admit everyone with `principal=None`. The old module global bought that property for free, because a live app kept its own copy; a plain per-request read would have given it away. It is unreachable under a running pod (immutable environ, nothing in `api/` writes `os.environ`) but routine in-process, which is why the tests need it. `tests/test_secret_read_timing.py` pins both import orderings and the latch explicitly rather than relying on a seed.

`api/sandbox_auth.py` reads `SANDBOX_TOKEN_SIGNING_KEY` and `SANDBOX_ENABLED` per call too (`genetics-results-suite-l7z`), which had been left on the import-time snapshot when `INTERNAL_API_SECRET` moved off it. **It is deliberately not latched, and copying `_internal_api_secret`'s latch here would be wrong**: that latch exists because `""` means "authentication was never configured" and takes a fail-**open** early return, so an emptied variable could disable authentication. The signing key governs no fail-open branch — an unset key raises and a rotated one fails the signature, so *every* value except the exact minting key rejects, and the only runtime transitions a plain read admits are unset → set (the ordering hazard, removed) and set → unset/changed (strictly stricter). `require_sandbox_config`'s "sandbox deployed ⇒ both secrets present" invariant stays **startup-only**, now with the reasoning recorded in its docstring rather than left open: for a process that got past the check there is nothing left at runtime to prevent, because an emptied `INTERNAL_API_SECRET` already 401s via the latch and an emptied signing key already rejects every sandbox token (this is not a claim that the bad state is unreachable in the abstract — a process started with *both* `INTERNAL_API_SECRET` and `SANDBOX_ENABLED` unset returns early, never latches, and would serve fail-open if `SANDBOX_ENABLED` later became true; what rules that out is the pod-spec argument below, not this one); its remedy (`sys.exit(1)`) is only correct at startup, where it stalls the rollout with the old pods serving, whereas from a request path it would let the one attacker-authored caller time a process kill; and `SANDBOX_ENABLED` comes from the pod spec, which cannot change without a new pod that re-runs the check. That check now also enforces a **minimum key length** (`genetics-results-suite-4h6.36`): presence alone let `"   "`, `"\n"`, `"x"` and `"0"` through — all truthy, all guessable HMAC keys that mint valid sandbox principals — so `SANDBOX_TOKEN_SIGNING_KEY` shorter than `MIN_SIGNING_KEY_BYTES` (32) bytes ignoring surrounding whitespace is `sys.exit(1)` too. 32 is RFC 7518 §3.2's HS256 minimum and the threshold PyJWT's own `InsecureKeyLengthWarning` names; `create-secrets.sh` (`openssl rand -base64 32`, 44 chars) and `dev-stack.sh` (`secrets.token_urlsafe(32)`, 43) clear it, and results-api's `app/core/sandbox_token.py` carries the identical constant because both verify with the same deployed key. **The gate measures the stripped key and then discards it — `_signing_key()` is untouched and must stay so**: chat-backend mints with its own copy of the secret, so stripping at a verifier would 401 every legitimate token minted from a key deployed with a trailing newline. Such a key produces a startup **warning** instead, so it is visible rather than silently load-bearing. This was never a live vulnerability — the sandbox is not deployed (`SANDBOX_ENABLED` is `false` on both services), and the snapshot failed closed — it is the same order-dependence hazard, in the one credential path whose input is attacker-authored.

`tests/conftest.py` additionally restores `INTERNAL_API_SECRET`/`SANDBOX_TOKEN_SIGNING_KEY`/`SANDBOX_ENABLED` around every test. That is a net, not the fix: the leak was `_reload` in `tests/test_sandbox_token_auth.py`, which popped those variables (and set `PROJECT_ID`) with nothing putting them back, and now routes them through `monkeypatch`. Since l7z all three are read at call time, so restoring all three is load-bearing rather than symmetric-looking — though only `INTERNAL_API_SECRET` can turn a leak into an authorization difference, the other two failing closed. `pytest-randomly` is now declared in `pyproject.toml`: it was present in the global pyenv interpreter but **not** in this project's `.venv`, so the shuffling that surfaced this was never part of the documented `uv pip install -e '.[dev]'` flow.

This was the only access control besides the cluster NetworkPolicy, which is not sufficient on its own: mcp-server is allowed to reach db-api *and* is itself reachable from outside the boundary, so anything that could drive mcp-server could reach BigQuery through it.

#### Query authorization

`/query` submits every statement as a **BigQuery dry run first** (`authorize_query`), and only runs it for real if the dry run passes two checks:

1. **`statementType` must be `SELECT`.** Anything else the dry run parses — `EXECUTE IMMEDIATE`, `CREATE TEMP TABLE`, `EXPORT DATA`, `CALL`, `LOAD`, `GRANT`, or a multi-statement script — is rejected with 400. DML and DDL against an exposed table never reach this check: the read-only service account lacks `bigquery.tables.updateData`/`create`, so the dry run raises `accessDenied` and they get the hedged 403 described in the notes below.
2. **Every entry in `referencedTables` must be an exposed view or the base table it wraps** (`_ALLOWED_TABLE_IDS`). Anything else is rejected with 403, listing the disallowed tables and the available views.

This replaced a keyword blocklist that scanned whitespace-delimited tokens. That approach was evadable — `EXECUTE IMMEDIATE`, `EXPORT DATA`, `CALL` and `LOAD` were not in the list at all, and comment/newline tricks broke tokenisation — and it never constrained *which* tables a `SELECT` could read. Since the API service account holds project-level `bigquery.dataViewer`, an unconstrained `SELECT` could read every dataset in the project, and `EXPORT DATA` could write results to GCS. Letting BigQuery parse the statement leaves nothing to pattern-match against: the dry run reports the real statement type and the real table set.

Notes:

- The dry-run job is created **outside** the endpoint's `try`/`except Exception` block, so its 400/403 reaches the client instead of being converted to a 500. A 400 from the probe is logged at info with the statement and BigQuery's message: a sandbox script's tool result is persisted nowhere, so this line is the only record of why a query was refused once the turn is over.
- **Each exception type the probe can raise is named explicitly, and the status says whose fault it was** — `BadRequest` and `NotFound` are 400, `Forbidden` is 403 or 503 depending on its `reason`. There is deliberately no `except Exception` around the probe: an unknown `google.api_core` failure escaping as a 500 is honest, whereas a blanket catch would report a broken service as the caller's bad query. Every path fails closed — the statement is rejected before anything runs — so the choice is only about what the caller is told.

  **`Forbidden` is not a synonym for "denied".** `google.api_core.exceptions.from_http_status` maps *every* HTTP 403 to `Forbidden` regardless of BigQuery's `reason`, and BigQuery returns 403 for a family of non-authorization failures — `quotaExceeded` (the project over a concurrent-query or `jobs.insert` quota), `billingNotEnabled`, `blocked` — and for `policyViolation`, which is what a table in a project behind a VPC Service Controls perimeter returns; that one takes the same 503 today although it is a caller-chosen reference rather than degradation. `quotaExceeded` is **not** in the client's `_RETRYABLE_REASONS` (`google/cloud/bigquery/retry.py`, which retries only `rateLimitExceeded`, `backendError`, `internalError`, `badGateway`), so a project that trips a quota under agent load surfaces here as a plain `Forbidden`. Answering that with the allow-list refusal would tell every caller their query referenced tables outside the exposed set while naming those very tables as available — false, a 4xx that retry logic will not retry, and invisible to alerting because a 403 reads as routine caller error. The handler therefore discriminates on `e.errors[0]["reason"]` (guarded, since `errors` may be absent or empty): anything other than `accessDenied` is logged at **error** and answered **503**, which is retryable and alertable.

  A genuine `accessDenied` is answered 403 — but the message may **not** claim the table was outside the exposed set, because that is not knowable here. Denial on an *allow-listed* table is reachable (an IAM edit, an expired IAM condition, column-level security or policy tags on a view's columns, a dataset-ACL edit), and the `referencedTables` that would tell the two cases apart do not exist, since the `query()` call raised before any job existed. The wording is hedged accordingly, and this branch also logs at **error**: a denial on an exposed table is an outage.

  BigQuery's denial text stays out of the response on the plain ground that **the caller has no use for it** — it names a fully-qualified table the caller may never have written, and nothing they could do with it changes the outcome. It is *not* muted to close an enumeration oracle: the status code alone already separates "does not exist" (400) from "exists but denied" (403), the allow-list 403 echoes the resolved fully-qualified `disallowed` ids, and the `NotFound`/`BadRequest` branch returns `e.message` verbatim, which hands out `PROJECT_ID.DATASET_ID` for a bare name. The detail goes to the log. The `Forbidden` handler on the *execution* path is muted for the same "no use to the caller" reason, and is unreachable through caller-chosen tables anyway, since the gate has already proved every referenced table is allow-listed. Pinned by `tests/test_authorize_query_errors.py`, which stubs BigQuery (no credentials, runs by default) and asserts per exception type and per `reason` both the status code and that only the dry run was submitted.
- When the caller passes `dry_run: true`, the authorization probe *is* the estimate — its `total_bytes_processed` is returned directly, so no second job is submitted.
- `referencedTables` for a view query may name the view, its base table, or both, depending on how BigQuery expands it; both forms are in the allow-list.
- The allow-list is derived from `VIEWS`, which is itself derived from the `exposed: true` flags in `configs/datasets.yaml`'s `tables` block (`api/yaml_loader.load_views`). A `tables` entry only documents a view; the flag is what exposes it, and it defaults to closed. A table that is loaded but has no view, or a view without the flag, is **not** queryable through `/query`. `tests/test_exposed_views.py` pins the exposed set by name, so a widening fails it in CI — that is a test, not a deploy-path gate: the config reaches a running pod as a ConfigMap without it running.

#### Other controls

- `maximum_bytes_billed` on every BigQuery job, including the internal ones behind `/schema`, `/stats` and `/tables/{name}/sample` (previously uncapped, so a large table could run up an unbounded scan). All four paths resolve the ceiling from the *requesting* caller's principal via `_caps_for()`, and the three internal ones share `_run_internal_query()`. `/schema`'s distinct-value scans used to pass no request at all and so ran at the operator ceiling — for a sandbox caller, twice its own per-query cap — and were charged to nobody
- **The sandbox aggregate byte budget (`SANDBOX_AGGREGATE_BYTES_BUDGET`, 200 GB per `jti`) spans all four paths, not just `/query`.** `/query` charges the dry run's estimate before the job runs and reconciles afterwards to `total_bytes_processed`, refunding the whole estimate in a `finally` if the job raises before it can be reconciled. The three internal paths have no dry run to price them, so `_run_internal_query()` refuses to start a job once the budget is spent and charges what the job processed once it finishes; the budget can therefore be overshot by at most one query's `maximum_bytes_billed`, which is exactly what the per-query cap bounds. `total_bytes_processed` rather than `total_bytes_billed` because a dry run reports only the former, so it is the one figure available on both sides of the correction. Charging `/schema`'s scans to the triggering caller does not contaminate the shared `_get_categorical_values` cache across callers: a job over that caller's ceiling fails and leaves the cache unpopulated for the next caller to retry. The counter is in-process, so db-api's `replicas: 1` is load-bearing — see the comment in the suite repo's `k8s/deployments/db-api.yaml`
- **Per-execution request count and concurrency (`api/sandbox_budget.py`, `genetics-results-suite-4h6.61`).** The byte budget above bounds spend and nothing else, and the paths that run no BigQuery job charge it nothing: `/health`, `/docs`, `/redoc`, `/openapi.json`, an unmatched path, `/schema` on a categorical-value cache hit, `/stats`' `get_table` metadata loop. A sandbox execution has 60-120 s of wall clock in which to loop those at unbounded concurrency, and this pod is `replicas: 1` at `cpu: 500m` / `memory: 512Mi` and also serves the browser's chat path through chat-backend — so the failure mode is an availability one, on a caller that is not the sandbox. `SandboxBudgetMiddleware` therefore admits a slot per request keyed on the token's `jti`, before routing, and releases it in a `finally`:

  | Limit | Default | Env var |
  |---|---|---|
  | Requests per execution | 1000 | `SANDBOX_MAX_REQUESTS_PER_EXECUTION` |
  | In-flight requests per execution | 4 | `SANDBOX_MAX_CONCURRENT_REQUESTS` |
  | In-flight sandbox requests pod-wide | 8 | `SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL` |
  | Tracked executions (map bound) | 4096 | `SANDBOX_MAX_TRACKED_EXECUTIONS` |

  The names, defaults, 429 payload (`detail`/`code`/`limit`/`observed`) and `code` vocabulary (`sandbox_request_count`, `sandbox_concurrency`, `sandbox_concurrency_pod`, `sandbox_execution_tracker_full`) are results-api's, so the two services are diagnosable the same way. Each value is validated at import: below 1 turns a `>=` ceiling into "reject every sandbox request", and a pod-wide bound below the per-execution one makes the per-execution number a lie — both refuse to start. **A request with no sandbox token is not touched at all**: chat-backend and mcp-server present `INTERNAL_API_SECRET` and the kubelet probes `/health` with nothing, and neither creates an entry or can be rejected. It has to be middleware rather than a dependency because `require_auth` is an app-level `Depends`, solved only for a **matched** route — an unmatched path 404s out of the router with no dependency entered, so a dependency placement would neither count it nor release its slot, and the sweep refuses to evict an entry with `in_flight > 0`. For the same reason the middleware resolves the sandbox principal from the raw ASGI headers itself instead of reading `request.state.principal`, which `require_auth` sets later. Eviction is by token expiry (an entry goes only once its token can no longer authenticate **and** nothing is in flight under it), not the LRU `_jti_bytes` uses — an evicted counter is a reset budget. That leaves two per-`jti` maps in this service with different eviction policies; the byte-budget map is deliberately unchanged
- **Unqualified table names are resolved by BigQuery, not by db-api.** Both job configs on the `/query` path — the execution config and `authorize_query()`'s dry-run probe — carry `default_dataset = _DEFAULT_DATASET` (`{PROJECT_ID}.{DATASET_ID}`), so `FROM credible_sets_v` and `FROM datasets` resolve server-side. The two must stay identical: the allow-list is checked against the dry run, so a dry run that resolved a bare name differently from the execution would be a gate bypass. `referencedTables` still comes back **fully qualified** (`{projectId, datasetId, tableId}`) however a name was written, which is what makes `_ALLOWED_TABLE_IDS` independent of the caller's spelling; an explicitly qualified table outside the allow-list still resolves to itself and is still 403'd. A bare name absent from the default dataset is a dry-run `NotFound`, converted to a 400 alongside `BadRequest` — nothing executes.
- **db-api no longer parses SQL, and must not start again.** It used to rewrite the caller's text: `_qualify_tables()` replaced a bare name after `FROM`/`JOIN` with the fully-qualified view, excluding names that `_cte_names()`' paren-depth scan believed a `WITH` clause had declared. That scan was a regex emulating SQL scoping, and three rounds each closed one lexical case and revealed the next, every one returning the **same silent wrong answer** — 889 rows of `datasets_v`, HTTP 200, where the caller asked for their one-row CTE: (1) a CTE aliased to a view or base-table name was rewritten; (2) a *backticked* CTE declaration was erased by the noise-blanking, so the scan saw no CTE; (3) BigQuery decodes escape sequences inside backtick-quoted identifiers, so `` WITH `\u0064atasets` AS (…) `` declares a CTE genuinely named `datasets` that no text match can find, while `` `a\`b` `` desynchronises backtick pairing and erases an arbitrary later span. The class was not exhausted and would not be, because the residuals are the difference between a regex and BigQuery's grammar. Deleting the rewrite deletes the whole class: `_qualify_tables`, `_cte_names`, `_SQL_NOISE`, `_IDENT_NOISE`, `_blank_noise`, `_CTE_SCAN`, `_QUALIFY_TARGETS` and the error hint that explained CTE shadowing are gone, along with the over-collecting residuals (`WINDOW w AS (…)`, `UNNEST(x) WITH OFFSET`, a backticked identifier containing `WITH`) they carried. A CTE now simply shadows, correctly, as in any SQL engine. Do not reintroduce a text rewrite as an optimisation. `_BASE_TABLES` survives for `/schema` and `/tables/{name}/sample` name lookups and for `_ALLOWED_TABLE_IDS`, not for rewriting. **One real behaviour change:** the rewrite used to turn `FROM credible_sets` into `credible_sets_v`, so a human writing a bare base-table name on `/query` now gets the base table — which is not column-identical to its view (`credible_sets_v` adds `maf` and reorders columns), so that query silently loses `maf`. The data is correct and the reach is unchanged (base tables were always allow-listed), and the MCP server is unaffected because it emits only `_v` names. The behaviour is pinned by `tests/test_query_name_resolution.py`, which asserts on the rows returned against a live BigQuery (set `LIVE_BQ_PROJECT_ID` / `LIVE_BQ_DATASET_ID`; the module skips otherwise), and — because this repo has no CI and those tests skip by default — by `tests/test_no_sql_rewriting.py`, which needs no BigQuery and asserts that none of the deleted helpers are back, that `/query` hands both the dry-run probe and the execution the caller's SQL byte for byte, and that the probe's `default_dataset` is taken from the caller's job config rather than a module constant
- IAM-level read-only enforcement on the API service account (see IAM Roles below)

### IAM Roles

All code uses Application Default Credentials (ADC), so role separation is achieved by which service account or user identity runs each component — no code changes needed.

**API (read-only):**
- `roles/bigquery.dataViewer` — read table data
- `roles/bigquery.jobUser` — execute queries

**Data loading and setup (write):**
- `roles/bigquery.dataEditor` — create/write/delete tables and data
- `roles/bigquery.jobUser` — execute load jobs and queries
- `roles/storage.objectViewer` — read source files from GCS

## Configuration

Configuration via environment variables. **Re-derive this table rather than trusting it** —
it silently lost `SANDBOX_ENABLED`, `SANDBOX_TOKEN_SIGNING_KEY` and `LOG_SOURCE` once already:

```sh
grep -rhEA1 '(os\.environ(\.get)?|os\.getenv|_env_int)\(' api/ \
  | grep -oE '"[A-Z][A-Z0-9_]{2,}"' | tr -d '"' | sort -u
```

That finds 15 names, i.e. every row below except `LOCATION` and `GCS_BUCKET`/`GCS_PREFIX`,
which are read only by `scripts/`. Neither of the two complications is optional: `-A1`
because the name is not always on the line that opens the call (`CORS_ORIGINS` in
`api/main.py` sits on the next line), and `_env_int` because the four
`SANDBOX_MAX_*` limits in `api/sandbox_budget.py` are read through that helper, so no
`os.environ` pattern of any kind finds them. A recipe missing either under-reports by five
rows while looking authoritative, which is worse than the stale list it replaces.

| Variable | Default | Description |
|----------|---------|-------------|
| PROJECT_ID | (from gcloud in the scripts; a placeholder in the API) | GCP project ID — the API's fallback is not a real project, so it must be set in a deployment |
| DATASET_ID | genetics_results | BigQuery dataset name — **the default is production**; see "Dev dataset" below |
| LOCATION | europe-west1 | BigQuery dataset location |
| MAX_ROWS | 100000 | Maximum rows returned per query |
| MAX_BYTES_BILLED | 107374182400 | Maximum bytes billed per query (100 GB) |
| PORT | 8080 | API server port |
| DATASETS_CONFIG_PATH | ./configs/datasets.yaml | Path to shared datasets YAML config |
| GCS_BUCKET / GCS_PREFIX | varies by loader (placeholder `bucket-name` with an empty prefix in most; `finngen-commons` with a `GCS_PREFIX` rooted at `results_api_data/` in the newer ones — some append a `mapping_files/` subdirectory, see the individual script for its exact default) | GCS source location for `scripts/load_*.sh`. Whether an explicitly empty `GCS_PREFIX=""` survives or is replaced by the loader's default is a per-loader choice, named at each call site as `resolve_gcs_prefix unset-only` / `unset-or-empty` (`scripts/lib/common.sh`) |
| CORS_ORIGINS | http://localhost:3000,http://127.0.0.1:3000 | Comma-separated origins allowed to call the API from a browser |
| INTERNAL_API_SECRET | (unset) | Shared secret required as `Authorization: Bearer` on every endpoint except `/health`. Unset disables authentication entirely (logs a warning at startup) |
| SANDBOX_ENABLED | (unset, i.e. off) | **Does not gate token acceptance**, despite the name. Its only reader is `require_sandbox_config` (`api/sandbox_auth.py`), called once at import from `api/main.py`, which keys a startup invariant on it: with the flag true and either `INTERNAL_API_SECRET` or `SANDBOX_TOKEN_SIGNING_KEY` missing, the process exits 1. `verify_sandbox_token` never consults it — it consults only the signing key — so with this flag unset and a signing key set, **sandbox tokens are still accepted**. The flag tracks whether the sandbox Deployment exists, nothing more. (`_sandbox_is_deployed` re-reads `os.environ` on each call for consistency with the key accessor, but only that one startup caller ever calls it.) |
| SANDBOX_TOKEN_SIGNING_KEY | (unset) | HS256 key the sandbox execution tokens chat-backend mints are verified against — this, alone, is what decides whether a sandbox token is accepted. Read per call (unset ⇒ every sandbox token 401s). Must be at least `MIN_SIGNING_KEY_BYTES` (32) bytes ignoring surrounding whitespace, or startup fails |
| LOG_SOURCE | genetics_db_api_prod | Value stamped into the `log_source` field of `endpoint_access` lines — three of the four such lines in `api/main.py`; the fourth, in `_aggregate_budget_exceeded`, carries neither `log_source` nor `service`. It is the **environment** axis, not a service discriminator — `jsonPayload.service` (the constant `"db-api"`) is that. See the suite's project-spec → Log sinks |
| SANDBOX_MAX_REQUESTS_PER_EXECUTION | 1000 | Requests one sandbox execution (`jti`) may issue. Applies **only** to a caller presenting a sandbox token |
| SANDBOX_MAX_CONCURRENT_REQUESTS | 4 | In-flight requests per sandbox execution |
| SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL | 8 | In-flight sandbox requests pod-wide, across all executions. Must be >= the per-execution value or the process refuses to start |
| SANDBOX_MAX_TRACKED_EXECUTIONS | 4096 | Bound on the per-execution counter map itself. A backstop, not a working limit |

### Dev dataset

`DATASET_ID` is the only *setting* in the suite that selects a dataset, and its default is
the production one. A local stack started without it queries
`phewas-development.genetics_results` — chat-api and the browser BFF have no dataset
setting of their own, they reach BigQuery only through this API, so the single variable
switches the entire chain.

It does **not** follow that no other service mentions a dataset name. Grepping the sibling
repos: `genetics-results-api` and `genetics-results-browser` contain no BigQuery dataset
name (their `dataset_id` fields are registry keys from `datasets.yaml`, a different thing),
but `genetics-mcp-server` hardcodes `genetics_results.<view>` throughout — in the typed
tools' generated SQL (`tools/executor.py`), in the `run_sql` tool description, and in the
schema docs it ships. Those queries reach BigQuery through this API, so pointing it at
`genetics_dev` does not redirect them: `authorize_query` builds `_ALLOWED_TABLE_IDS` from
`DATASET_ID`, the dry run resolves `genetics_results.<view>` against the default project,
and the request is rejected **403** as referencing tables outside the exposed set. It fails
closed — an MCP client cannot reach production through a dev-pointed API — but MCP custom
SQL and typed tools do not work against `genetics_dev` without changing the MCP server.

| | |
|---|---|
| Dev dataset | `phewas-development:genetics_dev`, location `europe-west1` |
| How to select it | `DATASET_ID=genetics_dev` in the environment that starts `api/main.py` |
| Schema | every table and view in `schemas/`, created by `scripts/setup_bigquery.sh` with `PROJECT_ID`/`DATASET_ID`/`LOCATION` set explicitly — except the rCNV tables (`dosage_sensitivity` and `rcnv_*`), not seeded here — `bq ls phewas-development:genetics_dev` is the live list |
| Data | ~3.6M rows / ~612 MB, against production's ~1.1B rows / ~224 GB |

The location must be the **region** `europe-west1`, matching the production datasets, not
the GKE zone `europe-west1-b`. A dataset's location cannot be altered after creation, and
a query joining datasets in different locations fails outright.

The subset is **chromosome 22 for the results tables**, capped at 500k rows for
`gene_burden_results` and `open_chromatin`, plus complete copies of `datasets`,
`phenotypes`, `gene_annotations` and `hla_associations` — the registry tables because any
subset of them makes dev misleading, and HLA because it is chromosome 6 by construction and
a chr22 filter would empty it. Every view returns rows.

The coloc triple is **not** capped blindly, because it is the one group where a missing row
misleads rather than merely thins. `coloc_credsets` is seeded from the credible-set IDs the
loaded `colocalization` rows actually reference (every `cs1_id` and `cs2_id`), and
`credible_sets` holds its chr22 slice **plus** every row for that same ID set. So both
directions of the pivot resolve: all 41,131 `colocalization_v` rows resolve both `cs1_id`
and `cs2_id` in `coloc_credsets_v`.

Three consequences worth stating, because all three are silent:

- A 500k-row cap takes an arbitrary slice, so cross-table results for the two capped tables
  are thinner than production's; absence of a row in dev is not evidence.
- The `credible_sets` ↔ `coloc_credsets` `cs_id` overlap is small (518 of the 3,908 IDs the
  coloc slice references) — but that is production's own overlap for these IDs, not a dev
  artifact. Seeding cannot raise it.
- Row counts and query timings here mean nothing for capacity or cost work. Benchmarks
  belong against production-scale data.

`genetics_dev` was populated by `INSERT INTO genetics_dev.<table> (cols) SELECT cols FROM
genetics_results.<table> WHERE chr = 22`, reading production rather than re-running the
GCS loaders, which have no subsetting mechanism and would have loaded all ~224 GB
(`coloc_credsets` and `credible_sets` use the `cs_id` seed described above instead of the
`chr = 22` filter, reloaded with `TRUNCATE TABLE` + `INSERT` — never `CREATE OR REPLACE
TABLE AS SELECT`, which flattens every column to NULLABLE and would drop the partitioning
and clustering). One
table needs its production **view** as the source instead: `genetics_results.credible_sets`
predates the clustering swap and stores neither `resource` nor `variant`, while the schema
in `schemas/credible_sets.sql` clusters on both, so only `credible_sets_v` exposes the full
dev column set. Reloading the dev dataset from scratch costs about $0.01 and two minutes.

CORS responses cannot use a wildcard origin: the API is configured with
`allow_credentials=True`, and browsers reject `Access-Control-Allow-Origin: *` on
credentialed requests. Set `CORS_ORIGINS` to the exact origins of any browser client.

## Project Structure

```
genetics-results-db/
├── schemas/
│   ├── credible_sets.sql      # BigQuery table definition
│   ├── credible_sets_v.sql    # View with variant and resource columns
│   ├── colocalization.sql     # BigQuery table definition
│   ├── colocalization_v.sql   # View with resource columns
│   ├── coloc_credsets.sql     # BigQuery table definition
│   ├── coloc_credsets_v.sql   # View with variant and resource columns
│   ├── exome_variant_results.sql      # GeneBASS variant results table
│   ├── exome_variant_results_v.sql    # View with variant and resource columns
│   ├── gene_burden_results.sql        # GeneBASS gene burden results table
│   ├── gene_burden_results_v.sql      # View with resource column
│   ├── asm_qtl.sql                    # deCODE allele-specific methylation QTL table
│   ├── asm_qtl_v.sql                  # View with variant, maf and resource columns
│   ├── gene_annotations.sql           # Whole-universe gene annotations table (HGNC + GENCODE)
│   ├── gene_annotations_v.sql         # View with resource column
│   ├── open_chromatin.sql             # Open-chromatin atlas table (accessible regions by cell type/tissue/condition)
│   ├── open_chromatin_v.sql           # View with resource column
│   ├── peak_to_gene.sql               # Open4Gene peak→gene links (joins peak-keyed caQTL results to genes)
│   ├── peak_to_gene_v.sql             # View with resource column
│   ├── variant_effect.sql             # Predicted variant-effect table (ChromBPNet/FLARE scores; stored variant column)
│   ├── variant_effect_v.sql           # View with resource column
│   ├── mpra.sql                       # Measured MPRA allelic activity table (Siraj et al.; stored variant column)
│   ├── mpra_v.sql                     # View with resource column
│   ├── hla_associations.sql           # Classical HLA allele associations (FinnGen R14; allele-keyed, no ref/alt)
│   ├── hla_associations_v.sql         # View with resource column (mapped to 'finngen')
│   ├── dosage_sensitivity.sql         # Collins et al. 2022 gene dosage-sensitivity scores (pHaplo/pTriplo)
│   ├── dosage_sensitivity_v.sql       # View with constant resource='rcnv'
│   ├── rcnv_gene_associations.sql     # Collins et al. 2022 per-phenotype DEL/DUP gene association statistics
│   ├── rcnv_gene_associations_v.sql   # View with resource column ('rcnv') + LEFT JOIN of the dosage-sensitivity scores
│   ├── rcnv_segments.sql              # Collins et al. 2022 disease-associated rare-CNV segments (Table S3)
│   ├── rcnv_segments_v.sql            # View with resource column ('rcnv') + the six list columns SPLIT into ARRAY<STRING>
│   ├── rcnv_window_associations.sql   # Collins et al. 2022 sliding-window DEL/DUP association statistics (GRCh38, GRCh37 pair kept)
│   ├── rcnv_window_associations_v.sql # View with resource column ('rcnv'); no join — a window has no gene
│   ├── variant_annotation.sql         # FinnGen R14 per-variant functional annotations (stored variant column)
│   ├── variant_annotation_v.sql       # View with constant resource='finngen'
│   ├── phenotypes.sql                 # Trait metadata keyed by (dataset, trait_original)
│   ├── phenotypes_v.sql               # Pass-through view (resource is a registry column)
│   ├── datasets.sql                   # Dataset registry keyed by results-view `dataset`
│   └── datasets_v.sql                 # Pass-through view (resource is a registry column)
├── scripts/
│   ├── lib/
│   │   └── common.sh      # helpers shared by scripts/load_*.sh — add here rather than duplicating across loaders
│   ├── setup_bigquery.sh      # Create dataset and tables
│   ├── load_data.py           # Python loader for tsv.gz files
│   ├── load_credsets_coloc.sh  # Load credible sets and colocalization data
│   ├── load_pseudo.sh         # Load pseudo credible sets (FinnGen+UKBB/MVP meta-analyses, external EXT file: COVID-19 HGI + PGC + GP2)
│   ├── load_genebass_variants.sh    # Load GeneBASS exome variant results (truncates table)
│   ├── load_genebass_gene.sh        # Load GeneBASS gene burden results, unfiltered per-trait files (truncates table)
│   ├── load_exome_variants_extra.sh # Append additional exome variant results (IBD)
│   ├── load_gene_burden_extra.sh    # Append additional gene burden results, unfiltered (BipEx, IBD, SCHEMA2)
│   ├── load_brava_gene.sh           # Append BRaVa gene burden results, unfiltered per-trait files (deletes dataset='BRaVa' first, no truncate)
│   ├── load_asm_qtl.sh        # Load ASM-QTL (allele-specific methylation) data from deCODE
│   ├── load_open_chromatin.sh # Load open-chromatin atlas (6 datasets; chr-string→INT64 conversion, truncate+append)
│   ├── load_peak_to_gene.sh   # Load Open4Gene peak→gene links (chr-string→INT64, cell_type prefix strip, WRITE_TRUNCATE)
│   ├── load_variant_effect.sh # Load predicted variant effects (marderstein chrombpnet+flare; chr-string→INT64 conversion, truncate+append)
│   ├── load_mpra.sh           # Load Siraj MPRA results (single LONG file; chr-string→INT64, dataset injected via --const-column)
│   ├── load_hla.sh            # Load FinnGen HLA allele associations (single combined file; chr-string→INT64, dataset injected via --const-column)
│   ├── load_dosage_sensitivity.sh # Load Collins et al. 2022 dosage-sensitivity scores (WRITE_TRUNCATE) + create dosage_sensitivity_v view
│   ├── load_rcnv_gene_associations.sh # Load Collins et al. 2022 rCNV gene associations (WRITE_TRUNCATE) + create rcnv_gene_associations_v view; run before load_phenotypes.sh
│   ├── load_rcnv_segments.sh      # Load Collins et al. 2022 rCNV segments (WRITE_TRUNCATE) + create rcnv_segments_v view
│   ├── load_rcnv_window_associations.sh # Load Collins et al. 2022 rCNV sliding-window associations (WRITE_TRUNCATE) + create rcnv_window_associations_v view
│   ├── load_variant_annotation.sh # Load FinnGen R14 variant annotations (same file the API serves; WRITE_TRUNCATE)
│   ├── load_gene_annotations.sh   # Build + load gene_annotations table (WRITE_TRUNCATE) + create gene_annotations_v view
│   ├── build_gene_annotations.py  # Build gene_annotations NDJSON from HGNC + GENCODE sources
│   ├── load_phenotypes.sh         # Build + load phenotypes and datasets metadata tables (WRITE_TRUNCATE)
│   ├── live_dataset_scope.py      # Derives the registry cross-check scope from datasets.yaml
│   ├── build_phenotypes.py        # Build phenotypes/datasets NDJSON from datasets.yaml + its metadata_file sources
│   └── generate_resource_sql.py # Generate/lint CASE/WHEN SQL from shared datasets.yaml
├── configs/
│   └── datasets.yaml          # Shared dataset/resource config — generated, gitignored;
│                              #   run ../genetics-results-suite/scripts/sync-datasets.sh
├── api/
│   ├── main.py                # FastAPI application
│   ├── sandbox_auth.py        # Per-execution sandbox JWT validation (the caps it gates are in main.py)
│   ├── sandbox_budget.py      # Per-jti request-count/concurrency gate + its ASGI middleware
│   └── yaml_loader.py         # Loads datasets.yaml into data structures used by main.py, VIEWS included
├── tests/                     # All client-free unless noted; none needs BigQuery credentials
│   ├── conftest.py            # Foreign-checkout guard + auth env restore
│   ├── test_api_auth.py       # Shared-secret authentication tests (never reach BigQuery)
│   ├── test_authorize_query_errors.py  # /query error mapping per BigQuery exception (stubbed)
│   ├── test_build_gene_annotations.py  # gene_annotations build unit tests
│   ├── test_build_phenotypes.py        # phenotypes/datasets NDJSON build unit tests
│   ├── test_endpoint_access_log.py     # endpoint_access attribution rows
│   ├── test_exposed_views.py           # pins the exposed view set / /query allow-list by name
│   ├── test_hla_view_columns.py        # hla_associations_v select list vs base-table schema
│   ├── test_internal_query_caps.py     # per-credential row/byte caps
│   ├── test_live_dataset_scope.py      # registry cross-check scope derived from datasets.yaml
│   ├── test_load_data_row_counts.py    # load_table's (job, rows_written) contract
│   ├── test_no_sql_rewriting.py        # asserts the deleted SQL-rewriting helpers stay deleted
│   ├── test_query_caps.py              # /query row and byte limits
│   ├── test_query_name_resolution.py   # bare-name resolution (skips without LIVE_BQ_* env)
│   ├── test_sandbox_budget.py          # per-jti request-count and concurrency gate
│   ├── test_sandbox_token_auth.py      # sandbox execution-token validation
│   └── test_secret_read_timing.py      # per-request secret read and its fail-closed latch
├── docs/
│   ├── credible-sets-clustering-swap.md  # credible_sets clustering swap runbook
│   └── project-spec.md        # This document
├── pyproject.toml             # Python project metadata and dependencies
├── Dockerfile                 # Container image (built & deployed by genetics-results-suite via k8s)
├── .dockerignore              # Keeps local secrets and caches out of the build context
├── README.md                  # Usage documentation
└── .gitignore
```

## Deployment

### Prerequisites

- Google Cloud SDK (`gcloud`) configured
- BigQuery API enabled
- **`configs/datasets.yaml` must exist.** It is generated, not committed — the canonical
  file lives in `genetics-results-suite`. Create the local copy with
  `../genetics-results-suite/scripts/sync-datasets.sh`, or set `DATASETS_CONFIG_PATH`.
  `api/main.py` raises at import without it, so the API and the test suite both fail to
  start on a fresh clone.
- **The project virtualenv must be activated** before running any `scripts/load_*.sh`.
  The loaders invoke bare `python3`, which resolves to the system interpreter — that
  one has no `google-cloud-bigquery` and the load dies with
  `ModuleNotFoundError: No module named 'google'`:
  ```bash
  source .venv/bin/activate      # create/refresh with: uv sync
  ```
  (Not needed inside the container: `Dockerfile` installs dependencies with
  `uv pip install --system`, so there is no `.venv` there.)

### Setup Steps

1. **Create BigQuery dataset and tables**:
   ```bash
   export PROJECT_ID=your-project-id
   ./scripts/setup_bigquery.sh
   ```
   This is not optional for the tables `scripts/load_data.py` loads through its staging table (`--const-column`, `CHR_STRING_TABLES`, `CELL_TYPE_PREFIX_TABLES`, `DERIVED_COLUMNS`): the loader `TRUNCATE`s and `INSERT`s into the table `setup_bigquery.sh` made from `schemas/*.sql` and refuses to create it, because a loader-created table would carry neither the NOT NULL modes nor the column descriptions.

2. **Load credible sets and colocalization data from GCS**:
   ```bash
   ./scripts/load_credsets_coloc.sh
   ```
   Loads run in parallel: each table's wipe (the `credible_sets` surgical `DELETE`, and the first `WRITE_TRUNCATE` load of each coloc table) is awaited before that table's files are appended concurrently. `credible_sets` loads first, then `colocalization` and `coloc_credsets` load concurrently.

   This script owns the genuinely fine-mapped credible sets, including `PGC_SCZ_2022` (the published PGC schizophrenia FINEMAP sets, Trubetskoy et al. 2022) — pseudo credible sets belong to `load_pseudo.sh` instead. `PGC_SCZ_2022` rows sit next to the `PGC` pseudo rows under resource `pgc` and cover the same trait code `SCZ`, so filtering on `resource` alone mixes fine-mapped and pseudo results.

3. **Load pseudo credible sets** (FinnGen+UKBB and FinnGen+MVP+UKBB meta-analysis pseudo credible sets, plus a single shared external `EXT_*` file bundling COVID-19 HGI (`covid_hgi`), PGC SCZ (`pgc_scz`), PGC BIP (`pgc_bip`), and GP2 PD (`gp2_pd`) pseudo credible sets — the pre-load DELETE clears the `COVID19_HGI`, `PGC`, and `GP2` dataset rows together):
   ```bash
   ./scripts/load_pseudo.sh
   ```
   The script reads `GCS_BUCKET` (default `finngen-commons`) and `GCS_PREFIX` (default `results_api_data/`). For the daly-finngenie bucket layout where credible_sets live at the bucket root, override with `GCS_BUCKET=daly-genetics-results GCS_PREFIX=""`.

4. **Load GeneBASS exome data from GCS** (truncates target tables):
   ```bash
   ./scripts/load_genebass_variants.sh
   ./scripts/load_genebass_gene.sh
   ```
   The gene burden load reads the ~4.5k unfiltered per-trait files under
   `exome_results/genebass/gene_burden_per_trait/` via a `*.tsv.gz` wildcard, so it moves
   ~343M rows and takes considerably longer than the variant load. The variant load stays
   on the mlog10p > 4 file.

5. **Append additional exome and gene burden results** (IBD, SCHEMA2, BipEx) on top of the GeneBASS load:
   ```bash
   ./scripts/load_exome_variants_extra.sh
   ./scripts/load_gene_burden_extra.sh
   ```

   BRaVa is appended by its own script, which is not tied to that ordering: it deletes the
   `dataset = 'BRaVa'` rows before appending, so a rerun is idempotent and a dataset without
   the GeneBASS load (the rehearsal dataset, via `DATASET_ID=genetics_results_brava_dev`) is
   a valid target.
   ```bash
   GCS_BUCKET=daly-genetics-results GCS_PREFIX= DATASET_ID=genetics_results_brava_dev \
     ./scripts/load_brava_gene.sh
   ```
   It loads `exome_results/brava/gene_burden_per_trait/*.tsv.gz` through one wildcard, then
   checks the distinct trait count against the number of matched objects — the per-trait
   object names carry the ancestry stratum (`AFib|EUR.tsv.gz`), so that check is what
   establishes the wildcard reached the names containing `|`.

   It is also one of the views `scripts/load_phenotypes.sh` depends on for the daly profile:
   `build_phenotypes.BQ_DATASETS_BY_DATASET_ID['brava_gene_based']` names `BRaVa`, and the
   registry cross-check fails for the whole profile while no results view carries that value.
   Run `load_brava_gene.sh` before `load_phenotypes.sh` against the same BigQuery dataset on
   daly.

6. **Build and load the gene_annotations reference table** (manual, on-demand; full rebuild via `WRITE_TRUNCATE`):
   ```bash
   HGNC_VERSION=2026-06-01 ./scripts/load_gene_annotations.sh
   ```
   `build_gene_annotations.py` joins the HGNC complete-set, GENCODE v49 coordinates, and the three HGNC gene-group CSVs from GCS `mapping_files/` into a single NEWLINE_DELIMITED_JSON file (required to carry the `gene_group_ids`/`gene_group_names` REPEATED array columns, which the CSV loader cannot populate), which `load_data.py` then loads with `WRITE_TRUNCATE`. The provenance columns `gencode_version`, `hgnc_version`, and `download_date` are stamped at build time. This table has no streaming/incremental load; rerun the script to refresh. Defaults to `GCS_BUCKET=finngen-commons`, `GCS_PREFIX=results_api_data/mapping_files/`; for the daly layout use `GCS_BUCKET=daly-genetics-results GCS_PREFIX=mapping_files/` (note this one is not the bucket root — the mapping files sit under `mapping_files/` in both buckets).

7. **Load Open4Gene peak-to-gene links** (full refresh via `WRITE_TRUNCATE`):
   ```bash
   ./scripts/load_peak_to_gene.sh
   ```
   Reads `gs://<bucket>/<prefix>atacseq/open4gene.all.results.sig.tsv.gz` — the same file the API serves — and injects `dataset=FinnGen_ATACseq` so the table joins `credible_sets` directly. Defaults to `GCS_BUCKET=finngen-commons`, `GCS_PREFIX=results_api_data/`; override with `GCS_BUCKET=daly-genetics-results GCS_PREFIX=""` for the daly layout.

8. **Load FinnGen variant annotations** (same source the API serves; full refresh via `WRITE_TRUNCATE`):
   ```bash
   ./scripts/load_variant_annotation.sh
   ```
   Defaults to `gs://finngen-commons/results_api_data/variant_annotations/R14_annotated_variants_v0.small.gz`. Override `GCS_BUCKET`, `GCS_PREFIX`, or `VA_FILE` for other bucket layouts (e.g. `GCS_BUCKET=daly-genetics-results GCS_PREFIX=""`).

9. **Load ASM-QTL results** (deCODE CpG + MDS; first file truncates, the second appends):
   ```bash
   ./scripts/load_asm_qtl.sh
   ```
   Reads `gs://<bucket>/<prefix>asm_qtl/deCODE_asmQTL_{CpG,MDS}.munged.tsv.gz` and injects the `dataset` value per file, since the munged TSVs carry no `dataset` column.

10. **Load the open-chromatin atlas** (6 datasets; first truncates, the rest append):
    ```bash
    ./scripts/load_open_chromatin.sh
    ```
    Reads `gs://<bucket>/<prefix>open_chromatin/<resource>/<dataset-id>.tsv.gz`. The canonical TSVs already carry `dataset`, and the chrom string is converted to INT64 on load.

11. **Load predicted variant effects** (marderstein chrombpnet + flare):
    ```bash
    ./scripts/load_variant_effect.sh
    ```
    Reads `gs://<bucket>/<prefix>variant_effect/<resource>/<dataset-id>.tsv.gz`, same layout and conventions as the open-chromatin load.

12. **Load MPRA results** (single LONG file, `WRITE_TRUNCATE`):
    ```bash
    ./scripts/load_mpra.sh
    ```
    Reads `gs://<bucket>/<prefix>mpra/siraj_mpra/siraj_mpra.tsv.gz` and injects `dataset=siraj_mpra`, since — unlike the open-chromatin and variant-effect files — the MPRA LONG file has no `dataset` column.

These four loaders default `GCS_BUCKET` to the placeholder `bucket-name`, so set `GCS_BUCKET` (and `GCS_PREFIX`, e.g. `results_api_data/` for finngen-commons, empty for the daly layout) explicitly.

13. **Build and load the phenotype/dataset metadata tables** (full rebuild via `WRITE_TRUNCATE`):
    ```bash
    PROFILE=finngen ./scripts/load_phenotypes.sh
    ```
    `build_phenotypes.py` reads the synced `configs/datasets.yaml` and every `metadata_file` it references from GCS, harmonizes them (mirroring genetics-results-api's `MetadataHarmonizer`), and writes two NEWLINE_DELIMITED_JSON files that `load_data.py` loads. **Re-run after any change to `datasets.yaml` or a metadata file** — nothing else propagates registry edits into BigQuery.

    `PROFILE` selects both the dataset registry and, through the registry's `metadata_file` URIs, the bucket the metadata is read from; `GCS_BUCKET`/`GCS_PREFIX` only control where the generated NDJSON is staged (default `finngen-commons` / `results_api_data/mapping_files/`).

    The builder owns `BQ_DATASETS_BY_DATASET_ID`, the registry-key → results-view-`dataset` map. That value is baked into the source credible-set TSVs by genetics-results-munge and `datasets.yaml` never records it, so **a new dataset must be added there** or it gets a `datasets` row with `dataset = NULL` and no `phenotypes` rows. The loader cross-checks the map against the live views in **all** directions, and any mismatch **fails the build**:

- a live `dataset` value with no registry entry,
- a registry claim that no results table contains,
- a `phenotypes` row keyed on a `dataset` no results table contains,
- a name suppressed by `ABSENT_FROM_RESULTS` for this profile that has since become live in it.

The **scope** of that cross-check is derived, not listed. `scripts/live_dataset_scope.py` reads the exposed view names from `configs/datasets.yaml` through the same `api/yaml_loader.load_views` `api/main.py` derives `VIEWS` from, so one definition of "exposed" serves both. They read separate *copies* of that registry, though — the API reads the ConfigMap `deploy.sh` builds from the suite repo's canonical file at deploy time, this script reads the checkout's `sync-datasets.sh`-generated copy at run time — so the two agree only while the sync and the deploy are both current. It reads `INFORMATION_SCHEMA.COLUMNS` for the `dataset` / `dataset1` / `dataset2` columns, and generates the `UNION ALL` the loader runs. Anything the API exposes is therefore in scope automatically: a newly added view puts its `dataset` values in front of the check the moment it is exposed, and an unmapped value fails the build. The earlier version unioned nine hardcoded table names, which meant a brand-new table contributed nothing and its drift could not be detected — the check failed *open* for exactly the case where drift is most likely. That is how `hla_associations` reached BigQuery with a `datasets` table holding zero `finngen_hla` rows while this loader reported success.

Views leave that scope only through `tables.<view>.dataset_cross_check.excluded_reason` in `configs/datasets.yaml`, which puts the exclusion and its reason beside the view it is about rather than in a list in this repo: the reference tables have no `dataset` column, and the registry-built views are built *from* the map under test, so including them would make the check confirm itself. Re-derive the set with `live_dataset_scope.excluded_views()` rather than trusting a list here; `tests/test_derived_view_lists.py` pins it. A view that is neither excluded nor has a dataset-bearing column **fails loudly** — being skipped for a missing column is the same fail-open trap one level down. (`peak_to_gene_v` is deliberately *not* excluded: contrary to an earlier note here it does carry a `dataset` column, a constant `FinnGen_ATACseq`, and including it costs nothing.)

If the cross-check query itself returns nothing (bad auth, quota, a renamed view) the loader **refuses to run** rather than loading unvalidated; `ALLOW_UNVALIDATED=1` overrides both that and the mismatch failures.

`hla_associations` names its trait column **`phenotype`** — a third spelling alongside `trait` and `trait_original`. Its 2,712 codes are the FinnGen R14 endpoint codes, but `finngen_hla` still gets its own `phenotypes` rows rather than borrowing `FinnGen_R14`'s: the table is keyed on `(dataset, trait_original)` so that every results-view `dataset` resolves its own names with one uniform join, and `FinnGen_R12` already duplicates 2,315 of R14's codes for the same reason. The join is `p.dataset = 'finngen_hla' AND p.trait_original = h.phenotype`.

`build_phenotypes.ABSENT_FROM_RESULTS` records the names deliberately mapped but absent from BigQuery — eQTL Catalogue sub-studies that are in the collection metadata but not in the imported release, and datasets registered before their fine-mapping was loaded. They are emitted with `dataset = NULL` and contribute no `phenotypes` rows, so nothing points at an empty result. Read the entries out of the code rather than from a list here.

Absence is **per-deployment state**, not a property of the dataset, so each entry carries the profiles it applies to (`ALL_PROFILES` for every profile) and `--profile` resolves the list before the build. Listing a name globally suppresses its rows in a deployment that *has* the data, with no error anywhere — `ibd_gwas` lost its IBD/UC/CD phenotype rows that way in a deployment whose `credible_sets_v` holds `IIBDGC`. The scope is an allow-list of profiles rather than an exclusion, so a profile added later inherits nobody's absence and fails `validate()` loudly if the data really is missing there.

### API deployment

The API is **not** deployed from this repo. The container image (`Dockerfile`) is
built from this repo and deployed to Kubernetes by the **genetics-results-suite**
repo (the `db-api` deployment). This repo provides the BigQuery schema, data
loaders, and API code only.

## Example Queries

The examples the API serves to clients (returned per view by `/schema`, rendered in
the browser's schema drawer and given to the LLM) live under `tables.<view>.examples`
in `configs/datasets.yaml` — edit them in the canonical
`../genetics-results-suite/configs/datasets.yaml` and sync. They are derived from
queries actually run against the deployment and encode the pitfalls seen there:
filter `chr` alongside `variant` for partition pruning, look up `trait_original`
codes before filtering, take the top-PIP row per `cs_id` for lead variants, test
region overlap (`region_start_min`/`region_end_max`) in `colocalization_v`, match
both `trait1` and `trait2`, and join `coloc_credsets_v` on `(cs_id, dataset)`.

The queries below are ad-hoc examples against the base tables.

### Genes with multiple high-confidence coding variants
```sql
SELECT gene_most_severe, COUNT(*) as n
FROM credible_sets
WHERE pip > 0.5
  AND most_severe IN ('missense_variant', 'frameshift_variant', 'stop_gained')
GROUP BY gene_most_severe
HAVING COUNT(*) > 2
ORDER BY n DESC
```

### Strong colocalizations (H4 > 0.9)
```sql
SELECT * FROM colocalization WHERE PP_H4_abf > 0.9 LIMIT 100
```

### Exome variants in a gene
```sql
SELECT chr, pos, ref, alt, annotation, trait, mlog10p, beta
FROM exome_variant_results
WHERE gene = 'BRCA1'
ORDER BY mlog10p DESC
LIMIT 100
```

### Significant gene burden test results
```sql
SELECT gene, trait, annotation, mlog10p_burden, beta
FROM gene_burden_results
WHERE mlog10p_burden > 5
ORDER BY mlog10p_burden DESC
LIMIT 100
```

### Functional annotation for specific variants
```sql
SELECT variant, rsid, most_severe, gene_most_severe, AF, INFO
FROM variant_annotation
WHERE variant IN ('19:44908684:T:C', '1:13668:G:A')
```

### Coding variants in a gene
```sql
SELECT variant, rsid, most_severe, AF
FROM variant_annotation
WHERE gene_most_severe = 'APOE'
  AND most_severe IN ('missense_variant', 'frameshift_variant', 'stop_gained')
ORDER BY AF DESC
```

### Genes in a gene group (any-group enumeration with coordinates)
```sql
SELECT symbol, chr, gene_start, gene_end
FROM gene_annotations_v
WHERE 139 IN UNNEST(gene_group_ids)
```
Because `gene_group_ids` carries the full lineage, this matches genes whose leaf group *or* any ancestor group is 139.

### cis-pQTL colocalizations (gene coordinates JOINed to colocalizations)
```sql
SELECT c.trait1, a.symbol, c.PP_H4_abf
FROM gene_annotations_v a
JOIN colocalization_v c ON c.trait2 = a.symbol
WHERE c.data_type2 = 'pQTL'
  AND SAFE_CAST(SPLIT(c.hit2, ':')[OFFSET(1)] AS INT64)
      BETWEEN a.gene_start - 1000000 AND a.gene_end + 1000000
```
Filters to cis colocalizations (QTL lead variant within ±1 Mb of the gene). Dropping or inverting the position predicate gives trans signals.

## Lint gate

`scripts/lint-staged.sh` runs ruff over the **staged** Python files from the `pre-commit`
hook and **fails the commit** when anything is reported. `git commit --no-verify` is the
bypass. It is the one pre-commit check that blocks; `scripts/check-doc-drift.sh` only warns.

- **Staged, not repo-wide.** A finding in a file the commit does not touch never blocks it.
  The other side of that coin is that a pre-existing finding is only ever cleared by
  touching its file; `scripts/lint-staged.sh --all` runs the repo-wide check.
- **The working tree, not the index.** It lints the working-tree copy of each staged path,
  because doing it properly means materialising the index somewhere and a hook that stashes
  can lose work if interrupted. The difference shows only when a file is partially staged,
  and that case is detected and printed rather than left to be found later.
- **Resolving ruff:** this checkout's `.venv`, then the **main checkout's** (a worktree has
  none of its own), then `PATH`, then `uvx ruff@<pin>`. With none of those it **fails the
  commit** rather than passing it unchecked — a gate that skips when its linter is missing
  is indistinguishable from a clean commit. A resolved ruff that is not the pinned version
  warns and proceeds.

The rule set matches the sibling repos, because the same gate runs in all five and a
per-repo rule set means the same file passes in one and fails in the next. Neither hook
runs until `scripts/install-git-hooks.sh` has been run once in the clone; `core.hooksPath`
is local git config that no clone carries, and it is shared across worktrees, so that one
run covers every worktree too.

## To Be Implemented

1. Per-user authentication for external access (OAuth, per-caller API keys). The current shared secret authenticates the *service*, not the user behind the request, so it cannot support per-user authorization or attribution.
2. Rate limiting for query endpoint
3. Query result caching for repeated queries
4. Possibly additional endpoints for common query patterns (variant lookup, gene lookup)
