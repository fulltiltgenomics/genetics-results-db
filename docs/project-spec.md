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
- Auto-qualification of table names in queries for simpler SQL (base table names are redirected to views)

## Architecture

```
GCS (tsv.gz files)
      ↓ (one-time load via bq load)
BigQuery Dataset
  ├── credible_sets (partitioned by chr, clustered by dataset, data_type, most_severe)
  │   └── credible_sets_v (view: adds variant, resource columns)
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
  └── peak_to_gene (unpartitioned link table, clustered by symbol, cell_type, peak_id)
      └── peak_to_gene_v (view: adds resource column)
      ↓
API (FastAPI) — exposes only views, not underlying tables
      ↓
AI Agents / Applications
```

## Data Model

### credible_sets

Fine-mapped credible set variants from multiple genetics datasets.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| dataset | STRING | Yes | Source dataset (FinnGen_R14, Open_Targets_26.06, etc.) |
| data_type | STRING | Yes | GWAS, eQTL, pQTL, sQTL, caQTL |
| trait | STRING | Yes | Phenotype/trait name. For `caQTL` rows this is a chromatin peak id (`chr-start-end`), never a gene — reach genes via [peak_to_gene](#peak_to_gene) |
| trait_original | STRING | Yes | Original trait name |
| cell_type | STRING | No | Cell/tissue type (null for GWAS) |
| chr | INT64 | Yes | Chromosome |
| pos | INT64 | Yes | Position |
| ref | STRING | Yes | Reference allele |
| alt | STRING | Yes | Alternate allele |
| mlog10p | FLOAT64 | No | -log10(p-value) |
| beta | FLOAT64 | Yes | Effect size |
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

Gene-level burden test results from exome sequencing studies (GeneBASS, BipEx2, IBD exome, SCHEMA2). All filtered to mlog10p_burden > 4. Data files are in `exome_results/` on GCS.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| dataset | STRING | Yes | Source dataset (genebass, BipEx2, IBD_exome, SCHEMA2) |
| trait | STRING | Yes | Trait identifier |
| gene | STRING | Yes | Gene symbol |
| gene_id | STRING | Yes | Ensembl gene ID |
| chr | INT64 | Yes | Chromosome |
| gene_start_pos | INT64 | Yes | Gene start position |
| gene_end_pos | INT64 | Yes | Gene end position |
| annotation | STRING | Yes | Annotation category (pLoF, nonsynonymous, etc.) |
| mlog10p_burden | FLOAT64 | Yes | -log10(p-value) for burden test |
| beta | FLOAT64 | Yes | Effect size |
| se | FLOAT64 | Yes | Standard error |
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

## Technical Implementation

### BigQuery Configuration

- **Partitioning**: Result tables partitioned by chromosome using `RANGE_BUCKET(chr, GENERATE_ARRAY(1, 23, 1))`. The two small reference/link tables (`gene_annotations`, `peak_to_gene`) are unpartitioned — a full scan of them is cheap and their access is gene-keyed rather than positional.
- **Clustering**: Tables clustered by frequently filtered columns (dataset, data_type, most_severe; `symbol` first for the gene-keyed tables)

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

Every query is authorized before it runs (see Security → Query authorization). Rejections are 400 for a non-`SELECT` statement or a syntax error, 403 for a `SELECT` that references a table outside the exposed views.

### Query Response Format

```json
{
  "columns": ["col1", "col2"],
  "rows": [["val1", "val2"], ...],
  "total_rows": 100,
  "bytes_processed": 1048576,
  "truncated": false
}
```

### Schema Response Format

`/schema` returns each view's columns with type/mode/description plus, for low-cardinality categorical columns, the actual allowed values discovered from the data. Column `mode` (NULLABLE/REQUIRED) and `row_count` are read from the underlying base table, since BigQuery views always report every column as NULLABLE. View-only derived columns are declared explicitly: `variant` and `resource`/`resource1`/`resource2` are REQUIRED (non-null transforms of REQUIRED base columns), while `maf` is NULLABLE (`LEAST(aaf, 1-aaf)` with nullable `aaf`). Two shapes:

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

Values are computed by querying `SELECT DISTINCT` on each view and cached in-process for one hour. New datasets show up automatically after the cache expires.

### Logging

Structured JSON logging to stdout, compatible with GCP Cloud Logging. Each endpoint (except `/health`) emits one log line per request with:

- `message`: endpoint name (query, schema, sample, stats)
- `log_type`: "endpoint_access"
- `duration_ms`: request duration in milliseconds
- Endpoint-specific fields: `sql`, `dry_run`, `total_rows`, `rows_returned`, `bytes_processed`, `estimated_cost_usd` (for `/query`); `table`, `tables_returned` (for `/schema`); `table`, `rows_returned` (for `/sample`)

BigQuery cost is estimated at $6.25 per TiB (on-demand pricing). Noisy loggers (uvicorn.access, google, urllib3, asyncio) are suppressed to WARNING level.

### Security

#### Authentication

Every endpoint except `/health` requires `Authorization: Bearer $INTERNAL_API_SECRET` — the same shared secret chat-backend and mcp-server already send on every call, so no client change was needed. The comparison is constant-time (`hmac.compare_digest`).

`/health` is exempt because kubelet probes and the monitor CronJob poll it without credentials. FastAPI mounts `/docs`, `/redoc` and `/openapi.json` with `add_route`, which bypasses app-level dependencies, so those three are re-declared as ordinary routes and are authenticated too.

**Fails open when `INTERNAL_API_SECRET` is unset**, logging a startup warning, so local development works unchanged and a cluster mid-rollout doesn't hard-fail. In the deployment the env var comes from the `genetics-secrets/internal-api-secret` key, which `create-secrets.sh` always populates.

This was the only access control besides the cluster NetworkPolicy, which is not sufficient on its own: mcp-server is allowed to reach db-api *and* is itself reachable from outside the boundary, so anything that could drive mcp-server could reach BigQuery through it.

#### Query authorization

`/query` submits every statement as a **BigQuery dry run first** (`authorize_query`), and only runs it for real if the dry run passes two checks:

1. **`statementType` must be `SELECT`.** Anything else — DDL, DML, `EXECUTE IMMEDIATE`, `EXPORT DATA`, `CALL`, `LOAD`, `GRANT`, or a multi-statement script — is rejected with 400.
2. **Every entry in `referencedTables` must be an exposed view or the base table it wraps** (`_ALLOWED_TABLE_IDS`). Anything else is rejected with 403, listing the disallowed tables and the available views.

This replaced a keyword blocklist that scanned whitespace-delimited tokens. That approach was evadable — `EXECUTE IMMEDIATE`, `EXPORT DATA`, `CALL` and `LOAD` were not in the list at all, and comment/newline tricks broke tokenisation — and it never constrained *which* tables a `SELECT` could read. Since the API service account holds project-level `bigquery.dataViewer`, an unconstrained `SELECT` could read every dataset in the project, and `EXPORT DATA` could write results to GCS. Letting BigQuery parse the statement leaves nothing to pattern-match against: the dry run reports the real statement type and the real table set.

Notes:

- The dry-run job is created **outside** the endpoint's `try`/`except Exception` block, so its 400/403 reaches the client instead of being converted to a 500.
- When the caller passes `dry_run: true`, the authorization probe *is* the estimate — its `total_bytes_processed` is returned directly, so no second job is submitted.
- `referencedTables` for a view query may name the view, its base table, or both, depending on how BigQuery expands it; both forms are in the allow-list.
- The allow-list is derived from `VIEWS`, so adding a view exposes it automatically. A table that is loaded but has no view is **not** queryable through `/query`.

#### Other controls

- `maximum_bytes_billed` on every BigQuery job, including the internal ones behind `/schema`, `/stats` and `/tables/{name}/sample` (previously uncapped, so a large table could run up an unbounded scan)
- Table names auto-qualified, and bare view names resolved via `_BASE_TABLES`
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

Configuration via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| PROJECT_ID | (from gcloud in the scripts; a placeholder in the API) | GCP project ID — the API's fallback is not a real project, so it must be set in a deployment |
| DATASET_ID | genetics_results | BigQuery dataset name |
| LOCATION | europe-west1 | BigQuery dataset location |
| MAX_ROWS | 100000 | Maximum rows returned per query |
| MAX_BYTES_BILLED | 107374182400 | Maximum bytes billed per query (100 GB) |
| PORT | 8080 | API server port |
| DATASETS_CONFIG_PATH | ./configs/datasets.yaml | Path to shared datasets YAML config |
| GCS_BUCKET / GCS_PREFIX | varies by loader (placeholder `bucket-name` with an empty prefix in most, `finngen-commons` + `results_api_data/` in the newer ones) | GCS source location for `scripts/load_*.sh` |
| CORS_ORIGINS | http://localhost:3000,http://127.0.0.1:3000 | Comma-separated origins allowed to call the API from a browser |
| INTERNAL_API_SECRET | (unset) | Shared secret required as `Authorization: Bearer` on every endpoint except `/health`. Unset disables authentication entirely (logs a warning at startup) |

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
│   ├── variant_annotation.sql         # FinnGen R14 per-variant functional annotations (stored variant column)
│   └── variant_annotation_v.sql       # View with constant resource='finngen'
├── scripts/
│   ├── setup_bigquery.sh      # Create dataset and tables
│   ├── load_data.py           # Python loader for tsv.gz files
│   ├── load_credsets_coloc.sh  # Load credible sets and colocalization data
│   ├── load_pseudo.sh         # Load pseudo credible sets (FinnGen+UKBB/MVP meta-analyses, external EXT file: COVID-19 HGI + PGC + GP2)
│   ├── load_genebass_variants.sh    # Load GeneBASS exome variant results (truncates table)
│   ├── load_genebass_gene.sh        # Load GeneBASS gene burden results (truncates table)
│   ├── load_exome_variants_extra.sh # Append additional exome variant results (IBD)
│   ├── load_gene_burden_extra.sh    # Append additional gene burden results (BipEx, IBD, SCHEMA2)
│   ├── load_asm_qtl.sh        # Load ASM-QTL (allele-specific methylation) data from deCODE
│   ├── load_open_chromatin.sh # Load open-chromatin atlas (6 datasets; chr-string→INT64 conversion, truncate+append)
│   ├── load_peak_to_gene.sh   # Load Open4Gene peak→gene links (chr-string→INT64, cell_type prefix strip, WRITE_TRUNCATE)
│   ├── load_variant_effect.sh # Load predicted variant effects (marderstein chrombpnet+flare; chr-string→INT64 conversion, truncate+append)
│   ├── load_mpra.sh           # Load Siraj MPRA results (single LONG file; chr-string→INT64, dataset injected via --const-column)
│   ├── load_variant_annotation.sh # Load FinnGen R14 variant annotations (same file the API serves; WRITE_TRUNCATE)
│   ├── load_gene_annotations.sh   # Build + load gene_annotations table (WRITE_TRUNCATE) + create gene_annotations_v view
│   ├── build_gene_annotations.py  # Build gene_annotations NDJSON from HGNC + GENCODE sources
│   └── generate_resource_sql.py # Generate/lint CASE/WHEN SQL from shared datasets.yaml
├── configs/
│   └── datasets.yaml          # Shared dataset/resource config (synced from suite repo)
├── api/
│   ├── main.py                # FastAPI application
│   └── yaml_loader.py         # Loads datasets.yaml into data structures used by main.py
├── tests/
│   ├── test_api_auth.py       # Shared-secret authentication tests (never reach BigQuery)
│   └── test_build_gene_annotations.py  # gene_annotations build unit tests
├── docs/
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

2. **Load credible sets and colocalization data from GCS**:
   ```bash
   ./scripts/load_credsets_coloc.sh
   ```
   Loads run in parallel: each table's wipe (the `credible_sets` surgical `DELETE`, and the first `WRITE_TRUNCATE` load of each coloc table) is awaited before that table's files are appended concurrently. `credible_sets` loads first, then `colocalization` and `coloc_credsets` load concurrently.

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

5. **Append additional exome and gene burden results** (IBD, SCHEMA2, BipEx) on top of the GeneBASS load:
   ```bash
   ./scripts/load_exome_variants_extra.sh
   ./scripts/load_gene_burden_extra.sh
   ```

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

## To Be Implemented

1. Per-user authentication for external access (OAuth, per-caller API keys). The current shared secret authenticates the *service*, not the user behind the request, so it cannot support per-user authorization or attribution.
2. Rate limiting for query endpoint
3. Query result caching for repeated queries
4. Possibly additional endpoints for common query patterns (variant lookup, gene lookup)
