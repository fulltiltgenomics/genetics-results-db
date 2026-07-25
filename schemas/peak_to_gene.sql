-- BigQuery schema for peak_to_gene table
-- Open4Gene peak-to-gene links: which genes a chromatin peak's accessibility is associated
-- with, in which cell type. One row per (peak, gene, cell type); only significant links are
-- published, so absence of a row is absence of a significant link, not evidence against one.
--
-- This is the join that turns peak-keyed caQTL results into gene-keyed ones: credible_sets
-- stores the caQTL trait as a PEAK id, so a gene-based caQTL question needs
--   credible_sets cs JOIN peak_to_gene l ON l.peak_id = cs.trait AND l.cell_type = cs.cell_type
-- (drop the cell_type predicate for the cell-type-agnostic, peak-level answer).
--
-- Small reference-style table (~1.1M rows): unpartitioned like gene_annotations, clustered by
-- symbol first because gene-keyed lookup is the dominant access pattern.
-- chr is stored as INT64 (chrX=23) although the source TSV encodes chrom as "chr1".."chrX";
-- the loader converts (CHR_STRING_TABLES in load_data.py).
-- cell_type is stored WITHOUT the source's "predicted.celltype." prefix so it joins
-- credible_sets.cell_type by equality; the loader strips it (CELL_TYPE_PREFIX_TABLES),
-- mirroring what the tabix API does at read time.
-- start/end are named peak_start/peak_end to match open_chromatin and avoid the `end` keyword.
-- The hurdle columns are the two components of the model Open4Gene fits per (peak, gene,
-- cell type): a zero/detection part and a count/expression-level part.

CREATE TABLE IF NOT EXISTS `genetics_results.peak_to_gene`
(
  chr INT64 NOT NULL OPTIONS(description="Chromosome (INT64; X=23)"),
  peak_start INT64 NOT NULL OPTIONS(description="Peak start position (0-based BED start)"),
  peak_end INT64 NOT NULL OPTIONS(description="Peak end position"),
  peak_id STRING NOT NULL OPTIONS(description="Peak identifier as chr-start-end; joins credible_sets.trait for caQTL rows"),
  gene_id STRING NOT NULL OPTIONS(description="Linked gene Ensembl ID (no version suffix)"),
  symbol STRING OPTIONS(description="Linked gene symbol"),
  cell_type STRING NOT NULL OPTIONS(description="Cell type the link was found in, without the source 'predicted.celltype.' prefix (e.g. l1.CD4_T); joins credible_sets.cell_type"),
  total_cell_num INT64 OPTIONS(description="Total number of cells in the analysis"),
  expr_cell_num INT64 OPTIONS(description="Number of cells expressing the gene"),
  open_cell_num INT64 OPTIONS(description="Number of cells with the peak accessible"),
  hurdle_zero_beta FLOAT64 OPTIONS(description="Effect size of peak accessibility on gene detection (zero component)"),
  hurdle_zero_se FLOAT64 OPTIONS(description="Standard error of hurdle_zero_beta"),
  hurdle_zero_z FLOAT64 OPTIONS(description="z statistic of the zero component"),
  hurdle_zero_nlog10p FLOAT64 OPTIONS(description="-log10(p-value) of the zero component"),
  hurdle_count_beta FLOAT64 OPTIONS(description="Effect size of peak accessibility on expression level (count component)"),
  hurdle_count_se FLOAT64 OPTIONS(description="Standard error of hurdle_count_beta"),
  hurdle_count_z FLOAT64 OPTIONS(description="z statistic of the count component"),
  hurdle_count_nlog10p FLOAT64 OPTIONS(description="-log10(p-value) of the count component"),
  hurdle_aic FLOAT64 OPTIONS(description="Akaike information criterion of the fitted model"),
  hurdle_bic FLOAT64 OPTIONS(description="Bayesian information criterion of the fitted model"),
  dataset STRING NOT NULL OPTIONS(description="Source dataset (constant 'FinnGen_ATACseq', matching credible_sets.dataset)")
)
CLUSTER BY symbol, cell_type, peak_id
OPTIONS(
  description="Open4Gene peak-to-gene links: chromatin peak accessibility associated with gene expression, per cell type",
  labels=[("domain", "genetics"), ("data_type", "peak_to_gene")]
);
