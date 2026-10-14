-- CELLxGENE cross-dataset summary: one row per (gene, rolled-up cell type, tissue_general),
-- pooled over the normal-tissue groups of every dataset that measured the gene. Loaded from
-- <release>/bq/summary.tsv.gz (genetics-results-munge scripts/cellxgene/publish.py, whose
-- SUMMARY_COLUMNS is the column order). Replaced whole, together with the other cellxgene_*
-- tables, by scripts/load_cellxgene.sh.
--
-- chr is the GENCODE v49 mapping file's own numeric coding, whose mitochondrial genes are 26,
-- so the partition range runs to 26. A chr predicate is the only thing that lowers a dry-run
-- estimate; the gene clustering lowers what a single-gene query is billed, not its estimate.
CREATE TABLE IF NOT EXISTS `genetics_results.cellxgene_summary`
(
  gene_id STRING NOT NULL OPTIONS(description="GENCODE v49 Ensembl gene ID, versioned (ENSG00000167286.10)"),
  chr INT64 NOT NULL OPTIONS(description="Chromosome (X=23, Y=24, MT=26)"),
  gene_start INT64 NOT NULL OPTIONS(description="Gene start (GRCh38, GENCODE v49)"),
  gene_end INT64 NOT NULL OPTIONS(description="Gene end (GRCh38, GENCODE v49)"),
  gene_name STRING OPTIONS(description="GENCODE v49 gene symbol"),
  source_gene_ids STRING NOT NULL OPTIONS(description="Comma-separated Ensembl IDs as the source datasets spelled them"),
  cell_type_rollup_ontology_term_id STRING NOT NULL OPTIONS(description="Rolled-up Cell Ontology term"),
  cell_type_rollup STRING OPTIONS(description="Rolled-up cell type label"),
  tissue_general_ontology_term_id STRING NOT NULL OPTIONS(description="UBERON tissue_general term"),
  tissue_general STRING OPTIONS(description="tissue_general label"),
  n_cells INT64 NOT NULL OPTIONS(description="Cells pooled, over the datasets that measured the gene"),
  n_donors INT64 NOT NULL OPTIONS(description="Donors pooled: distinct donors per dataset, summed over datasets"),
  n_datasets INT64 NOT NULL OPTIONS(description="Datasets that measured the gene and contribute cells"),
  mean_log1p_cp10k FLOAT64 NOT NULL OPTIONS(description="PRIMARY: cell-level mean of log1p(CP10k), zeros included"),
  frac_expressing FLOAT64 NOT NULL OPTIONS(description="PRIMARY: fraction of cells with nonzero counts"),
  donor_pseudobulk_mean_cp10k FLOAT64 OPTIONS(description="SECONDARY, does not rank: donor-pseudobulk mean CP10k (linear scale); NULL with fewer than 3 donors"),
  donor_pseudobulk_n_donors INT64 NOT NULL OPTIONS(description="SECONDARY: donors with a nonzero library behind donor_pseudobulk_mean_cp10k"),
  tau FLOAT64 OPTIONS(description="Tissue-specificity index (Yanai 2005) over the gene's cell-type profile, repeated on every row of the gene; NULL with fewer than 2 cell types or no expression")
)
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 27, 1))
CLUSTER BY gene_name, gene_id
OPTIONS(
  description="CELLxGENE Discover normal-tissue expression pooled across datasets per rolled-up cell type x tissue_general",
  labels=[("domain", "genetics"), ("data_type", "cellxgene_summary")]
);
