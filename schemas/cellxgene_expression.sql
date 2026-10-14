-- CELLxGENE per-dataset expression: one row per (gene, cell group) for every group of every
-- dataset, SPARSE — only a gene with at least one expressing cell in the group has a row.
-- Loaded from <release>/bq/per_dataset.tsv.gz (publish.py BQ_PER_DATASET_COLUMNS).
--
-- Narrow on purpose: the group's labels (tissue, cell type, disease, assay) live in
-- cellxgene_groups and the dataset's in cellxgene_datasets, joined in cellxgene_expression_v
-- by the integer keys. The keys are release-local, so every cellxgene_* table is replaced in
-- the same transaction (scripts/load_cellxgene.sh). Telling 0 from not-measured needs
-- cellxgene_measured_genes; the view does that.
--
-- chr: GENCODE v49 mapping-file coding, MT = 26 (see cellxgene_summary.sql).
--
-- Partitioned on chr, clustered by gene. A single-gene query on cellxgene_expression_v with a
-- chr predicate has a dry-run ESTIMATE of ~2.3 GB at full scale (up to ~12 GB worst case),
-- not under 1 GB: measured 95 MB on 8 datasets / 6.0M cells (85.6 B per row here, chr 1 =
-- 9.7% of rows), scaled by cells to ~147M (x24) or by datasets to 1,032 (x129). Accepted:
-- the estimate counts whole chr partitions, and only clustering on gene lowers what is billed.
CREATE TABLE IF NOT EXISTS `genetics_results.cellxgene_expression`
(
  chr INT64 NOT NULL OPTIONS(description="Chromosome (X=23, Y=24, MT=26)"),
  gene_start INT64 NOT NULL OPTIONS(description="Gene start (GRCh38, GENCODE v49)"),
  gene_id STRING NOT NULL OPTIONS(description="GENCODE v49 Ensembl gene ID, versioned"),
  gene_name STRING OPTIONS(description="GENCODE v49 gene symbol"),
  dataset_key INT64 NOT NULL OPTIONS(description="Release-local key into cellxgene_datasets"),
  group_key INT64 NOT NULL OPTIONS(description="Release-local key into cellxgene_groups"),
  mean_log1p_cp10k FLOAT64 NOT NULL OPTIONS(description="PRIMARY: cell-level mean of log1p(CP10k), zeros included"),
  frac_expressing FLOAT64 NOT NULL OPTIONS(description="PRIMARY: fraction of the group's cells with nonzero counts"),
  donor_pseudobulk_mean_cp10k FLOAT64 OPTIONS(description="SECONDARY, does not rank: donor-pseudobulk mean CP10k (linear scale); NULL with fewer than 3 donors")
)
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 27, 1))
CLUSTER BY gene_name, gene_id
OPTIONS(
  description="CELLxGENE Discover per-dataset, per-cell-group expression (sparse; join keys only)",
  labels=[("domain", "genetics"), ("data_type", "cellxgene_expression")]
);
