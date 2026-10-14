-- Which genes each CELLxGENE dataset measured: what makes a missing cellxgene_expression row
-- a 0 rather than not-measured. Loaded from <release>/bq/measured_genes.tsv.gz (publish.py
-- BQ_MEASURED_COLUMNS). Partitioned like cellxgene_expression so the join in
-- cellxgene_expression_v prunes both sides on the same chr predicate.
CREATE TABLE IF NOT EXISTS `genetics_results.cellxgene_measured_genes`
(
  chr INT64 NOT NULL OPTIONS(description="Chromosome (X=23, Y=24, MT=26)"),
  gene_start INT64 NOT NULL OPTIONS(description="Gene start (GRCh38, GENCODE v49)"),
  gene_id STRING NOT NULL OPTIONS(description="GENCODE v49 Ensembl gene ID, versioned"),
  gene_name STRING OPTIONS(description="GENCODE v49 gene symbol"),
  dataset_key INT64 NOT NULL OPTIONS(description="Release-local key into cellxgene_datasets")
)
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 27, 1))
CLUSTER BY gene_name, gene_id
OPTIONS(
  description="Genes measured by each CELLxGENE dataset (mapped to GENCODE v49)",
  labels=[("domain", "genetics"), ("data_type", "cellxgene_measured_genes")]
);
