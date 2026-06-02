-- BigQuery schema for gene_annotations table
-- Whole-universe, one row per gene: HGNC core annotations joined to GENCODE coordinates,
--   with full-lineage HGNC gene-group arrays (leaf group + all ancestors)
-- Small reference table: unpartitioned, clustered by symbol

CREATE TABLE IF NOT EXISTS `genetics_results.gene_annotations`
(
  hgnc_id STRING OPTIONS(description="HGNC ID (e.g. HGNC:5)"),
  symbol STRING OPTIONS(description="HGNC approved gene symbol"),
  name STRING OPTIONS(description="HGNC approved gene name"),
  prev_symbols STRING OPTIONS(description="Previous HGNC symbols, pipe-delimited"),
  alias_symbols STRING OPTIONS(description="Alias symbols, pipe-delimited"),
  ensembl_gene_id STRING OPTIONS(description="Ensembl gene ID"),
  ncbi_gene_id STRING OPTIONS(description="NCBI (Entrez) gene ID"),
  chr INT64 OPTIONS(description="Chromosome (GRCh38; X encoded as 23)"),
  gene_start INT64 OPTIONS(description="Gene start position (GRCh38, GENCODE)"),
  gene_end INT64 OPTIONS(description="Gene end position (GRCh38, GENCODE)"),
  strand STRING OPTIONS(description="Strand (+ or -)"),
  locus_type STRING OPTIONS(description="HGNC locus type (e.g. gene with protein product)"),
  gene_group_ids ARRAY<INT64> OPTIONS(description="Full-lineage HGNC gene-group IDs (leaf group plus all ancestors)"),
  gene_group_names ARRAY<STRING> OPTIONS(description="Full-lineage HGNC gene-group names (leaf group plus all ancestors)"),
  gencode_version STRING OPTIONS(description="GENCODE release used for coordinates"),
  hgnc_version STRING OPTIONS(description="HGNC complete-set version/date used"),
  download_date DATE OPTIONS(description="Date the source data was downloaded/built")
)
CLUSTER BY symbol
OPTIONS(
  description="Whole-universe gene annotations: HGNC core fields joined to GENCODE GRCh38 coordinates with full-lineage HGNC gene-group arrays",
  labels=[("domain", "genetics"), ("data_type", "gene_annotations")]
);
