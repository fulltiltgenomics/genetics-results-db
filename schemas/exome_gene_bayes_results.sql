-- BigQuery schema for exome_gene_bayes_results table
-- Gene-level Bayesian association statistics from exome studies that report a Bayes factor
--   and a Bayesian FDR instead of a p-value (TADA and its descendants). First source: the
--   Autism Sequencing Consortium 2026 release (dataset ASC2, trait ASD, 20,893 autosomal
--   genes). One row per (dataset, trait, gene_id); the counts the statistic was computed from
--   are in exome_gene_counts on the same key.
--
-- Both statistics are stored as the source spells them; neither is transformed. Rank on fdr:
--   the source's FDR is the running mean of (1 - posterior probability) down the BF-ranked
--   list, so it is NOT a function of bayes_factor alone - the genes floored at BF = 1 carry
--   different FDRs, and ordering by bayes_factor does not order by fdr.
--
-- qc_flagged genes are kept, flagged: the source excludes them from its reported gene list
--   for reasons (clonal expansion in spermatogonia, a mapping artefact, an overlapping
--   transcript) that a reader of the row needs to see rather than have hidden.

CREATE TABLE IF NOT EXISTS `genetics_results.exome_gene_bayes_results`
(
  dataset STRING NOT NULL OPTIONS(description="Source dataset (ASC2)"),
  trait STRING NOT NULL OPTIONS(description="Trait identifier"),
  gene STRING NOT NULL OPTIONS(description="Gene symbol from the GENCODE version the source was annotated with (ASC2: v29)"),
  gene_id STRING NOT NULL OPTIONS(description="Ensembl gene ID (unversioned). The key with dataset and trait, and the join key to exome_gene_counts"),
  chr INT64 NOT NULL OPTIONS(description="Chromosome, X is 23"),
  gene_start_pos INT64 NOT NULL OPTIONS(description="Gene start (GRCh38)"),
  gene_end_pos INT64 NOT NULL OPTIONS(description="Gene end (GRCh38)"),
  bayes_factor FLOAT64 NOT NULL OPTIONS(description="Overall gene-level Bayes factor as published (product over variant classes and inheritance modes, each floored at 1). Evidence, not the ranking statistic"),
  fdr FLOAT64 NOT NULL OPTIONS(description="Bayesian false discovery rate as published. The ranking and thresholding statistic: the source reports genes at fdr < 0.001 (exome-wide) and fdr < 0.05"),
  qc_flagged BOOL NOT NULL OPTIONS(description="TRUE for genes the source flagged and dropped from its reported list despite their statistics"),
  trait_original STRING NOT NULL OPTIONS(description="Original trait name in the respective dataset")
)
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 23, 1))
CLUSTER BY dataset, gene, trait
OPTIONS(
  description="Gene-level Bayes factor and Bayesian FDR from count-based exome studies (ASC2)",
  labels=[("domain", "genetics"), ("data_type", "gene_based")]
);
