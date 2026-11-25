-- BigQuery schema for gnomad_missense_constraint_regions table
-- Missense constraint regions from the gnomAD v4.1.1 regional missense constraint release
--   (gs://gcp-public-data--gnomad/papers/2026-rmc/gnomad_v4.1.1_all_mcrs.tsv), GRCh38: each
--   transcript's coding span cut into one or more regions, with the observed and expected
--   missense counts of each. A transcript with one region has no regional variation in
--   constraint; its row is the whole transcript. Unique on (transcript_id, region_start).
--
-- A region is a genomic interval and spans the introns between its coding bases.
--
-- is_outlier_transcript is not a source column: the release publishes a second file without
--   the transcripts it treats as outliers, and the flag is true for a transcript absent from
--   that file. The rows themselves are the same in both.
--
-- Small reference table: unpartitioned, clustered by gene.

CREATE TABLE IF NOT EXISTS `genetics_results.gnomad_missense_constraint_regions`
(
  chr INT64 NOT NULL OPTIONS(description="Chromosome, X is 23, Y is 24"),
  region_start INT64 NOT NULL OPTIONS(description="First position of the region (GRCh38, inclusive)"),
  region_end INT64 NOT NULL OPTIONS(description="Last position of the region (GRCh38, inclusive)"),
  gene STRING NOT NULL OPTIONS(description="Gene symbol of the transcript (GENCODE v39)"),
  gene_id STRING NOT NULL OPTIONS(description="Ensembl gene ID of the transcript (unversioned)"),
  transcript_id STRING NOT NULL OPTIONS(description="Ensembl transcript the regions belong to (unversioned)"),
  n_transcript_regions INT64 NOT NULL OPTIONS(description="Number of regions the transcript has; 1 means no regional variation was found"),
  region_obs INT64 NOT NULL OPTIONS(description="Observed missense variants in the region"),
  region_exp FLOAT64 NOT NULL OPTIONS(description="Expected missense variants in the region"),
  region_oe FLOAT64 NOT NULL OPTIONS(description="Observed / expected missense ratio; lower is more constrained"),
  region_oe_chisq FLOAT64 NOT NULL OPTIONS(description="Chi-square statistic of the region's observed against expected count"),
  region_oe_chisq_p FLOAT64 NOT NULL OPTIONS(description="P-value of region_oe_chisq"),
  is_high_coverage BOOL NOT NULL OPTIONS(description="The source's high-coverage flag for the region"),
  is_outlier_transcript BOOL NOT NULL OPTIONS(description="The transcript is one the release leaves out of its non-outlier file")
)
CLUSTER BY gene
OPTIONS(
  description="gnomAD v4.1.1 missense constraint regions: per-transcript regions with observed and expected missense counts",
  labels=[("domain", "genetics"), ("data_type", "annotation")]
);
