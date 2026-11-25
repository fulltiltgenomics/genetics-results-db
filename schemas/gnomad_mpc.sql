-- BigQuery schema for gnomad_mpc table
-- MPC missense deleteriousness scores from the gnomAD v4.1.1 regional missense constraint
--   release (gs://gcp-public-data--gnomad/papers/2026-rmc/gnomad_v4.1.1_mpc.tsv.bgz), GRCh38:
--   one row per possible missense SNV and transcript, whether or not the variant has been
--   observed. Unique on (chr, pos, ref, alt, transcript_id), NOT on the variant: a variant
--   that is missense in two genes' transcripts has a row and a score for each.
--
-- gene and gene_id are not in the source; the munge adds them from the transcript's
--   GENCODE v39 record and refuses a transcript it cannot place.
--
-- This is not the MPC the exome count tables carry: exome_variant_counts.mpc is whatever
--   version each study annotated with, and its values differ from these.
--
-- Clustered by pos for the same reason as gnomad_variant_annotation: the lookups that must
--   stay cheap are a variant (`chr = AND pos =`) and a region (`chr = AND pos BETWEEN`).

CREATE TABLE IF NOT EXISTS `genetics_results.gnomad_mpc`
(
  chr INT64 NOT NULL OPTIONS(description="Chromosome, X is 23, Y is 24"),
  pos INT64 NOT NULL OPTIONS(description="Position (GRCh38)"),
  ref STRING NOT NULL OPTIONS(description="Reference allele"),
  alt STRING NOT NULL OPTIONS(description="Alternate allele"),
  gene STRING NOT NULL OPTIONS(description="Gene symbol of the transcript (GENCODE v39)"),
  gene_id STRING NOT NULL OPTIONS(description="Ensembl gene ID of the transcript (unversioned)"),
  transcript_id STRING NOT NULL OPTIONS(description="Ensembl transcript the score was computed on (unversioned)"),
  mpc FLOAT64 NOT NULL OPTIONS(description="MPC score; higher is more deleterious")
)
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 26, 1))
CLUSTER BY pos
OPTIONS(
  description="gnomAD v4.1.1 MPC scores for every possible missense SNV, per transcript",
  labels=[("domain", "genetics"), ("data_type", "annotation")]
);
