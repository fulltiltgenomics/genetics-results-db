-- BigQuery schema for credible_sets table
-- Partitioned by chromosome, clustered by dataset, data_type, most_severe for efficient querying

CREATE TABLE IF NOT EXISTS `genetics_results.credible_sets`
(
  dataset STRING NOT NULL OPTIONS(description="Source dataset (FinnGen_R13, Open_Targets_25.12, etc.)"),
  data_type STRING NOT NULL OPTIONS(description="GWAS, eQTL, pQTL, sQTL, caQTL"),
  trait STRING NOT NULL OPTIONS(description="Phenotype/trait ID"),
  trait_original STRING OPTIONS(description="Original trait name"),
  cell_type STRING OPTIONS(description="Cell/tissue type (null for GWAS)"),
  chr INT64 NOT NULL OPTIONS(description="Chromosome"),
  pos INT64 NOT NULL OPTIONS(description="Position"),
  ref STRING NOT NULL OPTIONS(description="Reference allele"),
  alt STRING NOT NULL OPTIONS(description="Alternate allele"),
  mlog10p FLOAT64 OPTIONS(description="-log10(p-value)"),
  beta FLOAT64 OPTIONS(description="Effect size"),
  se FLOAT64 OPTIONS(description="Standard error"),
  pip FLOAT64 OPTIONS(description="Posterior inclusion probability"),
  cs_id STRING OPTIONS(description="Credible set ID"),
  cs_size INT64 OPTIONS(description="Credible set size"),
  cs_min_r2 FLOAT64 OPTIONS(description="Minimum R² in credible set"),
  aaf FLOAT64 OPTIONS(description="Alternate allele frequency"),
  most_severe STRING OPTIONS(description="Most severe variant consequence"),
  gene_most_severe STRING OPTIONS(description="Gene with most severe consequence")
)
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 23, 1))
CLUSTER BY dataset, data_type, gene_most_severe, most_severe
OPTIONS(
  description="Fine-mapped credible set variants from multiple genetics datasets",
  labels=[("domain", "genetics"), ("data_type", "credible_sets")]
);
