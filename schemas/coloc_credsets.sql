-- BigQuery schema for colocalization credible sets table
-- Contains variants that are part of colocalized credible sets

CREATE TABLE IF NOT EXISTS `genetics_results.coloc_credsets`
(
  dataset STRING NOT NULL OPTIONS(description="Source dataset"),
  data_type STRING NOT NULL OPTIONS(description="Data type (GWAS, eQTL, etc.)"),
  trait STRING NOT NULL OPTIONS(description="Trait ID"),
  trait_original STRING OPTIONS(description="Original trait name"),
  cell_type STRING OPTIONS(description="Cell/tissue type"),
  chr INT64 NOT NULL OPTIONS(description="Chromosome"),
  pos INT64 NOT NULL OPTIONS(description="Position"),
  ref STRING NOT NULL OPTIONS(description="Reference allele"),
  alt STRING NOT NULL OPTIONS(description="Alternate allele"),
  mlog10p FLOAT64 OPTIONS(description="-log10(p-value)"),
  beta FLOAT64 OPTIONS(description="Effect size"),
  se FLOAT64 OPTIONS(description="Standard error"),
  pip FLOAT64 OPTIONS(description="Posterior inclusion probability"),
  cs_id STRING OPTIONS(description="Credible set ID")
)
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 23, 1))
CLUSTER BY dataset, data_type, cs_id
OPTIONS(
  description="Variants in colocalized credible sets",
  labels=[("domain", "genetics"), ("data_type", "coloc_credsets")]
);
