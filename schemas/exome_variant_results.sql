CREATE TABLE IF NOT EXISTS `genetics_results.exome_variant_results`
(
  dataset STRING NOT NULL OPTIONS(description="Source dataset (genebass)"),
  chr INT64 NOT NULL OPTIONS(description="Chromosome"),
  pos INT64 NOT NULL OPTIONS(description="Position"),
  ref STRING NOT NULL OPTIONS(description="Reference allele"),
  alt STRING NOT NULL OPTIONS(description="Alternate allele"),
  gene STRING NOT NULL OPTIONS(description="Gene symbol"),
  annotation STRING OPTIONS(description="Variant annotation (LC, synonymous, missense, pLoF, etc.)"),
  mlog10p FLOAT64 OPTIONS(description="-log10(p-value)"),
  beta FLOAT64 OPTIONS(description="Effect size"),
  se FLOAT64 OPTIONS(description="Standard error"),
  af_overall FLOAT64 OPTIONS(description="Allele frequency overall"),
  af_cases FLOAT64 OPTIONS(description="Allele frequency in cases"),
  af_controls FLOAT64 OPTIONS(description="Allele frequency in controls"),
  ac INT64 OPTIONS(description="Allele count"),
  an INT64 OPTIONS(description="Allele number"),
  heritability FLOAT64 OPTIONS(description="Heritability estimate"),
  trait STRING NOT NULL OPTIONS(description="Trait identifier")
)
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 23, 1))
CLUSTER BY dataset, gene, trait
OPTIONS(
  description="Variant-level association results from GeneBASS exome sequencing",
  labels=[("domain", "genetics"), ("data_type", "exome_variant_results")]
);
