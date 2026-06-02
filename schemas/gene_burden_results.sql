CREATE TABLE IF NOT EXISTS `genetics_results.gene_burden_results`
(
  dataset STRING NOT NULL OPTIONS(description="Source dataset (genebass, BipEx2, IBD_exome, SCHEMA2)"),
  trait STRING NOT NULL OPTIONS(description="Trait identifier"),
  gene STRING NOT NULL OPTIONS(description="Gene symbol"),
  gene_id STRING NOT NULL OPTIONS(description="Ensembl gene ID"),
  chr INT64 NOT NULL OPTIONS(description="Chromosome"),
  gene_start_pos INT64 NOT NULL OPTIONS(description="Gene start position"),
  gene_end_pos INT64 NOT NULL OPTIONS(description="Gene end position"),
  annotation STRING NOT NULL OPTIONS(description="Annotation category (pLoF, nonsynonymous, etc.)"),
  mlog10p_burden FLOAT64 NOT NULL OPTIONS(description="-log10(p-value) for burden test"),
  beta FLOAT64 NOT NULL OPTIONS(description="Effect size"),
  se FLOAT64 NOT NULL OPTIONS(description="Standard error"),
  total_variants INT64 OPTIONS(description="Total variants in gene"),
  total_variants_pheno INT64 OPTIONS(description="Total variants in phenotype"),
  n_cases INT64 NOT NULL OPTIONS(description="Number of cases"),
  n_controls INT64 OPTIONS(description="Number of controls (NULL for quantitative traits)"),
  trait_original STRING NOT NULL OPTIONS(description="Original trait name in the respective dataset"),
  flags STRING OPTIONS(description="Quality or analysis flags (NA if none)")
)
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 23, 1))
CLUSTER BY dataset, gene, trait
OPTIONS(
  description="Gene-level burden test results from exome sequencing studies (GeneBASS, BipEx2, IBD exome, SCHEMA2)",
  labels=[("domain", "genetics"), ("data_type", "gene_burden_results")]
);
