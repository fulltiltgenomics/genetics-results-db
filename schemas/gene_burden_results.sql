CREATE TABLE IF NOT EXISTS `genetics_results.gene_burden_results`
(
  dataset STRING NOT NULL OPTIONS(description="Source dataset (genebass)"),
  trait STRING NOT NULL OPTIONS(description="Trait identifier"),
  gene STRING NOT NULL OPTIONS(description="Gene symbol"),
  gene_id STRING NOT NULL OPTIONS(description="Ensembl gene ID"),
  chr INT64 NOT NULL OPTIONS(description="Chromosome"),
  gene_start_pos INT64 NOT NULL OPTIONS(description="Gene start position"),
  gene_end_pos INT64 NOT NULL OPTIONS(description="Gene end position"),
  annotation STRING NOT NULL OPTIONS(description="Annotation category (synonymous, pLoF, missense, etc.)"),
  mlog10p_burden FLOAT64 OPTIONS(description="-log10(p-value) for burden test"),
  mlog10p_skat FLOAT64 OPTIONS(description="-log10(p-value) for SKAT test"),
  mlog10p_skato FLOAT64 OPTIONS(description="-log10(p-value) for SKAT-O test"),
  beta FLOAT64 OPTIONS(description="Effect size"),
  se FLOAT64 OPTIONS(description="Standard error"),
  total_variants INT64 OPTIONS(description="Total variants in gene"),
  total_variants_pheno INT64 OPTIONS(description="Total variants in phenotype"),
  n_cases INT64 OPTIONS(description="Number of cases"),
  n_controls INT64 OPTIONS(description="Number of controls (NULL for quantitative traits)"),
  description STRING OPTIONS(description="Trait description"),
  coding_description STRING OPTIONS(description="Additional coding description"),
  category STRING OPTIONS(description="Trait category")
)
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 23, 1))
CLUSTER BY dataset, gene, trait
OPTIONS(
  description="Gene-level burden test results from GeneBASS exome sequencing",
  labels=[("domain", "genetics"), ("data_type", "gene_burden_results")]
);
