-- BigQuery schema for asm_qtl table
-- Allele-specific methylation QTL results from deCODE
-- Partitioned by chromosome, clustered by dataset, gene_most_severe, most_severe for efficient querying

CREATE TABLE IF NOT EXISTS `genetics_results.asm_qtl`
(
  dataset STRING NOT NULL OPTIONS(description="Source dataset (deCODE_asmQTL_CpG, deCODE_asmQTL_MDS)"),
  chr INT64 NOT NULL OPTIONS(description="Chromosome"),
  pos INT64 NOT NULL OPTIONS(description="Position"),
  ref STRING OPTIONS(description="Reference allele"),
  alt STRING OPTIONS(description="Alternate allele"),
  rsid STRING OPTIONS(description="dbSNP rsID"),
  beta FLOAT64 OPTIONS(description="Effect size"),
  se FLOAT64 OPTIONS(description="Standard error"),
  mlog10p FLOAT64 OPTIONS(description="-log10(p-value)"),
  af FLOAT64 OPTIONS(description="Allele frequency"),
  most_severe STRING OPTIONS(description="Most severe variant consequence"),
  gene_most_severe STRING OPTIONS(description="Gene with most severe consequence"),
  target_start INT64 OPTIONS(description="Methylation target region start position"),
  target_end INT64 OPTIONS(description="Methylation target region end position"),
  ref_methylrate FLOAT64 OPTIONS(description="Methylation rate on reference haplotype"),
  alt_methylrate FLOAT64 OPTIONS(description="Methylation rate on alternate haplotype"),
  n_haplotypes INT64 OPTIONS(description="Number of haplotypes used in analysis"),
  variant_rank STRING OPTIONS(description="Variant rank: primary or secondary"),
  ld_count INT64 OPTIONS(description="Number of variants in LD with this variant"),
  vartype STRING OPTIONS(description="Variant type: SNV, SV, etc.")
)
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 23, 1))
CLUSTER BY dataset, gene_most_severe, most_severe
OPTIONS(
  description="Allele-specific methylation QTL results from deCODE",
  labels=[("domain", "genetics"), ("data_type", "asm_qtl")]
);
