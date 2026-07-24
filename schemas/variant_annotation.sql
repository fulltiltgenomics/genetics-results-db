-- BigQuery schema for variant_annotation table.
-- Per-variant functional annotations for FinnGen (R14), the same data the
-- genetics-results-api serves at /variant_annotation/finngen from the tabix
-- source R14_annotated_variants_v0.small.gz. One row per variant.
-- Partitioned by chromosome, clustered by most_severe, gene_most_severe for
-- efficient functional-consequence filtering.
-- chr is stored as INT64 (chrX=23); the source TSV already encodes it numerically,
-- so no chr-string conversion is needed on load.
-- `variant` (chr:pos:ref:alt) is stored (not view-derived) because the shared
-- canonical TSV served to the API already carries it as its first column,
-- keeping the positional CSV load aligned.

CREATE TABLE IF NOT EXISTS `genetics_results.variant_annotation`
(
  variant STRING OPTIONS(description="Variant identifier (chr:pos:ref:alt, GRCh38)"),
  chr INT64 NOT NULL OPTIONS(description="Chromosome (INT64; X=23)"),
  pos INT64 NOT NULL OPTIONS(description="Variant position (1-based, GRCh38)"),
  ref STRING NOT NULL OPTIONS(description="Reference allele"),
  alt STRING NOT NULL OPTIONS(description="Alternate allele"),
  INFO FLOAT64 OPTIONS(description="Imputation INFO score (imputation quality)"),
  AF FLOAT64 OPTIONS(description="Alternate allele frequency in FinnGen"),
  AC_Het INT64 OPTIONS(description="Heterozygous genotype count in FinnGen"),
  AC_Hom INT64 OPTIONS(description="Homozygous (alt) genotype count in FinnGen"),
  most_severe STRING OPTIONS(description="Most severe variant consequence (VEP)"),
  gene_most_severe STRING OPTIONS(description="Gene of the most severe consequence"),
  rsid STRING OPTIONS(description="dbSNP rsID, when available"),
  EXOME_enrichment_nfe FLOAT64 OPTIONS(description="Finnish vs non-Finnish European (NFE) allele-frequency enrichment from gnomAD exomes"),
  GENOME_enrichment_nfe FLOAT64 OPTIONS(description="Finnish vs non-Finnish European (NFE) allele-frequency enrichment from gnomAD genomes"),
  index INT64 OPTIONS(description="Row index in the source annotation file")
)
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 23, 1))
CLUSTER BY most_severe, gene_most_severe
OPTIONS(
  description="Per-variant functional annotations for FinnGen (R14); mirrors the API /variant_annotation/finngen source",
  labels=[("domain", "genetics"), ("data_type", "variant_annotation")]
);
