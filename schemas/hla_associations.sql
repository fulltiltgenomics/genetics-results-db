-- BigQuery schema for hla_associations table
-- Classical HLA allele associations from FinnGen R14: every imputed HLA allele tested
-- against every core R14 endpoint. One row per (phenotype, allele).
--
-- The association unit is an ALLELE, not a nucleotide variant, so there are deliberately
-- no ref/alt columns and these rows cannot be joined to credible_sets/variant_annotation
-- on chr/pos/ref/alt. The source models the test as ref='<absent>' / alt='<allele>'; the
-- munge rewrites that into explicit gene/allele, which is what the tabix files served by
-- results-api also carry.
--
-- `pos` is the HLA gene's single anchor position shared by all of its alleles, so it
-- locates the gene and not the allele. It is kept because it is what the tabix index is
-- built on, and it keeps the table consistent with every other positional table here.
--
-- chr is stored as INT64 (always 6) even though the canonical/tabix source TSV encodes
-- chrom as a numeric string; the loader converts via CHR_STRING_TABLES staging.
-- Clustered by phenotype, gene, allele: the two real access patterns are "all alleles for
-- a phenotype" and "all phenotypes for an allele", and both are served by this ordering.
-- The table is small (~507k rows) so it is not partitioned — every row is chr 6, which
-- would put the whole table in one partition anyway.

CREATE TABLE IF NOT EXISTS `genetics_results.hla_associations`
(
  chr INT64 NOT NULL OPTIONS(description="Chromosome (INT64; always 6, the MHC)"),
  pos INT64 NOT NULL OPTIONS(description="Anchor position (1-based, GRCh38) of the allele's HLA gene. Shared by every allele of that gene, so it does not locate the allele; HLA-DRB3/DRB4/DRB5 share the placeholder 32500000"),
  gene STRING NOT NULL OPTIONS(description="HLA gene symbol (HLA-A, HLA-B, HLA-C, HLA-DPB1, HLA-DQA1, HLA-DQB1, HLA-DRB1, HLA-DRB3, HLA-DRB4, HLA-DRB5)"),
  allele STRING NOT NULL OPTIONS(description="Imputed classical HLA allele at 4-digit (two-field) resolution, e.g. 'B*27:05'. Not gene-prefixed"),
  phenotype STRING NOT NULL OPTIONS(description="FinnGen R14 endpoint code, e.g. 'K11_COELIAC'"),
  pval FLOAT64 OPTIONS(description="Association p-value. Underflows to 0 for the strongest signals — rank on mlogp instead"),
  mlogp FLOAT64 OPTIONS(description="-log10 p-value; the field to rank and threshold on (genome-wide significance 7.3)"),
  beta FLOAT64 OPTIONS(description="Effect size per copy of the allele (log odds ratio for binary endpoints)"),
  sebeta FLOAT64 OPTIONS(description="Standard error of beta"),
  af_alt FLOAT64 OPTIONS(description="Allele frequency in the full cohort"),
  af_alt_cases FLOAT64 OPTIONS(description="Allele frequency in cases; NULL for quantitative endpoints"),
  af_alt_controls FLOAT64 OPTIONS(description="Allele frequency in controls; NULL for quantitative endpoints"),
  info FLOAT64 OPTIONS(description="Imputation INFO for the allele (constant per allele across phenotypes). Rare alleles below ~0.5 produce large unstable betas that are imputation artifacts"),
  dataset STRING NOT NULL OPTIONS(description="Source dataset (constant 'finngen_hla')")
)
CLUSTER BY phenotype, gene, allele
OPTIONS(
  description="Classical HLA allele associations from FinnGen R14 (imputed HLA alleles x core R14 endpoints)",
  labels=[("domain", "genetics"), ("data_type", "hla")]
);
