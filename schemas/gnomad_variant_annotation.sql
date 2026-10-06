-- BigQuery schema for gnomad_variant_annotation table.
-- gnomAD genomes+exomes sites, one row per variant, loaded from the same bgzip+tabix
-- file the genetics-results-api serves. Kept apart from variant_annotation so that
-- table's unfiltered queries stay FinnGen-sized.
--
-- The partition range ends at 26 because the upper bound is exclusive and the suite's
-- chromosome codes run to 25 (X=23, Y=24, M=25); a code outside the range would land in
-- the catch-all partition, where a `chr =` filter still prunes but nothing else does.
--
-- Clustered by pos alone: a point lookup (`chr = AND pos =`) and a region scan
-- (`chr = AND pos BETWEEN`) are the two shapes that must stay cheap on a table this
-- size, and both are ranges over pos. `variant` sorts lexicographically, so clustering
-- on it would serve equality and give up every region query; as a second key behind pos
-- it would prune nothing a pos filter has not already pruned.
--
-- `variant` (chr:pos:ref:alt) is stored, not view-derived, so it joins by equality to
-- the `variant` column of the association views. It is absent from the source file and
-- is computed on load (DERIVED_COLUMNS in scripts/load_data.py).
--
-- `consequences` is a typed array, not the JSON string the source file carries: every
-- struct leaf is its own column, so a query that reads one leaf is billed for that leaf
-- instead of for the whole annotation, which is most of the table's bytes. The struct
-- field names are the JSON keys, so this shape and the file the API serves describe the
-- same thing. The loader parses the string on projection and refuses a chromosome whose
-- values the typed array does not reproduce (JSON_ARRAY_COLUMNS in scripts/load_data.py).
--
-- A variant with no annotation (NA in the file) is an EMPTY array: BigQuery cannot store
-- a NULL array and reads one back as empty, so "no annotation" is ARRAY_LENGTH = 0 and
-- `consequences IS NULL` is never true. The same holds for the inner `consequences` list.

CREATE TABLE IF NOT EXISTS `genetics_results.gnomad_variant_annotation`
(
  chr INT64 NOT NULL OPTIONS(description="Chromosome (INT64; X=23, Y=24)"),
  pos INT64 NOT NULL OPTIONS(description="Variant position (1-based, GRCh38); clustering key"),
  ref STRING NOT NULL OPTIONS(description="Reference allele"),
  alt STRING NOT NULL OPTIONS(description="Alternate allele"),
  variant STRING NOT NULL OPTIONS(description="Variant identifier (chr:pos:ref:alt, GRCh38)"),
  rsids STRING OPTIONS(description="dbSNP rsIDs, comma-separated when there are several"),
  filters STRING OPTIONS(description="gnomAD site filters that failed, comma-separated; NULL when the site passed"),
  AN INT64 OPTIONS(description="Total allele number in the source the row was taken from"),
  AF FLOAT64 OPTIONS(description="Alternate allele frequency, all genetic ancestry groups"),
  AF_afr FLOAT64 OPTIONS(description="Alternate allele frequency, African/African American"),
  AF_amr FLOAT64 OPTIONS(description="Alternate allele frequency, Admixed American"),
  AF_asj FLOAT64 OPTIONS(description="Alternate allele frequency, Ashkenazi Jewish"),
  AF_eas FLOAT64 OPTIONS(description="Alternate allele frequency, East Asian"),
  AF_fin FLOAT64 OPTIONS(description="Alternate allele frequency, Finnish"),
  AF_mid FLOAT64 OPTIONS(description="Alternate allele frequency, Middle Eastern"),
  AF_nfe FLOAT64 OPTIONS(description="Alternate allele frequency, non-Finnish European"),
  AF_remaining FLOAT64 OPTIONS(description="Alternate allele frequency, remaining individuals"),
  AF_sas FLOAT64 OPTIONS(description="Alternate allele frequency, South Asian"),
  most_severe STRING OPTIONS(description="Most severe variant consequence (VEP)"),
  gene_most_severe STRING OPTIONS(description="Gene of the most severe consequence"),
  consequences ARRAY<STRUCT<
    gene_symbol STRING OPTIONS(description="Gene symbol; NULL when the gene has none"),
    gene_id STRING OPTIONS(description="Ensembl gene ID"),
    consequences ARRAY<STRING> OPTIONS(description="VEP consequence terms for this gene"),
    gene_symbol_source STRING OPTIONS(description="Authority the symbol comes from (e.g. HGNC); NULL with gene_symbol"),
    canonical INT64 OPTIONS(description="1 when the annotation is on the gene's canonical transcript, else NULL"),
    biotype STRING OPTIONS(description="Gene biotype (e.g. protein_coding, lncRNA)")
  >> OPTIONS(description="Per-gene VEP annotations in the source file's order; empty when the variant has none"),
  genome_or_exome STRING NOT NULL OPTIONS(description="Source of the row's statistics: g (genomes) or e (exomes)")
)
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 26, 1))
CLUSTER BY pos
OPTIONS(
  description="gnomAD genomes+exomes per-variant frequencies and VEP annotation, one row per variant",
  labels=[("domain", "genetics"), ("data_type", "variant_annotation")]
);
