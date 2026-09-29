-- BigQuery schema for exome_variant_counts table
-- Per-variant allele COUNTS from exome studies whose release carries counts but no
--   per-variant test statistic. First source: the Autism Sequencing Consortium 2026 release
--   (dataset ASC2, trait ASD): every SNV/indel in the release, 7,045,130 rows, unique on
--   (chr, pos, ref, alt). Not filtered on significance - there is nothing to filter on - and
--   not filtered on gnomad_af, which is an annotation here rather than the source's rarity
--   filter (it reaches 0.89).
--
-- The six count columns keep the source's names. They are allele counts per inheritance
--   mode, not the "independent" per-gene counts in exome_gene_counts: summing them over a
--   gene reproduces neither that table nor the Bayes factor, and neither table is derived
--   from the other.
--
-- The CNV classes have no per-variant rows; DEL/DUP counts are gene-level only.

CREATE TABLE IF NOT EXISTS `genetics_results.exome_variant_counts`
(
  dataset STRING NOT NULL OPTIONS(description="Source dataset (ASC2)"),
  chr INT64 NOT NULL OPTIONS(description="Chromosome, X is 23, Y is 24"),
  pos INT64 NOT NULL OPTIONS(description="Position (GRCh38)"),
  ref STRING NOT NULL OPTIONS(description="Reference allele"),
  alt STRING NOT NULL OPTIONS(description="Alternate allele"),
  gene STRING NOT NULL OPTIONS(description="Gene symbol from the GENCODE version the source was annotated with (ASC2: v29)"),
  gene_id STRING NOT NULL OPTIONS(description="Ensembl gene ID (unversioned) the variant was assigned to"),
  transcript_id STRING OPTIONS(description="Ensembl transcript the consequence was called on"),
  consequence STRING NOT NULL OPTIONS(description="VEP consequence as the source spells it (missense, synonymous, frameshift, stop gained, splice donor, ...)"),
  variant_class STRING NOT NULL OPTIONS(description="Source variant class: PTV, Mis2, Mis1, Mis0 or synonymous. Same spelling as exome_gene_counts.variant_class"),
  hgvsp STRING OPTIONS(description="Protein change (HGVS p.)"),
  mpc FLOAT64 OPTIONS(description="MPC missense deleteriousness score; NULL outside missense"),
  alpha_missense FLOAT64 OPTIONS(description="AlphaMissense pathogenicity score; NULL outside missense"),
  is_other_splice BOOL NOT NULL OPTIONS(description="LOFTEE 'other splice' annotation: an SNV that may disrupt splicing, counted as PTV by the source under its rules"),
  gnomad_af FLOAT64 NOT NULL OPTIONS(description="gnomAD allele frequency as annotated by the source. Not a rarity filter"),
  de_novo_ac_proband INT64 NOT NULL OPTIONS(description="De novo allele count in probands"),
  de_novo_ac_sibling INT64 NOT NULL OPTIONS(description="De novo allele count in unaffected siblings"),
  transmitted_ac_proband INT64 NOT NULL OPTIONS(description="Alleles transmitted to probands"),
  untransmitted_ac_proband INT64 NOT NULL OPTIONS(description="Alleles not transmitted to probands"),
  ac_case INT64 NOT NULL OPTIONS(description="Allele count in cases"),
  ac_ctrl INT64 NOT NULL OPTIONS(description="Allele count in controls"),
  trait STRING NOT NULL OPTIONS(description="Trait identifier"),
  trait_original STRING NOT NULL OPTIONS(description="Original trait name in the respective dataset")
)
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 23, 1))
CLUSTER BY dataset, gene, trait
OPTIONS(
  description="Per-variant allele counts by inheritance mode from count-based exome studies (ASC2)",
  labels=[("domain", "genetics"), ("data_type", "exome")]
);
