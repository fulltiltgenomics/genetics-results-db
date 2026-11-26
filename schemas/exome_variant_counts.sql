-- BigQuery schema for exome_variant_counts table
-- Per-variant allele COUNTS from exome studies whose release carries counts but no
--   per-variant test statistic. Unique on (dataset, chr, pos, ref, alt, variant_class), NOT
--   on the variant: ASC2 gives an "other splice" SNV two rows, a PTV row with its de novo
--   counts and a row classed by the underlying consequence with its inherited and
--   case-control counts. Nothing is derived on the way in, and nothing is filtered on
--   significance - there is nothing to filter on.
--
--   ASC2     Autism Sequencing Consortium 2026 release, trait ASD: every SNV/indel in the
--            release. Carries all six counts, a variant class and the other-splice flag; no
--            allele numbers.
--   SCHEMA2  Schizophrenia exome meta-analysis v2, trait SCZ: every variant of the release
--            that has a gene, with the source's in_analysis flag. Carries case and control
--            allele counts WITH allele numbers and a de novo count; no sibling or
--            transmission counts, no variant class.
--   BipEx2   Bipolar exome meta-analysis v2, trait BIP: as SCHEMA2, without a de novo count
--            and without a transcript.
--   SCHEMA1  The 2021 SCHEMA release (Singh et al. 2022), trait SCZ: the variants of its
--            analysis set that have a consequence, lifted from GRCh37. Columns as SCHEMA2,
--            without a transcript and without AlphaMissense. The release's per-variant
--            statistics are not loaded.
--
-- A column a source does not carry is NULL for that source, never zero: a NULL count means
--   "not reported", and only the dataset says which counts a row can have.
--
-- The count columns are allele counts per inheritance mode, not the "independent" per-gene
--   counts in exome_gene_counts: summing them over a gene reproduces neither that table nor
--   the Bayes factor, and neither table is derived from the other.
--
-- The CNV classes have no per-variant rows; DEL/DUP counts are gene-level only.

CREATE TABLE IF NOT EXISTS `genetics_results.exome_variant_counts`
(
  dataset STRING NOT NULL OPTIONS(description="Source dataset (ASC2, SCHEMA1, SCHEMA2, BipEx2)"),
  chr INT64 NOT NULL OPTIONS(description="Chromosome, X is 23, Y is 24, MT is 26"),
  pos INT64 NOT NULL OPTIONS(description="Position (GRCh38)"),
  ref STRING NOT NULL OPTIONS(description="Reference allele"),
  alt STRING NOT NULL OPTIONS(description="Alternate allele"),
  gene STRING NOT NULL OPTIONS(description="Gene symbol from the GENCODE version the dataset is munged against (ASC2: v29, SCHEMA2 and BipEx2: v39, SCHEMA1: v39, or v19 where v39 no longer has the gene)"),
  gene_id STRING NOT NULL OPTIONS(description="Ensembl gene ID (unversioned) the variant was assigned to"),
  transcript_id STRING OPTIONS(description="Ensembl transcript the consequence was called on. NULL for BipEx2 and SCHEMA1"),
  consequence STRING NOT NULL OPTIONS(description="VEP consequence as the source spells it, so the spelling differs by dataset (ASC2: missense, stop gained; SCHEMA2 and BipEx2: missense_variant, stop_gained, and an _LC suffix on low-confidence calls; SCHEMA1: VEP terms with missense split by MPC bin, missense_variant_mpc_<2, _2-3, _>=3)"),
  variant_class STRING OPTIONS(description="Source variant class: PTV, Mis2, Mis1, Mis0 or synonymous. Same spelling as exome_gene_counts.variant_class. NULL for SCHEMA1, SCHEMA2 and BipEx2, which have none"),
  hgvsc STRING OPTIONS(description="Coding change (HGVS c.)"),
  hgvsp STRING OPTIONS(description="Protein change (HGVS p.)"),
  mpc FLOAT64 OPTIONS(description="MPC missense deleteriousness score; NULL outside missense"),
  alpha_missense FLOAT64 OPTIONS(description="AlphaMissense pathogenicity score; NULL outside missense, and for SCHEMA1"),
  is_other_splice BOOL OPTIONS(description="LOFTEE 'other splice' annotation: an SNV that may disrupt splicing, counted as PTV by the source under its rules. NULL for SCHEMA1, SCHEMA2 and BipEx2"),
  in_analysis BOOL OPTIONS(description="SCHEMA1, SCHEMA2 and BipEx2: the source's flag for the variants its gene-level analysis used; false or NULL (the source's NA) for the others. NULL for ASC2, which has no such flag"),
  de_novo_ac_proband INT64 OPTIONS(description="De novo allele count in probands (SCHEMA2: the source's n_de_novo, SCHEMA1: its n_denovos). NULL for BipEx2"),
  de_novo_ac_sibling INT64 OPTIONS(description="De novo allele count in unaffected siblings. NULL for SCHEMA1, SCHEMA2 and BipEx2"),
  transmitted_ac_proband INT64 OPTIONS(description="Alleles transmitted to probands. NULL for SCHEMA1, SCHEMA2 and BipEx2"),
  untransmitted_ac_proband INT64 OPTIONS(description="Alleles not transmitted to probands. NULL for SCHEMA1, SCHEMA2 and BipEx2"),
  ac_case INT64 NOT NULL OPTIONS(description="Allele count in cases"),
  an_case INT64 OPTIONS(description="Allele number in cases (alleles genotyped). NULL for ASC2, and for the SCHEMA2 and BipEx2 rows where the source has it NA; 0 on SCHEMA2 variants seen only de novo"),
  ac_ctrl INT64 NOT NULL OPTIONS(description="Allele count in controls"),
  an_ctrl INT64 OPTIONS(description="Allele number in controls. NULL as an_case"),
  trait STRING NOT NULL OPTIONS(description="Trait identifier"),
  trait_original STRING NOT NULL OPTIONS(description="Original trait name in the respective dataset")
)
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 23, 1))
CLUSTER BY dataset, gene, trait
OPTIONS(
  description="Per-variant allele counts by inheritance mode from count-based exome studies (ASC2, SCHEMA1, SCHEMA2, BipEx2)",
  labels=[("domain", "genetics"), ("data_type", "exome")]
);
