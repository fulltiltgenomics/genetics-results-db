-- BigQuery schema for exome_gene_counts table
-- Per-gene rare-variant COUNTS by variant class and inheritance mode from exome studies whose
--   release carries counts and a Bayesian gene statistic but no p-value or effect size. First
--   source: the Autism Sequencing Consortium 2026 release (dataset ASC2, one trait ASD). LONG
--   layout: one row per (dataset, trait, gene, variant_class, inheritance_mode), the same
--   melt the burden tables apply to their annotation classes.
--
-- The two count columns are the pair the inheritance mode contrasts:
--   de_novo       n_affected = de novo variants in probands, n_unaffected = in unaffected siblings
--   inherited     n_affected = alleles transmitted to probands, n_unaffected = untransmitted
--   case_control  n_affected = alleles in cases, n_unaffected = in controls
-- They are the source's "independent" counts (one variant per person per gene, LOFTEE-filtered
--   inherited and case-control PTVs) and do NOT equal sums over exome_variant_counts; nothing
--   here is derived from that table.
--
-- Nothing in this table is a test statistic: the gene's Bayes factor and FDR live in
--   exome_gene_bayes_results, keyed on the same (dataset, trait, gene_id). The release carries
--   no p-value, odds ratio or allele frequency, and none is computed on the way in.
--
-- The CNV classes (DEL, DUP) have gene-level counts only; exome_variant_counts holds SNVs and
--   indels alone.

CREATE TABLE IF NOT EXISTS `genetics_results.exome_gene_counts`
(
  dataset STRING NOT NULL OPTIONS(description="Source dataset (ASC2)"),
  trait STRING NOT NULL OPTIONS(description="Trait identifier"),
  gene STRING NOT NULL OPTIONS(description="Gene symbol from the GENCODE version the source was annotated with (ASC2: v29)"),
  gene_id STRING NOT NULL OPTIONS(description="Ensembl gene ID (unversioned). The key with dataset, trait, variant_class and inheritance_mode"),
  chr INT64 NOT NULL OPTIONS(description="Chromosome, X is 23"),
  gene_start_pos INT64 NOT NULL OPTIONS(description="Gene start (GRCh38)"),
  gene_end_pos INT64 NOT NULL OPTIONS(description="Gene end (GRCh38)"),
  variant_class STRING NOT NULL OPTIONS(description="Variant class: PTV, Mis2 (MPC >= 2 and AlphaMissense >= 0.97), Mis1 (one of the two), Mis0 (neither), synonymous, DEL, DUP"),
  inheritance_mode STRING NOT NULL OPTIONS(description="de_novo, inherited or case_control; decides what the two counts are"),
  n_affected INT64 NOT NULL OPTIONS(description="Count on the affected side of the contrast: de novo in probands / transmitted to probands / in cases"),
  n_unaffected INT64 NOT NULL OPTIONS(description="Count on the unaffected side: de novo in siblings / untransmitted / in controls"),
  trait_original STRING NOT NULL OPTIONS(description="Original trait name in the respective dataset")
)
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 23, 1))
CLUSTER BY dataset, gene, trait
OPTIONS(
  description="Per-gene rare-variant counts by variant class and inheritance mode from count-based exome studies (ASC2)",
  labels=[("domain", "genetics"), ("data_type", "gene_based")]
);
