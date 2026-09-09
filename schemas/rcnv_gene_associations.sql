-- BigQuery schema for rcnv_gene_associations table
-- Gene-level rare-CNV association statistics from Collins et al. 2022 (Cell 185:3041,
--   doi 10.1016/j.cell.2022.06.036, Zenodo 6347673, CC-BY 4.0). LONG layout: one row per
--   (phenotype, cnv_type, gene) — 54 HPO phenotype groups x {DEL, DUP} x 17,263 autosomal
--   protein-coding genes.
--
-- There are deliberately no coordinate columns. The source BEDs carry GRCh37
--   canonical-transcript intervals and the suite is GRCh38 throughout, so a stored GRCh37
--   position is the one thing that would let a JOIN against a GRCh38 view be silently wrong.
--   Coordinates come from gene_annotations_v on ensembl_gene_id at query time.
--
-- (phenotype, cnv_type, ensembl_gene_id) is the key. `symbol` is the current HGNC spelling
--   and is NOT unique (four symbols name two genes each), so the view's join to
--   dosage_sensitivity is on ensembl_gene_id.
--
-- Unpartitioned: with no chromosome column there is nothing to RANGE_BUCKET on, and the
--   table is ~1.86M rows. Clustered by phenotype, cnv_type, ensembl_gene_id — clustering is
--   a sort prefix, so "which genes are associated with this phenotype" prunes and a
--   gene-only PheWAS across all 54 groups does not. `symbol` is not a fourth clustering
--   column because it is functionally determined by ensembl_gene_id, so it would sort
--   nothing that the third column has not already sorted.
--
-- 65% of rows carry NULL from `beta` onward: the gene was tested but the meta-analysis
--   returned no estimate (no qualifying CNV observed). Those rows are kept so "tested, no
--   estimate" stays distinguishable from "not tested", matching gene_burden_results_v's
--   unfiltered contract. The NULLs are NOT predicted by n_nominal_cohorts, so a caller
--   wanting only estimated associations filters `beta IS NOT NULL`.
--
-- The *_secondary columns are the leave-top-cohort-out sensitivity analysis (the meta
--   re-run with `top_cohort` dropped) and are NULL for 86% of rows.

CREATE TABLE IF NOT EXISTS `genetics_results.rcnv_gene_associations`
(
  -- these key columns are NOT NULL by construction (every gene x phenotype x cnv_type
  --   combination is present); if a future release emits the loader's `NA` null marker
  --   against one of them, the load fails loudly instead of silently writing an empty string
  dataset STRING NOT NULL OPTIONS(description="Source dataset (constant 'Collins_rCNV_2022')"),
  phenotype STRING NOT NULL OPTIONS(description="HPO phenotype group code without the colon, e.g. HP0012759. HP0000118 is every case pooled, UNKNOWN is cases matching none of the listed terms"),
  cnv_type STRING NOT NULL OPTIONS(description="Copy-number variant class tested: DEL (deletion) or DUP (duplication)"),
  symbol STRING NOT NULL OPTIONS(description="Current HGNC gene symbol, mapped from the source GENCODE v19 symbol via ENSG. Not unique"),
  symbol_gencode_v19 STRING NOT NULL OPTIONS(description="Gene symbol as published by Collins et al. (GENCODE v19 spelling)"),
  ensembl_gene_id STRING NOT NULL OPTIONS(description="Ensembl gene ID (unversioned). Part of the key and the join key to dosage_sensitivity"),
  n_nominal_cohorts INT64 NOT NULL OPTIONS(description="Number of contributing cohorts with nominal (P<0.05) evidence. One of two ways to satisfy the paper's secondary-evidence requirement (this >= 2, OR mlog10p_secondary > -LOG10(0.05)) alongside the primary FDR or exome-wide threshold"),
  top_cohort STRING NOT NULL OPTIONS(description="Cohort contributing the strongest single-cohort signal; the one dropped in the *_secondary leave-one-out re-analysis"),
  cohorts_excluded STRING OPTIONS(description="Semicolon-separated cohorts excluded from this meta-analysis (too few cases for this phenotype, or no CNV data). NULL where none were excluded"),
  case_freq FLOAT64 OPTIONS(description="Carrier frequency of a qualifying CNV overlapping the gene in cases"),
  control_freq FLOAT64 OPTIONS(description="Carrier frequency of a qualifying CNV overlapping the gene in controls"),
  beta FLOAT64 OPTIONS(description="Meta-analysis effect size as ln(odds ratio); OR = EXP(beta). NULL where the gene was tested but no estimate was produced"),
  beta_lower FLOAT64 OPTIONS(description="Lower bound of the 95% confidence interval on beta (ln scale)"),
  beta_upper FLOAT64 OPTIONS(description="Upper bound of the 95% confidence interval on beta (ln scale)"),
  z FLOAT64 OPTIONS(description="Meta-analysis Z score"),
  mlog10p FLOAT64 OPTIONS(description="-log10 of the meta-analysis p-value. NOT the field the paper calls significance on — see mlog10_fdr_q"),
  mlog10_fdr_q FLOAT64 OPTIONS(description="-log10 of the FDR q-value. The paper's FDR-significance threshold is FDR<1% (mlog10_fdr_q > -LOG10(0.01)) plus the secondary-evidence requirement on n_nominal_cohorts or mlog10p_secondary"),
  beta_secondary FLOAT64 OPTIONS(description="beta from the leave-top-cohort-out re-analysis"),
  beta_lower_secondary FLOAT64 OPTIONS(description="Lower 95% CI bound of beta_secondary"),
  beta_upper_secondary FLOAT64 OPTIONS(description="Upper 95% CI bound of beta_secondary"),
  z_secondary FLOAT64 OPTIONS(description="Z score from the leave-top-cohort-out re-analysis"),
  mlog10p_secondary FLOAT64 OPTIONS(description="-log10 p-value from the leave-top-cohort-out re-analysis"),
  mlog10_fdr_q_secondary FLOAT64 OPTIONS(description="-log10 FDR q-value from the leave-top-cohort-out re-analysis")
)
CLUSTER BY phenotype, cnv_type, ensembl_gene_id
OPTIONS(
  description="Gene-level rare-CNV (DEL/DUP) association statistics from Collins et al. 2022, 54 HPO phenotype groups x 17,263 genes",
  labels=[("domain", "genetics"), ("data_type", "rcnv")]
);
