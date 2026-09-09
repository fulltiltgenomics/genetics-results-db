-- BigQuery schema for rcnv_window_associations table
-- Sliding-window rare-CNV association statistics from Collins et al. 2022 (Cell 185:3041,
--   doi 10.1016/j.cell.2022.06.036, Zenodo 6347673, CC-BY 4.0). LONG layout: one row per
--   (phenotype, cnv_type, window) over 259,795 windows — 108 (phenotype, cnv_type) groups,
--   17,114-257,726 rows each.
--
-- This is the position-keyed counterpart to rcnv_gene_associations: a row is the
--   association of the CNVs OVERLAPPING an interval, not of any gene in it. Nothing links a
--   window to a gene here; that join is the caller's, via gene_annotations_v coordinates.
--
-- Coordinates are stored TWICE and both are needed. window_start/window_end are GRCh38,
--   lifted from the published GRCh37 intervals with UCSC liftOver (whole-interval BED4,
--   same-chromosome, lifted length within 180-220 kb); window_start_grch37/
--   window_end_grch37 are the published originals and are the only stable identity a
--   window has. The published windows are a regular 200 kb / 10 kb-step grid in GRCh37;
--   the LIFTED set is NOT — 378 adjacent pairs reorder, lifted lengths run 190,000-219,265
--   and only 90.5% are exactly 200 kb. So neither the width nor the step may be assumed of
--   the GRCh38 columns, and (chr, window_start_grch37, window_end_grch37) — not the GRCh38
--   pair — is what counts distinct windows.
--
-- 4,880 of the 267,237 published windows (1.83%) do not lift and are absent. The loss is
--   not uniform: chr9 7.7%, chr21 4.4%, chr22 3.6%, chr1 3.2% (pericentromeric and
--   subtelomeric regions), so a "no window here" answer near those regions means the
--   window was dropped, not that it was tested and null.
--
-- Unlike rcnv_gene_associations there are NO "tested, no estimate" rows: the munge drops
--   rows whose statistics are NA (chromosome ends, mostly), so every row here carries a
--   beta. A caller does not need a `beta IS NOT NULL` filter on this table.
--
-- Partitioned by chromosome exactly as credible_sets is, so a chr predicate prunes.
--   Clustered by phenotype, cnv_type, window_start: clustering is a sort prefix, so
--   "the strongest windows for this phenotype and CNV class in this region" prunes, and
--   window_start last keeps a coordinate range inside a phenotype contiguous. The GRCh37
--   pair is not a clustering column — callers query in GRCh38.

CREATE TABLE IF NOT EXISTS `genetics_results.rcnv_window_associations`
(
  -- these key columns are NOT NULL by construction; if a future release emits the loader's
  --   `NA` null marker against one of them, the load fails loudly instead of silently
  --   writing an empty string or a null coordinate
  dataset STRING NOT NULL OPTIONS(description="Source dataset (constant 'Collins_rCNV_2022')"),
  phenotype STRING NOT NULL OPTIONS(description="HPO phenotype group code without the colon, e.g. HP0012759. HP0000118 is every case pooled, UNKNOWN is cases matching none of the listed terms"),
  cnv_type STRING NOT NULL OPTIONS(description="Copy-number variant class tested: DEL (deletion) or DUP (duplication)"),
  chr INT64 NOT NULL OPTIONS(description="Chromosome as an integer (autosomes only; the study tested no sex chromosome). The partitioning column"),
  window_start INT64 NOT NULL OPTIONS(description="Window start in GRCh38, lifted from window_start_grch37. Lifted windows are not a regular grid — do not assume a 200 kb width or a 10 kb step"),
  window_end INT64 NOT NULL OPTIONS(description="Window end in GRCh38, lifted from window_end_grch37"),
  window_start_grch37 INT64 NOT NULL OPTIONS(description="Published GRCh37 window start. Part of the window's identity: (chr, window_start_grch37, window_end_grch37) is what distinguishes windows"),
  window_end_grch37 INT64 NOT NULL OPTIONS(description="Published GRCh37 window end. The published grid is 200 kb wide with a 10 kb step; the GRCh38 pair is not"),
  n_nominal_cohorts INT64 NOT NULL OPTIONS(description="Number of contributing cohorts with nominal (P<0.05) evidence. One of two ways to satisfy the paper's secondary-evidence requirement (this >= 2, OR mlog10p_secondary > -LOG10(0.05)) alongside the primary FDR or genome-wide threshold"),
  top_cohort STRING NOT NULL OPTIONS(description="Cohort contributing the strongest single-cohort signal; the one dropped in the *_secondary leave-one-out re-analysis"),
  cohorts_excluded STRING OPTIONS(description="Semicolon-separated cohorts excluded from this meta-analysis (too few cases for this phenotype, or no CNV data). NULL where none were excluded"),
  case_freq FLOAT64 OPTIONS(description="Carrier frequency of a qualifying CNV overlapping the window in cases"),
  control_freq FLOAT64 OPTIONS(description="Carrier frequency of a qualifying CNV overlapping the window in controls"),
  beta FLOAT64 OPTIONS(description="Meta-analysis effect size as ln(odds ratio); OR = EXP(beta). Never NULL here — rows without an estimate are dropped at munge"),
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
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 23, 1))
CLUSTER BY phenotype, cnv_type, window_start
OPTIONS(
  description="Sliding-window rare-CNV (DEL/DUP) association statistics from Collins et al. 2022, 108 (phenotype, cnv_type) groups over 259,795 GRCh38-lifted windows",
  labels=[("domain", "genetics"), ("data_type", "rcnv")]
);
