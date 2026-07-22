-- BigQuery schema for mpra table
-- MEASURED cis-regulatory allelic activity from a massively parallel reporter assay
-- (Siraj et al. 2026 MPRA). One row per variant per cell_line (LONG layout), where
-- cell_line is 'meta' (cross-cell-line meta-analysis) or one of the 5 tested lines.
-- Partitioned by chromosome, clustered by dataset, cell_line for efficient querying.
-- chr is stored as INT64 (chrX=23, chrY=24, chrM/MT=25) even though the canonical/tabix
-- source TSV encodes chrom as a numeric string ("1".."25"); the loader converts.
-- `variant` (chr:pos:ref:alt) is stored (not view-derived) because the shared canonical TSV
-- served to the API already carries it, keeping the positional CSV load aligned.
-- emVar/active are BOOL: the source cells are literal TRUE/FALSE (NA -> NULL), which the
-- CSV loader parses case-insensitively.

CREATE TABLE IF NOT EXISTS `genetics_results.mpra`
(
  chr INT64 NOT NULL OPTIONS(description="Chromosome (INT64; X=23, Y=24, M/MT=25)"),
  pos INT64 NOT NULL OPTIONS(description="Variant position (1-based)"),
  variant STRING OPTIONS(description="Variant identifier (chr:pos:ref:alt)"),
  ref STRING NOT NULL OPTIONS(description="Reference allele"),
  alt STRING NOT NULL OPTIONS(description="Alternate (tested) allele"),
  cohort STRING OPTIONS(description="Fine-mapping cohort the variant was drawn from (GTEx, UKBB, BBJ, control); NULL if no meta-analysis row"),
  cell_line STRING OPTIONS(description="MPRA context: 'meta' (cross-cell-line meta-analysis) or one of K562, HEPG2, SKNSH, HCT116, A549"),
  emVar BOOL OPTIONS(description="Whether the allele modulates reporter expression in this context (allelic skew significant)"),
  active BOOL OPTIONS(description="Whether the element drives reporter expression above background in this context"),
  log2Skew FLOAT64 OPTIONS(description="Signed allelic effect, log2(alt/ref) of reporter activity (positive = alt drives higher expression)"),
  log2Skew_se FLOAT64 OPTIONS(description="Standard error of log2Skew; populated only for cell_line='meta' rows, NULL for per-cell-line rows"),
  log2Skew_mlog10p FLOAT64 OPTIONS(description="-log10 p for allelic skew (RAW for meta rows, adjusted for per-cell-line rows)"),
  log2FC FLOAT64 OPTIONS(description="Element activity vs background, log2 fold change of reporter expression over background"),
  log2FC_mlog10p FLOAT64 OPTIONS(description="-log10 p for element activity (RAW for meta rows, Bonferroni-adjusted for per-cell-line rows)"),
  mean_RNA_ref FLOAT64 OPTIONS(description="Mean reporter RNA level for the ref allele; populated only for per-cell-line rows, NULL for meta rows"),
  mean_RNA_alt FLOAT64 OPTIONS(description="Mean reporter RNA level for the alt allele; populated only for per-cell-line rows, NULL for meta rows"),
  dataset STRING NOT NULL OPTIONS(description="Source dataset (constant 'siraj_mpra')")
)
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 23, 1))
CLUSTER BY dataset, cell_line
OPTIONS(
  description="Measured cis-regulatory allelic activity from a massively parallel reporter assay (Siraj et al. 2026 MPRA)",
  labels=[("domain", "genetics"), ("data_type", "mpra")]
);
