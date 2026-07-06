-- BigQuery schema for variant_effect table
-- In-silico predicted variant effect on chromatin accessibility (Product B).
-- Model-generic: the `model` column distinguishes chrombpnet / flare (and future models).
-- One row per variant per model per context (LONG layout).
-- Partitioned by chromosome, clustered by dataset, tissue, model for efficient querying.
-- chr is stored as INT64 (chrX=23, chrY=24, chrM/MT=25) even though the canonical/tabix
-- source TSV encodes chrom as a chr-prefixed string ("chr1".."chrX"); the loader converts.
-- `variant` (chr:pos:ref:alt) is stored (not view-derived) because the shared canonical TSV
-- served to the API already carries it, keeping the positional CSV load aligned.

CREATE TABLE IF NOT EXISTS `genetics_results.variant_effect`
(
  chr INT64 NOT NULL OPTIONS(description="Chromosome (INT64; X=23, Y=24, M/MT=25)"),
  pos INT64 NOT NULL OPTIONS(description="Variant position (1-based)"),
  ref STRING NOT NULL OPTIONS(description="Reference allele"),
  alt STRING NOT NULL OPTIONS(description="Alternate allele"),
  variant STRING OPTIONS(description="Variant identifier (chr:pos:ref:alt)"),
  rsid STRING OPTIONS(description="dbSNP rsID, when available"),
  dataset STRING NOT NULL OPTIONS(description="Source dataset (marderstein_chrombpnet, marderstein_flare)"),
  model STRING OPTIONS(description="Prediction model: chrombpnet, flare"),
  cell_type STRING OPTIONS(description="Free-text source cell-type label (provenance only, not a join key)"),
  tissue STRING OPTIONS(description="Harmonized tissue axis (e.g. brain, heart, immune)"),
  life_stage STRING OPTIONS(description="Harmonized life stage (e.g. fetal, adult)"),
  score FLOAT64 OPTIONS(description="Predicted effect score; interpretation depends on score_type/model"),
  score_type STRING OPTIONS(description="Categorical score type (e.g. chrombpnet_logfc, flare_score); scores are never unit-harmonized"),
  mlog10p FLOAT64 OPTIONS(description="-log10(p-value) for the predicted effect, when available"),
  predicted_direction STRING OPTIONS(description="Predicted direction of effect (e.g. gain, loss), when available"),
  quantile_rank FLOAT64 OPTIONS(description="Quantile rank of the score within the model's distribution"),
  is_significant BOOL OPTIONS(description="Whether the predicted effect passes the model's significance threshold"),
  version STRING OPTIONS(description="Dataset version/build stamp")
)
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 23, 1))
CLUSTER BY dataset, tissue, model
OPTIONS(
  description="In-silico predicted variant effects on chromatin accessibility (ChromBPNet, FLARE)",
  labels=[("domain", "genetics"), ("data_type", "variant_effect")]
);
