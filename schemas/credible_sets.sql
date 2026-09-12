-- BigQuery schema for credible_sets table
-- Partitioned by chromosome, clustered by data_type, resource, variant, pos.
--
-- `variant` (chr:pos:ref:alt) and `resource` are STORED columns, not view-derived.
-- They were derived in credible_sets_v until the query-shape measurement showed the
-- two filters every caller is told to use (`resource =`, `variant =`) pruned nothing:
-- a derived column cannot be a clustering key, so `WHERE resource = 'finngen'` scanned
-- MORE than an unfiltered scan (4.18 GB vs 2.52 GB) because it additionally had to read
-- `dataset` to evaluate the CASE. Materialising both and clustering on them cut the
-- weighted cost of the 10 commonest logged query shapes from 333.14 GB to 43.45 GB
-- (-87.0%), at the cost of +13.5% logical bytes and slower `dataset =` (+267%),
-- `gene_most_severe` (+44%) and `most_severe` (+8%) filters. See
-- docs/credible-sets-clustering-swap.md.
--
-- Column ORDER matters twice: it must match the benchmarked layout, and
-- credible_sets_v re-projects it so the view's output schema stays byte-identical
-- to the pre-swap one (the two new columns are appended there, not interleaved).
--
-- `resource` is materialised by scripts/load_data.py from the dataset_to_resource_rules
-- in datasets.yaml (via scripts/generate_resource_sql.py), so it is no longer a CASE in
-- the view. Changing a mapping rule therefore requires a reload or backfill of this
-- column — re-running the view no longer picks the change up.
-- `maf` is deliberately left view-derived: nothing filters or clusters on it.
--
-- Both new columns are NOT NULL, unlike the benchmark table credible_sets_exp_drvp
-- (a CTAS, which flattens every column to NULLABLE). They are deterministic non-null
-- transforms of NOT NULL columns, and api/main.py reports a view column's mode from the
-- base table — declaring them NULLABLE would flip `/schema` from REQUIRED to NULLABLE
-- for variant/resource, which the MCP agents read. The rebuild must therefore be
-- CREATE-then-INSERT, not CTAS.

CREATE TABLE IF NOT EXISTS `genetics_results.credible_sets`
(
  dataset STRING NOT NULL OPTIONS(description="Source dataset (FinnGen_R14, Open_Targets_26.06, etc.)"),
  resource STRING NOT NULL OPTIONS(description="Resource identifier (lowercase) derived from dataset at load time; clustering key"),
  data_type STRING NOT NULL OPTIONS(description="GWAS, eQTL, pQTL, sQTL, caQTL"),
  trait STRING NOT NULL OPTIONS(description="Phenotype/trait ID"),
  trait_original STRING NOT NULL OPTIONS(description="Original trait name"),
  cell_type STRING OPTIONS(description="Cell/tissue type (null for GWAS)"),
  chr INT64 NOT NULL OPTIONS(description="Chromosome"),
  pos INT64 NOT NULL OPTIONS(description="Position"),
  ref STRING NOT NULL OPTIONS(description="Reference allele"),
  alt STRING NOT NULL OPTIONS(description="Alternate allele"),
  variant STRING NOT NULL OPTIONS(description="Variant identifier (chr:pos:ref:alt); clustering key"),
  mlog10p FLOAT64 OPTIONS(description="-log10(p-value)"),
  -- nullable, like mlog10p and se beside it. The EstBB-UKBB NMR fine-mapping publishes no
  -- effect size for 5,004 of its variants: their z-score overflowed in the source and is not
  -- recoverable, so requiring beta would mean dropping exactly the strongest signals
  beta FLOAT64 OPTIONS(description="Effect size"),
  se FLOAT64 OPTIONS(description="Standard error"),
  pip FLOAT64 NOT NULL OPTIONS(description="Posterior inclusion probability"),
  cs_id STRING NOT NULL OPTIONS(description="Credible set ID"),
  cs_size INT64 NOT NULL OPTIONS(description="Credible set size"),
  cs_min_r2 FLOAT64 OPTIONS(description="Minimum R² in credible set"),
  aaf FLOAT64 OPTIONS(description="Alternate allele frequency"),
  most_severe STRING OPTIONS(description="Most severe variant consequence"),
  gene_most_severe STRING OPTIONS(description="Gene with most severe consequence")
)
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 23, 1))
CLUSTER BY data_type, resource, variant, pos
OPTIONS(
  description="Fine-mapped credible set variants from multiple genetics datasets",
  labels=[("domain", "genetics"), ("data_type", "credible_sets")]
);
