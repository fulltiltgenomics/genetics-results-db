-- View adding the derived `maf` column to credible_sets.
--
-- `variant` and `resource` are stored columns on the base table (they are clustering
-- keys — a view-derived column cannot prune anything), so they are NOT re-derived here.
-- The `resource` CASE now lives in scripts/load_data.py, generated from the
-- dataset_to_resource_rules in datasets.yaml by scripts/generate_resource_sql.py.
--
-- The EXCEPT + explicit re-projection is load-bearing, not cosmetic: the base table
-- stores `resource` at ordinal 1 and `variant` at ordinal 10, but every downstream
-- reader (API, MCP tools, browser) sees this view's schema, which must stay
-- byte-identical to the pre-swap one — the 19 original columns in their original
-- order, then variant, maf, resource. A bare `SELECT *` would silently reorder it.
--
-- `maf` stays derived: nothing filters or clusters on it, so materialising it would
-- only cost storage.
CREATE OR REPLACE VIEW `genetics_results.credible_sets_v` AS
SELECT
  * EXCEPT(variant, resource),
  variant,
  LEAST(aaf, 1 - aaf) AS maf,
  resource
FROM `genetics_results.credible_sets`;
