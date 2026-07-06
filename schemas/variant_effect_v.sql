-- View adding the derived `resource` column to variant_effect.
-- `variant` (chr:pos:ref:alt) is already a stored column, so it is not re-derived here.
CREATE OR REPLACE VIEW `genetics_results.variant_effect_v` AS
SELECT
  *,
  CASE
    WHEN LOWER(dataset) LIKE 'marderstein%' THEN 'marderstein'
    ELSE LOWER(dataset)
  END AS resource
FROM `genetics_results.variant_effect`;
