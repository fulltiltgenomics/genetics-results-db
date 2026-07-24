-- View adding the derived `resource` column to variant_annotation.
-- The table is single-source (FinnGen R14), so `resource` is the constant
-- 'finngen' rather than a dataset-derived CASE. It is therefore not part of the
-- generate_resource_sql.py / datasets.yaml linting (no dataset discriminator column).
-- `variant` (chr:pos:ref:alt) is already a stored column, so it is not re-derived here.
CREATE OR REPLACE VIEW `genetics_results.variant_annotation_v` AS
SELECT
  *,
  'finngen' AS resource
FROM `genetics_results.variant_annotation`;
