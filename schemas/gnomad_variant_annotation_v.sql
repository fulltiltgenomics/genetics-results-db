-- View adding the derived `resource` column to gnomad_variant_annotation.
-- The table is single-source, so `resource` is a constant rather than a
-- dataset-derived CASE (there is no dataset discriminator column to switch on).
-- `variant` (chr:pos:ref:alt) is already a stored column, so it is not re-derived here.
CREATE OR REPLACE VIEW `genetics_results.gnomad_variant_annotation_v` AS
SELECT
  *,
  'gnomad' AS resource
FROM `genetics_results.gnomad_variant_annotation`;
