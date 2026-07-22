-- View adding the derived `resource` column to mpra.
-- `variant` (chr:pos:ref:alt) is already a stored column, so it is not re-derived here.
CREATE OR REPLACE VIEW `genetics_results.mpra_v` AS
SELECT
  *,
  CASE
    WHEN LOWER(dataset) LIKE 'siraj_mpra%' THEN 'siraj_mpra'
    ELSE LOWER(dataset)
  END AS resource
FROM `genetics_results.mpra`;
