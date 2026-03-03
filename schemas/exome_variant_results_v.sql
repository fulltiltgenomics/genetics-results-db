CREATE OR REPLACE VIEW `genetics_results.exome_variant_results_v` AS
SELECT
  *,
  CONCAT(chr, ':', pos, ':', ref, ':', alt) AS variant,
  CASE
    WHEN dataset = 'genebass' THEN 'genebass'
    ELSE LOWER(dataset)
  END AS resource
FROM `genetics_results.exome_variant_results`;
