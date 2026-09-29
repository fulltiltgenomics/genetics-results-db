CREATE OR REPLACE VIEW `genetics_results.exome_variant_counts_v` AS
SELECT
  *,
  CONCAT(chr, ':', pos, ':', ref, ':', alt) AS variant,
  CASE
    WHEN LOWER(dataset) = 'asc2' THEN 'asc2'
    ELSE LOWER(dataset)
  END AS resource
FROM `genetics_results.exome_variant_counts`;
