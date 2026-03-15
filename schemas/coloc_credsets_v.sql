-- View adding derived columns to coloc_credsets
CREATE OR REPLACE VIEW `genetics_results.coloc_credsets_v` AS
SELECT
  *,
  CONCAT(chr, ':', pos, ':', ref, ':', alt) AS variant,
  CASE
    WHEN dataset LIKE 'FinnGen%MVP_UKBB%' THEN 'finngen_mvp_ukbb'
    WHEN dataset LIKE 'FinnGen%UKBB%' THEN 'finngen_ukbb'
    WHEN dataset LIKE 'FinnGen%' THEN 'finngen'
    WHEN dataset LIKE 'UKB%' THEN 'ukbb'
    WHEN dataset LIKE 'Open_Targets%' THEN 'open_targets'
    ELSE LOWER(dataset)
  END AS resource
FROM `genetics_results.coloc_credsets`;
