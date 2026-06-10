-- View adding derived columns to coloc_credsets
CREATE OR REPLACE VIEW `genetics_results.coloc_credsets_v` AS
SELECT
  *,
  CONCAT(chr, ':', pos, ':', ref, ':', alt) AS variant,
  CASE
    WHEN LOWER(dataset) LIKE 'finngen%mvp_ukbb%' THEN 'finngen_mvp_ukbb'
    WHEN LOWER(dataset) LIKE 'finngen%ukbb%' THEN 'finngen_ukbb'
    WHEN LOWER(dataset) LIKE 'finngen%' THEN 'finngen'
    WHEN LOWER(dataset) LIKE 'ukb%' THEN 'ukbb'
    WHEN LOWER(dataset) LIKE 'open_targets%' THEN 'open_targets'
    WHEN LOWER(dataset) LIKE 'covid19_hgi%' THEN 'covid_hgi'
    WHEN LOWER(dataset) = 'pgc' THEN 'pgc'
    WHEN LOWER(dataset) = 'gp2' THEN 'gp2'
    WHEN LOWER(dataset) = 'iibdgc' THEN 'ibd_gwas'
    ELSE LOWER(dataset)
  END AS resource
FROM `genetics_results.coloc_credsets`;
