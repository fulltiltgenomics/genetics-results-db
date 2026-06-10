-- View adding derived columns to credible_sets
CREATE OR REPLACE VIEW `genetics_results.credible_sets_v` AS
SELECT
  *,
  CONCAT(chr, ':', pos, ':', ref, ':', alt) AS variant,
  LEAST(aaf, 1 - aaf) AS maf,
  CASE
    WHEN dataset LIKE 'FinnGen%MVP_UKBB%' THEN 'finngen_mvp_ukbb'
    WHEN dataset LIKE 'FinnGen%UKBB%' THEN 'finngen_ukbb'
    WHEN dataset LIKE 'FinnGen%' THEN 'finngen'
    WHEN dataset LIKE 'UKB%' THEN 'ukbb'
    WHEN dataset LIKE 'Open_Targets%' THEN 'open_targets'
    WHEN dataset LIKE 'COVID19_HGI%' THEN 'covid_hgi'
    WHEN dataset = 'PGC' THEN 'pgc'
    WHEN dataset = 'GP2' THEN 'gp2'
    WHEN dataset = 'IIBDGC' THEN 'ibd_gwas'
    ELSE LOWER(dataset)
  END AS resource
FROM `genetics_results.credible_sets`;
