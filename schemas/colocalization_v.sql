-- View adding derived resource columns to colocalization
CREATE OR REPLACE VIEW `genetics_results.colocalization_v` AS
SELECT
  *,
  CASE
    WHEN dataset1 LIKE 'FinnGen%MVP_UKBB%' THEN 'finngen_mvp_ukbb'
    WHEN dataset1 LIKE 'FinnGen%UKBB%' THEN 'finngen_ukbb'
    WHEN dataset1 LIKE 'FinnGen%' THEN 'finngen'
    WHEN dataset1 LIKE 'UKB%' THEN 'ukbb'
    WHEN dataset1 LIKE 'Open_Targets%' THEN 'open_targets'
    WHEN dataset1 LIKE 'COVID19_HGI%' THEN 'covid_hgi'
    WHEN dataset1 = 'PGC' THEN 'pgc'
    WHEN dataset1 = 'GP2' THEN 'gp2'
    WHEN dataset1 = 'IIBDGC' THEN 'ibd_gwas'
    ELSE LOWER(dataset1)
  END AS resource1,
  CASE
    WHEN dataset2 LIKE 'FinnGen%MVP_UKBB%' THEN 'finngen_mvp_ukbb'
    WHEN dataset2 LIKE 'FinnGen%UKBB%' THEN 'finngen_ukbb'
    WHEN dataset2 LIKE 'FinnGen%' THEN 'finngen'
    WHEN dataset2 LIKE 'UKB%' THEN 'ukbb'
    WHEN dataset2 LIKE 'Open_Targets%' THEN 'open_targets'
    WHEN dataset2 LIKE 'COVID19_HGI%' THEN 'covid_hgi'
    WHEN dataset2 = 'PGC' THEN 'pgc'
    WHEN dataset2 = 'GP2' THEN 'gp2'
    WHEN dataset2 = 'IIBDGC' THEN 'ibd_gwas'
    ELSE LOWER(dataset2)
  END AS resource2
FROM `genetics_results.colocalization`;
