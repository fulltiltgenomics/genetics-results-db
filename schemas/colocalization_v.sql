-- View adding derived resource columns to colocalization
CREATE OR REPLACE VIEW `genetics_results.colocalization_v` AS
SELECT
  *,
  CASE
    WHEN LOWER(dataset1) LIKE 'finngen%mvp_ukbb%' THEN 'finngen_mvp_ukbb'
    WHEN LOWER(dataset1) LIKE 'finngen%ukbb%' THEN 'finngen_ukbb'
    WHEN LOWER(dataset1) LIKE 'finngen%' THEN 'finngen'
    WHEN LOWER(dataset1) LIKE 'ukb%' THEN 'ukbb'
    WHEN LOWER(dataset1) LIKE 'open_targets%' THEN 'open_targets'
    WHEN LOWER(dataset1) LIKE 'covid19_hgi%' THEN 'covid_hgi'
    WHEN LOWER(dataset1) = 'pgc' THEN 'pgc'
    WHEN LOWER(dataset1) = 'gp2' THEN 'gp2'
    WHEN LOWER(dataset1) = 'iibdgc' THEN 'ibd_gwas'
    ELSE LOWER(dataset1)
  END AS resource1,
  CASE
    WHEN LOWER(dataset2) LIKE 'finngen%mvp_ukbb%' THEN 'finngen_mvp_ukbb'
    WHEN LOWER(dataset2) LIKE 'finngen%ukbb%' THEN 'finngen_ukbb'
    WHEN LOWER(dataset2) LIKE 'finngen%' THEN 'finngen'
    WHEN LOWER(dataset2) LIKE 'ukb%' THEN 'ukbb'
    WHEN LOWER(dataset2) LIKE 'open_targets%' THEN 'open_targets'
    WHEN LOWER(dataset2) LIKE 'covid19_hgi%' THEN 'covid_hgi'
    WHEN LOWER(dataset2) = 'pgc' THEN 'pgc'
    WHEN LOWER(dataset2) = 'gp2' THEN 'gp2'
    WHEN LOWER(dataset2) = 'iibdgc' THEN 'ibd_gwas'
    ELSE LOWER(dataset2)
  END AS resource2
FROM `genetics_results.colocalization`;
