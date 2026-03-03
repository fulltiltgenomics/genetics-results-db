-- View adding derived resource columns to colocalization
CREATE OR REPLACE VIEW `genetics_results.colocalization_v` AS
SELECT
  *,
  CASE
    WHEN dataset1 LIKE 'FinnGen%' THEN 'finngen'
    WHEN dataset1 LIKE 'UKB%' THEN 'ukbb'
    WHEN dataset1 LIKE 'Open_Targets%' THEN 'open_targets'
    ELSE LOWER(dataset1)
  END AS resource1,
  CASE
    WHEN dataset2 LIKE 'FinnGen%' THEN 'finngen'
    WHEN dataset2 LIKE 'UKB%' THEN 'ukbb'
    WHEN dataset2 LIKE 'Open_Targets%' THEN 'open_targets'
    ELSE LOWER(dataset2)
  END AS resource2
FROM `genetics_results.colocalization`;
