CREATE OR REPLACE VIEW `genetics_results.gene_burden_results_v` AS
SELECT
  *,
  CASE
    WHEN LOWER(dataset) = 'genebass' THEN 'genebass'
    WHEN LOWER(dataset) = 'ibd_exome' THEN 'ibd_exome_2026'
    ELSE LOWER(dataset)
  END AS resource
FROM `genetics_results.gene_burden_results`;
