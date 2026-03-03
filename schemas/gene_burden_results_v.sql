CREATE OR REPLACE VIEW `genetics_results.gene_burden_results_v` AS
SELECT
  *,
  CASE
    WHEN dataset = 'genebass' THEN 'genebass'
    ELSE LOWER(dataset)
  END AS resource
FROM `genetics_results.gene_burden_results`;
