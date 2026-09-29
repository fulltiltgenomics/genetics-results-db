CREATE OR REPLACE VIEW `genetics_results.exome_gene_bayes_results_v` AS
SELECT
  *,
  CASE
    WHEN LOWER(dataset) = 'asc2' THEN 'asc2'
    ELSE LOWER(dataset)
  END AS resource
FROM `genetics_results.exome_gene_bayes_results`;
