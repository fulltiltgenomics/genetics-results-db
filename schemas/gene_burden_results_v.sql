CREATE OR REPLACE VIEW `genetics_results.gene_burden_results_v` AS
SELECT
  * EXCEPT(mlog10p_skat, mlog10p_skato),
  CASE
    WHEN dataset = 'genebass' THEN 'genebass'
    WHEN dataset = 'IBD_exome_2026' THEN 'ibd_exome_2026'
    ELSE LOWER(dataset)
  END AS resource
FROM `genetics_results.gene_burden_results`;
