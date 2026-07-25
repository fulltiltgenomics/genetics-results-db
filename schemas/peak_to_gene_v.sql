-- View adding the derived `resource` column to peak_to_gene.
-- peak_to_gene is link data (not variant data), so no variant/maf columns are added.
-- The CASE block is generated from datasets.yaml — run
-- `python scripts/generate_resource_sql.py generate peak_to_gene_v` after changing the rules.
CREATE OR REPLACE VIEW `genetics_results.peak_to_gene_v` AS
SELECT
  *,
  CASE
    WHEN LOWER(dataset) LIKE 'finngen%' THEN 'finngen'
    ELSE LOWER(dataset)
  END AS resource
FROM `genetics_results.peak_to_gene`;
