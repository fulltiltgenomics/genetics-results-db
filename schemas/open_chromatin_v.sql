-- View adding the derived `resource` column to open_chromatin.
-- open_chromatin is region data (not variant data), so no variant/maf columns are added.
CREATE OR REPLACE VIEW `genetics_results.open_chromatin_v` AS
SELECT
  *,
  CASE
    WHEN LOWER(dataset) LIKE 'marderstein%' THEN 'marderstein'
    WHEN LOWER(dataset) LIKE 'li_brain%' THEN 'li_brain_atac'
    WHEN LOWER(dataset) LIKE 'catlas%' THEN 'catlas'
    WHEN LOWER(dataset) LIKE 'epimap%' THEN 'epimap'
    WHEN LOWER(dataset) LIKE 'calderon%' THEN 'calderon_immune'
    WHEN LOWER(dataset) LIKE 'rosmap%' THEN 'rosmap_brain'
    ELSE LOWER(dataset)
  END AS resource
FROM `genetics_results.open_chromatin`;
