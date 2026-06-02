CREATE OR REPLACE VIEW `genetics_results.gene_annotations_v` AS
SELECT
  *,
  'hgnc' AS resource
FROM `genetics_results.gene_annotations`;
