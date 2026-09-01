CREATE OR REPLACE VIEW `genetics_results.gene_annotations_v` AS
SELECT
  *,
  -- `resource` is a routing/grouping tag callers filter on, not a provenance claim, so a
  -- constant here does not conflict with the table's Ensembl/GENCODE/HGNC sourcing.
  'hgnc' AS resource
FROM `genetics_results.gene_annotations`;
