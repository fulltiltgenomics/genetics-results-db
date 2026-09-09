-- View adding the derived `resource` column to rcnv_window_associations.
--
-- There is no join here, unlike rcnv_gene_associations_v: a window has no gene, so there is
-- nothing to attach dosage_sensitivity's per-gene scores to. Genes overlapping a window are
-- found through gene_annotations_v coordinates, and their scores through
-- dosage_sensitivity_v or rcnv_gene_associations_v.
--
-- Mirrors the `Collins_rCNV%` rule in configs/datasets.yaml dataset_to_resource_rules; the
-- lowercase fallback would give 'collins_rcnv_2022', not the 'rcnv' resource the other rCNV
-- views carry, and the two have to agree for a caller filtering `resource = 'rcnv'` to see
-- all of them.
CREATE OR REPLACE VIEW `genetics_results.rcnv_window_associations_v` AS
SELECT
  *,
  CASE
    WHEN LOWER(dataset) LIKE 'collins_rcnv%' THEN 'rcnv'
    ELSE LOWER(dataset)
  END AS resource
FROM `genetics_results.rcnv_window_associations`;
