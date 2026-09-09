-- View adding the derived `resource` column to rcnv_gene_associations, plus the gene's
-- dosage-sensitivity scores.
--
-- The LEFT JOIN to dosage_sensitivity is what makes "is this gene dosage sensitive AND is
-- a deletion of it associated with this phenotype" one query instead of two: pHaplo/pTriplo
-- ride on every association row. The join is on ensembl_gene_id, never symbol: symbol names
-- two genes each in a handful of cases and would duplicate rows.
--
-- It is LEFT because an association row losing its statistics for want of a score would be a
-- silent wrong answer rather than a missing one. The two products are independently
-- gene-scoped (17,263 genes tested, 18,641 scored), so that is a real risk in a future
-- release even though the current pair happens to nest — every tested gene has a score, so
-- an INNER JOIN would return the same rows today.
--
-- Mirrors the `Collins_rCNV%` rule in configs/datasets.yaml dataset_to_resource_rules; the
-- lowercase fallback would give 'collins_rcnv_2022', not the 'rcnv' resource the
-- dosage_sensitivity_v scores are tagged with, and the two have to agree for a caller
-- filtering `resource = 'rcnv'` to see both.
CREATE OR REPLACE VIEW `genetics_results.rcnv_gene_associations_v` AS
SELECT
  a.*,
  d.phaplo,
  d.ptriplo,
  d.haploinsufficient,
  d.triplosensitive,
  CASE
    WHEN LOWER(dataset) LIKE 'collins_rcnv%' THEN 'rcnv'
    ELSE LOWER(dataset)
  END AS resource
FROM `genetics_results.rcnv_gene_associations` a
LEFT JOIN `genetics_results.dosage_sensitivity` d USING (ensembl_gene_id);
