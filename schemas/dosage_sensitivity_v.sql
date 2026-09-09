CREATE OR REPLACE VIEW `genetics_results.dosage_sensitivity_v` AS
SELECT
  *,
  -- single-source table, so there is no `dataset` column for a CASE to switch on;
  -- `resource` is the routing/grouping tag callers filter on, not a provenance claim.
  'rcnv' AS resource
FROM `genetics_results.dosage_sensitivity`;
