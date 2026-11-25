-- View adding the constant `resource` to gnomad_missense_constraint_regions. The table is
-- single-source, so there is no dataset column for a CASE to switch on.
CREATE OR REPLACE VIEW `genetics_results.gnomad_missense_constraint_regions_v` AS
SELECT
  *,
  'gnomad' AS resource
FROM `genetics_results.gnomad_missense_constraint_regions`;
