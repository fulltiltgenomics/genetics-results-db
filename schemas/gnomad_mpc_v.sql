-- View adding `variant` and the constant `resource` to gnomad_mpc. The table is
-- single-source, so there is no dataset column for a CASE to switch on.
CREATE OR REPLACE VIEW `genetics_results.gnomad_mpc_v` AS
SELECT
  *,
  CONCAT(chr, ':', pos, ':', ref, ':', alt) AS variant,
  'gnomad' AS resource
FROM `genetics_results.gnomad_mpc`;
