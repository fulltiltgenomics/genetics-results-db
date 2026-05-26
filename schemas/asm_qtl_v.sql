-- View adding derived columns to asm_qtl
CREATE OR REPLACE VIEW `genetics_results.asm_qtl_v` AS
SELECT
  *,
  CONCAT(chr, ':', pos, ':', ref, ':', alt) AS variant,
  LEAST(af, 1 - af) AS maf,
  CASE
    WHEN dataset LIKE 'deCODE%' THEN 'decode'
    ELSE LOWER(dataset)
  END AS resource
FROM `genetics_results.asm_qtl`;
