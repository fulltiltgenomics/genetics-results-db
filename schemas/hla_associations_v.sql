-- View adding the derived `resource` column to hla_associations.
-- The HLA results belong to the same 'finngen' resource as the FinnGen GWAS they were
-- run alongside, so the lowercase-dataset fallback used by the other product views would
-- give the wrong answer ('finngen_hla') and the resource is mapped explicitly. Mirrors the
-- `finngen_hla%` rule in configs/datasets.yaml dataset_to_resource_rules.
CREATE OR REPLACE VIEW `genetics_results.hla_associations_v` AS
SELECT
  *,
  CASE
    WHEN LOWER(dataset) LIKE 'finngen_hla%' THEN 'finngen'
    ELSE LOWER(dataset)
  END AS resource
FROM `genetics_results.hla_associations`;
