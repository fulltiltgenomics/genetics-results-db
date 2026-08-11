-- View adding the derived `resource` column to hla_associations.
-- The HLA results belong to the same 'finngen' resource as the FinnGen GWAS they were
-- run alongside, so the lowercase-dataset fallback used by the other product views would
-- give the wrong answer ('finngen_hla') and the resource is mapped explicitly. Mirrors the
-- `finngen_hla%` rule in configs/datasets.yaml dataset_to_resource_rules.
--
-- The statistic columns are also renamed to the suite's house spelling. The staged file
-- and the base table keep FinnGen's native mlogp/sebeta/af_alt names, but every other
-- surface in the suite — results-api's /v1/hla response among them — spells the same
-- quantities mlog10p/se/af/af_cases/af_controls. The values are byte-identical (same
-- source rows, same munge pass), so this is a spelling alignment, not a transformation:
-- it stops the SDK's hla() returning different column names depending on which selector
-- was passed. Columns are therefore listed explicitly rather than SELECT *, so adding a
-- column to hla_associations requires deciding its name here too.
CREATE OR REPLACE VIEW `genetics_results.hla_associations_v` AS
SELECT
  chr,
  pos,
  gene,
  allele,
  phenotype,
  pval,
  mlogp AS mlog10p,
  beta,
  sebeta AS se,
  af_alt AS af,
  af_alt_cases AS af_cases,
  af_alt_controls AS af_controls,
  info,
  dataset,
  CASE
    WHEN LOWER(dataset) LIKE 'finngen_hla%' THEN 'finngen'
    ELSE LOWER(dataset)
  END AS resource
FROM `genetics_results.hla_associations`;
