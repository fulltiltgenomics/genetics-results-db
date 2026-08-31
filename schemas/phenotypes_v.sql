-- View over phenotypes.
-- Unlike the results views, no `resource` CASE block is generated here: `resource` is a real
-- column carried straight from the datasets.yaml registry, which is authoritative, whereas the
-- CASE blocks only exist to recover the resource from a dataset NAME where the registry is not
-- available in the row. That is why its `resource_derivation.mode` in configs/datasets.yaml is
-- `none`, and it must stay `none`: any other mode puts the view in the lint scope and demands a
-- generated CASE that would override the authoritative column.
-- The view exists so the API exposes views uniformly (api/main.py VIEWS) and so columns can be
-- added or renamed later without breaking callers.
CREATE OR REPLACE VIEW `genetics_results.phenotypes_v` AS
SELECT *
FROM `genetics_results.phenotypes`;
