-- View over phenotypes.
-- Unlike the results views, no `resource` CASE block is generated here: `resource` is a real
-- column carried straight from the datasets.yaml registry, which is authoritative, whereas the
-- CASE blocks only exist to recover the resource from a dataset NAME where the registry is not
-- available in the row. Do not add this view to scripts/generate_resource_sql.py's ALL_VIEWS.
-- The view exists so the API exposes views uniformly (api/main.py VIEWS) and so columns can be
-- added or renamed later without breaking callers.
CREATE OR REPLACE VIEW `genetics_results.phenotypes_v` AS
SELECT *
FROM `genetics_results.phenotypes`;
