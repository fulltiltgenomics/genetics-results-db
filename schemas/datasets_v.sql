-- View over datasets.
-- No generated `resource` CASE block: `resource` comes from the datasets.yaml registry and is
-- authoritative (the CASE blocks in the results views only approximate it from the dataset
-- name). Do not add this view to scripts/generate_resource_sql.py's ALL_VIEWS.
-- The view exists so the API exposes views uniformly (api/main.py VIEWS).
CREATE OR REPLACE VIEW `genetics_results.datasets_v` AS
SELECT *
FROM `genetics_results.datasets`;
