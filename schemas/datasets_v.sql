-- View over datasets.
-- No generated `resource` CASE block: `resource` comes from the datasets.yaml registry and is
-- authoritative (the CASE blocks in the results views only approximate it from the dataset
-- name). Its `resource_derivation.mode` in configs/datasets.yaml is `none` for that reason, and
-- must stay `none`: any other mode puts the view in the lint scope and demands a generated CASE
-- that would override the authoritative column.
-- The view exists so the API exposes views uniformly (api/main.py VIEWS).
CREATE OR REPLACE VIEW `genetics_results.datasets_v` AS
SELECT *
FROM `genetics_results.datasets`;
