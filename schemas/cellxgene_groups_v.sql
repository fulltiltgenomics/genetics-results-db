CREATE OR REPLACE VIEW `genetics_results.cellxgene_groups_v` AS
SELECT
  g.*,
  d.collection_name, d.title
FROM `genetics_results.cellxgene_groups` g
JOIN `genetics_results.cellxgene_datasets` d USING (dataset_key);
