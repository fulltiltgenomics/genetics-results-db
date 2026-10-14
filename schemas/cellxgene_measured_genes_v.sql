CREATE OR REPLACE VIEW `genetics_results.cellxgene_measured_genes_v` AS
SELECT
  m.chr, m.gene_start, m.gene_id, m.gene_name, m.dataset_key,
  d.dataset_id, d.dataset_version_id
FROM `genetics_results.cellxgene_measured_genes` m
JOIN `genetics_results.cellxgene_datasets` d USING (dataset_key);
