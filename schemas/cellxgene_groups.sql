-- One row per CELLxGENE cell group: a dataset's cells sharing tissue, cell type, disease, assay
-- and suspension type. Every group the workers produced, including groups below the
-- per-dataset suppression threshold, which have no cellxgene_expression rows (see
-- cellxgene_expression_v). Loaded from <release>/bq/groups.tsv.gz (publish.py
-- BQ_GROUP_COLUMNS). Small (a few hundred thousand rows): unpartitioned.
CREATE TABLE IF NOT EXISTS `genetics_results.cellxgene_groups`
(
  group_key INT64 NOT NULL OPTIONS(description="Release-local key, unique across datasets"),
  dataset_key INT64 NOT NULL OPTIONS(description="Release-local key into cellxgene_datasets"),
  dataset_version_id STRING NOT NULL OPTIONS(description="CELLxGENE dataset version"),
  dataset_id STRING OPTIONS(description="CELLxGENE dataset (stable across versions)"),
  tissue_ontology_term_id STRING OPTIONS(description="Native tissue term (UBERON, or CL for cell cultures)"),
  tissue STRING OPTIONS(description="Native tissue label"),
  tissue_type STRING OPTIONS(description="tissue, organoid or cell culture; never pooled across"),
  tissue_general_ontology_term_id STRING OPTIONS(description="Rolled-up tissue term"),
  tissue_general STRING OPTIONS(description="Rolled-up tissue label"),
  cell_type_ontology_term_id STRING OPTIONS(description="Native Cell Ontology term"),
  cell_type STRING OPTIONS(description="Native cell type label"),
  cell_type_rollup_ontology_term_id STRING OPTIONS(description="Rolled-up Cell Ontology term"),
  cell_type_rollup STRING OPTIONS(description="Rolled-up cell type label"),
  disease_ontology_term_id STRING OPTIONS(description="Disease term (PATO:0000461 = normal)"),
  disease STRING OPTIONS(description="Disease label"),
  assay_ontology_term_id STRING OPTIONS(description="Assay term (EFO)"),
  assay STRING OPTIONS(description="Assay label"),
  suspension_type STRING OPTIONS(description="cell, nucleus or na"),
  spatial BOOL OPTIONS(description="Spatial assay (a Visium spot is not a cell)"),
  n_cells INT64 NOT NULL OPTIONS(description="Cells in the group"),
  n_donors INT64 NOT NULL OPTIONS(description="Distinct donors in the group")
)
CLUSTER BY dataset_key, group_key
OPTIONS(
  description="CELLxGENE cell groups (dataset x tissue x cell type x disease x assay)",
  labels=[("domain", "genetics"), ("data_type", "cellxgene_groups")]
);
