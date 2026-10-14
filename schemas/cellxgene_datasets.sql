-- One row per CELLxGENE dataset version in the loaded release. Loaded from
-- <release>/bq/datasets.tsv (publish.py DATASET_COLUMNS) plus three columns the loader injects
-- from the release manifest, so they commit in the same transaction as the data:
--   release                the release name, as in the bucket's releases/CURRENT `release`
--                          field; what a staleness check compares with CURRENT
--   min_cells, min_donors  publish's per-dataset suppression thresholds, which is how
--                          cellxgene_expression_v tells a suppressed group from a 0
CREATE TABLE IF NOT EXISTS `genetics_results.cellxgene_datasets`
(
  dataset_key INT64 NOT NULL OPTIONS(description="Release-local key"),
  dataset_id STRING OPTIONS(description="CELLxGENE dataset (stable across versions)"),
  dataset_version_id STRING NOT NULL OPTIONS(description="CELLxGENE dataset version"),
  collection_id STRING OPTIONS(description="CELLxGENE collection"),
  collection_name STRING OPTIONS(description="Collection name"),
  title STRING OPTIONS(description="Dataset title"),
  collection_doi STRING OPTIONS(description="Publication DOI of the collection"),
  citation STRING OPTIONS(description="Citation string from the CELLxGENE curation API"),
  cells_kept INT64 NOT NULL OPTIONS(description="Primary cells aggregated"),
  n_groups INT64 NOT NULL OPTIONS(description="Cell groups, before suppression"),
  n_genes INT64 NOT NULL OPTIONS(description="Features in the source file"),
  n_genes_measured INT64 NOT NULL OPTIONS(description="Features the dataset measured"),
  n_genes_mapped INT64 NOT NULL OPTIONS(description="Measured features mapped to GENCODE v49"),
  release STRING NOT NULL OPTIONS(description="Release these rows were loaded from"),
  min_cells INT64 NOT NULL OPTIONS(description="Release's suppression threshold: cells per group"),
  min_donors INT64 NOT NULL OPTIONS(description="Release's suppression threshold: donors per group")
)
CLUSTER BY dataset_key
OPTIONS(
  description="CELLxGENE dataset versions in the loaded release",
  labels=[("domain", "genetics"), ("data_type", "cellxgene_datasets")]
);
