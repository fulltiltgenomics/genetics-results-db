-- BigQuery schema for open_chromatin table
-- Atlas of accessible/active chromatin regions labeled by cell type / tissue / condition
-- (Product A). One row per peak per context (LONG layout).
-- Partitioned by chromosome, clustered by dataset, tissue, cell_type for efficient querying.
-- chr is stored as INT64 (chrX=23, chrY=24, chrM/MT=25) even though the canonical/tabix
-- source TSV encodes chrom as a chr-prefixed string ("chr1".."chrX"); the loader converts.
-- start/end are named peak_start/peak_end to avoid the `end` reserved word.

CREATE TABLE IF NOT EXISTS `genetics_results.open_chromatin`
(
  chr INT64 NOT NULL OPTIONS(description="Chromosome (INT64; X=23, Y=24, M/MT=25)"),
  peak_start INT64 NOT NULL OPTIONS(description="Peak/region start position (0-based BED start)"),
  peak_end INT64 NOT NULL OPTIONS(description="Peak/region end position"),
  peak_id STRING OPTIONS(description="Source peak/element identifier"),
  dataset STRING NOT NULL OPTIONS(description="Source dataset (e.g. marderstein_open_chromatin, catlas_open_chromatin)"),
  cell_type STRING OPTIONS(description="Free-text source cell-type label (provenance only, not a join key)"),
  tissue STRING OPTIONS(description="Harmonized tissue axis (e.g. brain, heart, immune)"),
  life_stage STRING OPTIONS(description="Harmonized life stage (e.g. fetal, adult)"),
  condition STRING OPTIONS(description="Harmonized condition (e.g. resting, stimulated, AD, control)"),
  assay STRING OPTIONS(description="Assay type: scATAC, snATAC, bulk_ATAC, chromHMM"),
  score FLOAT64 OPTIONS(description="Peak score/signal; NULL for presence-only baselines"),
  score_type STRING OPTIONS(description="Categorical score type (e.g. presence, chromhmm_18state); scores are never unit-harmonized"),
  n_cells INT64 OPTIONS(description="Number of cells/nuclei supporting the peak, when available"),
  cell_ontology_id STRING OPTIONS(description="Cell Ontology (CL) identifier, when available"),
  uberon_id STRING OPTIONS(description="UBERON tissue identifier, when available"),
  target_gene STRING OPTIONS(description="Linked target gene symbol (enhancer/cCRE-to-gene link), when available"),
  target_gene_id STRING OPTIONS(description="Linked target gene Ensembl id, when available"),
  version STRING OPTIONS(description="Dataset version/build stamp")
)
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 23, 1))
CLUSTER BY dataset, tissue, cell_type
OPTIONS(
  description="Open-chromatin atlas: accessible/active regions labeled by cell type/tissue/condition",
  labels=[("domain", "genetics"), ("data_type", "open_chromatin")]
);
