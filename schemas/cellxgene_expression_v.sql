-- Per-dataset expression with labels, DENSE over the genes each dataset measured:
--   a row with an expression value  - the gene was measured and the group passed suppression
--   mean_log1p_cp10k = 0, frac 0    - measured, not detected in any cell of the group
--   no row for the dataset          - the dataset did not measure the gene (NA, not 0)
-- Groups below the release's suppression thresholds are left out rather than shown as 0,
-- because publish drops their rows whether or not the gene was detected.
--
-- Filter on chr: it is the partition column of both large tables, and the join on chr is
-- what lets the predicate prune cellxgene_expression too. Even so a single-gene query's
-- dry-run estimate is ~2.3 GB at full scale (see cellxgene_expression.sql for the arithmetic).
CREATE OR REPLACE VIEW `genetics_results.cellxgene_expression_v` AS
SELECT
  m.chr,
  m.gene_start,
  m.gene_id,
  m.gene_name,
  d.dataset_id,
  d.dataset_version_id,
  d.collection_name,
  g.tissue_ontology_term_id,
  g.tissue,
  g.tissue_type,
  g.tissue_general_ontology_term_id,
  g.tissue_general,
  g.cell_type_ontology_term_id,
  g.cell_type,
  g.cell_type_rollup_ontology_term_id,
  g.cell_type_rollup,
  g.disease_ontology_term_id,
  g.disease,
  g.assay_ontology_term_id,
  g.assay,
  g.suspension_type,
  g.spatial,
  g.n_cells,
  g.n_donors,
  IFNULL(e.mean_log1p_cp10k, 0) AS mean_log1p_cp10k,
  IFNULL(e.frac_expressing, 0) AS frac_expressing,
  -- NULL on a zero-filled row: whether the pseudobulk is defined depends on the group's donors
  -- with a nonzero library, which no table here carries, and results-api leaves it NA too
  e.donor_pseudobulk_mean_cp10k,
  m.dataset_key,
  g.group_key
FROM `genetics_results.cellxgene_measured_genes` m
JOIN `genetics_results.cellxgene_datasets` d USING (dataset_key)
JOIN `genetics_results.cellxgene_groups` g
  ON g.dataset_key = m.dataset_key AND g.n_cells >= d.min_cells AND g.n_donors >= d.min_donors
LEFT JOIN `genetics_results.cellxgene_expression` e
  ON e.chr = m.chr AND e.gene_id = m.gene_id AND e.group_key = g.group_key;
