-- View adding the derived `resource` column to rcnv_segments and exposing the six
-- semicolon-joined list columns as ARRAY<STRING>.
--
-- The SPLIT lives here rather than in the loader because load_data.py's DERIVED_COLUMNS
-- materialises columns the source TSV does NOT contain; these six are columns of the file,
-- so converting them at load time would need new staging machinery for one 163-row table.
--
-- Two of the six need more than a bare SPLIT:
--   `credints` is NULL only where n_credints = 1 and that single interval failed to lift —
--     the whole field was then the literal 'NA', which the loader's null marker turned into
--     NULL. A multi-interval segment with every interval failing would read 'NA;NA;...'
--     instead, a non-NULL string SPLIT handles directly. Restoring the literal keeps the
--     array the same length as credints_grch37, so position i means the same interval in both.
--   the gene lists are empty for the 12 segments containing no protein-coding gene, and
--     SPLIT('') returns [''] — one empty string, not an empty array — which would make
--     ARRAY_LENGTH disagree with n_genes.
--
-- Mirrors the `Collins_rCNV%` rule in configs/datasets.yaml dataset_to_resource_rules; the
-- lowercase fallback would give 'collins_rcnv_2022', not the 'rcnv' resource the
-- dosage_sensitivity_v scores and rcnv_gene_associations_v carry.
CREATE OR REPLACE VIEW `genetics_results.rcnv_segments_v` AS
SELECT
  * EXCEPT(associated_hpos, credints, credints_grch37, genes, genes_gencode_v19, gene_ensembl_ids),
  SPLIT(associated_hpos, ';') AS associated_hpos,
  SPLIT(IFNULL(credints, 'NA'), ';') AS credints,
  SPLIT(credints_grch37, ';') AS credints_grch37,
  IF(COALESCE(genes, '') = '', ARRAY<STRING>[], SPLIT(genes, ';')) AS genes,
  IF(COALESCE(genes_gencode_v19, '') = '', ARRAY<STRING>[], SPLIT(genes_gencode_v19, ';')) AS genes_gencode_v19,
  -- the COALESCE guard treats an all-NA gene_ensembl_ids the same as empty; a single-gene
  -- segment whose only ENSG were the literal 'NA' would drop that gene silently and
  -- misalign the array against `genes`, but the munge asserts no segment's gene list is 'NA'
  IF(COALESCE(gene_ensembl_ids, '') = '', ARRAY<STRING>[], SPLIT(gene_ensembl_ids, ';')) AS gene_ensembl_ids,
  CASE
    WHEN LOWER(dataset) LIKE 'collins_rcnv%' THEN 'rcnv'
    ELSE LOWER(dataset)
  END AS resource
FROM `genetics_results.rcnv_segments`;
