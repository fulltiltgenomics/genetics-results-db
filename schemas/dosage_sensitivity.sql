-- BigQuery schema for dosage_sensitivity table
-- Genome-wide dosage-sensitivity scores from Collins et al. 2022 (Cell 185:3041,
--   doi 10.1016/j.cell.2022.06.036, Zenodo 6347673, CC-BY 4.0): one row per
--   autosomal protein-coding gene, pHaplo and pTriplo from ~1M rare-CNV carriers.
-- Small reference table: unpartitioned, clustered by symbol. There are deliberately no
--   coordinate columns — the scores are gene-keyed and therefore build-independent, so
--   nothing here needs the GRCh37 the source was called on. Join gene_annotations_v on
--   ensembl_gene_id (or symbol) when coordinates are wanted.
-- symbol is not unique, so a symbol join can legitimately return two rows;
--   ensembl_gene_id is the key. GENCODE itself gives two genes the same symbol
--   for some rows.
-- Every column is REQUIRED: the staged file carries no NA, so a NULL arriving here is a
--   broken munge and should fail the load rather than load quietly.
-- The haploinsufficient/triplosensitive booleans store the paper's published cutoffs
--   (pHaplo >= 0.86, pTriplo >= 0.94) so callers do not have to remember them and
--   cannot silently pick a different threshold; the underlying scores stay available
--   for anyone who wants one.

CREATE TABLE IF NOT EXISTS `genetics_results.dosage_sensitivity`
(
  symbol STRING NOT NULL OPTIONS(description="Current HGNC gene symbol, mapped from the source GENCODE v19 symbol via ENSG"),
  symbol_gencode_v19 STRING NOT NULL OPTIONS(description="Gene symbol as published by Collins et al. (GENCODE v19 spelling)"),
  ensembl_gene_id STRING NOT NULL OPTIONS(description="Ensembl gene ID (unversioned, e.g. ENSG00000169174). Unique — the table's key"),
  phaplo FLOAT64 NOT NULL OPTIONS(description="pHaplo: probability the gene is haploinsufficient (0-1)"),
  ptriplo FLOAT64 NOT NULL OPTIONS(description="pTriplo: probability the gene is triplosensitive (0-1)"),
  haploinsufficient BOOL NOT NULL OPTIONS(description="phaplo >= 0.86, the paper's published haploinsufficiency cutoff"),
  triplosensitive BOOL NOT NULL OPTIONS(description="ptriplo >= 0.94, the paper's published triplosensitivity cutoff")
)
CLUSTER BY symbol
OPTIONS(
  description="Gene-level dosage sensitivity (pHaplo/pTriplo) from the Collins et al. 2022 cross-disorder rare-CNV map",
  labels=[("domain", "genetics"), ("data_type", "rcnv")]
);
