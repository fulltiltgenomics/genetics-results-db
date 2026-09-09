-- BigQuery schema for rcnv_segments table
-- The 163 disease-associated rare-CNV segments of Collins et al. 2022 (Cell 185:3041,
--   doi 10.1016/j.cell.2022.06.036, Zenodo 6347673, CC-BY 4.0) — Table S3. One row per
--   segment: 69 DEL and 94 DUP, 88 genome-wide significant and 75 FDR significant.
--
-- Coordinates are dual. `segment_start_grch37`/`segment_end_grch37` are the published
--   intervals and are always present; `segment_start`/`segment_end` are the GRCh38 lift and
--   are NULL where the munge could lift neither the whole interval nor the two published
--   200 kb sliding windows that begin and end on the segment's boundaries (six segments on
--   the chain the current load was built from, among them the 22q11.21 duplication; the set
--   is a property of the liftOver chain, and genetics-results-munge's
--   docs/rcnv-dosage-sensitivity.md carries the current one). A coordinate query against
--   `segment_start`/`segment_end` cannot see those rows, which is why the GRCh37 pair is
--   kept rather than dropped as provenance.
--
-- The columns are `segment_start`/`segment_end`, not the bare `start`/`end` the source TSV
--   uses: `end` is a reserved GoogleSQL keyword, and this table is queried by model-written
--   SQL — a caller that forgets to backtick a reserved identifier gets a syntax error rather
--   than a result, and an LLM generating ad hoc queries is exactly the caller likely to
--   forget. Renaming at load time removes the hazard instead of documenting it.
--
-- The six ';'-joined list columns are stored as written and exposed as ARRAY<STRING> by
--   rcnv_segments_v. They are not split at load time because load_data.py's DERIVED_COLUMNS
--   materialises columns that are ABSENT from the source TSV (it drops them from the staging
--   schema); these six are present in the file, so converting them in place would mean new
--   loader machinery on the CHR_STRING_TABLES model for one 163-row table. SPLIT in the view
--   costs nothing at this size and keeps the raw text queryable.
--
-- `credints` carries the literal 'NA' in the positions whose interval failed to lift, so it
--   stays positionally aligned with `credints_grch37`. Where a segment's ONLY credible
--   interval failed, the whole field is 'NA' and the loader's null marker turns it into
--   NULL — rcnv_segments_v puts the placeholder back so the alignment holds for every row.
--
-- Unpartitioned. The repo partitions results tables by RANGE_BUCKET(chr, ...), but 163 rows
--   over 22 chromosomes is ~7 rows a partition — partition metadata would cost more than the
--   pruning saves, and the same reasoning kept dosage_sensitivity unpartitioned. Clustered by
--   chr, segment_start: clustering is a sort prefix, so a chromosome-scoped overlap scan
--   (the query this table exists for) prunes on both.

CREATE TABLE IF NOT EXISTS `genetics_results.rcnv_segments`
(
  dataset STRING NOT NULL OPTIONS(description="Source dataset (constant 'Collins_rCNV_2022')"),
  segment_id STRING NOT NULL OPTIONS(description="Segment identifier as published, e.g. merged_DEL_segment_22q11.21. Unique within the table"),
  cnv_type STRING NOT NULL OPTIONS(description="Copy-number variant class: DEL (deletion) or DUP (duplication)"),
  chr INT64 NOT NULL OPTIONS(description="Chromosome number, 1-22. The published segments are autosomal only"),
  segment_start INT64 OPTIONS(description="Segment start, GRCh38, 0-based half-open as published. NULL for the few segments whose GRCh37 interval lifts neither whole nor via the sliding windows on its boundaries"),
  segment_end INT64 OPTIONS(description="Segment end, GRCh38. NULL for the same segments as segment_start"),
  segment_start_grch37 INT64 NOT NULL OPTIONS(description="Segment start as published (GRCh37). Always present; the fallback for the segments that do not lift"),
  segment_end_grch37 INT64 NOT NULL OPTIONS(description="Segment end as published (GRCh37). Always present"),
  cytoband STRING NOT NULL OPTIONS(description="Cytogenetic band range the segment spans, e.g. 22q11.21"),
  best_significance STRING NOT NULL OPTIONS(description="Strongest significance tier the segment reached across its phenotypes: 'Genome-wide' or 'FDR'"),
  control_freq FLOAT64 OPTIONS(description="Pooled carrier frequency of the segment in controls"),
  case_freq FLOAT64 OPTIONS(description="Pooled carrier frequency of the segment in cases"),
  beta FLOAT64 OPTIONS(description="Pooled effect size as ln(odds ratio) across the associated phenotypes; OR = EXP(beta)"),
  beta_lower FLOAT64 OPTIONS(description="Lower bound of the 95% confidence interval on the pooled beta (ln scale)"),
  beta_upper FLOAT64 OPTIONS(description="Upper bound of the 95% confidence interval on the pooled beta (ln scale)"),
  beta_min FLOAT64 OPTIONS(description="Smallest per-phenotype ln(OR) among the segment's associated phenotypes"),
  beta_max FLOAT64 OPTIONS(description="Largest per-phenotype ln(OR) among the segment's associated phenotypes"),
  n_hpos INT64 NOT NULL OPTIONS(description="Number of associated HPO phenotype groups; the length of associated_hpos"),
  associated_hpos STRING NOT NULL OPTIONS(description="Semicolon-joined HPO phenotype group codes without the colon. SPLIT into ARRAY<STRING> by rcnv_segments_v"),
  n_credints INT64 NOT NULL OPTIONS(description="Number of 95% credible intervals fine-mapped within the segment; the length of credints and credints_grch37"),
  credints STRING OPTIONS(description="Semicolon-joined 95% credible intervals as chr:start-end (GRCh38), positionally aligned with credints_grch37 with the literal 'NA' where an interval did not lift. NULL where none of them lifted"),
  credints_grch37 STRING NOT NULL OPTIONS(description="Semicolon-joined 95% credible intervals as chr:start-end (GRCh37), as published. Always complete"),
  credint_size INT64 NOT NULL OPTIONS(description="Total span in base pairs covered by the segment's credible intervals (GRCh37)"),
  n_genes INT64 NOT NULL OPTIONS(description="Number of protein-coding genes in the segment; the length of genes, genes_gencode_v19 and gene_ensembl_ids. 0 for 12 gene-poor segments"),
  genes STRING OPTIONS(description="Semicolon-joined current HGNC gene symbols, mapped from the source GENCODE v19 symbols via ENSG. Empty for the 12 segments with no genes"),
  genes_gencode_v19 STRING OPTIONS(description="Semicolon-joined gene symbols as published by Collins et al. (GENCODE v19 spelling), aligned with genes"),
  gene_ensembl_ids STRING OPTIONS(description="Semicolon-joined Ensembl gene IDs (unversioned), aligned with genes. The join key to gene_annotations and dosage_sensitivity")
)
CLUSTER BY chr, segment_start
OPTIONS(
  description="Disease-associated rare-CNV segments from Collins et al. 2022 (Table S3), 163 segments with GRCh38 and GRCh37 coordinates",
  labels=[("domain", "genetics"), ("data_type", "rcnv")]
);
