-- BigQuery schema for phenotypes table
-- Trait/phenotype metadata: the human-readable name, trait type and sample sizes behind the
-- opaque phenotype codes stored in the results views (AB1_ACTINOMYCOSIS,
-- median_3000963_age_adjusted_IRN, continuous_30040_both_sexes__irnt, GCST90004895).
-- Built from the per-dataset metadata_file JSON/TSVs referenced by datasets.yaml, which were
-- previously reachable only through genetics-results-api - so resolving a trait code cost a
-- separate round trip. Here it is a JOIN inside the same query:
--   SELECT c.*, p.trait_name
--   FROM credible_sets_v c JOIN phenotypes_v p USING (dataset, trait_original)
--
-- KEY: (dataset, trait_original), unique.
--
-- JOIN ON trait_original, NOT trait. In every results view, `trait_original` is the phenotype
-- code and `trait` is a display form that differs for a majority of the rows: FinnGen R14
-- stores trait='Height,_inverse-rank_normalized' / trait_original='HEIGHT_IRN', Genebass
-- stores trait='Mean corpuscular volume' / trait_original='continuous_30040_both_sexes__irnt',
-- and the QTL datasets store trait=gene symbol / trait_original=Ensembl id. Joining on `trait`
-- returns zero rows silently.
--
-- `dataset` is the value that appears in the results views, NOT the datasets.yaml registry
-- key - the two relate many-to-many (pgc_scz and pgc_bip both ship inside dataset 'PGC';
-- finngen_pqtl is 'FinnGen_Olink' in credible_sets but 'FinnGen_Olink_3K' in colocalization),
-- so keying on `resource` or the registry key would conflate distinct studies. `dataset_id`
-- is retained as the provenance of each row.
--
-- Small reference-style table (~33k rows): unpartitioned like gene_annotations and
-- peak_to_gene, clustered by dataset then trait_original because the dominant access is the
-- (dataset, trait_original) join and the second is "list this dataset's traits".
--
-- No genomic coordinates are carried, so the chr encoding used elsewhere (X=23) does not apply.
--
-- Coverage note: only datasets whose registry entry has a metadata_file AND a phenotype-level
-- harmonizer produce rows. eQTL Catalogue is deliberately excluded - its QTD identifiers are
-- sub-STUDIES that appear in the `dataset` column, not traits, and they live in `datasets`.
-- Datasets with no metadata_file at all (FinnGen NMR/SomaScan, PGC, GP2, ...) have no rows;
-- their trait codes are already human-readable or are gene/protein identifiers.

CREATE TABLE IF NOT EXISTS `genetics_results.phenotypes`
(
  dataset STRING NOT NULL OPTIONS(description="Results-view dataset name; joins credible_sets.dataset, colocalization.dataset1/dataset2, coloc_credsets.dataset, exome_variant_results.dataset, gene_burden_results.dataset"),
  trait_original STRING NOT NULL OPTIONS(description="Phenotype code exactly as stored in the results views; joins credible_sets.trait_original, colocalization.trait1_original/trait2_original, coloc_credsets.trait_original, exome_variant_results.trait_original, gene_burden_results.trait_original. NOT the views `trait` column, which is a display string for several datasets"),
  trait_name STRING OPTIONS(description="Human-readable trait name (NULL for the handful of source rows that carry no name)"),
  trait_type STRING OPTIONS(description="'binary' or 'quantitative'; derived from case/control counts where the source does not state it, NULL when neither is available"),
  category STRING OPTIONS(description="Source-provided grouping, e.g. the FinnGen ICD chapter, the FinnGen Kanta 'Quantitative'/'Binary' class, the Open Targets project id or the Genebass trait_type"),
  n_samples INT64 OPTIONS(description="Total analysed sample size; NULL when the source does not report it (not 0)"),
  n_cases INT64 OPTIONS(description="Number of cases; NULL for quantitative traits and where unreported"),
  n_controls INT64 OPTIONS(description="Number of controls; NULL for quantitative traits and where unreported"),
  dataset_id STRING NOT NULL OPTIONS(description="ONE contributing datasets.yaml registry key - provenance, NOT a join key. Where several registry entries merge into one `datasets` row that row keeps only the first as its `dataset_id`, so values like 'finngen_kanta_r12' and 'genebass_gene_based' match no datasets row at all. Join `datasets` on `dataset`, or on `dataset_id IN UNNEST(datasets.dataset_ids)` to reach the merged-away keys"),
  resource STRING NOT NULL OPTIONS(description="Resource the dataset belongs to (finngen, open_targets, genebass, ...)"),
  author STRING OPTIONS(description="Study author; per-study for Open Targets, otherwise the dataset's author"),
  publication_date DATE OPTIONS(description="Publication date; per-study for Open Targets where it is a full date, otherwise the dataset's. NULL rather than invented when the source gives only a year"),
  version STRING OPTIONS(description="Dataset version label, e.g. R14, R12, 26.06"),
  coloc_partner_only BOOL NOT NULL OPTIONS(description="TRUE when the trait's dataset ships no independently queryable product and exists only as a colocalization partner (FinnGen R12 core and R12 Kanta). Those traits appear in colocalization_v but not in credible_sets_v; results-api's phenotype search index also omits them, so filter them out to mirror it")
)
CLUSTER BY dataset, trait_original
OPTIONS(
  description="Trait/phenotype metadata keyed by (dataset, trait_original) for resolving and filtering phenotype codes without a results-api round trip. Join on trait_original, never on trait",
  labels=[("domain", "genetics"), ("data_type", "metadata")]
);
