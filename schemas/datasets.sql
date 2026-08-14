-- BigQuery schema for datasets table
-- The dataset registry: what every `dataset` value appearing in the results views actually is,
-- plus the resource it belongs to. Built from the profile's `profiles.<p>.datasets` and
-- `resources` blocks of datasets.yaml, and from the eQTL Catalogue collection metadata whose
-- QTD sub-studies appear in the results views as `dataset` values of their own.
--
-- KEY: `dataset`, unique among non-NULL values, so `JOIN datasets_v USING (dataset)` never
-- fans results out. Where several registry entries share one results-view dataset (pgc_scz +
-- pgc_bip inside 'PGC', the two Genebass products inside 'genebass', the two IBD exome
-- products inside 'IBD_exome') they are merged into one row and `dataset_ids` lists every
-- contributor.
--
-- `dataset` is NULL for registry entries with no BigQuery presence, which also covers the
-- handful that ARE mapped to a results-view name but whose data is not loaded in this
-- project (build_phenotypes.ABSENT_FROM_RESULTS; the load fails if that list goes stale).
-- The other NULLs are summary-stats-only
-- products, expression, chromatin peaks and gene-disease sets that only results-api serves.
-- They are kept so this table answers "what data exists at all", but a NULL `dataset` means
-- "do not look for it in the results views".
--
-- Small reference-style table (~890 rows, most of them eQTL Catalogue QTD sub-studies):
-- unpartitioned, clustered by dataset then resource.

CREATE TABLE IF NOT EXISTS `genetics_results.datasets`
(
  dataset STRING OPTIONS(description="Results-view dataset name; joins the `dataset` column of every results view (dataset1/dataset2 in colocalization_v). NULL when the dataset has no BigQuery presence"),
  dataset_id STRING NOT NULL OPTIONS(description="Primary datasets.yaml registry key: the FIRST contributor where entries were merged, so it does NOT match every phenotypes.dataset_id. Join phenotypes on `dataset`, or on `phenotypes.dataset_id IN UNNEST(dataset_ids)`"),
  dataset_ids ARRAY<STRING> OPTIONS(description="Every registry key merged into this row - more than one when several registry entries share a results-view dataset"),
  resource STRING NOT NULL OPTIONS(description="Resource name; matches the derived `resource` column of the results views"),
  resource_label STRING OPTIONS(description="Display label for the resource, e.g. 'FinnGen', 'Open Targets Genetics'"),
  resource_aliases ARRAY<STRING> OPTIONS(description="Alternative names users write for the resource; use for resolving free-text resource mentions"),
  version STRING OPTIONS(description="Dataset version label, e.g. R14, 26.06, PPP"),
  description STRING OPTIONS(description="What the dataset is, its cohort and caveats. Descriptions of merged registry entries are concatenated with ' | '"),
  author STRING OPTIONS(description="Producing consortium or study; for eQTL Catalogue sub-studies the source study label, e.g. 'Alasoo_2018'"),
  publication_date DATE OPTIONS(description="Release/publication date; NULL where the registry records none"),
  data_type STRING OPTIONS(description="gwas, eQTL, sQTL, pqtl, caqtl, gene_based, ... 'mixed' where merged entries disagree. Lower-case registry vocabulary, which is NOT the upper-case vocabulary of the results views' own data_type column"),
  trait_type STRING OPTIONS(description="binary, quantitative or mixed at the dataset level; per-trait resolution lives in phenotypes.trait_type"),
  n_samples INT64 OPTIONS(description="Dataset-level sample size where the registry states one; NULL otherwise (per-trait sizes live in phenotypes)"),
  pseudo_credible_sets BOOL NOT NULL OPTIONS(description="TRUE when this dataset's credible sets are PSEUDO credible sets (LD-clumped proxies), not SuSiE fine-mapping. Their pip/cs_size columns are not posterior quantities and must not be compared with fine-mapped ones"),
  coloc_partner_only BOOL NOT NULL OPTIONS(description="TRUE when the dataset exists only as a colocalization partner and ships no independently queryable product"),
  collection BOOL NOT NULL OPTIONS(description="TRUE for a collection whose sub-studies are the rows carrying subdataset_of = this dataset_id (eQTL Catalogue). A collection row itself never appears as a `dataset` value"),
  subdataset_of STRING OPTIONS(description="Parent collection's dataset_id for sub-studies (QTD ids under eqtl_catalogue); NULL otherwise")
)
CLUSTER BY dataset, resource
OPTIONS(
  description="Dataset registry: what each results-view `dataset` value is, its resource, version, provenance and credible-set caveats",
  labels=[("domain", "genetics"), ("data_type", "metadata")]
);
