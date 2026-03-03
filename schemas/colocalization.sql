-- BigQuery schema for colocalization results table
-- Contains coloc posterior probabilities and overlap metrics for colocalized credible sets

CREATE TABLE IF NOT EXISTS `genetics_results.colocalization`
(
  dataset1 STRING NOT NULL OPTIONS(description="First dataset name"),
  dataset2 STRING NOT NULL OPTIONS(description="Second dataset name"),
  data_type1 STRING NOT NULL OPTIONS(description="First dataset type (GWAS, eQTL, etc.)"),
  data_type2 STRING NOT NULL OPTIONS(description="Second dataset type"),
  trait1 STRING OPTIONS(description="First trait ID"),
  trait1_original STRING NOT NULL OPTIONS(description="First original trait name"),
  trait2 STRING OPTIONS(description="Second trait ID"),
  trait2_original STRING NOT NULL OPTIONS(description="Second original trait name"),
  cell_type1 STRING OPTIONS(description="First cell/tissue type"),
  cell_type2 STRING OPTIONS(description="Second cell/tissue type"),
  cs1_id STRING NOT NULL OPTIONS(description="First credible set ID"),
  cs2_id STRING NOT NULL OPTIONS(description="Second credible set ID"),
  hit1 STRING OPTIONS(description="Lead variant in first credible set"),
  hit2 STRING OPTIONS(description="Lead variant in second credible set"),
  hit1_beta FLOAT64 OPTIONS(description="Effect size of lead variant in first set"),
  hit1_mlog10p FLOAT64 OPTIONS(description="-log10(p-value) of lead variant in first set"),
  hit2_beta FLOAT64 OPTIONS(description="Effect size of lead variant in second set"),
  hit2_mlog10p FLOAT64 OPTIONS(description="-log10(p-value) of lead variant in second set"),
  chr INT64 NOT NULL OPTIONS(description="Chromosome"),
  region_start_min INT64 OPTIONS(description="Region start position"),
  region_end_max INT64 OPTIONS(description="Region end position"),
  PP_H0_abf FLOAT64 OPTIONS(description="Posterior probability H0: no association in either"),
  PP_H1_abf FLOAT64 OPTIONS(description="Posterior probability H1: association in dataset 1 only"),
  PP_H2_abf FLOAT64 OPTIONS(description="Posterior probability H2: association in dataset 2 only"),
  PP_H3_abf FLOAT64 OPTIONS(description="Posterior probability H3: both associated, different variants"),
  PP_H4_abf FLOAT64 OPTIONS(description="Posterior probability H4: both associated, shared variant"),
  nsnps INT64 OPTIONS(description="Number of SNPs in region"),
  nsnps1 INT64 OPTIONS(description="Number of SNPs in first credible set"),
  nsnps2 INT64 OPTIONS(description="Number of SNPs in second credible set"),
  cs1_log10bf FLOAT64 OPTIONS(description="Log10 Bayes factor for first credible set"),
  cs2_log10bf FLOAT64 OPTIONS(description="Log10 Bayes factor for second credible set"),
  clpp FLOAT64 OPTIONS(description="Colocalization posterior probability"),
  clpa FLOAT64 OPTIONS(description="Colocalization prior adjusted"),
  cs1_size INT64 OPTIONS(description="First credible set size"),
  cs2_size INT64 OPTIONS(description="Second credible set size"),
  cs_overlap INT64 OPTIONS(description="Number of overlapping variants"),
  topInOverlap STRING OPTIONS(description="Top variant in overlap region")
)
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 23, 1))
CLUSTER BY dataset1, data_type1, dataset2, data_type2
OPTIONS(
  description="Colocalization analysis results between credible sets",
  labels=[("domain", "genetics"), ("data_type", "colocalization")]
);
