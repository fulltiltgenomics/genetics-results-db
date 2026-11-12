-- Variant-to-residue map of the protein structure release: one row per (possible missense
-- variant, UniProt accession), loaded from <release>/variants.tsv.gz, the same bgzip+tabix
-- file the genetics-results-api serves (genetics-results-munge
-- scripts/protein_structure_contract.py, VARIANT_COLUMNS, is the column order). Replaced
-- whole by scripts/load_missense_residue.sh.
--
-- A variant that falls on more than one protein has a row per protein, so `variant` is not
-- a key here; (variant, accession) is.
--
-- The partition range ends at 26 for the reason gnomad_variant_annotation's does: the upper
-- bound is exclusive and the suite's chromosome codes run to 25 (X=23, Y=24, M=25).
--
-- Clustered by pos alone, as gnomad_variant_annotation is and for the same two query
-- shapes: a point lookup (`chr = AND pos =`) and a region scan (`chr = AND pos BETWEEN`).
-- A join from an association view arrives with chr and pos as well as `variant`.
--
-- `release` is not in the file: the loader writes the release name on every row in the
-- transaction that replaces the table, so the rows and the name cannot disagree.
CREATE TABLE IF NOT EXISTS `genetics_results.missense_residue`
(
  chr INT64 NOT NULL OPTIONS(description="Chromosome (INT64; X=23)"),
  pos INT64 NOT NULL OPTIONS(description="Variant position (1-based, GRCh38); clustering key"),
  ref STRING NOT NULL OPTIONS(description="Reference allele"),
  alt STRING NOT NULL OPTIONS(description="Alternate allele"),
  variant STRING NOT NULL OPTIONS(description="Variant identifier (chr:pos:ref:alt, GRCh38)"),
  accession STRING NOT NULL OPTIONS(description="UniProt accession of the protein whose canonical isoform the variant changes"),
  gene_id STRING OPTIONS(description="Unversioned Ensembl gene ID: the accession's gene whose GENCODE v49 span contains pos; NULL when none or more than one does"),
  gene_name STRING OPTIONS(description="GENCODE v49 symbol of gene_id; NULL with it"),
  aa_pos INT64 NOT NULL OPTIONS(description="Protein position of the changed residue (1-based, canonical UniProt isoform)"),
  aa_ref STRING NOT NULL OPTIONS(description="Reference amino acid, one-letter code (the sequence's residue)"),
  aa_alt STRING NOT NULL OPTIONS(description="Alternate amino acid, one-letter code"),
  model_file_id STRING NOT NULL OPTIONS(description="Structure model file that owns aa_pos"),
  file_position INT64 NOT NULL OPTIONS(description="Position of the residue within model_file_id (1-based)"),
  mu_snp FLOAT64 OPTIONS(description="Per-variant mutation rate; NULL where the source has none"),
  release STRING NOT NULL OPTIONS(description="Protein structure release the row was loaded from; the same on every row")
)
PARTITION BY RANGE_BUCKET(chr, GENERATE_ARRAY(1, 26, 1))
CLUSTER BY pos
OPTIONS(
  description="Possible missense variants mapped to the protein residue they change, one row per (variant, UniProt accession)",
  labels=[("domain", "genetics"), ("data_type", "missense_residue")]
);
