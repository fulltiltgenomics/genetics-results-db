-- Pass-through view of missense_residue. The release is single-source and has no `dataset`
-- discriminator, so no `resource` is derived; `variant` is a stored column.
CREATE OR REPLACE VIEW `genetics_results.missense_residue_v`
OPTIONS(
  description="Possible missense variants mapped to the protein residue they change, one row per (variant, UniProt accession). A variant with no row is not a missense change of the canonical UniProt isoform under the map's annotation (VEP 115 / Ensembl 115), and nothing stronger: not that it is non-coding, synonymous, or missense in no transcript."
) AS
SELECT *
FROM `genetics_results.missense_residue`;
