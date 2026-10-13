-- One row per deCODE pQTL pseudo credible set, flagging whether its lead is the strongest
-- lead of its aptamer within ±2 Mb (`window_top`).
--
-- The deCODE pseudo credible sets come from LD clumping with the FinnGen R12 panel, which
-- cannot tell an LD shadow of a strong pQTL from an independent signal: every variant the
-- panel does not list as a partner of a stronger lead becomes a locus of its own. On the
-- 2026-09-27 load, 26,904 of 130,685 sets are window tops; the rest sit within 2 Mb of a
-- stronger lead of the same aptamer, and counting them as signals counts LD shadows. The
-- full derivation is in genetics-results-munge docs/pseudo-credible-sets.md.
--
-- No base table: the flag is a function of credible_sets alone, so storing it would only
-- add a reload to keep in step. A query scans the deCODE rows of credible_sets, about
-- 1.1 GB; the data_type and resource predicates are what lets clustering prune to them.
--
-- The window is autoreporting's locus_width_kb (±2 Mb), the distance over which one
-- clump could have absorbed the other. The lead is the variant cs_id names, not the max-PIP
-- row: the two differ in a few hundred sets.
CREATE OR REPLACE VIEW `genetics_results.decode_pqtl_signals_v` AS
WITH sets AS (
  SELECT
    dataset, data_type, trait, trait_original, cell_type, cs_id, resource,
    ANY_VALUE(cs_size) AS cs_size,
    ANY_VALUE(chr) AS chr,
    ANY_VALUE(IF(CONCAT('chr', REPLACE(variant, ':', '_')) = cs_id,
                 STRUCT(variant, pos, ref, alt, mlog10p), NULL)) AS lead
  FROM `genetics_results.credible_sets`
  WHERE data_type = 'pQTL' AND resource = 'decode' AND dataset = 'deCODE_pQTL_2021'
  GROUP BY dataset, data_type, trait, trait_original, cell_type, cs_id, resource
),
stronger AS (
  SELECT
    s.trait_original, s.cs_id,
    ARRAY_AGG(STRUCT(o.cs_id, o.lead.mlog10p AS mlog10p, ABS(o.lead.pos - s.lead.pos) AS distance)
              ORDER BY o.lead.mlog10p DESC LIMIT 1)[OFFSET(0)] AS top
  FROM sets s
  JOIN sets o
    ON o.trait_original = s.trait_original AND o.chr = s.chr
   AND ABS(o.lead.pos - s.lead.pos) <= 2000000 AND o.lead.mlog10p > s.lead.mlog10p
  GROUP BY s.trait_original, s.cs_id
)
SELECT
  s.dataset,
  s.data_type,
  s.trait,
  s.trait_original,
  s.cell_type,
  s.cs_id,
  s.cs_size,
  s.chr,
  s.lead.pos AS pos,
  s.lead.ref AS ref,
  s.lead.alt AS alt,
  s.lead.variant AS variant,
  s.lead.mlog10p AS mlog10p,
  t.top IS NULL AS window_top,
  t.top.cs_id AS stronger_cs_id,
  t.top.mlog10p AS stronger_mlog10p,
  t.top.distance AS stronger_distance,
  s.resource
FROM sets s
LEFT JOIN stronger t USING (trait_original, cs_id);
