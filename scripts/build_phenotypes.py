#!/usr/bin/env python3
"""Build the `phenotypes` and `datasets` NDJSON load files from datasets.yaml.

Why this exists: the trait codes stored in the results views are opaque
(`AB1_ACTINOMYCOSIS`, `median_3000963_age_adjusted_IRN`, `continuous_3546_both_sexes__irnt`).
Resolving them to human-readable names used to require a round trip to genetics-results-api;
as BigQuery tables the resolution becomes a JOIN inside the same query.

Two outputs:

* `phenotypes` - one row per (dataset, trait_original):
      credible_sets_v c JOIN phenotypes_v p USING (dataset, trait_original)
  The key column is `trait_original`, NOT `trait`. In credible_sets, colocalization,
  coloc_credsets, exome_variant_results and gene_burden_results, `trait_original` is the
  phenocode while `trait` is a display string for several datasets (FinnGen R14
  'Height,_inverse-rank_normalized' vs HEIGHT_IRN; Genebass 'Mean corpuscular volume' vs
  continuous_30040_both_sexes__irnt). Keying on `trait` produces a silent zero-row join.
* `datasets` - one row per results-view `dataset` value, plus a NULL-`dataset` row for each
  registry entry with no BigQuery presence (served only by results-api).

The registry key -> results-view `dataset` value mapping (BQ_DATASETS_BY_DATASET_ID below)
is NOT derivable from any config: the `dataset` column is baked into the source credible-set
TSVs by genetics-results-munge, and datasets.yaml never records it. It is therefore
maintained here explicitly and cross-checked against the live tables by --validate-against
(scripts/load_phenotypes.sh passes it). Any drift - in either direction - fails the build
rather than silently producing a table that never joins.

The harmonization below mirrors genetics-results-api's MetadataHarmonizer
(app/services/metadata_harmonizer.py). It is duplicated rather than imported because the two
repos deploy independently; when that file changes, change this one too.
"""

import argparse
import csv
import datetime
import gzip
import io
import json
import re
import sys
from collections import defaultdict

import fsspec
import yaml

# results-view `dataset` values for each datasets.yaml registry key. The relation is
# many-to-many in both directions, which is exactly why neither `resource` nor the registry
# key is usable as the join key:
#   - several registry entries share one results-view dataset (pgc_scz + pgc_bip -> "PGC",
#     genebass_exome + genebass_gene_based -> "genebass", ibd_exome + ibd_gene_based)
#   - one registry entry appears under two names across tables (finngen_pqtl is
#     "FinnGen_Olink" in credible_sets but "FinnGen_Olink_3K" in colocalization)
# Entries absent from this map have no BigQuery presence (summary-stats-only pQTL
# meta-analyses, expression, chromatin peaks, gene-disease); they still get a `datasets`
# row, with dataset = NULL.
BQ_DATASETS_BY_DATASET_ID = {
    "finngen_gwas": ["FinnGen_R14"],
    # hla_associations spells its trait column `phenotype`, and its values ARE the R14
    # endpoint codes (all 2,712 distinct ones join FinnGen_R14 phenotypes). It still gets its
    # own phenotype rows rather than borrowing FinnGen_R14's, because `datasets` has a row for
    # finngen_hla and the documented, tool-advertised join is
    #   p.dataset = <the results view's dataset> AND p.trait_original = <code>.
    # Without finngen_hla rows that join returns ZERO ROWS SILENTLY for the one dataset this
    # whole cross-check exists to fix, and resolving HLA trait names would instead require an
    # agent to know out of band that finngen_hla is secretly R14 - a special case with no
    # discoverable signal anywhere in the schema. (FinnGen_R12 likewise duplicates 2,315 of
    # R14's codes, so per-dataset duplication is already the table's normal shape.)
    "finngen_hla": ["finngen_hla"],
    "finngen_gwas_r12": ["FinnGen_R12"],
    "finngen_kanta": ["FinnGen_kanta"],
    # the R12 Kanta fine-mapping shares the results-view name with the current R14 Kanta
    # release (it is only reachable as the caQTL/eQTL coloc partner). The two phenocode
    # spaces are disjoint - R12 uses bare OMOP ids ("3000963"), R14 uses
    # "median_3000963_age_adjusted_IRN" - so the union stays unique on (dataset, phenocode).
    "finngen_kanta_r12": ["FinnGen_kanta"],
    "finngen_drugs": ["FinnGen_drugs"],
    "finngen_mvp_ukbb": ["FinnGen_R13_MVP_UKBB"],
    "finngen_mvp_ukbb_labs": ["FinnGen_R13_MVP_UKBB_labs"],
    "finngen_ukbb": ["FinnGen_R13_UKBB"],
    "finngen_ukbb_labs": ["FinnGen_R13_UKBB_labs"],
    "finngen_eqtl": ["FinnGen_snRNAseq"],
    "finngen_caqtl": ["FinnGen_ATACseq"],
    "finngen_pqtl": ["FinnGen_Olink", "FinnGen_Olink_3K"],
    "finngen_pqtl_5k": ["FinnGen_Olink_5K"],
    "finngen_nmr": ["FinnGen_NMR"],
    "finngen_somascan": ["FinnGen_SomaScan"],
    "finnliver": ["FinnLiver"],
    "generisk": ["GeneRisk"],
    "interval": ["INTERVAL"],
    "ukbb_pqtl": ["UKB_PPP"],
    "ukbb_finucane": ["UKB_Finucane"],
    "covid_hgi": ["COVID19_HGI"],
    "ibd_gwas": ["IIBDGC"],
    "gp2_pd": ["GP2"],
    "pgc_scz": ["PGC"],
    "pgc_bip": ["PGC"],
    "pgc_scz_finemap": ["PGC_SCZ_2022"],
    "open_targets": ["Open_Targets_26.06"],
    "genebass_exome": ["genebass"],
    "genebass_gene_based": ["genebass"],
    "bipex_gene_based": ["BipEx2"],
    "schema_gene_based": ["SCHEMA2"],
    "ibd_exome": ["IBD_exome"],
    "ibd_gene_based": ["IBD_exome"],
    "decode_asmqtl_cpg": ["deCODE_asmQTL_CpG"],
    "decode_asmqtl_mds": ["deCODE_asmQTL_MDS"],
    "marderstein_open_chromatin": ["marderstein_open_chromatin"],
    "li_brain_open_chromatin": ["li_brain_open_chromatin"],
    "catlas_open_chromatin": ["catlas_open_chromatin"],
    "epimap_open_chromatin": ["epimap_open_chromatin"],
    "calderon_open_chromatin": ["calderon_open_chromatin"],
    "rosmap_open_chromatin": ["rosmap_open_chromatin"],
    "marderstein_chrombpnet": ["marderstein_chrombpnet"],
    "marderstein_flare": ["marderstein_flare"],
    "siraj_mpra": ["siraj_mpra"],
}

# results-view `dataset` names the registry claims but that NO results view actually
# contains (checked live against every view api/main.py exposes that carries a `dataset`
# column - see scripts/live_dataset_scope.py, which derives that set rather than listing it).
# Emitting them as queryable datasets is the failure this whole table exists
# to prevent: an agent filtering on them gets an empty result with no hint why. They are
# instead emitted with dataset = NULL, the table's documented "exists in the registry, has no
# BigQuery presence" state, and contribute no `phenotypes` rows.
# validate() fails if a name listed here IS live, so the list cannot rot silently - delete a
# name here as soon as its data lands.
ABSENT_FROM_RESULTS = {
    # registered and resource-derivation rules exist, but the fine-mapping has never been
    # loaded into this project; it also produced 3 orphan phenotypes rows (IBD/CD/UC)
    "IIBDGC": "ibd_gwas registered but its credible sets are not loaded",
    # eQTL Catalogue sub-studies present in the collection metadata whose fine-mapping is not
    # part of the imported release; the other ~840 QTD ids are live
    "QTD000736": "eQTL Catalogue sub-study not in the imported release",
    "QTD000863": "eQTL Catalogue sub-study not in the imported release",
    "QTD000865": "eQTL Catalogue sub-study not in the imported release",
    "QTD000869": "eQTL Catalogue sub-study not in the imported release",
    "QTD000910": "eQTL Catalogue sub-study not in the imported release",
    "QTD000915": "eQTL Catalogue sub-study not in the imported release",
}

# registry entries retained only as colocalization partners: they ship no independently
# queryable product, and results-api's search index leaves them out
# (config_util.get_metadata_dataset_ids_for_resource(..., include_coloc_partners=True)).
# Their phenocodes DO appear in colocalization_v, so their names must still resolve.
COLOC_PARTNER_ONLY_DATASET_IDS = {"finngen_gwas_r12", "finngen_kanta_r12"}

# eQTL Catalogue quantification method -> the data_type the results views use
QUANT_METHOD_TO_DATA_TYPE = {
    "ge": "eQTL", "exon": "eQTL", "tx": "eQTL", "microarray": "eQTL",
    "txrev": "sQTL", "leafcutter": "sQTL", "aptamer": "pQTL",
}

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parse_date(value):
    """Return an ISO date string, or None when the source has no usable date.

    Registry publication_date values are always full ISO dates; Open Targets per-study
    publicationDate values are sometimes empty or year-only. A year alone is not a date,
    so it is dropped rather than invented as January 1st.
    """
    if not value:
        return None
    value = str(value).strip()
    return value if _DATE_RE.match(value) else None


def safe_int(value):
    """Int or None. Sentinels ('NA', '', None) and negatives become None."""
    if value in (None, "", "NA", "nan"):
        return None
    try:
        result = int(float(value))
    except (TypeError, ValueError):
        return None
    return result if result >= 0 else None


def read_metadata_file(uri):
    """Read a metadata file (JSON or TSV, optionally bgzip/gzip) into a list of dicts."""
    compressed = uri.endswith((".gz", ".bgz"))
    mode = "rb" if compressed else "rt"
    with fsspec.open(uri, mode) as handle:
        raw = handle.read()
    if compressed:
        raw = gzip.decompress(raw).decode("utf-8")
    if ".json" in uri:
        data = json.loads(raw)
        return data if isinstance(data, list) else [data]
    return list(csv.DictReader(io.StringIO(raw), delimiter="\t"))


def _finngen_pheweb(items, entry):
    """FinnGen pheweb-style JSON (core GWAS, meta-analyses, R12 core).

    The meta-analysis files carry the trait name under `name` while core FinnGen uses
    `phenostring`; an empty name would make the row useless, so both are tried.

    results-api hardcodes trait_type="binary" for this harmonizer. That is wrong for the
    quantitative endpoints in the same file, so trait_type is derived from the counts
    instead: a GWAS with zero controls is a quantitative trait.
    """
    rows = []
    for item in items:
        n_cases = safe_int(item.get("num_cases"))
        n_controls = safe_int(item.get("num_controls"))
        if n_controls is None and n_cases is None:
            trait_type = entry.get("trait_type") if entry.get("trait_type") != "mixed" else None
        elif n_controls:
            trait_type = "binary"
        else:
            trait_type = "quantitative"
        rows.append({
            "trait": item.get("phenocode") or "",
            "trait_name": item.get("phenostring") or item.get("name") or None,
            "trait_type": trait_type,
            "category": item.get("category") or None,
            "n_cases": n_cases,
            "n_controls": n_controls,
            "n_samples": None if n_cases is None else n_cases + (n_controls or 0),
        })
    return rows


def _finngen_kanta(items, entry):
    """FinnGen Kanta lab values. `category` is "Quantitative"/"Binary" (+", derived")."""
    rows = []
    for item in items:
        category = item.get("category") or "Quantitative"
        rows.append({
            "trait": item.get("phenocode") or "",
            "trait_name": item.get("phenostring") or None,
            "trait_type": "quantitative" if category.startswith("Quantitative") else "binary",
            "category": category,
            "n_cases": safe_int(item.get("num_cases")),
            "n_controls": safe_int(item.get("num_controls")),
            "n_samples": safe_int(item.get("num_total")),
        })
    return rows


def _finngen_drugs(items, entry):
    """FinnGen drug-purchase GWAS: inverse-normalized counts, no sample sizes in the file."""
    return [{
        "trait": item.get("phenocode") or "",
        "trait_name": item.get("phenostring") or None,
        "trait_type": "quantitative",
        "category": item.get("category") or None,
        "n_cases": None,
        "n_controls": None,
        "n_samples": safe_int(entry.get("n_samples")),
    } for item in items]


def _open_targets(items, entry):
    rows = []
    for item in items:
        n_cases = safe_int(item.get("nCases"))
        rows.append({
            "trait": item.get("studyId") or "",
            "trait_name": item.get("traitFromSource") or None,
            "trait_type": "binary" if n_cases else "quantitative",
            "category": item.get("projectId") or None,
            "n_cases": n_cases,
            "n_controls": safe_int(item.get("nControls")),
            "n_samples": safe_int(item.get("nSamples")),
            "author": item.get("publicationFirstAuthor") or None,
            "publication_date": parse_date(item.get("publicationDate")),
        })
    return rows


def _genebass(items, entry):
    """Genebass phenotype manifest.

    The trait code is the five-part composite the results tables store, not `phenocode`
    alone: trait_type_phenocode_pheno_sex_coding_modifier.
    """
    rows = []
    for item in items:
        n_cases = safe_int(item.get("n_cases"))
        n_controls = safe_int(item.get("n_controls"))
        n_defined = safe_int(item.get("n_cases_defined"))
        if n_defined is not None:
            n_samples = n_defined
        elif n_cases is not None and n_controls is not None:
            n_samples = n_cases + n_controls
        else:
            n_samples = None
        raw_type = item.get("trait_type") or ""
        description = item.get("description")
        coding_description = item.get("coding_description")
        if not description or description == "NA":
            trait_name = item.get("phenocode") or None
        elif coding_description and coding_description != "NA":
            trait_name = f"{description}: {coding_description}"
        else:
            trait_name = description
        rows.append({
            "trait": "_".join(str(item.get(k, "") or "") for k in
                              ("trait_type", "phenocode", "pheno_sex", "coding", "modifier")),
            "trait_name": trait_name,
            "trait_type": "quantitative" if raw_type in ("continuous", "quantitative") else "binary",
            "category": raw_type or None,
            "n_cases": n_cases,
            "n_controls": n_controls,
            "n_samples": n_samples,
        })
    return rows


# eqtl_catalogue is deliberately absent: its "phenotypes" are QTD sub-STUDIES, which become
# rows of `datasets` (the results views put the QTD id in the `dataset` column, and `trait`
# holds a gene id). Routing it through _PHENOTYPE_HARMONIZERS would produce rows that never join.
_PHENOTYPE_HARMONIZERS = {
    "finngen_r13": _finngen_pheweb,
    "finngen_kanta": _finngen_kanta,
    "finngen_drugs": _finngen_drugs,
    "open_targets": _open_targets,
    "genebass": _genebass,
}


def build_phenotypes(registry, metadata_by_dataset_id):
    """Assemble phenotype rows, unique on (dataset, trait_original).

    Collisions are resolved in favour of the currently released dataset over a
    coloc-partner-only one, so the (dataset, trait_original) join stays 1:1.
    """
    by_key = {}
    dropped = []
    for dataset_id, entry in registry.items():
        harmonizer = _PHENOTYPE_HARMONIZERS.get(entry.get("metadata_harmonizer"))
        if harmonizer is None:
            continue
        items = metadata_by_dataset_id.get(dataset_id)
        if not items:
            continue
        partner_only = dataset_id in COLOC_PARTNER_ONLY_DATASET_IDS
        harmonized = harmonizer(items, entry)
        for dataset in BQ_DATASETS_BY_DATASET_ID.get(dataset_id, []):
            if dataset in ABSENT_FROM_RESULTS:
                continue  # a phenotype row keyed on a dataset no results view has joins nothing
            for row in harmonized:
                if not row["trait"]:
                    continue
                record = {
                    "dataset": dataset,
                    "dataset_id": dataset_id,
                    "resource": entry.get("resource"),
                    "trait_original": row["trait"],
                    "trait_name": row.get("trait_name"),
                    "trait_type": row.get("trait_type"),
                    "category": row.get("category"),
                    "n_samples": row.get("n_samples"),
                    "n_cases": row.get("n_cases"),
                    "n_controls": row.get("n_controls"),
                    "author": row.get("author") or entry.get("author"),
                    "publication_date": row.get("publication_date")
                    or parse_date(entry.get("publication_date")),
                    "version": entry.get("version"),
                    "coloc_partner_only": partner_only,
                }
                key = (dataset, row["trait"])  # (dataset, phenocode)
                existing = by_key.get(key)
                if existing is None:
                    by_key[key] = record
                elif existing["coloc_partner_only"] and not partner_only:
                    by_key[key] = record
                    dropped.append((key, existing["dataset_id"]))
                else:
                    dropped.append((key, dataset_id))
    return list(by_key.values()), dropped


def build_datasets(registry, resources, metadata_by_dataset_id):
    """Assemble dataset rows: every registry entry, plus eQTL Catalogue QTD sub-studies.

    Rows are unique on `dataset` so `JOIN datasets_v USING (dataset)` never fans results
    out. Where several registry entries share one results-view dataset (pgc_scz + pgc_bip,
    the two genebass products, the two IBD exome products) they are merged into one row
    whose `dataset_ids` lists every contributor and whose description concatenates theirs.

    `dataset` is NULL for registry entries with no BigQuery presence - they exist in the
    registry but are served only by results-api, and saying otherwise would send agents
    looking for tables that hold nothing. Those rows stay one-per-registry-entry.
    """
    rows = []
    by_dataset = {}

    def _emit(row):
        name = row["dataset"]
        if name is None:
            rows.append(row)
            return
        existing = by_dataset.get(name)
        if existing is None:
            by_dataset[name] = row
            rows.append(row)
            return
        existing["dataset_ids"].extend(
            i for i in row["dataset_ids"] if i not in existing["dataset_ids"]
        )
        if row["description"] and row["description"] not in (existing["description"] or ""):
            existing["description"] = " | ".join(
                p for p in (existing["description"], row["description"]) if p
            )
        if existing["data_type"] != row["data_type"] and row["data_type"]:
            existing["data_type"] = "mixed"
        if existing["n_samples"] is None:
            existing["n_samples"] = row["n_samples"]
        existing["pseudo_credible_sets"] |= row["pseudo_credible_sets"]
        existing["coloc_partner_only"] &= row["coloc_partner_only"]

    for dataset_id, entry in registry.items():
        resource = entry.get("resource")
        resource_meta = resources.get(resource, {})
        base = {
            "dataset_id": dataset_id,
            "dataset_ids": [dataset_id],
            "resource": resource,
            "resource_label": resource_meta.get("label"),
            "resource_aliases": resource_meta.get("aliases") or [],
            "version": entry.get("version"),
            "description": " ".join((entry.get("description") or "").split()) or None,
            "author": entry.get("author"),
            "publication_date": parse_date(entry.get("publication_date")),
            "data_type": entry.get("data_type"),
            "trait_type": entry.get("trait_type"),
            "n_samples": safe_int(entry.get("n_samples")),
            "pseudo_credible_sets": bool(entry.get("pseudo_credible_sets", False)),
            "coloc_partner_only": dataset_id in COLOC_PARTNER_ONLY_DATASET_IDS,
            "collection": bool(entry.get("collection", False)),
            "subdataset_of": None,
        }
        names = [
            name for name in BQ_DATASETS_BY_DATASET_ID.get(dataset_id, [])
            if name not in ABSENT_FROM_RESULTS
        ] or [None]
        for name in names:
            _emit({**base, "dataset": name, "dataset_ids": list(base["dataset_ids"])})

        if not entry.get("collection"):
            continue
        # a collection's metadata file describes sub-studies that appear in the results
        # views as their own `dataset` values (QTD ids), so each gets its own row
        for item in metadata_by_dataset_id.get(dataset_id, []):
            sub_id = item.get("dataset_id") or ""
            if not sub_id:
                continue
            label_parts = [item.get(k) for k in ("sample_group", "tissue_label", "condition_label")]
            _emit({
                **base,
                "dataset": None if sub_id in ABSENT_FROM_RESULTS else sub_id,
                "dataset_id": sub_id,
                "dataset_ids": [sub_id],
                "description": " - ".join(p for p in label_parts if p) or None,
                "author": item.get("study_label") or entry.get("author"),
                "data_type": QUANT_METHOD_TO_DATA_TYPE.get(
                    item.get("quant_method"), entry.get("data_type")),
                "trait_type": "quantitative",
                "n_samples": safe_int(item.get("sample_size")),
                "collection": False,
                "subdataset_of": dataset_id,
            })
    return rows


def validate(dataset_rows, phenotype_rows, live_datasets):
    """Cross-check the registry <-> results-view `dataset` mapping in every direction.

    All four checks are fatal to the build (see main): a mapping that exists in no config
    and is only ever warned about is not a guard - the IIBDGC orphan rows shipped through
    exactly that gap.
    """
    mapped = {r["dataset"] for r in dataset_rows if r["dataset"]}
    problems = []
    for name in sorted(live_datasets - mapped):
        problems.append(f"results views contain dataset={name!r} with no registry entry")
    # names suppressed via ABSENT_FROM_RESULTS never reach `mapped`, so anything left here is
    # an undeclared registry claim that would resolve to nothing
    for name in sorted(mapped - live_datasets):
        problems.append(
            f"registry maps dataset={name!r} but no results table contains it - fix the "
            f"mapping, or add it to ABSENT_FROM_RESULTS with a reason")
    for name in sorted(set(ABSENT_FROM_RESULTS) & live_datasets):
        problems.append(
            f"dataset={name!r} is in ABSENT_FROM_RESULTS but IS live now - remove it there so "
            f"its rows stop being suppressed")
    with_phenotypes = {r["dataset"] for r in phenotype_rows}
    for name in sorted(with_phenotypes - live_datasets):
        problems.append(f"phenotypes built for dataset={name!r} which no results view contains")
    return problems


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--datasets-yaml", required=True,
                        help="Path to datasets.yaml (the synced copy in configs/)")
    parser.add_argument("--profile", default="finngen",
                        help="Profile whose dataset registry (and GCS metadata paths) to use")
    parser.add_argument("--phenotypes-out", required=True, help="Output NDJSON for `phenotypes`")
    parser.add_argument("--datasets-out", required=True, help="Output NDJSON for `datasets`")
    parser.add_argument("--validate-against", default=None,
                        help="Comma-separated results-view `dataset` values to cross-check")
    parser.add_argument("--allow-unvalidated", action="store_true",
                        help="Write the NDJSON even when the cross-check fails or cannot run")
    args = parser.parse_args()

    with open(args.datasets_yaml) as handle:
        config = yaml.safe_load(handle)
    profile = config["profiles"][args.profile]
    registry = profile["datasets"]
    resources = config.get("resources", {})

    # a metadata file shared by several registry entries is fetched once
    cache = {}
    metadata_by_dataset_id = {}
    for dataset_id, entry in registry.items():
        uri = entry.get("metadata_file")
        if not uri:
            continue
        if uri not in cache:
            print(f"reading {uri}", file=sys.stderr)
            cache[uri] = read_metadata_file(uri)
        metadata_by_dataset_id[dataset_id] = cache[uri]

    phenotype_rows, dropped = build_phenotypes(registry, metadata_by_dataset_id)
    dataset_rows = build_datasets(registry, resources, metadata_by_dataset_id)

    if dropped:
        counts = defaultdict(int)
        for _, dataset_id in dropped:
            counts[dataset_id] += 1
        print(f"deduplicated {len(dropped)} (dataset, phenocode) collisions: {dict(counts)}",
              file=sys.stderr)

    live = {name.strip() for name in (args.validate_against or "").split(",") if name.strip()}
    if live:
        problems = validate(dataset_rows, phenotype_rows, live)
        for problem in problems:
            print(f"ERROR: {problem}", file=sys.stderr)
        if problems and not args.allow_unvalidated:
            sys.exit(f"refusing to write: {len(problems)} registry/results mismatch(es); "
                     f"pass --allow-unvalidated to load anyway")
    elif not args.allow_unvalidated:
        sys.exit("refusing to write: --validate-against is empty, so the registry -> "
                 "results-view mapping cannot be cross-checked; pass --allow-unvalidated "
                 "to load anyway")

    for path, rows in ((args.phenotypes_out, phenotype_rows), (args.datasets_out, dataset_rows)):
        with fsspec.open(path, "wt") as handle:
            for row in rows:
                handle.write(json.dumps(row) + "\n")
        print(f"wrote {len(rows)} rows to {path}", file=sys.stderr)

    generated = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
    print(f"done at {generated}", file=sys.stderr)


if __name__ == "__main__":
    main()
