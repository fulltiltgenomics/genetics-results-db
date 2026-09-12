# Genetics Results Database

BigQuery database for genetics fine-mapping, colocalization, and exome sequencing results with a REST API.

This is deployed as part of FinnGenie AI assistant (see [https://github.com/fulltiltgenomics/genetics-results-suite](https://github.com/fulltiltgenomics/genetics-results-suite)).

Cannot yet be used as is without access to restricted data.

## Loading data to BigQuery

[scripts/setup_bigquery.sh](scripts/setup_bigquery.sh) creates the BigQuery dataset and tables

[scripts/load_credsets_coloc.sh](scripts/load_credsets_coloc.sh) loads credible sets and colocalization results

[scripts/load_pseudo.sh](scripts/load_pseudo.sh) loads meta-analysis pseudo credible sets (FinnGen+UKBB/MVP, plus external COVID-19 HGI, PGC and GP2 results)

[scripts/load_genebass_variants.sh](scripts/load_genebass_variants.sh) loads GeneBASS exome variant results (truncates `exome_variant_results`)

[scripts/load_genebass_gene.sh](scripts/load_genebass_gene.sh) loads GeneBASS gene burden results, unfiltered, from the ~4.5k per-trait files (truncates `gene_burden_results`)

[scripts/load_exome_variants_extra.sh](scripts/load_exome_variants_extra.sh) appends additional exome variant results (IBD)

[scripts/load_gene_burden_extra.sh](scripts/load_gene_burden_extra.sh) appends additional gene burden results, unfiltered (BipEx, IBD, SCHEMA2)

[scripts/load_asm_qtl.sh](scripts/load_asm_qtl.sh) loads ASM-QTL (allele-specific methylation) results

[scripts/load_peak_to_gene.sh](scripts/load_peak_to_gene.sh) loads Open4Gene peak-to-gene links (truncates `peak_to_gene`), which join peak-keyed caQTL credible sets to genes

[scripts/load_phenotypes.sh](scripts/load_phenotypes.sh) builds and loads the `phenotypes` and `datasets` metadata tables from `datasets.yaml` and the metadata files it references; re-run after any registry change

[scripts/load_open_chromatin.sh](scripts/load_open_chromatin.sh) loads the open-chromatin atlas (6 datasets)

[scripts/load_variant_effect.sh](scripts/load_variant_effect.sh) loads in-silico predicted variant effects on chromatin accessibility (ChromBPNet, FLARE)

[scripts/load_mpra.sh](scripts/load_mpra.sh) loads measured MPRA allelic activity (truncates `mpra`)

[scripts/load_hla.sh](scripts/load_hla.sh) loads FinnGen R14 classical HLA allele associations (truncates `hla_associations`)

[scripts/load_variant_annotation.sh](scripts/load_variant_annotation.sh) loads FinnGen R14 per-variant functional annotations (truncates `variant_annotation`)

[scripts/load_gene_annotations.sh](scripts/load_gene_annotations.sh) builds and loads the HGNC/GENCODE gene reference table (truncates `gene_annotations`)

[scripts/load_dosage_sensitivity.sh](scripts/load_dosage_sensitivity.sh) loads the Collins et al. 2022 gene dosage-sensitivity scores (truncates `dosage_sensitivity`)

## Server setup

Requires [uv](https://docs.astral.sh/uv/):

```bash
uv venv
uv pip install -r pyproject.toml
```

To run `tests/` as well, install the dev extra and run pytest from the repo root. The extra is
pytest, httpx (which starlette's `TestClient` needs) and **pytest-randomly**, which shuffles the
test order on every run and prints the seed — so two runs of the same tree legitimately execute
in different orders. Reproduce a run with `-p randomly --randomly-seed=<seed>`, or pin a fixed
order with `-p no:randomly` when bisecting:

```bash
uv pip install -r pyproject.toml --extra dev
pytest tests/
```

**Not `uv pip install -e '.[dev]'`.** That command cannot succeed here and never has:
`pyproject.toml` declares no `[build-system]`, so the build falls back to setuptools,
which refuses a flat layout carrying more than one top-level directory (`api`,
`schemas`, and `configs` once `sync-datasets.sh` has run). Nothing is lost by not
installing the project — `api` is reached through `sys.path` rather than as an installed
package, as described below — but the dev extra has to be requested against the
requirements file instead, or `ruff` and `pytest` never arrive.

`api` is a **namespace package** reached through `sys.path`, not an installed one, and
namespace packages merge every matching directory on `sys.path`. A `PYTHONPATH` pointing at
another checkout of this repo would therefore add that tree's `api/` to `api.__path__` and
let tests import source from it. `tests/conftest.py` aborts the run in `pytest_configure`
when any `api.__path__` entry falls outside the pytest rootdir
(genetics-results-suite-6o3); it is silent otherwise.

## Linting

```bash
ruff check                      # the whole repo
scripts/lint-staged.sh          # only what is staged — what the pre-commit hook runs
scripts/lint-staged.sh --all    # the whole repo, via the same resolution logic
```

Run `scripts/install-git-hooks.sh` once per clone. It wires `core.hooksPath`, which no
clone carries, so that `pre-commit` runs both `scripts/check-doc-drift.sh` (warns) and
`scripts/lint-staged.sh` (**blocks the commit** on a finding). `core.hooksPath` is shared
across worktrees, so that one run covers every worktree too.

The gate looks for ruff in this checkout's `.venv`, then the **main checkout's** (a
worktree has none of its own), then `PATH`, then `uvx` — and fails the commit if it finds
none, rather than passing it unchecked. `git commit --no-verify` is the deliberate bypass.

## Run the REST API server

Requires Google Cloud credentials configured.

```bash
PROJECT_ID=my-google-project DATASET_ID=genetics_results PORT=8080 python api/main.py
```

### Point local development at the dev dataset

`DATASET_ID` defaults to `genetics_results`, which is **production**. Starting the API
without setting it makes every local query — and therefore the whole local chain, since
chat-api and the browser BFF reach BigQuery only through this service — read production
data. Set it explicitly:

```bash
PROJECT_ID=phewas-development DATASET_ID=genetics_dev PORT=8080 python api/main.py
```

`phewas-development:genetics_dev` (`europe-west1`) holds every table and view in
`schemas/` (except the rCNV tables — `dosage_sensitivity` and `rcnv_*` — which are not seeded
there; `bq ls phewas-development:genetics_dev` is the live list) with a small subset of the
data (~3.6M rows / ~612 MB): chromosome 22 only for the
results tables (capped at 500k rows for `gene_burden_results` and `open_chromatin`),
`coloc_credsets` and `credible_sets` seeded from the credible-set IDs the loaded
`colocalization` rows reference so both directions of that pivot resolve, and complete
copies of the small tables — `datasets`, `phenotypes`, `gene_annotations` and
`hla_associations`, the last because it is chromosome 6 by construction. Every view returns
rows, but result *values* are not comparable with production and the dataset is not a
benchmark target.

No other *service setting* selects a dataset, but `genetics-mcp-server` hardcodes
`genetics_results.<view>` in its generated SQL and tool descriptions, so its queries are
rejected 403 by a `genetics_dev`-pointed API rather than following it. `genetics-results-api`
and `genetics-results-browser` name no BigQuery dataset. See `docs/project-spec.md`.

## API endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/schema` | GET | Table schemas with column descriptions |
| `/stats` | GET | Database statistics and row counts |
| `/tables/{name}/sample` | GET | Sample rows from a table |
| `/query` | POST | Execute a SQL query |
| `/docs`, `/redoc`, `/openapi.json` | GET | Interactive API docs and OpenAPI schema |

Only single `SELECT` statements over the exposed views are accepted: every query is
dry-run first, and anything else (DDL, DML, scripts, or a `SELECT` touching other
tables) is rejected.

Every endpoint except `/health` requires `Authorization: Bearer $INTERNAL_API_SECRET`
when `INTERNAL_API_SECRET` is set. It is unset by default, which disables
authentication and logs a warning at startup — convenient locally, so the examples
below send no header.

## Example query

```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT gene_most_severe, chr, pos, pip FROM credible_sets WHERE pip > 0.9 ORDER BY pip DESC LIMIT 10"
  }'
```

Dry run (estimate bytes processed without executing):

```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM credible_sets WHERE chr = 1", "dry_run": true}'
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `PROJECT_ID` | google-project-id | GCP project ID (the default is a placeholder, set it) |
| `DATASET_ID` | genetics_results | BigQuery dataset name |
| `MAX_ROWS` | 100000 | Max rows returned per query |
| `MAX_BYTES_BILLED` | 107374182400 | Max bytes billed per query |
| `PORT` | 8080 | Server port |
| `CORS_ORIGINS` | http://localhost:3000,http://127.0.0.1:3000 | Comma-separated browser origins allowed by CORS |
| `DATASETS_CONFIG_PATH` | ./configs/datasets.yaml | Shared dataset/table metadata the API serves in `/schema` |
| `INTERNAL_API_SECRET` | (unset) | Shared secret required as `Authorization: Bearer` on every endpoint except `/health` |

## Tables

Queries go through a view (`<table>_v`) per table, which adds derived columns such as
`variant`, `maf` and `resource`; bare base table names in a query are redirected to the view.

- **credible_sets** — fine-mapped credible set variants (FinnGen, Open Targets, eQTL Catalogue)
- **colocalization** — colocalization analysis results between datasets
- **coloc_credsets** — variants in colocalized credible sets
- **exome_variant_results** — exome variant associations (Genebass, IBD exome)
- **gene_burden_results** — gene burden test results, unfiltered (Genebass, BipEx2, IBD exome, SCHEMA2)
- **asm_qtl** — allele-specific methylation QTL results (deCODE)
- **gene_annotations** — whole-universe gene reference table (HGNC + GENCODE, gene-group lineage)
- **open_chromatin** — atlas of accessible/active chromatin regions by cell type/tissue/condition
- **variant_effect** — in-silico predicted variant effects on chromatin accessibility (ChromBPNet, FLARE)
- **mpra** — measured cis-regulatory allelic activity from a reporter assay (Siraj et al.)
- **variant_annotation** — FinnGen R14 per-variant functional annotations and allele frequencies
- **peak_to_gene** — Open4Gene peak-to-gene links, joining peak-keyed caQTL results to genes
- **hla_associations** — classical HLA allele associations (FinnGen R14; keyed by allele, not by variant)
- **dosage_sensitivity** — gene-level pHaplo/pTriplo dosage-sensitivity scores (Collins et al. 2022 rare-CNV map)
- **rcnv_gene_associations** — per-phenotype rare-CNV DEL/DUP gene association statistics (Collins et al. 2022; 54 HPO groups x 2 CNV types x 17,263 genes)
- **rcnv_segments** — the 163 disease-associated rare-CNV segments of the same study (Collins et al. 2022 Table S3), with GRCh38 and GRCh37 coordinates
- **rcnv_window_associations** — the same study's genome-wide sliding-window DEL/DUP association statistics, lifted to GRCh38 with the published GRCh37 interval kept (108 (phenotype, cnv_type) groups, 17,114-257,726 rows each, over 259,795 windows)
- **phenotypes** — trait metadata behind the results tables' phenotype codes, keyed by `(dataset, trait_original)`
- **datasets** — dataset registry: what each results-view `dataset` value is, its resource, version and credible-set caveats

## License

MIT

