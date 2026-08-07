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

[scripts/load_open_chromatin.sh](scripts/load_open_chromatin.sh) loads the open-chromatin atlas (6 datasets)

[scripts/load_variant_effect.sh](scripts/load_variant_effect.sh) loads in-silico predicted variant effects on chromatin accessibility (ChromBPNet, FLARE)

[scripts/load_mpra.sh](scripts/load_mpra.sh) loads measured MPRA allelic activity (truncates `mpra`)

[scripts/load_hla.sh](scripts/load_hla.sh) loads FinnGen R14 classical HLA allele associations (truncates `hla_associations`)

[scripts/load_variant_annotation.sh](scripts/load_variant_annotation.sh) loads FinnGen R14 per-variant functional annotations (truncates `variant_annotation`)

[scripts/load_gene_annotations.sh](scripts/load_gene_annotations.sh) builds and loads the HGNC/GENCODE gene reference table (truncates `gene_annotations`)

## Server setup

Requires [uv](https://docs.astral.sh/uv/):

```bash
uv venv
uv pip install -r pyproject.toml
```

## Run the REST API server

Requires Google Cloud credentials configured.

```bash
PROJECT_ID=my-google-project DATASET_ID=genetics_results PORT=8080 python api/main.py
```

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

## License

MIT

