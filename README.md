# Genetics Results Database

BigQuery database for genetics fine-mapping, colocalization, and exome sequencing results with a REST API.

This is deployed as part of FinnGenie AI assistant (see [https://github.com/fulltiltgenomics/genetics-results-suite](https://github.com/fulltiltgenomics/genetics-results-suite)).

Cannot yet be used without access to restricted data.

## Loading data to BigQuery

[scripts/setup_bigquery.sh](scripts/setup_bigquery.sh) creates the BigQuery dataset and tables

[scripts/load_all_data.sh](scripts/load_all_data.sh) loads in credible sets and colocalization results

[scripts/load_exome_data.sh](scripts/load_exome_data.sh) loads in exome sequencing results

## Server setup

```bash
pip install -r api/requirements.txt
```

## Run the REST API server

Requires Google Cloud credentials configured.

```bash
python api/main.py
# overriding vars
PROJECT_ID=my-project DATASET_ID=genetics_results PORT=8080 python api/main.py
```

## API endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/schema` | GET | Table schemas with column descriptions |
| `/stats` | GET | Database statistics and row counts |
| `/tables/{name}/sample` | GET | Sample rows from a table |
| `/query` | POST | Execute a SQL query |

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
| `PROJECT_ID` | phewas-development | GCP project ID |
| `DATASET_ID` | genetics_results | BigQuery dataset name |
| `MAX_ROWS` | 10000 | Max rows returned per query |
| `MAX_BYTES_BILLED` | 107374182400 | Max bytes billed per query |
| `PORT` | 8080 | Server port |

## Tables

- **credible_sets** — fine-mapped credible set variants (FinnGen, Open Targets, eQTL Catalogue)
- **colocalization** — colocalization analysis results between datasets
- **coloc_credsets** — variants in colocalized credible sets
- **exome_variant_results** — exome variant associations (Genebass)
- **gene_burden_results** — gene burden test results (Genebass)
