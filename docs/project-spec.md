# genetics-results-db - Project specification

## Introduction

genetics-results-db is a BigQuery-based database solution for storing and querying genetics fine-mapping, colocalization, and exome sequencing results. It provides a REST API for AI agents and applications to query credible set variants, colocalization analysis results, exome variant associations, gene burden tests, and related genetics data across multiple datasets including FinnGen, Open Targets, eQTL Catalogue, and GeneBASS.

## Purpose and Goals

- Provide a scalable, serverless database for hundreds of millions of rows of genetics results data
- Enable SQL-based querying of fine-mapped credible sets and colocalization results
- Support AI agent workflows with a simple REST API
- Minimize operational overhead and cost through BigQuery's pay-per-query model
- Keep infrastructure simple and reproducible with shell scripts and standard GCP tooling

## Key Features

- BigQuery tables with partitioning by chromosome and clustering by dataset/data_type for typical queries
- REST API (FastAPI) with human/agent usable endpoints for SQL queries, schema discovery, and statistics
- Query sanitization to prevent write operations (though read-only access is recommended in any case)
- Cost controls via configurable bytes-billed limits and dry-run support
- Direct loading of tsv.gz files from GCS with schema validation
- Auto-qualification of table names in queries for simpler SQL

## Architecture

```
GCS (tsv.gz files)
      ↓ (one-time load via bq load)
BigQuery Dataset
  ├── credible_sets (partitioned by chr, clustered by dataset, data_type, most_severe)
  ├── colocalization (partitioned by chr, clustered by dataset pairs)
  ├── coloc_credsets (partitioned by chr, clustered by dataset, data_type)
  ├── exome_variant_results (partitioned by chr, clustered by dataset, gene, trait)
  └── gene_burden_results (partitioned by chr, clustered by dataset, gene, trait)
      ↓
API (FastAPI)
      ↓
AI Agents / Applications
```

## Data Model

### credible_sets

Fine-mapped credible set variants from multiple genetics datasets.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| dataset | STRING | Yes | Source dataset (FinnGen_R13, Open_Targets_25.12, etc.) |
| data_type | STRING | Yes | GWAS, eQTL, pQTL, sQTL, caQTL |
| trait | STRING | Yes | Phenotype/trait name |
| trait_original | STRING | No | Original trait name |
| cell_type | STRING | No | Cell/tissue type (null for GWAS) |
| chr | INT64 | Yes | Chromosome |
| pos | INT64 | Yes | Position |
| ref | STRING | Yes | Reference allele |
| alt | STRING | Yes | Alternate allele |
| mlog10p | FLOAT64 | No | -log10(p-value) |
| beta | FLOAT64 | No | Effect size |
| se | FLOAT64 | No | Standard error |
| pip | FLOAT64 | No | Posterior inclusion probability |
| cs_id | STRING | No | Credible set ID |
| cs_size | INT64 | No | Credible set size |
| cs_min_r2 | FLOAT64 | No | Minimum R² between variants in credible set |
| aaf | FLOAT64 | No | Alternate allele frequency |
| most_severe | STRING | No | Most severe variant consequence |
| gene_most_severe | STRING | No | Gene with most severe consequence |

### colocalization

Colocalization analysis results between associations from different datasets.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| dataset1 | STRING | Yes | First dataset name |
| dataset2 | STRING | Yes | Second dataset name |
| data_type1 | STRING | Yes | First dataset type |
| data_type2 | STRING | Yes | Second dataset type |
| trait1 | STRING | Yes | First trait name |
| trait1_original | STRING | No | First original trait name |
| trait2 | STRING | Yes | Second trait name |
| trait2_original | STRING | No | Second original trait name |
| cell_type1 | STRING | No | First cell/tissue type |
| cell_type2 | STRING | No | Second cell/tissue type |
| cs1_id | STRING | Yes | First credible set ID |
| cs2_id | STRING | Yes | Second credible set ID |
| hit1 | STRING | No | Lead variant in first credible set |
| hit2 | STRING | No | Lead variant in second credible set |
| hit1_beta | FLOAT64 | No | Effect size of lead variant in first set |
| hit1_mlog10p | FLOAT64 | No | -log10(p-value) of lead variant in first set |
| hit2_beta | FLOAT64 | No | Effect size of lead variant in second set |
| hit2_mlog10p | FLOAT64 | No | -log10(p-value) of lead variant in second set |
| chr | INT64 | Yes | Chromosome |
| region_start_min | INT64 | No | Region start position |
| region_end_max | INT64 | No | Region end position |
| PP_H0_abf | FLOAT64 | No | Posterior probability H0: no association in either |
| PP_H1_abf | FLOAT64 | No | Posterior probability H1: association in dataset 1 only |
| PP_H2_abf | FLOAT64 | No | Posterior probability H2: association in dataset 2 only |
| PP_H3_abf | FLOAT64 | No | Posterior probability H3: both associated, different variants |
| PP_H4_abf | FLOAT64 | No | Posterior probability H4: both associated, shared variant |
| nsnps | INT64 | No | Number of SNPs in region |
| nsnps1 | INT64 | No | Number of SNPs in first trait region |
| nsnps2 | INT64 | No | Number of SNPs in second trait region |
| cs1_log10bf | FLOAT64 | No | Log10 Bayes factor for first credible set |
| cs2_log10bf | FLOAT64 | No | Log10 Bayes factor for second credible set |
| clpp | FLOAT64 | No | Causal posterior probability |
| clpa | FLOAT64 | No | Causal posterior agreement |
| cs1_size | INT64 | No | First credible set size |
| cs2_size | INT64 | No | Second credible set size |
| cs_overlap | INT64 | No | Number of overlapping variants in the credible sets |
| topInOverlap | STRING | No | Whether the top variant is in the credible set overlap |

### coloc_credsets

Variants belonging to colocalized credible sets.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| dataset | STRING | Yes | Source dataset |
| data_type | STRING | Yes | Data type |
| trait | STRING | Yes | Trait name |
| trait_original | STRING | No | Original trait name |
| cell_type | STRING | No | Cell/tissue type |
| chr | INT64 | Yes | Chromosome |
| pos | INT64 | Yes | Position |
| ref | STRING | Yes | Reference allele |
| alt | STRING | Yes | Alternate allele |
| mlog10p | FLOAT64 | No | -log10(p-value) |
| beta | FLOAT64 | No | Effect size |
| se | FLOAT64 | No | Standard error |
| pip | FLOAT64 | No | Posterior inclusion probability |
| cs_id | STRING | No | Credible set ID |

### exome_variant_results

Variant-level association results from UKBB exome sequencing (GeneBASS). Filtered for -log10(p-value) >= e.g. 4.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| dataset | STRING | Yes | Source dataset (genebass) |
| chr | INT64 | Yes | Chromosome |
| pos | INT64 | Yes | Position |
| ref | STRING | Yes | Reference allele |
| alt | STRING | Yes | Alternate allele |
| gene | STRING | Yes | Gene symbol |
| annotation | STRING | Yes | Variant annotation (LC, synonymous, missense, pLoF, etc.) |
| mlog10p | FLOAT64 | No | -log10(p-value) |
| beta | FLOAT64 | No | Effect size |
| se | FLOAT64 | No | Standard error |
| af_overall | FLOAT64 | No | Allele frequency overall |
| af_cases | FLOAT64 | No | Allele frequency in cases |
| af_controls | FLOAT64 | No | Allele frequency in controls |
| ac | INT64 | No | Allele count |
| an | INT64 | No | Allele number |
| heritability | FLOAT64 | No | Heritability estimate |
| trait | STRING | Yes | Trait identifier |

### gene_burden_results

Gene-level burden test results from UKBB exome sequencing (GeneBASS). Includes burden, SKAT, and SKAT-O tests. Filtered for -log10(p-value of any test) >= e.g. 4.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| dataset | STRING | Yes | Source dataset (genebass) |
| trait | STRING | Yes | Trait identifier |
| gene | STRING | Yes | Gene symbol |
| gene_id | STRING | Yes | Ensembl gene ID |
| chr | INT64 | Yes | Chromosome |
| gene_start_pos | INT64 | Yes | Gene start position |
| gene_end_pos | INT64 | Yes | Gene end position |
| annotation | STRING | Yes | Annotation category (pLoF, missense, etc.) |
| mlog10p_burden | FLOAT64 | No | -log10(p-value) for burden test |
| mlog10p_skat | FLOAT64 | No | -log10(p-value) for SKAT test |
| mlog10p_skato | FLOAT64 | No | -log10(p-value) for SKAT-O test |
| beta | FLOAT64 | No | Effect size |
| se | FLOAT64 | No | Standard error |
| total_variants | INT64 | No | Number of variants in gene |
| total_variants_pheno | INT64 | No | Number of variants in gene for this trait |
| n_cases | INT64 | No | Number of cases, or number of samples for quantitative traits |
| n_controls | INT64 | No | Number of controls (NULL for quantitative traits) |
| description | STRING | No | Trait description |
| coding_description | STRING | No | Additional trait coding description |
| category | STRING | No | Trait category |

## Technical Implementation

### BigQuery Configuration

- **Partitioning**: All tables partitioned by chromosome using `RANGE_BUCKET(chr, GENERATE_ARRAY(1, 23, 1))`
- **Clustering**: Tables clustered by frequently filtered columns (dataset, data_type, most_severe)

### API Service

- **Framework**: FastAPI with uvicorn

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/schema` | GET | Get table schemas with column descriptions |
| `/stats` | GET | Get database statistics and row counts |
| `/tables/{name}/sample` | GET | Get sample rows from a table |
| `/query` | POST | Execute SQL query |

### Query Endpoint Parameters

```json
{
  "sql": "SELECT * FROM credible_sets LIMIT 10",
  "max_rows": 1000,
  "dry_run": false
}
```

- `sql` (required): SQL query to execute
- `max_rows` (default 1000, max 10000): Maximum rows to return
- `dry_run` (default false): Estimate query cost without executing

### Query Response Format

```json
{
  "columns": ["col1", "col2"],
  "rows": [["val1", "val2"], ...],
  "total_rows": 100,
  "bytes_processed": 1048576,
  "truncated": false
}
```

### Security

- Write operations blocked (INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE, MERGE)
- Maximum bytes billed limit
- Table names auto-qualified to prevent injection
- IAM-level read-only enforcement on the API service account (see IAM Roles below)

### IAM Roles

All code uses Application Default Credentials (ADC), so role separation is achieved by which service account or user identity runs each component — no code changes needed.

**API (read-only):**
- `roles/bigquery.dataViewer` — read table data
- `roles/bigquery.jobUser` — execute queries

**Data loading and setup (write):**
- `roles/bigquery.dataEditor` — create/write/delete tables and data
- `roles/bigquery.jobUser` — execute load jobs and queries
- `roles/storage.objectViewer` — read source files from GCS

## Configuration

Configuration via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| PROJECT_ID | (from gcloud) | GCP project ID |
| DATASET_ID | genetics_results | BigQuery dataset name |
| LOCATION | europe-west1 | BigQuery dataset location |
| MAX_ROWS | 10000 | Maximum rows returned per query |
| MAX_BYTES_BILLED | 107374182400 | Maximum bytes billed per query (100 GB) |
| PORT | 8080 | API server port |

## Project Structure

```
genetics-results-db/
├── schemas/
│   ├── credible_sets.sql      # BigQuery table definition
│   ├── credible_sets_v.sql    # View with variant and resource columns
│   ├── colocalization.sql     # BigQuery table definition
│   ├── colocalization_v.sql   # View with resource columns
│   ├── coloc_credsets.sql     # BigQuery table definition
│   ├── coloc_credsets_v.sql   # View with variant and resource columns
│   ├── exome_variant_results.sql      # GeneBASS variant results table
│   ├── exome_variant_results_v.sql    # View with variant and resource columns
│   ├── gene_burden_results.sql        # GeneBASS gene burden results table
│   └── gene_burden_results_v.sql      # View with resource column
├── scripts/
│   ├── setup_bigquery.sh      # Create dataset and tables
│   ├── load_data.py           # Python loader for tsv.gz files
│   ├── load_all_data.sh       # Batch load credible sets and colocalization data
│   ├── load_exome_data.sh     # Batch load exome results data
│   └── deploy.sh              # Deploy API to Cloud Run
├── api/
│   ├── main.py                # FastAPI application
│   └── requirements.txt       # Python dependencies
├── docs/
│   └── project-spec.md        # This document
├── Dockerfile                 # Container image definition
├── cloudbuild.yaml            # CI/CD configuration
├── README.md                  # Usage documentation
└── .gitignore
```

## Deployment

### Prerequisites

- Google Cloud SDK (`gcloud`) configured
- BigQuery API enabled

### Setup Steps

1. **Create BigQuery dataset and tables**:
   ```bash
   export PROJECT_ID=your-project-id
   ./scripts/setup_bigquery.sh
   ```

2. **Load credible sets and colocalization data from GCS**:
   ```bash
   ./scripts/load_all_data.sh
   ```

3. **Load exome results data from GCS**:
   ```bash
   ./scripts/load_exome_data.sh
   ```

4. **Deploy API to Cloud Run** (optional):
   ```bash
   ./scripts/deploy.sh
   ```

## Example Queries

### Genes with multiple high-confidence coding variants
```sql
SELECT gene_most_severe, COUNT(*) as n
FROM credible_sets
WHERE pip > 0.5
  AND most_severe IN ('missense_variant', 'frameshift_variant', 'stop_gained')
GROUP BY gene_most_severe
HAVING COUNT(*) > 2
ORDER BY n DESC
```

### Strong colocalizations (H4 > 0.9)
```sql
SELECT * FROM colocalization WHERE h4 > 0.9 LIMIT 100
```

### Exome variants in a gene
```sql
SELECT chr, pos, ref, alt, annotation, trait, mlog10p, beta
FROM exome_variant_results
WHERE gene = 'BRCA1'
ORDER BY mlog10p DESC
LIMIT 100
```

### Significant gene burden test results
```sql
SELECT gene, trait, annotation, mlog10p_burden, beta
FROM gene_burden_results
WHERE mlog10p_burden > 5
ORDER BY mlog10p_burden DESC
LIMIT 100
```

## To Be Implemented

1. Authentication/authorization for external access (API keys, OAuth, etc.)
2. Rate limiting for query endpoint
3. Query result caching for repeated queries
4. Possibly additional endpoints for common query patterns (variant lookup, gene lookup)
