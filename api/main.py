"""
Genetics Results API - BigQuery query service for AI agents.
Provides SQL query interface to genetics fine-mapping and colocalization data.
"""

import json
import os
import logging
import sys
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest, Forbidden


class _GCPJsonFormatter(logging.Formatter):
    """JSON formatter compatible with GCP Cloud Logging."""

    SEVERITY_MAP = {
        logging.DEBUG: "DEBUG",
        logging.INFO: "INFO",
        logging.WARNING: "WARNING",
        logging.ERROR: "ERROR",
        logging.CRITICAL: "CRITICAL",
    }

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "severity": self.SEVERITY_MAP.get(record.levelno, "DEFAULT"),
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry)


_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(_GCPJsonFormatter())
logging.root.handlers = [_handler]
logging.root.setLevel(logging.INFO)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Genetics Results API",
    description="Query interface for genetics fine-mapping and colocalization data",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ID = os.environ.get("PROJECT_ID", "google-project-id")
DATASET_ID = os.environ.get("DATASET_ID", "genetics_results")
MAX_ROWS = int(os.environ.get("MAX_ROWS", "100000"))
MAX_BYTES_BILLED = int(os.environ.get("MAX_BYTES_BILLED", str(100 * 1024**3)))  # 100 GB default

bq_client = bigquery.Client(project=PROJECT_ID)

# expose views (not underlying tables) so AI agents use the enriched schemas
VIEWS = ["credible_sets_v", "colocalization_v", "coloc_credsets_v", "exome_variant_results_v", "gene_burden_results_v"]
# map base table names to views for backwards-compatible query auto-qualification
_BASE_TABLES = {name.removesuffix("_v"): name for name in VIEWS}

# description overrides for computed view columns that BigQuery doesn't describe
_COLUMN_DESCRIPTIONS = {
    "credible_sets_v": {
        "maf": "Minor allele frequency, derived as LEAST(aaf, 1-aaf). Use this column directly for MAF filtering instead of computing from aaf.",
        "variant": "Variant identifier as chr:pos:ref:alt",
        "resource": "Normalized dataset resource name",
    },
}


class QueryRequest(BaseModel):
    """SQL query request."""

    sql: str = Field(..., description="SQL query to execute", min_length=1)
    max_rows: int = Field(default=1000, le=MAX_ROWS, description="Maximum rows to return")
    dry_run: bool = Field(default=False, description="Estimate query cost without executing")


class QueryResponse(BaseModel):
    """Query result response."""

    columns: list[str]
    rows: list[list[Any]]
    total_rows: int
    bytes_processed: int
    truncated: bool


class TableInfo(BaseModel):
    """Table metadata."""

    name: str
    description: str
    row_count: int
    columns: list[dict[str, str]]


class SchemaResponse(BaseModel):
    """Database schema response."""

    tables: list[TableInfo]


def sanitize_query(sql: str) -> str:
    """Basic query sanitization - ensure read-only operations."""
    sql_upper = sql.upper().strip()

    # block write operations
    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE", "MERGE"]
    for keyword in forbidden:
        if keyword in sql_upper.split():
            raise HTTPException(status_code=400, detail=f"Write operations not allowed: {keyword}")

    return sql


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.get("/schema", response_model=SchemaResponse)
async def get_schema():
    """Get database schema information."""
    tables = []

    for table_name in VIEWS:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        try:
            table = bq_client.get_table(table_ref)
            overrides = _COLUMN_DESCRIPTIONS.get(table_name, {})
            columns = [
                {
                    "name": field.name,
                    "type": field.field_type,
                    "mode": field.mode,
                    "description": overrides.get(field.name, field.description or ""),
                }
                for field in table.schema
            ]
            tables.append(
                TableInfo(
                    name=table_name,
                    description=table.description or "",
                    row_count=table.num_rows,
                    columns=columns,
                )
            )
        except Exception as e:
            logger.warning(f"Could not get schema for {table_name}: {e}")

    return SchemaResponse(tables=tables)


@app.post("/query", response_model=QueryResponse)
async def execute_query(request: QueryRequest):
    """Execute a SQL query against the genetics database."""
    sql = sanitize_query(request.sql)

    # auto-qualify table names and redirect base table names to views
    for view in VIEWS:
        fq = f"`{PROJECT_ID}.{DATASET_ID}.{view}`"
        sql = sql.replace(f" {view}", f" {fq}")
        sql = sql.replace(f"FROM {view}", f"FROM {fq}")
        sql = sql.replace(f"JOIN {view}", f"JOIN {fq}")
    for base, view in _BASE_TABLES.items():
        fq = f"`{PROJECT_ID}.{DATASET_ID}.{view}`"
        sql = sql.replace(f" {base}", f" {fq}")
        sql = sql.replace(f"FROM {base}", f"FROM {fq}")
        sql = sql.replace(f"JOIN {base}", f"JOIN {fq}")

    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=MAX_BYTES_BILLED,
        dry_run=request.dry_run,
    )

    try:
        query_job = bq_client.query(sql, job_config=job_config)

        if request.dry_run:
            return QueryResponse(
                columns=[],
                rows=[],
                total_rows=0,
                bytes_processed=query_job.total_bytes_processed,
                truncated=False,
            )

        results = query_job.result()
        rows = []
        columns = [field.name for field in results.schema]

        for i, row in enumerate(results):
            if i >= request.max_rows:
                break
            rows.append([_serialize_value(v) for v in row.values()])

        return QueryResponse(
            columns=columns,
            rows=rows,
            total_rows=results.total_rows,
            bytes_processed=query_job.total_bytes_processed,
            truncated=results.total_rows > request.max_rows,
        )

    except BadRequest as e:
        raise HTTPException(status_code=400, detail=f"Invalid query: {e.message}")
    except Forbidden as e:
        raise HTTPException(status_code=403, detail=f"Query forbidden: {e.message}")
    except Exception as e:
        logger.exception("Query execution failed")
        raise HTTPException(status_code=500, detail=str(e))


def _serialize_value(value: Any) -> Any:
    """Serialize BigQuery values to JSON-compatible types."""
    if value is None:
        return None
    if isinstance(value, (int, float, str, bool)):
        return value
    return str(value)


@app.get("/tables/{table_name}/sample")
async def get_sample(table_name: str, limit: int = 10):
    """Get sample rows from a table."""
    # accept both view names and base table names
    resolved = _BASE_TABLES.get(table_name, table_name)
    if resolved not in VIEWS:
        raise HTTPException(status_code=404, detail=f"Table not found: {table_name}")

    limit = min(limit, 100)
    sql = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{resolved}` LIMIT {limit}"

    query_job = bq_client.query(sql)
    results = query_job.result()

    columns = [field.name for field in results.schema]
    rows = [[_serialize_value(v) for v in row.values()] for row in results]

    return {"columns": columns, "rows": rows}


@app.get("/stats")
async def get_stats():
    """Get summary statistics for the database."""
    stats = {}

    # row counts
    for view in VIEWS:
        try:
            # get row count from underlying table (views don't have num_rows)
            base_table = view.removesuffix("_v")
            table_ref = f"{PROJECT_ID}.{DATASET_ID}.{base_table}"
            table_info = bq_client.get_table(table_ref)
            stats[f"{view}_rows"] = table_info.num_rows
        except Exception:
            stats[f"{view}_rows"] = None

    # credible sets breakdown
    try:
        sql = f"""
        SELECT
            dataset,
            data_type,
            COUNT(*) as count
        FROM `{PROJECT_ID}.{DATASET_ID}.credible_sets_v`
        GROUP BY dataset, data_type
        ORDER BY count DESC
        """
        results = bq_client.query(sql).result()
        stats["credible_sets_by_source"] = [
            {"dataset": row.dataset, "data_type": row.data_type, "count": row.count}
            for row in results
        ]
    except Exception as e:
        logger.warning(f"Could not get credible sets breakdown: {e}")

    return stats


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
