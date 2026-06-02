"""
Genetics Results API - BigQuery query service for AI agents.
Provides SQL query interface to genetics fine-mapping and colocalization data.
"""

import json
import os
import logging
import sys
import time
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
        msg = record.msg
        if isinstance(msg, dict):
            log_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "severity": self.SEVERITY_MAP.get(record.levelno, "DEFAULT"),
                "logger": record.name,
                **msg,
            }
        else:
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
for _name in ("uvicorn.access", "google", "urllib3", "asyncio"):
    logging.getLogger(_name).setLevel(logging.WARNING)

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
VIEWS = ["credible_sets_v", "colocalization_v", "coloc_credsets_v", "exome_variant_results_v", "gene_burden_results_v", "gene_annotations_v"]
# map base table names to views for backwards-compatible query auto-qualification
_BASE_TABLES = {name.removesuffix("_v"): name for name in VIEWS}

# load all metadata from shared datasets.yaml (single source of truth)
try:
    from api.yaml_loader import load_all as _load_yaml_config
except ImportError:
    from yaml_loader import load_all as _load_yaml_config

_yaml_config = _load_yaml_config()
if _yaml_config is None:
    raise RuntimeError(
        "datasets.yaml could not be loaded. Set DATASETS_CONFIG_PATH or "
        "run scripts/sync-datasets.sh to create configs/datasets.yaml"
    )

_RESOURCE_METADATA: dict[str, dict[str, Any]] = _yaml_config["resource_metadata"]
_COLLECTION_RESOURCE_PREFIXES: dict[str, dict[str, str]] = _yaml_config["collection_resource_prefixes"]
_TABLE_DESCRIPTIONS: dict[str, str] = _yaml_config["table_descriptions"]
_COLUMN_DESCRIPTIONS: dict[str, dict[str, str]] = _yaml_config["column_descriptions"]
_TABLE_EXAMPLES: dict[str, list[dict[str, str]]] = _yaml_config["table_examples"]
_CATEGORICAL_COLUMNS: dict[str, dict[str, str | None]] = _yaml_config["categorical_columns"]
logger.info("Loaded dataset config from YAML (%d resources, %d tables)",
            len(_RESOURCE_METADATA), len(_TABLE_DESCRIPTIONS))

_VALUES_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_VALUES_CACHE_TTL_SECONDS = 3600

# View-only derived columns aren't in any base table, and BigQuery reports all
# view columns as NULLABLE. These are deterministic non-null transforms of
# REQUIRED base columns, so declare their true mode here (anything not listed
# falls back to NULLABLE): `variant` = CONCAT of REQUIRED chr/pos/ref/alt;
# `resource*` = CASE over REQUIRED dataset with a non-null ELSE. `maf` is
# intentionally absent — LEAST(aaf, 1-aaf) is NULL when the nullable aaf is.
_DERIVED_COLUMN_MODES = {
    "variant": "REQUIRED",
    "resource": "REQUIRED",
    "resource1": "REQUIRED",
    "resource2": "REQUIRED",
}


def _get_categorical_values(view_name: str) -> dict[str, Any]:
    """Return distinct values for a view's categorical columns.

    Result keys are either the column name (flat list of allowed values) or
    `<col>_by_<dep>` (mapping from dependency value to allowed values). Cached
    in-process for `_VALUES_CACHE_TTL_SECONDS` to keep `/schema` cheap.
    """
    config = _CATEGORICAL_COLUMNS.get(view_name)
    if not config:
        return {}

    cached = _VALUES_CACHE.get(view_name)
    now = time.time()
    if cached and now - cached[0] < _VALUES_CACHE_TTL_SECONDS:
        return cached[1]

    flat_cols = [col for col, dep in config.items() if dep is None]
    dep_cols = [(col, dep) for col, dep in config.items() if dep is not None]

    result: dict[str, Any] = {}
    fq = f"`{PROJECT_ID}.{DATASET_ID}.{view_name}`"

    if flat_cols:
        agg = ", ".join(f"ARRAY_AGG(DISTINCT {c} IGNORE NULLS) AS {c}" for c in flat_cols)
        sql = f"SELECT {agg} FROM {fq}"
        try:
            row = next(iter(bq_client.query(sql).result()))
            for c in flat_cols:
                result[c] = sorted(row[c] or [])
        except Exception as e:
            logger.warning(f"Distinct value fetch failed for {view_name}: {e}")

    deps_grouped: dict[str, list[str]] = {}
    for col, dep in dep_cols:
        deps_grouped.setdefault(dep, []).append(col)

    for dep, cols in deps_grouped.items():
        agg = ", ".join(f"ARRAY_AGG(DISTINCT {c} IGNORE NULLS) AS {c}" for c in cols)
        sql = f"SELECT {dep}, {agg} FROM {fq} WHERE {dep} IS NOT NULL GROUP BY {dep}"
        try:
            for row in bq_client.query(sql).result():
                key = row[dep]
                for c in cols:
                    result.setdefault(f"{c}_by_{dep}", {})[key] = sorted(row[c] or [])
        except Exception as e:
            logger.warning(f"Grouped distinct value fetch failed for {view_name}.{dep}: {e}")

    if result:
        _VALUES_CACHE[view_name] = (now, result)
    return result


def _compact_categorical_values(cat_values: dict[str, Any]) -> dict[str, Any]:
    """Collapse collection resources (e.g. qtdNNNNNN) into summaries.

    Keeps named resources inline and replaces hundreds of collection IDs
    with a compact description, reducing schema size dramatically.
    """
    result: dict[str, Any] = {}
    for key, values in cat_values.items():
        if isinstance(values, list):
            # flat allowed_values list — separate named vs collection
            named = []
            collections: dict[str, int] = {}
            for v in values:
                matched = False
                for prefix in _COLLECTION_RESOURCE_PREFIXES:
                    if v.lower().startswith(prefix):
                        collections[prefix] = collections.get(prefix, 0) + 1
                        matched = True
                        break
                if not matched:
                    named.append(v)
            if collections:
                result[key] = named
                for prefix, count in collections.items():
                    meta = _COLLECTION_RESOURCE_PREFIXES[prefix]
                    result.setdefault("collection_resources", {})[meta["label"]] = {
                        "count": count,
                        "pattern": f"{prefix}NNNNNN (e.g. {prefix}000001)",
                        "description": meta["description"],
                        "data_types": meta.get("data_types", ""),
                    }
            else:
                result[key] = values
        elif isinstance(values, dict):
            # grouped allowed_values_by_X — keep only named resources
            compacted = {}
            collection_types: set[str] = set()
            for group_key, group_vals in values.items():
                is_collection = any(
                    group_key.lower().startswith(prefix)
                    for prefix in _COLLECTION_RESOURCE_PREFIXES
                )
                if is_collection:
                    for v in group_vals:
                        collection_types.add(v)
                else:
                    compacted[group_key] = group_vals
            if collection_types:
                compacted["_eqtl_catalogue_resources"] = (
                    f"All eQTL Catalogue (qtdNNNNNN) resources have data_types: {sorted(collection_types)}"
                )
            result[key] = compacted
        else:
            result[key] = values
    return result


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
    columns: list[dict[str, Any]]
    examples: list[dict[str, str]] = []


class SchemaWarning(BaseModel):
    """Per-view error encountered while building the schema response."""

    view: str
    error: str


class SchemaResponse(BaseModel):
    """Database schema response."""

    resources: dict[str, Any] = {}
    tables: list[TableInfo]
    warnings: list[SchemaWarning] = []


def _estimate_bq_cost(bytes_processed: int) -> float:
    """Estimate BigQuery on-demand query cost (USD). $6.25 per TiB."""
    return round((bytes_processed / (1024**4)) * 6.25, 6)


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
async def get_schema(table: str | None = None):
    """Get database schema information. Optionally filter to a single table."""
    start_time = time.perf_counter()
    if table and table not in VIEWS:
        resolved = _BASE_TABLES.get(table)
        if resolved:
            table = resolved
        else:
            raise HTTPException(status_code=404, detail=f"Unknown table: {table}. Available: {VIEWS}")

    views_to_fetch = [table] if table else VIEWS
    tables = []
    warnings: list[dict[str, str]] = []

    for table_name in views_to_fetch:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        try:
            table_meta = bq_client.get_table(table_ref)
            overrides = _COLUMN_DESCRIPTIONS.get(table_name, {})
            raw_cat_values = _get_categorical_values(table_name)
            cat_values = _compact_categorical_values(raw_cat_values)

            # get row count and column modes from the base table: views report
            # 0 rows and always-NULLABLE modes, so read the underlying table for
            # both. View-only derived columns (resource, variant, maf) aren't in
            # the base table and fall back to the view's mode.
            row_count = 0
            base_modes: dict[str, str] = {}
            try:
                base_table = table_name.removesuffix("_v")
                base_ref = f"{PROJECT_ID}.{DATASET_ID}.{base_table}"
                base_meta = bq_client.get_table(base_ref)
                row_count = base_meta.num_rows or 0
                base_modes = {f.name: f.mode for f in base_meta.schema}
            except Exception:
                pass

            # build column list with collection_resources pulled out to table level
            collection_resources = cat_values.pop("collection_resources", None)
            columns: list[dict[str, Any]] = []
            for field in table_meta.schema:
                col: dict[str, Any] = {
                    "name": field.name,
                    "type": field.field_type,
                    "mode": base_modes.get(field.name) or _DERIVED_COLUMN_MODES.get(field.name, field.mode),
                    "description": overrides.get(field.name, field.description or ""),
                }
                if field.name in cat_values:
                    col["allowed_values"] = cat_values[field.name]
                grouped_key = next(
                    (k for k in cat_values if k.startswith(f"{field.name}_by_")), None
                )
                if grouped_key:
                    col[grouped_key.replace(field.name, "allowed_values", 1)] = cat_values[
                        grouped_key
                    ]
                columns.append(col)

            table_info = TableInfo(
                name=table_name,
                description=_TABLE_DESCRIPTIONS.get(table_name, table_meta.description or ""),
                row_count=row_count,
                columns=columns,
                examples=_TABLE_EXAMPLES.get(table_name, []),
            )
            # attach collection_resources as extra field on the dict
            table_dict = table_info.model_dump()
            if collection_resources:
                table_dict["collection_resources"] = collection_resources
            tables.append(table_dict)
        except HTTPException:
            raise
        except Exception as e:
            logger.warning(f"Could not get schema for {table_name}: {e}")
            warnings.append({"view": table_name, "error": f"{type(e).__name__}: {e}"})

    logger.info({
        "message": "schema",
        "log_type": "endpoint_access",
        "table": table or "all",
        "tables_returned": len(tables),
        "warnings": len(warnings),
        "duration_ms": round((time.perf_counter() - start_time) * 1000, 2),
    })

    # surface total upstream failure rather than silently returning an empty list
    if views_to_fetch and not tables:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "schema unavailable: no views could be loaded from BigQuery",
                "warnings": warnings,
            },
        )

    return {"resources": _RESOURCE_METADATA, "tables": tables, "warnings": warnings}


@app.post("/query", response_model=QueryResponse)
async def execute_query(request: QueryRequest):
    """Execute a SQL query against the genetics database."""
    start_time = time.perf_counter()
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
            bytes_processed = query_job.total_bytes_processed
            logger.info({
                "message": "query",
                "log_type": "endpoint_access",
                "sql": request.sql,
                "dry_run": True,
                "total_rows": 0,
                "bytes_processed": bytes_processed,
                "estimated_cost_usd": _estimate_bq_cost(bytes_processed),
                "duration_ms": round((time.perf_counter() - start_time) * 1000, 2),
            })
            return QueryResponse(
                columns=[],
                rows=[],
                total_rows=0,
                bytes_processed=bytes_processed,
                truncated=False,
            )

        results = query_job.result()
        rows = []
        columns = [field.name for field in results.schema]

        for i, row in enumerate(results):
            if i >= request.max_rows:
                break
            rows.append([_serialize_value(v) for v in row.values()])

        bytes_processed = query_job.total_bytes_processed
        total_rows = results.total_rows
        logger.info({
            "message": "query",
            "log_type": "endpoint_access",
            "sql": request.sql,
            "dry_run": False,
            "total_rows": total_rows,
            "rows_returned": len(rows),
            "bytes_processed": bytes_processed,
            "estimated_cost_usd": _estimate_bq_cost(bytes_processed),
            "duration_ms": round((time.perf_counter() - start_time) * 1000, 2),
        })
        return QueryResponse(
            columns=columns,
            rows=rows,
            total_rows=total_rows,
            bytes_processed=bytes_processed,
            truncated=total_rows > request.max_rows,
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
    start_time = time.perf_counter()
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

    logger.info({
        "message": "sample",
        "log_type": "endpoint_access",
        "table": resolved,
        "rows_returned": len(rows),
        "duration_ms": round((time.perf_counter() - start_time) * 1000, 2),
    })
    return {"columns": columns, "rows": rows}


@app.get("/stats")
async def get_stats():
    """Get summary statistics for the database."""
    start_time = time.perf_counter()
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

    logger.info({
        "message": "stats",
        "log_type": "endpoint_access",
        "duration_ms": round((time.perf_counter() - start_time) * 1000, 2),
    })
    return stats


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")), log_config=None)
