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

# human-readable table descriptions for agents
_TABLE_DESCRIPTIONS = {
    "credible_sets_v": (
        "Fine-mapped credible sets with variant-level posterior inclusion probabilities (PIP). "
        "Contains GWAS, eQTL, pQTL, sQTL, caQTL results across multiple resources."
    ),
    "colocalization_v": (
        "Colocalization analysis results (coloc.susie) between pairs of studies. "
        "PP_H4_abf indicates shared causal variant probability. "
    ),
    "coloc_credsets_v": (
        "Individual credible set variants in colocalized credible sets. "
        "Join with colocalization_v on cs1_id/cs2_id for full colocalization context."
    ),
    "exome_variant_results_v": (
        "Single-variant exome association results (e.g. Genebass/UK Biobank exomes)."
    ),
    "gene_burden_results_v": (
        "Gene-level burden test results from exome sequencing studies "
        "(e.g. BipEx2, SCHEMA2, Genebass, IBD exome)."
    ),
}

# column descriptions — overrides BQ field descriptions and fills in missing ones
_COLUMN_DESCRIPTIONS = {
    "credible_sets_v": {
        "dataset": "Specific study/release name within a resource (e.g. FinnGen_R13, BipEx2)",
        "data_type": "Type of association: GWAS, eQTL, pQTL, sQTL, caQTL, or metaboQTL",
        "trait": "Trait/phenotype name",
        "trait_original": "Original trait name in the respective dataset, e.g. phenotype code",
        "cell_type": "Cell type or tissue context (for QTL data)",
        "chr": "Chromosome number",
        "pos": "Genomic position (GRCh38)",
        "ref": "Reference allele",
        "alt": "Alternative (effect) allele",
        "mlog10p": "-log10(p-value). Higher = more significant. 7.30103 = genome-wide significance (p-value 5e-8)",
        "beta": "Effect size estimate (log-OR for binary traits)",
        "se": "Standard error of beta",
        "pip": "Posterior inclusion probability from fine-mapping (0-1). Higher = more likely causal",
        "cs_id": "Credible set identifier — groups variants in the same causal signal",
        "cs_size": "Number of variants in this credible set",
        "cs_min_r2": "Minimum pairwise LD r² within the credible set",
        "aaf": "Alternative allele frequency. Prefer maf column for minor allele frequency",
        "most_severe": "Most severe VEP-predicted variant consequence",
        "gene_most_severe": "Gene symbol associated with the most severe consequence",
        "variant": "Variant identifier as chr:pos:ref:alt, chromosome X is 23",
        "maf": "Minor allele frequency = LEAST(aaf, 1-aaf). Use directly instead of computing from aaf",
        "resource": "Data source identifier (lowercase). Always filter by this column, not dataset",
    },
    "colocalization_v": {
        "dataset1": "Study 1 dataset name",
        "dataset2": "Study 2 dataset name",
        "data_type1": "Study 1 association type (GWAS, eQTL, pQTL, sQTL, caQTL, metaboQTL)",
        "data_type2": "Study 2 association type (GWAS, eQTL, pQTL, sQTL, caQTL, metaboQTL)",
        "trait1": "Trait/phenotype name for study 1",
        "trait1_original": "Original trait name for study 1, e.g. phenotype code",
        "trait2": "Trait/phenotype name for study 2",
        "trait2_original": "Original trait name for study 2, e.g. phenotype code",
        "cell_type1": "Cell/tissue context for trait 1",
        "cell_type2": "Cell/tissue context for trait 2",
        "cs1_id": "Credible set ID for trait 1",
        "cs2_id": "Credible set ID for trait 2",
        "hit1": "Variant that coloc predicted to be the most likely causal variant in trait 1 (chr:pos:ref:alt)",
        "hit2": "Variant that coloc predicted to be the most likely causal variant in trait 2 (chr:pos:ref:alt)",
        "hit1_beta": "Effect size for lead variant in trait 1",
        "hit1_mlog10p": "-log10(p) for lead variant in trait 1",
        "hit2_beta": "Effect size for lead variant in trait 2",
        "hit2_mlog10p": "-log10(p) for lead variant in trait 2",
        "chr": "Chromosome number, chromosome X is 23",
        "region_start_min": "Start of the analyzed region (GRCh38)",
        "region_end_max": "End of the analyzed region (GRCh38)",
        "PP_H0_abf": "Posterior probability neither trait has association in the region",
        "PP_H1_abf": "Posterior probability only trait 1 has association",
        "PP_H2_abf": "Posterior probability only trait 2 has association",
        "PP_H3_abf": "Posterior probability both traits associate via DIFFERENT causal variants",
        "PP_H4_abf": "Posterior probability both traits associate and share the SAME causal variant. Generally >0.8 suggests colocalization",
        "nsnps": "Number of SNPs in the region in both credible sets",
        "nsnps1": "SNPs in the region for trait 1",
        "nsnps2": "SNPs in the region for trait 2",
        "cs1_log10bf": "log10 Bayes factor for credible set 1",
        "cs2_log10bf": "log10 Bayes factor for credible set 2",
        "clpp": "Causal posterior probability",
        "clpa": "Causal posterior agreement",
        "cs1_size": "Number of variants in credible set 1",
        "cs2_size": "Number of variants in credible set 2",
        "cs_overlap": "Number of variants shared between both credible sets",
        "topInOverlap": "Whether the maximum PIP variant was in overlap of regions or not, for both traits, e.g. 1,1 or 0,0",
        "resource1": "Data source for study 1 (lowercase)",
        "resource2": "Data source for study 2 (lowercase)",
    },
    "coloc_credsets_v": {
        "dataset": "Study/release name",
        "data_type": "Association type (GWAS, eQTL, pQTL, etc.)",
        "trait": "Trait/phenotype name",
        "trait_original": "Original trait name in the respective dataset, e.g. phenotype code",
        "cell_type": "Cell/tissue context",
        "chr": "Chromosome number, chromosome X is 23",
        "pos": "Genomic position (GRCh38)",
        "ref": "Reference allele",
        "alt": "Alternative (effect) allele",
        "mlog10p": "-log10(p-value)",
        "beta": "Effect size estimate (log-OR for binary traits)",
        "se": "Standard error of beta",
        "pip": "Posterior inclusion probability from fine-mapping (0-1). Higher = more likely causal",
        "cs_id": "Credible set identifier (matches cs1_id or cs2_id in colocalization_v)",
        "variant": "Variant identifier as chr:pos:ref:alt",
        "resource": "Data source identifier (lowercase)",
    },
    "exome_variant_results_v": {
        "dataset": "Study name (e.g. genebass)",
        "chr": "Chromosome number, chromosome X is 23",
        "pos": "Genomic position (GRCh38)",
        "ref": "Reference allele",
        "alt": "Alternative (effect) allele",
        "gene": "Gene symbol (e.g. PCSK9)",
        "annotation": "Variant annotation filter (e.g. pLoF, missense, synonymous, LC)", # TODO harmonize in data
        "mlog10p": "-log10(p-value) for the single-variant association. Higher = more significant",
        "beta": "Effect size estimate (log-OR for binary traits)",
        "se": "Standard error of beta",
        "af_overall": "Overall alternative allele frequency",
        "af_cases": "Alternative allele frequency in cases",
        "af_controls": "Alternative allele frequency in controls",
        "ac": "Alternative allele count",
        "an": "Alternative allele number (total alleles genotyped)",
        "heritability": "SNP heritability estimate for the trait",
        "trait": "Trait/phenotype name",
        "variant": "Variant identifier as chr:pos:ref:alt",
        "resource": "Data source identifier (lowercase)",
    },
    "gene_burden_results_v": {
        "dataset": "Study name (e.g. BipEx2, SCHEMA2, genebass, IBD_exome_2026)",
        "trait": "Trait/phenotype name",
        "gene": "Gene symbol (e.g. PCSK9)",
        "gene_id": "Ensembl gene ID (e.g. ENSG00000169174)",
        "chr": "Chromosome number, chromosome X is 23",
        "gene_start_pos": "Gene start position (GRCh38)",
        "gene_end_pos": "Gene end position (GRCh38)",
        "annotation": "Variant annotation filter used in burden test (e.g. pLoF, missense, PTV)", # TODO harmonize in data
        "mlog10p_burden": "-log10(p-value) from the gene-level burden test. Higher = more significant",
        "beta": "Effect size estimate from burden test",
        "se": "Standard error of beta",
        "total_variants": "Total qualifying variants in the gene",
        "total_variants_pheno": "Qualifying variants with phenotype data",
        "n_cases": "Number of cases in the analysis",
        "n_controls": "Number of controls in the analysis",
        "description": "Human-readable trait/phenotype description",
        "coding_description": "Coded trait description",
        "category": "Trait category grouping",
        "resource": "Data source identifier (lowercase). Use this for filtering, not dataset",
    },
}

# resource metadata with human-readable labels, descriptions, and common aliases
# so agents can map user intent (e.g. "bipex") to the correct filter value ("bipex2")
_RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "finngen": {
        "label": "FinnGen",
        "description": "FinnGen study — GWAS, eQTL, pQTL, caQTL results from Finnish biobank data",
        "aliases": ["FinnGen"],
    },
    "finngen_ukbb": {
        "label": "FinnGen+UKB",
        "description": "FinnGen + UK Biobank GWAS meta-analysis",
        "aliases": ["finngen ukbb", "finngen uk biobank"],
    },
    "finngen_mvp_ukbb": {
        "label": "FinnGen+MVP+UKB",
        "description": "FinnGen + Million Veteran Program + UK Biobank GWAS meta-analysis",
        "aliases": ["finngen mvp", "finngen mvp ukbb"],
    },
    "open_targets": {
        "label": "Open Targets Genetics",
        "description": "Open Targets GWAS summary statistics and fine-mapping",
        "aliases": ["open targets", "OT", "OTG"],
    },
    "ukbb": {
        "label": "UK Biobank",
        "description": "UK Biobank pQTL and GWAS results",
        "aliases": ["uk biobank", "UKB", "UKBB"],
    },
    "bipex2": {
        "label": "BipEx2",
        "description": "Bipolar Exome Sequencing study v2 — gene burden results for bipolar disorder",
        "aliases": ["bipex", "BipEx", "bipolar exome"],
    },
    "schema2": {
        "label": "SCHEMA2",
        "description": "Schizophrenia Exome Meta-analysis v2 — gene burden results for schizophrenia",
        "aliases": ["schema", "SCHEMA", "schizophrenia exome"],
    },
    "genebass": {
        "label": "Genebass",
        "description": "Gene burden results and EWAS exome variant results from UK Biobank",
        "aliases": ["gene bass", "uk biobank exome", "UKB exome"],
    },
    "ibd_exome_2026": {
        "label": "IBD Exome 2026",
        "description": "IBD (inflammatory bowel disease) exome sequencing project gene burden and variant analysis",
        "aliases": ["ibd exome", "IBD"],
    },
    "finnliver": {
        "label": "FinnLiver",
        "description": "FinnLiver liver eQTL study",
        "aliases": ["finn liver", "liver eQTL"],
    },
    "generisk": {
        "label": "GeneRisk",
        "description": "GeneRisk metabolome GWAS study",
        "aliases": [],
    },
    "interval": {
        "label": "INTERVAL",
        "description": "INTERVAL pQTL study",
        "aliases": [],
    },
}

# prefix patterns for collection resources (many IDs following a pattern)
# these are collapsed in the schema to avoid flooding the response
_COLLECTION_RESOURCE_PREFIXES: dict[str, dict[str, str]] = {
    "qtd": {
        "label": "eQTL Catalogue",
        "description": (
            "eQTL Catalogue datasets — hundreds of tissue/cell-type QTL studies. "
            "Dataset IDs follow the pattern QTDNNNNNN (e.g. QTD000001). "
            "Use the list_datasets tool to browse available eQTL Catalogue datasets." # TODO tools are not related to the db
        ),
        "data_types": "eQTL, sQTL",
    },
}

# example SQL queries per table to guide agents
_TABLE_EXAMPLES: dict[str, list[dict[str, str]]] = {
    "credible_sets_v": [
        {
            "description": "Fine-mapped variants for a gene from FinnGen with high PIP",
            "sql": "SELECT trait, variant, pip, mlog10p, beta FROM credible_sets_v WHERE gene_most_severe = 'PCSK9' AND resource = 'finngen' AND pip > 0.1 ORDER BY pip DESC",
        },
        {
            "description": "Credible sets for a phenotype across resources",
            "sql": "SELECT resource, dataset, variant, pip, mlog10p FROM credible_sets_v WHERE trait = 'T2D' AND pip > 0.5 ORDER BY pip DESC LIMIT 50",
        },
    ],
    "colocalization_v": [
        {
            "description": "Find GWAS-eQTL colocalizations for a gene",
            "sql": "SELECT trait1, trait2, cell_type2, PP_H4_abf, hit1, dataset2 FROM colocalization_v WHERE trait2 = 'PCSK9' AND data_type2 = 'eQTL' AND PP_H4_abf > 0.8 ORDER BY PP_H4_abf DESC",
        },
    ],
    "coloc_credsets_v": [
        {
            "description": "Get variants in a colocalization credible set",
            "sql": "SELECT variant, pip, mlog10p, beta FROM coloc_credsets_v WHERE cs_id = 'cs_id_here' ORDER BY pip DESC",
        },
    ],
    "exome_variant_results_v": [
        {
            "description": "Significant exome variant associations for a gene",
            "sql": "SELECT gene, variant, annotation, mlog10p, beta, trait FROM exome_variant_results_v WHERE gene = 'PCSK9' AND mlog10p > 3 ORDER BY mlog10p DESC",
        },
    ],
    "gene_burden_results_v": [
        {
            "description": "Significant gene burden results from BipEx2 bipolar exome study",
            "sql": "SELECT gene, annotation, mlog10p_burden, beta, n_cases, n_controls, description FROM gene_burden_results_v WHERE resource = 'bipex2' AND mlog10p_burden > 3 ORDER BY mlog10p_burden DESC LIMIT 20",
        },
        {
            "description": "Compare burden results for a gene across all exome studies",
            "sql": "SELECT resource, dataset, annotation, mlog10p_burden, beta, description FROM gene_burden_results_v WHERE gene = 'PCSK9' ORDER BY mlog10p_burden DESC",
        },
    ],
}

# Low-cardinality categorical columns whose distinct values are exposed in /schema
# so agents can filter without guessing. Values mapped to None are flat lists; values
# mapped to a column name indicate the column's allowed values depend on that column
# (e.g. data_type values differ per resource).
_CATEGORICAL_COLUMNS: dict[str, dict[str, str | None]] = {
    "credible_sets_v": {
        "resource": None,
        "dataset": "resource",
        "data_type": "resource",
        "most_severe": None,
    },
    "colocalization_v": {
        "resource1": None,
        "resource2": None,
        "dataset1": "resource1",
        "dataset2": "resource2",
        "data_type1": "resource1",
        "data_type2": "resource2",
    },
    "coloc_credsets_v": {
        "resource": None,
        "dataset": "resource",
        "data_type": "resource",
    },
    "exome_variant_results_v": {
        "resource": None,
        "dataset": "resource",
        "annotation": "resource",
    },
    "gene_burden_results_v": {
        "resource": None,
        "dataset": "resource",
        "annotation": "resource",
    },
}

# override hardcoded dicts with YAML-loaded versions when available
try:
    from api.yaml_loader import load_all as _load_yaml_config
except ImportError:
    from yaml_loader import load_all as _load_yaml_config

_yaml_config = _load_yaml_config()
if _yaml_config is not None:
    _RESOURCE_METADATA = _yaml_config["resource_metadata"]
    _COLLECTION_RESOURCE_PREFIXES = _yaml_config["collection_resource_prefixes"]
    _TABLE_DESCRIPTIONS = _yaml_config["table_descriptions"]
    _COLUMN_DESCRIPTIONS = _yaml_config["column_descriptions"]
    _TABLE_EXAMPLES = _yaml_config["table_examples"]
    _CATEGORICAL_COLUMNS = _yaml_config["categorical_columns"]
    logger.info("Loaded dataset config from YAML (%d resources, %d tables)",
                len(_RESOURCE_METADATA), len(_TABLE_DESCRIPTIONS))
else:
    logger.info("Using hardcoded dataset config (YAML not available)")

_VALUES_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_VALUES_CACHE_TTL_SECONDS = 3600


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


class SchemaResponse(BaseModel):
    """Database schema response."""

    resources: dict[str, Any] = {}
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
async def get_schema(table: str | None = None):
    """Get database schema information. Optionally filter to a single table."""
    if table and table not in VIEWS:
        resolved = _BASE_TABLES.get(table)
        if resolved:
            table = resolved
        else:
            raise HTTPException(status_code=404, detail=f"Unknown table: {table}. Available: {VIEWS}")

    views_to_fetch = [table] if table else VIEWS
    tables = []

    for table_name in views_to_fetch:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        try:
            table_meta = bq_client.get_table(table_ref)
            overrides = _COLUMN_DESCRIPTIONS.get(table_name, {})
            raw_cat_values = _get_categorical_values(table_name)
            cat_values = _compact_categorical_values(raw_cat_values)

            # get row count from base table (views report 0)
            row_count = 0
            try:
                base_table = table_name.removesuffix("_v")
                base_ref = f"{PROJECT_ID}.{DATASET_ID}.{base_table}"
                row_count = bq_client.get_table(base_ref).num_rows or 0
            except Exception:
                pass

            # build column list with collection_resources pulled out to table level
            collection_resources = cat_values.pop("collection_resources", None)
            columns: list[dict[str, Any]] = []
            for field in table_meta.schema:
                col: dict[str, Any] = {
                    "name": field.name,
                    "type": field.field_type,
                    "mode": field.mode,
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

    return {"resources": _RESOURCE_METADATA, "tables": tables}


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
