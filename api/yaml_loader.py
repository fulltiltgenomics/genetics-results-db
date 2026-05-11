"""
Load dataset/resource/table metadata from the shared datasets.yaml config.

Produces the same data structures previously hardcoded in main.py, enabling
a single source of truth across services.
"""

import logging
import os
from typing import Any

import yaml

logger = logging.getLogger(__name__)

_DATASETS_CONFIG_PATH = os.environ.get("DATASETS_CONFIG_PATH", "./configs/datasets.yaml")


def _load_yaml() -> dict[str, Any] | None:
    """Load and parse the datasets YAML file, or return None on failure."""
    path = _DATASETS_CONFIG_PATH
    try:
        with open(path) as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.warning("datasets.yaml not found at %s, using hardcoded fallback", path)
        return None
    except Exception:
        logger.exception("Failed to parse datasets.yaml at %s, using hardcoded fallback", path)
        return None


def load_resource_metadata(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Build _RESOURCE_METADATA from the resources section.

    Only includes resources that have collection=true or lack the collection
    flag AND don't belong to the API-only set (resources only used by the
    results-api, not present in BQ views). We include all resources that
    were in the original hardcoded dict — identified by not having any
    API-only marker.
    """
    resources = config.get("resources", {})
    # the original _RESOURCE_METADATA included exactly these resources that
    # appear in BQ views; API-only resources are excluded
    _API_ONLY_RESOURCES = {
        "ukbb_finucane", "finngen_nmr", "gtex", "hpa", "gencc", "monarch", "pgc",
    }

    result: dict[str, dict[str, Any]] = {}
    for resource_id, meta in resources.items():
        if resource_id in _API_ONLY_RESOURCES:
            continue
        if meta.get("collection"):
            # collection resources go into _COLLECTION_RESOURCE_PREFIXES, not here
            continue

        result[resource_id] = {
            "label": meta["label"],
            "description": meta["description"],
            "aliases": meta.get("aliases", []),
        }
    return result


def load_collection_resource_prefixes(config: dict[str, Any]) -> dict[str, dict[str, str]]:
    """Build _COLLECTION_RESOURCE_PREFIXES from resources with collection=true."""
    resources = config.get("resources", {})
    result: dict[str, dict[str, str]] = {}
    for _resource_id, meta in resources.items():
        if not meta.get("collection"):
            continue
        prefix = meta.get("collection_id_prefix")
        if not prefix:
            continue
        data_types = meta.get("collection_data_types", [])
        result[prefix] = {
            "label": meta["label"],
            "description": meta["description"],
            "data_types": ", ".join(data_types),
        }
    return result


def load_table_descriptions(config: dict[str, Any]) -> dict[str, str]:
    """Build _TABLE_DESCRIPTIONS from the tables section."""
    tables = config.get("tables", {})
    return {name: info["description"] for name, info in tables.items() if "description" in info}


def load_column_descriptions(config: dict[str, Any]) -> dict[str, dict[str, str]]:
    """Build _COLUMN_DESCRIPTIONS from the tables section."""
    tables = config.get("tables", {})
    result: dict[str, dict[str, str]] = {}
    for name, info in tables.items():
        cols = info.get("columns")
        if cols:
            result[name] = dict(cols)
    return result


def load_table_examples(config: dict[str, Any]) -> dict[str, list[dict[str, str]]]:
    """Build _TABLE_EXAMPLES from the tables section."""
    tables = config.get("tables", {})
    result: dict[str, list[dict[str, str]]] = {}
    for name, info in tables.items():
        examples = info.get("examples")
        if examples:
            result[name] = [
                {"description": ex["description"], "sql": ex["sql"]}
                for ex in examples
            ]
    return result


def load_categorical_columns(config: dict[str, Any]) -> dict[str, dict[str, str | None]]:
    """Build _CATEGORICAL_COLUMNS from the tables section."""
    tables = config.get("tables", {})
    result: dict[str, dict[str, str | None]] = {}
    for name, info in tables.items():
        cat = info.get("categorical_columns")
        if cat:
            result[name] = dict(cat)
    return result


def load_all() -> dict[str, Any] | None:
    """Load all data structures from YAML. Returns None if YAML is unavailable.

    On success returns a dict with keys:
        resource_metadata, collection_resource_prefixes,
        table_descriptions, column_descriptions,
        table_examples, categorical_columns
    """
    config = _load_yaml()
    if config is None:
        return None

    return {
        "resource_metadata": load_resource_metadata(config),
        "collection_resource_prefixes": load_collection_resource_prefixes(config),
        "table_descriptions": load_table_descriptions(config),
        "column_descriptions": load_column_descriptions(config),
        "table_examples": load_table_examples(config),
        "categorical_columns": load_categorical_columns(config),
    }
