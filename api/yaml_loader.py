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
        logger.error("datasets.yaml not found at %s; service will fail to start", path)
        return None
    except Exception:
        logger.exception("Failed to parse datasets.yaml at %s; service will fail to start", path)
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


def load_views(config: dict[str, Any]) -> list[str]:
    """Build VIEWS from the tables section — the names the API exposes.

    The `tables` block is a documentation registry first: the sandbox schema docs are
    generated from every entry in it. Reachability is a separate, explicit per-table
    decision, and it fails closed — an entry without `exposed: true` is documented and
    unreachable — so the allow-list cannot widen by someone adding a table or omitting
    a field.
    """
    tables = config.get("tables") or {}
    return [name for name, info in tables.items() if (info or {}).get("exposed") is True]


RESOURCE_DERIVATION_MODES = ("view_case", "load_time", "none")


def load_resource_derivation(config: dict[str, Any]) -> dict[str, str]:
    """Map every table to how its `resource` column is produced.

    `view_case`  — a CASE generated from `dataset_to_resource_rules` sits in the view SQL and
                   is linted against the rules;
    `load_time`  — the same CASE is applied by the loader and stored on the base table, so the
                   view SQL must contain none;
    `none`       — `resource` is not derived from a `dataset` discriminator at all, and the
                   entry says why.

    A missing or unknown mode raises rather than defaulting: a view that drops out of the lint
    scope silently is a CASE nobody compares against the rules.
    """
    tables = config.get("tables") or {}
    result: dict[str, str] = {}
    for name, info in tables.items():
        entry = (info or {}).get("resource_derivation")
        entry = entry if isinstance(entry, dict) else {}
        mode = entry.get("mode")
        if mode not in RESOURCE_DERIVATION_MODES:
            raise ValueError(
                f"tables.{name}: `resource_derivation.mode` must be one of "
                f"{', '.join(RESOURCE_DERIVATION_MODES)}, got {mode!r}")
        if mode != "view_case" and not str(entry.get("reason") or "").strip():
            raise ValueError(
                f"tables.{name}: `resource_derivation.mode: {mode}` needs a `reason` — an "
                "exception with no reason cannot be told apart from an oversight")
        result[name] = mode
    return result


def load_dataset_cross_check_exclusions(config: dict[str, Any]) -> dict[str, str]:
    """Map each table excluded from the live `dataset`-value cross-check to its reason.

    Presence of the key is the opt-in, so an empty or null block raises rather than quietly
    reading as "not excluded" — that is the same shape as a blank reason.

    Exclusion is opt-in and reason-bearing, and it is a different question from
    `resource_derivation`: a view can carry a `dataset` column the check needs while its
    `resource` is a constant, and vice versa.
    """
    tables = config.get("tables") or {}
    result: dict[str, str] = {}
    for name, info in tables.items():
        info = info or {}
        if "dataset_cross_check" not in info:
            continue
        entry = info["dataset_cross_check"]
        entry = entry if isinstance(entry, dict) else {}
        reason = str(entry.get("excluded_reason") or "").strip()
        if not reason:
            raise ValueError(
                f"tables.{name}: `dataset_cross_check` needs a non-empty `excluded_reason`")
        result[name] = reason
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
        views, resource_metadata, collection_resource_prefixes,
        table_descriptions, column_descriptions,
        table_examples, categorical_columns
    """
    config = _load_yaml()
    if config is None:
        return None

    return {
        "views": load_views(config),
        "resource_metadata": load_resource_metadata(config),
        "collection_resource_prefixes": load_collection_resource_prefixes(config),
        "table_descriptions": load_table_descriptions(config),
        "column_descriptions": load_column_descriptions(config),
        "table_examples": load_table_examples(config),
        "categorical_columns": load_categorical_columns(config),
    }
