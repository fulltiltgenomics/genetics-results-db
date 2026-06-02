#!/usr/bin/env python3
"""
Load genetics results data from GCS into BigQuery.
Handles TSV.gz files with appropriate schema mapping.

By default a TSV file must contain every column declared in the target schema,
in the same order. If the file omits a column that the schema requires (e.g. a
`dataset` discriminator that distinguishes per-source rows in a shared table),
pass `--const-column NAME=VALUE` to inject the value at load time. The data is
loaded into a temporary staging table (without those columns), then projected
into the target table with the constants filled in. The flag is repeatable.
"""

import argparse
import sys
import uuid
from pathlib import Path
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, SourceFormat, WriteDisposition


# schema definitions matching the SQL table definitions
SCHEMAS = {
    "credible_sets": [
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("data_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait_original", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cell_type", "STRING"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ref", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("alt", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("mlog10p", "FLOAT64"),
        bigquery.SchemaField("beta", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("se", "FLOAT64"),
        bigquery.SchemaField("pip", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("cs_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cs_size", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("cs_min_r2", "FLOAT64"),
        bigquery.SchemaField("aaf", "FLOAT64"),
        bigquery.SchemaField("most_severe", "STRING"),
        bigquery.SchemaField("gene_most_severe", "STRING"),
    ],
    # column order must match TSV file exactly
    "colocalization": [
        bigquery.SchemaField("dataset1", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("dataset2", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("data_type1", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("data_type2", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait1", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait1_original", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait2", "STRING"),
        bigquery.SchemaField("trait2_original", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cell_type1", "STRING"),
        bigquery.SchemaField("cell_type2", "STRING"),
        bigquery.SchemaField("cs1_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cs2_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("hit1", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("hit2", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("hit1_beta", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("hit1_mlog10p", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("hit2_beta", "FLOAT64"),
        bigquery.SchemaField("hit2_mlog10p", "FLOAT64"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("region_start_min", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("region_end_max", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("PP_H0_abf", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("PP_H1_abf", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("PP_H2_abf", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("PP_H3_abf", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("PP_H4_abf", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("nsnps", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("nsnps1", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("nsnps2", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("cs1_log10bf", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("cs2_log10bf", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("clpp", "FLOAT64"),
        bigquery.SchemaField("clpa", "FLOAT64"),
        bigquery.SchemaField("cs1_size", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("cs2_size", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("cs_overlap", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("topInOverlap", "STRING", mode="REQUIRED"),
    ],
    # column order must match TSV file exactly
    "coloc_credsets": [
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("data_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait_original", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cell_type", "STRING"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ref", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("alt", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("mlog10p", "FLOAT64"),
        bigquery.SchemaField("beta", "FLOAT64"),
        bigquery.SchemaField("se", "FLOAT64"),
        bigquery.SchemaField("pip", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("cs_id", "STRING", mode="REQUIRED"),
    ],
    # column order must match TSV file exactly
    "asm_qtl": [
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ref", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("alt", "STRING"),
        bigquery.SchemaField("rsid", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("beta", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("se", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("mlog10p", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("af", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("most_severe", "STRING"),
        bigquery.SchemaField("gene_most_severe", "STRING"),
        bigquery.SchemaField("target_start", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("target_end", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ref_methylrate", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("alt_methylrate", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("n_haplotypes", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("variant_rank", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ld_count", "INT64"),
        bigquery.SchemaField("vartype", "STRING", mode="REQUIRED"),
    ],
    # column order must match TSV file exactly
    "exome_variant_results": [
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ref", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("alt", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gene", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("annotation", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("mlog10p", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("beta", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("se", "FLOAT64"),
        bigquery.SchemaField("af_overall", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("af_cases", "FLOAT64"),
        bigquery.SchemaField("af_controls", "FLOAT64"),
        bigquery.SchemaField("ac", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("an", "INT64"),
        bigquery.SchemaField("n_cases", "INT64"),
        bigquery.SchemaField("n_controls", "INT64"),
        bigquery.SchemaField("trait", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait_original", "STRING", mode="REQUIRED"),
    ],
    # column order must match TSV file exactly
    "gene_burden_results": [
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gene", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gene_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("gene_start_pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("gene_end_pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("annotation", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("mlog10p_burden", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("beta", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("se", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("total_variants", "INT64"),
        bigquery.SchemaField("total_variants_pheno", "INT64"),
        bigquery.SchemaField("n_cases", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("n_controls", "INT64"),
        bigquery.SchemaField("trait_original", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("flags", "STRING"),
    ],
}


def _coerce_const(value: str, bq_type: str):
    """Coerce a string CLI argument to the Python type matching the schema field."""
    t = bq_type.upper()
    if t in ("INT64", "INTEGER"):
        return int(value)
    if t in ("FLOAT64", "FLOAT", "NUMERIC", "BIGNUMERIC"):
        return float(value)
    if t in ("BOOL", "BOOLEAN"):
        return value.strip().lower() in ("true", "1", "yes", "y", "t")
    return value


def load_table(
    client: bigquery.Client,
    gcs_uri: str,
    table_id: str,
    table_type: str,
    write_disposition: str = "WRITE_APPEND",
    skip_leading_rows: int = 1,
    const_columns: dict | None = None,
):
    """Load a GCS file into a BigQuery table.

    When `const_columns` is empty/None: loads directly with the full schema
    (original behavior). When provided: the source file is expected to omit
    those columns, and the values are injected via a staging-table indirection.
    Returns the job that produced rows in the target table.
    """

    if table_type not in SCHEMAS:
        raise ValueError(f"Unknown table type: {table_type}. Must be one of {list(SCHEMAS.keys())}")

    full_schema = SCHEMAS[table_type]
    const_columns = const_columns or {}

    if not const_columns:
        # original direct-load path
        job_config = LoadJobConfig(
            schema=full_schema,
            source_format=SourceFormat.CSV,
            field_delimiter="\t",
            skip_leading_rows=skip_leading_rows,
            write_disposition=getattr(WriteDisposition, write_disposition),
            allow_quoted_newlines=True,
            null_marker="NA",
        )
        print(f"Loading {gcs_uri} into {table_id}...")
        return client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)

    schema_by_name = {f.name: f for f in full_schema}
    unknown = sorted(set(const_columns) - set(schema_by_name))
    if unknown:
        raise ValueError(
            f"--const-column references columns not in schema '{table_type}': {unknown}"
        )

    staging_schema = [f for f in full_schema if f.name not in const_columns]
    staging_id = f"{table_id}__staging_{uuid.uuid4().hex[:8]}"

    load_config = LoadJobConfig(
        schema=staging_schema,
        source_format=SourceFormat.CSV,
        field_delimiter="\t",
        skip_leading_rows=skip_leading_rows,
        write_disposition=WriteDisposition.WRITE_TRUNCATE,
        allow_quoted_newlines=True,
        null_marker="NA",
    )
    print(f"Loading {gcs_uri} into staging {staging_id}...")
    load_job = client.load_table_from_uri(gcs_uri, staging_id, job_config=load_config)
    load_job.result()
    print(f"  staged {load_job.output_rows} rows")

    try:
        # project staging into the target with constants filled in
        col_exprs = []
        params = []
        for f in full_schema:
            if f.name in const_columns:
                pname = f"const_{f.name}"
                py_value = _coerce_const(const_columns[f.name], f.field_type)
                col_exprs.append(f"@{pname} AS `{f.name}`")
                params.append(bigquery.ScalarQueryParameter(pname, f.field_type, py_value))
            else:
                col_exprs.append(f"`{f.name}`")

        sql = f"SELECT {', '.join(col_exprs)} FROM `{staging_id}`"
        query_config = bigquery.QueryJobConfig(
            destination=table_id,
            write_disposition=getattr(WriteDisposition, write_disposition),
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            query_parameters=params,
        )
        consts_repr = ", ".join(f"{k}={v!r}" for k, v in const_columns.items())
        print(f"Projecting staging -> {table_id} ({consts_repr})...")
        query_job = client.query(sql, job_config=query_config)
        query_job.result()
        return query_job
    finally:
        client.delete_table(staging_id, not_found_ok=True)
        print(f"  dropped staging {staging_id}")


def main():
    parser = argparse.ArgumentParser(description="Load genetics data from GCS into BigQuery")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--dataset", default="genetics_results", help="BigQuery dataset ID")
    parser.add_argument("--table", required=True, help="Target table")
    parser.add_argument("--schema", choices=list(SCHEMAS.keys()), help="Schema key (defaults to --table value)")
    parser.add_argument("--gcs-uri", required=True, help="GCS URI (gs://bucket/path/*.tsv.gz)")
    parser.add_argument(
        "--write-disposition",
        default="WRITE_APPEND",
        choices=["WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"],
        help="How to handle existing data",
    )
    parser.add_argument("--skip-rows", type=int, default=1, help="Header rows to skip")
    parser.add_argument(
        "--const-column",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help=(
            "Inject a constant value for a schema column that is absent from the source "
            "file. Repeatable. Triggers a staging-table load: data is first loaded into a "
            "temporary table with the truncated schema, then projected into the target with "
            "the constants filled in. Example: --const-column dataset=deCODE_asmQTL_CpG"
        ),
    )

    args = parser.parse_args()

    const_columns: dict[str, str] = {}
    for spec in args.const_column:
        if "=" not in spec:
            parser.error(f"--const-column expects NAME=VALUE, got: {spec!r}")
        name, _, value = spec.partition("=")
        name = name.strip()
        if not name:
            parser.error(f"--const-column has empty NAME: {spec!r}")
        const_columns[name] = value

    schema_key = args.schema or args.table
    if schema_key not in SCHEMAS:
        parser.error(f"Unknown schema '{schema_key}'. Must be one of {list(SCHEMAS.keys())}")

    client = bigquery.Client(project=args.project)
    table_id = f"{args.project}.{args.dataset}.{args.table}"

    job = load_table(
        client,
        args.gcs_uri,
        table_id,
        schema_key,
        args.write_disposition,
        args.skip_rows,
        const_columns,
    )

    # wait for job to complete (a no-op when load_table already awaited)
    try:
        job.result()
        rows = getattr(job, "output_rows", None)
        if rows is None:
            rows = getattr(job, "num_dml_affected_rows", None)
        print(f"Loaded {rows} rows into {table_id}")
    except Exception as e:
        print(f"Error loading data: {e}")
        if job.errors:
            for error in job.errors:
                print(f"  - {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
