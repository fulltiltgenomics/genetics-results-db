#!/usr/bin/env python3
"""
Load genetics results data from GCS into BigQuery.
Handles TSV.gz files with appropriate schema mapping.
"""

import argparse
import sys
from pathlib import Path
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, SourceFormat, WriteDisposition


# schema definitions matching the SQL table definitions
SCHEMAS = {
    "credible_sets": [
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("data_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait_original", "STRING"),
        bigquery.SchemaField("cell_type", "STRING"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ref", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("alt", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("mlog10p", "FLOAT64"),
        bigquery.SchemaField("beta", "FLOAT64"),
        bigquery.SchemaField("se", "FLOAT64"),
        bigquery.SchemaField("pip", "FLOAT64"),
        bigquery.SchemaField("cs_id", "STRING"),
        bigquery.SchemaField("cs_size", "INT64"),
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
        bigquery.SchemaField("trait1", "STRING"),
        bigquery.SchemaField("trait1_original", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait2", "STRING"),
        bigquery.SchemaField("trait2_original", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cell_type1", "STRING"),
        bigquery.SchemaField("cell_type2", "STRING"),
        bigquery.SchemaField("cs1_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cs2_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("hit1", "STRING"),
        bigquery.SchemaField("hit2", "STRING"),
        bigquery.SchemaField("hit1_beta", "FLOAT64"),
        bigquery.SchemaField("hit1_mlog10p", "FLOAT64"),
        bigquery.SchemaField("hit2_beta", "FLOAT64"),
        bigquery.SchemaField("hit2_mlog10p", "FLOAT64"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("region_start_min", "INT64"),
        bigquery.SchemaField("region_end_max", "INT64"),
        bigquery.SchemaField("PP_H0_abf", "FLOAT64"),
        bigquery.SchemaField("PP_H1_abf", "FLOAT64"),
        bigquery.SchemaField("PP_H2_abf", "FLOAT64"),
        bigquery.SchemaField("PP_H3_abf", "FLOAT64"),
        bigquery.SchemaField("PP_H4_abf", "FLOAT64"),
        bigquery.SchemaField("nsnps", "INT64"),
        bigquery.SchemaField("nsnps1", "INT64"),
        bigquery.SchemaField("nsnps2", "INT64"),
        bigquery.SchemaField("cs1_log10bf", "FLOAT64"),
        bigquery.SchemaField("cs2_log10bf", "FLOAT64"),
        bigquery.SchemaField("clpp", "FLOAT64"),
        bigquery.SchemaField("clpa", "FLOAT64"),
        bigquery.SchemaField("cs1_size", "INT64"),
        bigquery.SchemaField("cs2_size", "INT64"),
        bigquery.SchemaField("cs_overlap", "INT64"),
        bigquery.SchemaField("topInOverlap", "STRING"),
    ],
    # column order must match TSV file exactly
    "coloc_credsets": [
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("data_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("trait_original", "STRING"),
        bigquery.SchemaField("cell_type", "STRING"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ref", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("alt", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("mlog10p", "FLOAT64"),
        bigquery.SchemaField("beta", "FLOAT64"),
        bigquery.SchemaField("se", "FLOAT64"),
        bigquery.SchemaField("pip", "FLOAT64"),
        bigquery.SchemaField("cs_id", "STRING"),
    ],
    # column order must match TSV file exactly
    "exome_variant_results": [
        bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("chr", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("pos", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ref", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("alt", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gene", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("annotation", "STRING"),
        bigquery.SchemaField("mlog10p", "FLOAT64"),
        bigquery.SchemaField("beta", "FLOAT64"),
        bigquery.SchemaField("se", "FLOAT64"),
        bigquery.SchemaField("af_overall", "FLOAT64"),
        bigquery.SchemaField("af_cases", "FLOAT64"),
        bigquery.SchemaField("af_controls", "FLOAT64"),
        bigquery.SchemaField("ac", "INT64"),
        bigquery.SchemaField("an", "INT64"),
        bigquery.SchemaField("heritability", "FLOAT64"),
        bigquery.SchemaField("trait", "STRING", mode="REQUIRED"),
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
        bigquery.SchemaField("mlog10p_burden", "FLOAT64"),
        bigquery.SchemaField("mlog10p_skat", "FLOAT64"),
        bigquery.SchemaField("mlog10p_skato", "FLOAT64"),
        bigquery.SchemaField("beta", "FLOAT64"),
        bigquery.SchemaField("se", "FLOAT64"),
        bigquery.SchemaField("total_variants", "INT64"),
        bigquery.SchemaField("total_variants_pheno", "INT64"),
        bigquery.SchemaField("n_cases", "INT64"),
        bigquery.SchemaField("n_controls", "INT64"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("coding_description", "STRING"),
        bigquery.SchemaField("category", "STRING"),
    ],
}


def load_table(
    client: bigquery.Client,
    gcs_uri: str,
    table_id: str,
    table_type: str,
    write_disposition: str = "WRITE_APPEND",
    skip_leading_rows: int = 1,
) -> bigquery.LoadJob:
    """Load a GCS file into a BigQuery table."""

    if table_type not in SCHEMAS:
        raise ValueError(f"Unknown table type: {table_type}. Must be one of {list(SCHEMAS.keys())}")

    job_config = LoadJobConfig(
        schema=SCHEMAS[table_type],
        source_format=SourceFormat.CSV,
        field_delimiter="\t",
        skip_leading_rows=skip_leading_rows,
        write_disposition=getattr(WriteDisposition, write_disposition),
        allow_quoted_newlines=True,
        null_marker="NA",
    )

    print(f"Loading {gcs_uri} into {table_id}...")
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)

    return load_job


def main():
    parser = argparse.ArgumentParser(description="Load genetics data from GCS into BigQuery")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--dataset", default="genetics_results", help="BigQuery dataset ID")
    parser.add_argument("--table", required=True, choices=list(SCHEMAS.keys()), help="Target table")
    parser.add_argument("--gcs-uri", required=True, help="GCS URI (gs://bucket/path/*.tsv.gz)")
    parser.add_argument(
        "--write-disposition",
        default="WRITE_APPEND",
        choices=["WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"],
        help="How to handle existing data",
    )
    parser.add_argument("--skip-rows", type=int, default=1, help="Header rows to skip")

    args = parser.parse_args()

    client = bigquery.Client(project=args.project)
    table_id = f"{args.project}.{args.dataset}.{args.table}"

    job = load_table(
        client,
        args.gcs_uri,
        table_id,
        args.table,
        args.write_disposition,
        args.skip_rows,
    )

    # wait for job to complete
    try:
        job.result()
        print(f"Loaded {job.output_rows} rows into {table_id}")
    except Exception as e:
        print(f"Error loading data: {e}")
        if job.errors:
            for error in job.errors:
                print(f"  - {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
