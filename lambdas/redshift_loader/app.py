import json
import os
import re
import time

import boto3


redshift_data = boto3.client("redshift-data")
s3 = boto3.client("s3")

WORKGROUP_NAME = os.environ["REDSHIFT_WORKGROUP"]
DATABASE = os.environ["REDSHIFT_DATABASE"]
SECRET_ARN = os.environ["REDSHIFT_SECRET_ARN"]
PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]
REDSHIFT_COPY_ROLE_ARN = os.environ["REDSHIFT_COPY_ROLE_ARN"]
REDSHIFT_SKIP_DDL = os.environ.get("REDSHIFT_SKIP_DDL", "false").lower() == "true"


def _safe_identifier(value: str, fallback: str = "public") -> str:
    candidate = (value or fallback).strip().lower()
    if re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", candidate):
        return candidate
    return fallback


REDSHIFT_SCHEMA = _safe_identifier(os.environ.get("REDSHIFT_SCHEMA", "public"))

COPY_COLUMNS = [
    "date",
    "client_id",
    "discount_code",
    "show_name",
    "revenue",
    "orders",
    "new_orders",
    "lapsed_orders",
    "active_orders",
    "impressions",
    "revenue_per_order",
    "revenue_per_impression",
    "impressions_per_order",
    "source_file",
    "run_id",
]


def execute_and_wait(sql: str) -> dict:
    execute_resp = redshift_data.execute_statement(
        WorkgroupName=WORKGROUP_NAME,
        Database=DATABASE,
        SecretArn=SECRET_ARN,
        Sql=sql,
    )
    statement_id = execute_resp["Id"]

    while True:
        desc = redshift_data.describe_statement(Id=statement_id)
        status = desc["Status"]
        if status in {"FINISHED", "FAILED", "ABORTED"}:
            if status != "FINISHED":
                error_text = str(desc.get("Error", "")).lower()
                if "permission denied for database" in error_text:
                    raise RuntimeError(
                        "Redshift user lacks CREATE permissions on the database. "
                        "Use a pre-provisioned schema/tables setup (REDSHIFT_SKIP_DDL=true) "
                        "or grant CREATE privileges."
                    )
                if REDSHIFT_SKIP_DDL and (
                    ('schema "' in error_text and '" does not exist' in error_text)
                    or ('relation "' in error_text and '" does not exist' in error_text)
                ):
                    raise RuntimeError(
                        f"Schema/table is missing in Redshift ({REDSHIFT_SCHEMA}.*) while REDSHIFT_SKIP_DDL=true. "
                        "Either pre-create the target objects or set REDSHIFT_SKIP_DDL=false."
                    )
                raise RuntimeError(f"Redshift statement {statement_id} failed: {desc}")
            return desc
        time.sleep(2)


def sql_quote(value: str) -> str:
    return value.replace("'", "''")


def ensure_tables():
    create_schema_sql = ""
    if REDSHIFT_SCHEMA != "public":
        create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {REDSHIFT_SCHEMA};"

    ddl = """
    %s
    CREATE TABLE IF NOT EXISTS %s.fact_uploads (
      date DATE,
      client_id VARCHAR(64),
      discount_code VARCHAR(256),
      show_name VARCHAR(512),
      revenue DOUBLE PRECISION,
      orders DOUBLE PRECISION,
      new_orders DOUBLE PRECISION,
      lapsed_orders DOUBLE PRECISION,
      active_orders DOUBLE PRECISION,
      impressions DOUBLE PRECISION,
      revenue_per_order DOUBLE PRECISION,
      revenue_per_impression DOUBLE PRECISION,
      impressions_per_order DOUBLE PRECISION,
      source_file VARCHAR(1024),
      run_id VARCHAR(128),
      loaded_at TIMESTAMP DEFAULT GETDATE()
    );
    CREATE TABLE IF NOT EXISTS %s.fact_adscribe (
      date DATE,
      client_id VARCHAR(64),
      discount_code VARCHAR(256),
      show_name VARCHAR(512),
      revenue DOUBLE PRECISION,
      orders DOUBLE PRECISION,
      new_orders DOUBLE PRECISION,
      lapsed_orders DOUBLE PRECISION,
      active_orders DOUBLE PRECISION,
      impressions DOUBLE PRECISION,
      revenue_per_order DOUBLE PRECISION,
      revenue_per_impression DOUBLE PRECISION,
      impressions_per_order DOUBLE PRECISION,
      source_file VARCHAR(1024),
      run_id VARCHAR(128),
      loaded_at TIMESTAMP DEFAULT GETDATE()
    );
    CREATE TABLE IF NOT EXISTS %s.fact_uploads_staging (LIKE %s.fact_uploads);
    CREATE TABLE IF NOT EXISTS %s.fact_adscribe_staging (LIKE %s.fact_adscribe);
    """ % (
        create_schema_sql,
        REDSHIFT_SCHEMA,
        REDSHIFT_SCHEMA,
        REDSHIFT_SCHEMA,
        REDSHIFT_SCHEMA,
        REDSHIFT_SCHEMA,
        REDSHIFT_SCHEMA,
    )
    import random

    for attempt in range(5):
        try:
            execute_and_wait(ddl)
            break
        except RuntimeError as e:
            if attempt < 4 and "deadlock" in str(e).lower():
                time.sleep(random.uniform(3, 8))
            else:
                raise

def lambda_handler(event, context):
    client_id = event["client_id"].lower()
    strategy = event.get("strategy", "upsert").lower()
    run_id = event.get("run_id") or event.get("execution_name")
    if not run_id:
        raise ValueError("run_id is required for Redshift loading")

    run_id_quoted = sql_quote(run_id)
    client_id_quoted = sql_quote(client_id)

    # Existing enterprise/shared Redshift environments can require pre-provisioned schemas.
    if not REDSHIFT_SKIP_DDL:
        ensure_tables()

    target_table = f"{REDSHIFT_SCHEMA}.fact_adscribe" if client_id == "adscribe" else f"{REDSHIFT_SCHEMA}.fact_uploads"
    staging_table = (
        f"{REDSHIFT_SCHEMA}.fact_adscribe_staging" if client_id == "adscribe" else f"{REDSHIFT_SCHEMA}.fact_uploads_staging"
    )
    source_prefix_key = f"silver/load/client_id={client_id}/run_id={run_id}/"
    source_prefix = f"s3://{PROCESSED_BUCKET}/{source_prefix_key}"

    listed = s3.list_objects_v2(Bucket=PROCESSED_BUCKET, Prefix=source_prefix_key)
    has_parquet = any(obj["Key"].endswith(".parquet") for obj in listed.get("Contents", []))
    if not has_parquet:
        return {
            **event,
            "status": "skipped_no_load_rows",
            "target_table": target_table,
            "source_prefix": source_prefix,
        }

    execute_and_wait(f"DELETE FROM {staging_table} WHERE run_id = '{run_id_quoted}';")

    copy_columns_sql = ", ".join(COPY_COLUMNS)
    iam_role_clause = (
        f"IAM_ROLE '{REDSHIFT_COPY_ROLE_ARN}'"
        if REDSHIFT_COPY_ROLE_ARN and REDSHIFT_COPY_ROLE_ARN.strip()
        else "IAM_ROLE default"
    )

    copy_sql = f"""
    COPY {staging_table} ({copy_columns_sql})
    FROM '{source_prefix}'
    {iam_role_clause}
    FORMAT AS PARQUET;
    """
    execute_and_wait(copy_sql)

    if strategy == "delete_insert":
        merge_sql = f"""
        DELETE FROM {target_table}
        WHERE client_id = '{client_id_quoted}'
          AND date IN (SELECT DISTINCT date FROM {staging_table} WHERE run_id = '{run_id_quoted}');

        INSERT INTO {target_table}
        SELECT * FROM {staging_table}
        WHERE run_id = '{run_id_quoted}';
        """
    else:
        merge_sql = f"""
        DELETE FROM {target_table}
        USING (
          SELECT DISTINCT
            date,
            client_id,
            COALESCE(discount_code, '') AS discount_code,
            COALESCE(show_name, '') AS show_name
          FROM {staging_table}
          WHERE run_id = '{run_id_quoted}'
        ) s
        WHERE {target_table}.date = s.date
          AND {target_table}.client_id = s.client_id
          AND COALESCE({target_table}.discount_code, '') = s.discount_code
          AND COALESCE({target_table}.show_name, '') = s.show_name;

        INSERT INTO {target_table}
        SELECT * FROM {staging_table}
        WHERE run_id = '{run_id_quoted}';
        """

    execute_and_wait(merge_sql)
    execute_and_wait(f"DELETE FROM {staging_table} WHERE run_id = '{run_id_quoted}';")

    return {
      **event,
      "status": "loaded",
      "target_table": target_table,
      "source_prefix": source_prefix
    }
