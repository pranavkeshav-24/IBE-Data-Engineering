import json
import os
import time

import boto3


redshift_data = boto3.client("redshift-data")
s3 = boto3.client("s3")

WORKGROUP_NAME = os.environ["REDSHIFT_WORKGROUP"]
DATABASE = os.environ["REDSHIFT_DATABASE"]
SECRET_ARN = os.environ["REDSHIFT_SECRET_ARN"]
PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]
REDSHIFT_COPY_ROLE_ARN = os.environ["REDSHIFT_COPY_ROLE_ARN"]

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
                raise RuntimeError(f"Redshift statement {statement_id} failed: {desc}")
            return desc
        time.sleep(2)


def sql_quote(value: str) -> str:
    return value.replace("'", "''")


def ensure_tables():
    ddl = """
    CREATE SCHEMA IF NOT EXISTS analytics;
    CREATE TABLE IF NOT EXISTS analytics.fact_uploads (
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
    CREATE TABLE IF NOT EXISTS analytics.fact_adscribe (
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
    CREATE TABLE IF NOT EXISTS analytics.fact_uploads_staging (LIKE analytics.fact_uploads);
    CREATE TABLE IF NOT EXISTS analytics.fact_adscribe_staging (LIKE analytics.fact_adscribe);
    """
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

    ensure_tables()

    target_table = "analytics.fact_adscribe" if client_id == "adscribe" else "analytics.fact_uploads"
    staging_table = "analytics.fact_adscribe_staging" if client_id == "adscribe" else "analytics.fact_uploads_staging"
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
