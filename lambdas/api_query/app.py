import json
import os
import re
import time
from datetime import date, timedelta

import boto3


redshift_data = boto3.client("redshift-data")
sfn = boto3.client("stepfunctions")

WORKGROUP_NAME = os.environ["REDSHIFT_WORKGROUP"]
DATABASE = os.environ["REDSHIFT_DATABASE"]
SECRET_ARN = os.environ["REDSHIFT_SECRET_ARN"]
STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN", "")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "*")


def _safe_identifier(value: str, fallback: str = "public") -> str:
    candidate = (value or fallback).strip().lower()
    if re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", candidate):
        return candidate
    return fallback


REDSHIFT_SCHEMA = _safe_identifier(os.environ.get("REDSHIFT_SCHEMA", "public"))


def _response(status: int, body: dict):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        },
        "body": json.dumps(body, default=str),
    }


def _wait_statement(statement_id: str):
    while True:
        desc = redshift_data.describe_statement(Id=statement_id)
        if desc["Status"] == "FINISHED":
            return desc
        if desc["Status"] in {"FAILED", "ABORTED"}:
            raise RuntimeError(f"Redshift query failed: {desc}")
        time.sleep(1)


def _records_to_json(statement_id: str):
    result = redshift_data.get_statement_result(Id=statement_id)
    cols = [c["name"] for c in result["ColumnMetadata"]]

    rows = []
    for rec in result["Records"]:
        row = {}
        for idx, val in enumerate(rec):
            typed_val = next(iter(val.values())) if val else None
            row[cols[idx]] = typed_val
        rows.append(row)
    return rows


def _build_query(params: dict):
    start_date = params.get("start_date") or (date.today() - timedelta(days=30)).isoformat()
    end_date = params.get("end_date") or date.today().isoformat()
    client_id = params.get("client_id")

    fact_uploads_table = f"{REDSHIFT_SCHEMA}.fact_uploads"
    fact_adscribe_table = f"{REDSHIFT_SCHEMA}.fact_adscribe"

    base = f"""
    SELECT
      date,
      client_id,
      COALESCE(discount_code, 'n/a') AS discount_code,
      COALESCE(show_name, 'n/a') AS show_name,
      SUM(revenue) AS revenue,
      SUM(orders) AS orders,
      SUM(new_orders) AS new_orders,
      SUM(lapsed_orders) AS lapsed_orders,
      SUM(active_orders) AS active_orders,
      SUM(impressions) AS impressions
    FROM (
      SELECT date, client_id, discount_code, show_name, revenue, orders, new_orders, lapsed_orders, active_orders, impressions FROM {fact_uploads_table}
      UNION ALL
      SELECT date, client_id, discount_code, show_name, revenue, orders, new_orders, lapsed_orders, active_orders, impressions FROM {fact_adscribe_table}
    ) x
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    """
    if client_id:
        client_safe = str(client_id).replace("'", "''")
        base += f" AND client_id = '{client_safe}' "

    base += """
    GROUP BY 1,2,3,4
    ORDER BY date DESC, client_id, discount_code
    LIMIT 1000;
    """
    return base


def _query_redshift(params: dict):
    sql = _build_query(params)
    resp = redshift_data.execute_statement(
        WorkgroupName=WORKGROUP_NAME,
        Database=DATABASE,
        SecretArn=SECRET_ARN,
        Sql=sql,
    )
    statement_id = resp["Id"]
    _wait_statement(statement_id)
    return _records_to_json(statement_id)


def lambda_handler(event, context):
    method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
    path = event.get("rawPath", "")

    if method == "OPTIONS":
        return _response(200, {"ok": True})

    try:
        if path.endswith("/refresh") and method == "POST":
            payload = json.loads(event.get("body") or "{}")
            rows = _query_redshift(payload)
            started_execution = None
            if payload.get("trigger_adscribe_ingestion") and STATE_MACHINE_ARN:
                start = sfn.start_execution(
                    stateMachineArn=STATE_MACHINE_ARN,
                    input=json.dumps(
                        {
                            "bucket": "manual",
                            "key": "manual/refresh",
                            "client_id": "adscribe",
                            "file_hash": f"manual-{int(time.time())}",
                            "manual_refresh": True,
                        }
                    ),
                )
                started_execution = start["executionArn"]
            return _response(
                200,
                {
                    "mode": "manual_refresh",
                    "rows": rows,
                    "triggered_execution": started_execution,
                },
            )

        params = event.get("queryStringParameters") or {}
        rows = _query_redshift(params)
        return _response(200, {"mode": "query", "rows": rows})
    except Exception as exc:
        return _response(500, {"error": str(exc)})
