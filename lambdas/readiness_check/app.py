import json
import os
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError


s3 = boto3.client("s3")
dynamodb = boto3.client("dynamodb")

CONFIG_BUCKET = os.environ["CONFIG_BUCKET"]
CONFIG_PREFIX = os.environ.get("CONFIG_PREFIX", "clients")
CONFIG_INDEX_KEY = os.environ.get("CONFIG_INDEX_KEY", "client_config.json")
TABLE_NAME = os.environ["DEDUP_TABLE_NAME"]


def _load_client_config(client_id: str, explicit_config_key: str = ""):
    config_key = explicit_config_key or f"{CONFIG_PREFIX}/{client_id}.json"
    obj = s3.get_object(Bucket=CONFIG_BUCKET, Key=config_key)
    return config_key, json.loads(obj["Body"].read().decode("utf-8"))


def _folder_prefix(key: str) -> str:
    if "/" not in key:
        return ""
    return key.rsplit("/", 1)[0] + "/"


def _parse_s3_uri(uri: str):
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Invalid S3 URI: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def _read_header_columns(bucket: str, key: str):
    obj = s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-16384")
    chunk = obj["Body"].read().decode("utf-8", errors="replace")
    first_line = chunk.splitlines()[0] if chunk.splitlines() else ""
    return [c.strip() for c in first_line.split(",")] if first_line else []


def _validate_required_columns(input_files, required_columns_cfg):
    validation_errors = {}
    for file_type, required_cols in (required_columns_cfg or {}).items():
        if file_type not in input_files or not required_cols:
            continue
        bucket, key = _parse_s3_uri(input_files[file_type])
        headers = _read_header_columns(bucket, key)
        missing = [col for col in required_cols if col not in headers]
        if missing:
            validation_errors[file_type] = {"missing_columns": missing, "headers": headers}
    return validation_errors


def _object_hash(bucket: str, key: str):
    head = s3.head_object(Bucket=bucket, Key=key)
    etag = head.get("ETag", "").replace('"', "")
    if etag:
        return etag
    return f"{bucket}:{key}:{head.get('ContentLength', 0)}:{head.get('LastModified')}"


def _object_size(bucket: str, key: str):
    head = s3.head_object(Bucket=bucket, Key=key)
    return int(head.get("ContentLength", 0))


def _acquire_run_lock(client_id: str, run_id: str, ttl_seconds: int):
    now_epoch = int(datetime.now(timezone.utc).timestamp())
    expires_at = now_epoch + max(ttl_seconds, 120)
    lock_key = f"lock#{client_id}"

    try:
        dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                "file_hash": {"S": lock_key},
                "status": {"S": "RUNNING"},
                "run_id": {"S": run_id},
                "client_id": {"S": client_id},
                "expires_at": {"N": str(expires_at)},
                "updated_at": {"S": datetime.now(timezone.utc).isoformat()},
            },
            ConditionExpression="attribute_not_exists(file_hash) OR expires_at < :now",
            ExpressionAttributeValues={":now": {"N": str(now_epoch)}},
        )
        return True, lock_key, expires_at
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False, lock_key, None
        raise


def lambda_handler(event, context):
    client_id = event.get("client_id", "unknown").lower()
    bucket = event["bucket"]
    key = event["key"]
    folder = _folder_prefix(key)

    config_key, client_cfg = _load_client_config(client_id, event.get("config_key", ""))
    expected_prefix = client_cfg.get("prefix", "")
    if expected_prefix and not (key.startswith(expected_prefix) or key.startswith("manual/")):
        raise ValueError(f"Object key '{key}' does not match expected client prefix '{expected_prefix}'")

    required_patterns = [p.lower() for p in client_cfg.get("required_patterns", [])]
    optional_patterns = [p.lower() for p in client_cfg.get("optional_patterns", [])]
    file_type = (event.get("file_type") or "").lower()
    if file_type == "unknown":
        raise ValueError(f"Could not infer file type for key '{key}' and client '{client_id}'")

    strategy = client_cfg.get("strategy", "upsert").lower()
    lock_ttl_seconds = int(client_cfg.get("lock_ttl_seconds", 900))

    listed = s3.list_objects_v2(Bucket=bucket, Prefix=folder)
    contents = listed.get("Contents", [])
    lower_keys = [(obj["Key"], obj["LastModified"]) for obj in contents if not obj["Key"].endswith("/")]

    input_files = {}
    for pattern in required_patterns + optional_patterns:
        matches = [item for item in lower_keys if pattern in item[0].lower()]
        if matches:
            latest_key = sorted(matches, key=lambda x: x[1], reverse=True)[0][0]
            input_files[pattern] = f"s3://{bucket}/{latest_key}"

    missing_patterns = [p for p in required_patterns if p not in input_files]
    if missing_patterns:
        return {
            **event,
            "ready": False,
            "missing_patterns": missing_patterns,
            "strategy": strategy,
            "input_files": input_files,
            "input_hashes": {},
            "config_key": config_key,
            "readiness_checked_at": datetime.now(timezone.utc).isoformat(),
            "processing_folder": folder,
            "lock_acquired": False,
        }

    schema_errors = _validate_required_columns(input_files, client_cfg.get("required_columns", {}))
    if schema_errors:
        raise ValueError(f"Input schema validation failed for client {client_id}: {json.dumps(schema_errors)}")

    zero_byte_files = {}
    for pattern, uri in input_files.items():
        src_bucket, src_key = _parse_s3_uri(uri)
        size = _object_size(src_bucket, src_key)
        if pattern in required_patterns and size == 0:
            zero_byte_files[pattern] = uri
    if zero_byte_files:
        raise ValueError(f"Zero-byte required input files detected: {json.dumps(zero_byte_files)}")

    input_hashes = {}
    for pattern, uri in input_files.items():
        src_bucket, src_key = _parse_s3_uri(uri)
        input_hashes[pattern] = _object_hash(src_bucket, src_key)

    lock_acquired, lock_key, lock_expires_at = _acquire_run_lock(client_id, event["run_id"], lock_ttl_seconds)
    if not lock_acquired:
        return {
            **event,
            "ready": False,
            "missing_patterns": [],
            "strategy": strategy,
            "input_files": input_files,
            "input_hashes": input_hashes,
            "config_key": config_key,
            "readiness_checked_at": datetime.now(timezone.utc).isoformat(),
            "processing_folder": folder,
            "lock_acquired": False,
            "lock_busy": True,
            "lock_key": lock_key,
        }

    return {
        **event,
        "ready": True,
        "missing_patterns": [],
        "strategy": strategy,
        "input_files": input_files,
        "input_hashes": input_hashes,
        "config_key": config_key,
        "readiness_checked_at": datetime.now(timezone.utc).isoformat(),
        "processing_folder": folder,
        "lock_acquired": True,
        "lock_key": lock_key,
        "lock_expires_at": lock_expires_at,
        "required_patterns": required_patterns,
    }
