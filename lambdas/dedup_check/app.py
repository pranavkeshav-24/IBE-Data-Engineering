import json
import os
from datetime import datetime, timezone

import boto3


dynamodb = boto3.client("dynamodb")
s3 = boto3.client("s3")

TABLE_NAME = os.environ["DEDUP_TABLE_NAME"]
CONFIG_BUCKET = os.environ["CONFIG_BUCKET"]
CONFIG_PREFIX = os.environ.get("CONFIG_PREFIX", "clients")


def _ddb_key(value: str):
    return {"file_hash": {"S": value}}


def _get_item(key_value: str):
    result = dynamodb.get_item(
        TableName=TABLE_NAME,
        Key=_ddb_key(key_value),
        ConsistentRead=True,
    )
    return result.get("Item")


def _load_client_config(client_id: str):
    config_key = f"{CONFIG_PREFIX}/{client_id}.json"
    obj = s3.get_object(Bucket=CONFIG_BUCKET, Key=config_key)
    return config_key, json.loads(obj["Body"].read().decode("utf-8"))


def _infer_file_type(key: str, patterns):
    if key.lower().startswith("manual/") and patterns:
        return patterns[0]
    key_l = key.lower()
    for pattern in patterns:
        if pattern.lower() in key_l:
            return pattern.lower()
    return "unknown"


def _latest_hash(client_id: str, file_type: str):
    item = _get_item(f"latest#{client_id}#{file_type}")
    if not item:
        return None
    return item.get("latest_hash", {}).get("S")


def _version_item(client_id: str, file_type: str, file_hash: str):
    return _get_item(f"version#{client_id}#{file_type}#{file_hash}")


def _decode_processed_with(item):
    if not item:
        return {}
    raw = item.get("processed_with", {}).get("S", "{}")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def lambda_handler(event, context):
    file_hash = event["file_hash"]
    key = event["key"]
    client_id = event.get("client_id", "unknown").lower()

    config_key, cfg = _load_client_config(client_id)
    required_patterns = [p.lower() for p in cfg.get("required_patterns", [])]
    optional_patterns = [p.lower() for p in cfg.get("optional_patterns", [])]
    file_type = _infer_file_type(key, required_patterns + optional_patterns)

    version = _version_item(client_id, file_type, file_hash)
    version_status = version.get("status", {}).get("S") if version else "NOT_FOUND"
    processed_with = _decode_processed_with(version)

    current_partner_hashes = {}
    for pattern in required_patterns:
        if pattern == file_type:
            continue
        current_partner_hashes[pattern] = _latest_hash(client_id, pattern)

    already_processed = False
    dedup_reason = "new_or_changed"
    if version_status == "SUCCESS":
        partner_unchanged = all(
            processed_with.get(partner) == current_partner_hashes.get(partner)
            for partner in current_partner_hashes
        )
        if partner_unchanged:
            already_processed = True
            dedup_reason = "same_file_hash_and_same_partner_hashes"
        else:
            dedup_reason = "same_file_hash_but_partner_changed"

    return {
        **event,
        "already_processed": already_processed,
        "dedup_checked_at": datetime.now(timezone.utc).isoformat(),
        "dedup_status": version_status,
        "dedup_reason": dedup_reason,
        "source_object": key,
        "client_id": client_id,
        "file_type": file_type,
        "required_patterns": required_patterns,
        "optional_patterns": optional_patterns,
        "config_key": config_key,
        "current_partner_hashes": current_partner_hashes,
    }
