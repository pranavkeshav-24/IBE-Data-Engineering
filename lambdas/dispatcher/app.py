import json
import os
import re
import hashlib
from urllib.parse import unquote_plus

import boto3


sfn = boto3.client("stepfunctions")
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
KNOWN_CLIENTS = {"alpha", "beta", "gamma", "adscribe"}


def _client_from_key(key: str) -> str:
    parts = key.split("/")
    if len(parts) >= 2 and parts[0] == "raw":
        return parts[1].lower()
    return "unknown"


def _execution_name(client_id: str, key: str, identity_token: str) -> str:
    stable_id = f"{client_id}:{key}:{identity_token}"
    digest = hashlib.sha1(stable_id.encode("utf-8")).hexdigest()[:28]
    key_name = key.rsplit("/", 1)[-1].lower()
    safe_key = re.sub(r"[^a-z0-9-]", "-", key_name).strip("-")
    safe_key = safe_key[:28] if safe_key else "file"
    return f"{client_id}-{safe_key}-{digest}"[:80]


def lambda_handler(event, context):
    records = event.get("Records", [])
    started = []
    ignored_duplicates = []
    skipped_unknown_clients = []

    for record in records:
        if record.get("eventSource") != "aws:s3":
            continue

        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        etag = record["s3"]["object"].get("eTag", "").replace('"', "")
        version_id = record["s3"]["object"].get("versionId", "")
        size = record["s3"]["object"].get("size", 0)
        client_id = _client_from_key(key)
        file_hash = etag or f"{bucket}:{key}:{size}"
        identity_token = version_id or file_hash

        if client_id not in KNOWN_CLIENTS:
            skipped_unknown_clients.append({"bucket": bucket, "key": key})
            continue

        payload = {
            "bucket": bucket,
            "key": key,
            "client_id": client_id,
            "file_hash": file_hash,
            "object_version_id": version_id,
            "object_size": size,
            "event_time": record.get("eventTime"),
        }

        execution_name = _execution_name(client_id, key, identity_token)
        try:
            response = sfn.start_execution(
                stateMachineArn=STATE_MACHINE_ARN,
                name=execution_name,
                input=json.dumps(payload),
            )
            started.append(response["executionArn"])
        except sfn.exceptions.ExecutionAlreadyExists:
            ignored_duplicates.append(execution_name)

    return {
        "started_executions": started,
        "ignored_duplicates": ignored_duplicates,
        "skipped_unknown_clients": skipped_unknown_clients,
        "count": len(started),
    }
