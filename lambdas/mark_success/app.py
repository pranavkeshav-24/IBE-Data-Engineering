import json
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError


dynamodb = boto3.client("dynamodb")
TABLE_NAME = os.environ["DEDUP_TABLE_NAME"]


def _put_item(item):
    dynamodb.put_item(TableName=TABLE_NAME, Item=item)


def lambda_handler(event, context):
    now = datetime.now(timezone.utc).isoformat()
    now_epoch = int(datetime.now(timezone.utc).timestamp())
    client_id = event.get("client_id", "unknown").lower()
    run_id = event.get("run_id", "")
    input_files = event.get("input_files", {})
    input_hashes = event.get("input_hashes", {})
    required_patterns = event.get("required_patterns") or list(input_hashes.keys())

    if not input_hashes and event.get("file_hash"):
        input_hashes = {"source": event["file_hash"]}
        input_files = {"source": f"s3://{event.get('bucket', '')}/{event.get('key', '')}"}
        required_patterns = ["source"]

    for file_type, file_hash in input_hashes.items():
        if not file_hash:
            continue

        processed_with = {
            partner: partner_hash
            for partner, partner_hash in input_hashes.items()
            if partner != file_type and partner in required_patterns
        }
        source_uri = input_files.get(file_type, "")

        _put_item(
            {
                "file_hash": {"S": f"version#{client_id}#{file_type}#{file_hash}"},
                "status": {"S": "SUCCESS"},
                "client_id": {"S": client_id},
                "file_type": {"S": file_type},
                "content_hash": {"S": file_hash},
                "source_object": {"S": source_uri},
                "run_id": {"S": run_id},
                "processed_with": {"S": json.dumps(processed_with, separators=(",", ":"))},
                "updated_at": {"S": now},
            }
        )

        _put_item(
            {
                "file_hash": {"S": f"latest#{client_id}#{file_type}"},
                "status": {"S": "SUCCESS"},
                "client_id": {"S": client_id},
                "file_type": {"S": file_type},
                "latest_hash": {"S": file_hash},
                "run_id": {"S": run_id},
                "updated_at": {"S": now},
            }
        )

    if required_patterns:
        pair_hash_parts = []
        for pattern in sorted(required_patterns):
            pair_hash_parts.append(f"{pattern}:{input_hashes.get(pattern, '')}")
        pair_fingerprint = "|".join(pair_hash_parts)
        _put_item(
            {
                "file_hash": {"S": f"pair#{client_id}#{pair_fingerprint}"},
                "status": {"S": "SUCCESS"},
                "client_id": {"S": client_id},
                "run_id": {"S": run_id},
                "input_hashes": {"S": json.dumps(input_hashes, separators=(",", ":"))},
                "updated_at": {"S": now},
            }
        )

    # Compatibility record for existing traces/queries.
    if event.get("file_hash"):
        _put_item(
            {
                "file_hash": {"S": event["file_hash"]},
                "status": {"S": "SUCCESS"},
                "client_id": {"S": client_id},
                "source_object": {"S": event.get("key", "")},
                "run_id": {"S": run_id},
                "updated_at": {"S": now},
            }
        )

    lock_key = event.get("lock_key") or f"lock#{client_id}"
    try:
        dynamodb.update_item(
            TableName=TABLE_NAME,
            Key={"file_hash": {"S": lock_key}},
            UpdateExpression="SET #s = :released, updated_at = :u, expires_at = :e",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":released": {"S": "RELEASED"},
                ":u": {"S": now},
                ":e": {"N": str(now_epoch - 1)},
            },
        )
    except ClientError:
        pass

    return {**event, "marked_success_at": now}
