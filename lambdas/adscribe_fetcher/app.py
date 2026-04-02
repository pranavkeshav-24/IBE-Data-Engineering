import json
import os
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import boto3


s3 = boto3.client("s3")

API_ENDPOINT = os.environ["ADSCRIBE_API_ENDPOINT"]
RAW_BUCKET = os.environ["RAW_BUCKET"]
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/adscribe")
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "1"))
MAX_RANGE_DAYS = 7


def _http_json(url: str, payload: dict) -> dict:
    body = json.dumps(payload).encode("utf-8")
    req = Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _clamped_range(today_utc: date):
    window_start = date(today_utc.year, 3, 15)
    window_end = date(today_utc.year, 4, 15)

    end_date = min(today_utc - timedelta(days=1), window_end)
    if end_date < window_start:
        return None

    requested_days = max(1, min(LOOKBACK_DAYS, MAX_RANGE_DAYS))
    start_date = end_date - timedelta(days=requested_days - 1)
    if start_date < window_start:
        start_date = window_start
    if (end_date - start_date).days + 1 > MAX_RANGE_DAYS:
        start_date = end_date - timedelta(days=MAX_RANGE_DAYS - 1)

    return start_date, end_date


def _upload_csv_from_url(download_url: str, key: str):
    parsed = urlparse(download_url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("Invalid download URL scheme")

    with urlopen(download_url, timeout=900) as source:
        s3.upload_fileobj(
            source,
            RAW_BUCKET,
            key,
            ExtraArgs={"ContentType": "text/csv"},
        )


def lambda_handler(event, context):
    today = datetime.now(timezone.utc).date()
    clamped = _clamped_range(today)
    if not clamped:
        return {
            "status": "skipped",
            "reason": "outside_allowed_adscribe_date_window",
            "today": today.isoformat(),
        }

    start_date, end_date = clamped
    payload = {
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
    }

    key = f"{RAW_PREFIX}/{end_date.strftime('%Y-%m-%d')}/adscribe_{payload['start_date']}_{payload['end_date']}.csv"

    # Retry once with a fresh presigned URL if the first URL expires or download fails.
    for attempt in range(2):
        api_response = _http_json(API_ENDPOINT, payload)
        download_url = api_response.get("download_url")
        if not download_url:
            raise RuntimeError(f"download_url missing in API response: {api_response}")

        try:
            _upload_csv_from_url(download_url, key)
            break
        except Exception:
            if attempt == 1:
                raise

    return {
        "status": "uploaded",
        "bucket": RAW_BUCKET,
        "key": key,
        "request": payload,
    }
