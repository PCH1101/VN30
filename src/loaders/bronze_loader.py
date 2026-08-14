"""
Bronze loader — lưu raw API payload vào MinIO object storage.
Mỗi batch = 1 JSON file per ticker, đường dẫn theo convention:
    intraday/{YYYY-MM-DD}/{ticker}/{HH-MM-SS}.json
"""

from __future__ import annotations

import io
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any

from src.db.minio_client import upload_json, ensure_bucket, get_minio
from src.config import settings

logger = logging.getLogger(__name__)
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")



def save_intraday_bronze(
    raw_data: dict[str, list[dict[str, Any]]],
    *,
    timestamp: datetime | None = None,
    bucket: str | None = None,
) -> dict[str, str]:
    """
    Upload raw intraday tick data cho từng ticker lên MinIO Bronze layer.

    Args:
        raw_data:   {ticker: [raw_tick_dicts]} từ extractor.
        timestamp:  Override timestamp (defaults to now()).
        bucket:     Override MinIO bucket.

    Returns:
        Dict {ticker: object_key} cho những ticker đã upload thành công.
    """
    if not raw_data:
        return {}

    bucket = bucket or settings.minio.bucket_bronze
    ensure_bucket(bucket)

    ts = timestamp or datetime.now(VN_TZ)
    uploaded: dict[str, str] = {}

    for ticker, rows in raw_data.items():
        if not rows:
            logger.debug("Skipping Bronze upload for %s — empty payload.", ticker)
            continue
        try:
            key = upload_json(rows, ticker=ticker, data_type="intraday", timestamp=ts, bucket=bucket)
            uploaded[ticker] = key
        except Exception as exc:  # noqa: BLE001
            logger.error("Bronze upload failed for %s: %s", ticker, exc)

    logger.info("Bronze: uploaded %d/%d tickers.", len(uploaded), len(raw_data))
    return uploaded


def save_ohlcv_bronze(
    raw_data: dict[str, list[dict[str, Any]]],
    *,
    timestamp: datetime | None = None,
    bucket: str | None = None,
) -> dict[str, str]:
    """
    Upload raw EOD OHLCV data lên MinIO Bronze layer.

    Returns:
        Dict {ticker: object_key}.
    """
    if not raw_data:
        return {}

    bucket = bucket or settings.minio.bucket_bronze
    ensure_bucket(bucket)

    ts = timestamp or datetime.now(VN_TZ)
    uploaded: dict[str, str] = {}

    for ticker, rows in raw_data.items():
        if not rows:
            continue
        try:
            key = upload_json(rows, ticker=ticker, data_type="ohlcv", timestamp=ts, bucket=bucket)
            uploaded[ticker] = key
        except Exception as exc:  # noqa: BLE001
            logger.error("OHLCV Bronze upload failed for %s: %s", ticker, exc)

    logger.info("OHLCV Bronze: uploaded %d/%d tickers.", len(uploaded), len(raw_data))
    return uploaded


def save_july_2026_daily_bronze(
    raw_data: dict[str, list[dict[str, Any]]],
    *,
    bucket: str = "vn30-bronze-july2026",
) -> int:
    """
    Upload raw July 2026 OHLCV data split by individual trading date into dedicated MinIO bucket.

    Object key pattern:
        {YYYY-MM-DD}/{ticker}.json (e.g. 2026-07-01/ACB.json)

    Args:
        raw_data: Dict {ticker: [ohlcv_dicts]}.
        bucket:   Target MinIO bucket (defaults to 'vn30-bronze-july2026').

    Returns:
        Number of daily ticker objects uploaded.
    """
    if not raw_data:
        return 0

    ensure_bucket(bucket)
    client = get_minio()
    count = 0

    for ticker, rows in raw_data.items():
        for row in rows:
            raw_date = row.get("date") or row.get("time") or row.get("t")
            if not raw_date:
                continue
            date_str = str(raw_date)[:10]  # Format: YYYY-MM-DD

            # Build enriched daily payload
            daily_payload = {"ticker": ticker, "trade_date": date_str, **row}
            object_key = f"{date_str}/{ticker}.json"

            raw_bytes = json.dumps(daily_payload, ensure_ascii=False, default=str).encode("utf-8")
            data_stream = io.BytesIO(raw_bytes)

            try:
                client.put_object(
                    bucket_name=bucket,
                    object_name=object_key,
                    data=data_stream,
                    length=len(raw_bytes),
                    content_type="application/json",
                )
                count += 1
            except Exception as exc:  # noqa: BLE001
                logger.error("Failed to upload %s/%s: %s", bucket, object_key, exc)

    logger.info("Bronze July 2026: uploaded %d daily ticker objects to bucket '%s'.", count, bucket)
    return count

