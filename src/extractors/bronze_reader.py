"""
Bronze Reader — read raw JSON objects from MinIO Bronze layer buckets.
Enables true Medallion Architecture: MinIO (Bronze) -> Polars (Transform) -> ClickHouse (Silver).
"""

from __future__ import annotations

import json
import logging
from typing import Any

from src.db.minio_client import get_minio, ensure_bucket
from src.config import settings

logger = logging.getLogger(__name__)


def read_bronze_objects(
    bucket: str | None = None,
    prefix: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """
    Read raw JSON payloads from MinIO Bronze bucket and group them by ticker.

    Args:
        bucket: MinIO bucket name (defaults to 'vn30-bronze-july2026').
        prefix: Optional key prefix filter (e.g., '2026-07-01/').

    Returns:
        Dict {ticker: [raw_dict1, raw_dict2, ...]}.
    """
    target_bucket = bucket or "vn30-bronze-july2026"
    ensure_bucket(target_bucket)
    client = get_minio()

    logger.info("Reading Bronze raw objects from MinIO bucket '%s' (prefix='%s')...", target_bucket, prefix or "")

    raw_by_ticker: dict[str, list[dict[str, Any]]] = {}
    objects = client.list_objects(target_bucket, prefix=prefix, recursive=True)

    count = 0
    for obj in objects:
        try:
            response = client.get_object(target_bucket, obj.object_name)
            content = response.read().decode("utf-8")
            data = json.loads(content)
            response.close()
            response.release_conn()

            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                records = [data]
            else:
                continue

            for record in records:
                ticker = record.get("ticker") or record.get("symbol")
                if not ticker and "/" in obj.object_name:
                    # Fallback infer ticker from filename: YYYY-MM-DD/ACB.json
                    filename = obj.object_name.split("/")[-1]
                    ticker = filename.replace(".json", "")

                if ticker:
                    raw_by_ticker.setdefault(ticker, []).append(record)
                    count += 1

        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to read Bronze object %s/%s: %s", target_bucket, obj.object_name, exc)

    logger.info("Bronze Reader: successfully loaded %d raw records across %d tickers from '%s'.", count, len(raw_by_ticker), target_bucket)
    return raw_by_ticker
