"""
MinIO client wrapper — S3-compatible object storage cho Bronze layer.
Sử dụng minio-py SDK; bucket mặc định: vn30-bronze.
"""

from __future__ import annotations

import io
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error

from src.config import settings

logger = logging.getLogger(__name__)
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


# ---------------------------------------------------------------------------
# Singleton client (thread-safe in minio-py)
# ---------------------------------------------------------------------------

def _make_client() -> Minio:
    cfg = settings.minio
    return Minio(
        endpoint=cfg.endpoint,
        access_key=cfg.access_key,
        secret_key=cfg.secret_key,
        secure=cfg.secure,
    )


_client: Minio | None = None


def get_minio() -> Minio:
    """Return singleton MinIO client (lazy init)."""
    global _client  # noqa: PLW0603
    if _client is None:
        _client = _make_client()
    return _client


# ---------------------------------------------------------------------------
# Bucket helpers
# ---------------------------------------------------------------------------

def ensure_bucket(bucket: str | None = None) -> None:
    """Create bucket if it doesn't exist (idempotent)."""
    bucket = bucket or settings.minio.bucket_bronze
    client = get_minio()
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        logger.info("MinIO: created bucket '%s'.", bucket)


# ---------------------------------------------------------------------------
# Bronze upload helpers
# ---------------------------------------------------------------------------

def upload_json(
    payload: list[dict[str, Any]] | dict[str, Any],
    ticker: str,
    data_type: str = "intraday",
    *,
    timestamp: datetime | None = None,
    bucket: str | None = None,
) -> str:
    """
    Serialize payload to JSON and upload to MinIO Bronze layer.

    Object key pattern:
        {data_type}/{YYYY-MM-DD}/{ticker}/{HH-MM-SS}.json

    Args:
        payload:   Raw data from vnstock3 (list or dict).
        ticker:    Stock ticker, e.g. 'HPG'.
        data_type: Subfolder prefix ('intraday' or 'ohlcv').
        timestamp: Override timestamp (defaults to now()).
        bucket:    Override bucket name.

    Returns:
        Object key that was uploaded.
    """
    ts = timestamp or datetime.now(VN_TZ)
    date_str = ts.strftime("%Y-%m-%d")
    time_str = ts.strftime("%H-%M-%S")
    object_key = f"{data_type}/{date_str}/{ticker}/{time_str}.json"
    bucket = bucket or settings.minio.bucket_bronze

    raw_bytes = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    data_stream = io.BytesIO(raw_bytes)

    try:
        get_minio().put_object(
            bucket_name=bucket,
            object_name=object_key,
            data=data_stream,
            length=len(raw_bytes),
            content_type="application/json",
        )
        logger.debug("MinIO upload: %s/%s (%d bytes)", bucket, object_key, len(raw_bytes))
        return object_key
    except S3Error as exc:
        logger.error("MinIO upload failed for %s: %s", object_key, exc)
        raise


def upload_parquet(
    table: pa.Table,
    object_key: str,
    *,
    bucket: str | None = None,
) -> str:
    """
    Upload a PyArrow Table as Parquet to MinIO.

    Args:
        table:      PyArrow Table to serialize.
        object_key: Full object path inside bucket.
        bucket:     Override bucket name.

    Returns:
        Object key uploaded.
    """
    bucket = bucket or settings.minio.bucket_bronze
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    parquet_bytes = buf.getvalue()

    try:
        get_minio().put_object(
            bucket_name=bucket,
            object_name=object_key,
            data=io.BytesIO(parquet_bytes),
            length=len(parquet_bytes),
            content_type="application/octet-stream",
        )
        logger.debug("MinIO parquet upload: %s/%s", bucket, object_key)
        return object_key
    except S3Error as exc:
        logger.error("MinIO parquet upload failed: %s", exc)
        raise


def ping() -> bool:
    """Return True if MinIO is reachable and bucket exists."""
    try:
        bucket = settings.minio.bucket_bronze
        return get_minio().bucket_exists(bucket)
    except Exception as exc:  # noqa: BLE001
        logger.error("MinIO ping failed: %s", exc)
        return False
