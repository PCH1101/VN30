"""
ClickHouse client wrapper — connection pool, typed helpers, retry logic.
Uses clickhouse-driver (native protocol port 9000).
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Generator

from clickhouse_driver import Client
from clickhouse_driver.errors import NetworkError, SocketTimeoutError

from src.config import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connection factory
# ---------------------------------------------------------------------------

def _make_client() -> Client:
    """Create a new ClickHouse native-protocol client."""
    cfg = settings.clickhouse
    return Client(
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        user=cfg.user,
        password=cfg.password,
        settings={
            "use_numpy": False,
            "insert_block_size": 100_000,
        },
        connect_timeout=10,
        send_receive_timeout=30,
    )


@contextmanager
def get_client() -> Generator[Client, None, None]:
    """Context manager: yields a connected Client, closes on exit."""
    client = _make_client()
    try:
        yield client
    finally:
        client.disconnect()


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def execute(
    query: str,
    params: dict[str, Any] | None = None,
    *,
    retries: int = 3,
) -> list[tuple]:
    """
    Execute a SELECT / DDL query and return rows.

    Args:
        query:   ClickHouse SQL string (use %(name)s placeholders).
        params:  Dict of named parameters.
        retries: Auto-retry on transient network errors.

    Returns:
        List of row tuples.
    """
    attempt = 0
    last_exc: Exception | None = None

    while attempt < retries:
        try:
            with get_client() as client:
                return client.execute(query, params or {})
        except (NetworkError, SocketTimeoutError) as exc:
            attempt += 1
            last_exc = exc
            logger.warning("ClickHouse network error (attempt %d/%d): %s", attempt, retries, exc)

    raise RuntimeError(f"ClickHouse query failed after {retries} retries") from last_exc


def insert_rows(
    table: str,
    rows: list[dict[str, Any]],
    *,
    columns: list[str] | None = None,
    retries: int = 3,
) -> int:
    """
    Batch insert a list of dicts into a ClickHouse table.

    Args:
        table:   Target table name, e.g. 'vn30.raw_intraday_ticks'.
        rows:    List of row dicts.
        columns: Optional column names.
        retries: Auto-retry count.

    Returns:
        Number of rows inserted.
    """
    if not rows:
        return 0

    cols = columns or list(rows[0].keys())
    attempt = 0
    last_exc: Exception | None = None

    while attempt < retries:
        try:
            with get_client() as client:
                client.execute(
                    f"INSERT INTO {table} ({', '.join(cols)}) VALUES",
                    rows,
                )
            logger.info("insert_rows: inserted %d rows into '%s'.", len(rows), table)
            return len(rows)
        except (NetworkError, SocketTimeoutError) as exc:
            attempt += 1
            last_exc = exc
            logger.warning("insert_rows network error (attempt %d/%d): %s", attempt, retries, exc)

    raise RuntimeError(f"insert_rows into '{table}' failed after {retries} retries") from last_exc


def ping() -> bool:
    """Return True if ClickHouse is reachable."""
    try:
        result = execute("SELECT 1")
        return result == [(1,)]
    except Exception as exc:  # noqa: BLE001
        logger.error("ClickHouse ping failed: %s", exc)
        return False
