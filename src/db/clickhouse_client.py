"""
ClickHouse client wrapper — connection pool, typed helpers, retry logic.
Sử dụng clickhouse-driver (native protocol port 9000) để đạt throughput cao nhất.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Generator

import polars as pl
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


def insert_df(
    table: str,
    df: pl.DataFrame,
    *,
    column_names: list[str] | None = None,
    retries: int = 3,
) -> int:
    """
    Batch insert a Polars DataFrame into a ClickHouse table.

    Uses INSERT ... FORMAT VALUES for compatibility; columns must match
    the DataFrame schema or the explicit column_names list.

    Args:
        table:        Fully-qualified table name, e.g. 'vn30.silver_intraday_ticks'.
        df:           Polars DataFrame to insert.
        column_names: Override column order (defaults to df.columns).
        retries:      Auto-retry on transient errors.

    Returns:
        Number of rows inserted.
    """
    if df.is_empty():
        logger.debug("insert_df: empty DataFrame for table '%s', skipping.", table)
        return 0

    from datetime import date as dt_date

    cols = column_names or df.columns
    # Convert Polars → list of dicts for clickhouse-driver
    rows: list[dict] = df.select(cols).to_dicts()

    # Normalize ISO string dates -> datetime.date for ClickHouse Date columns
    for row in rows:
        for k, v in row.items():
            if isinstance(v, str) and len(v) == 10 and v[4] == "-" and v[7] == "-":
                try:
                    row[k] = dt_date.fromisoformat(v)
                except ValueError:
                    pass

    attempt = 0
    last_exc: Exception | None = None

    while attempt < retries:
        try:
            with get_client() as client:
                client.execute(
                    f"INSERT INTO {table} ({', '.join(cols)}) VALUES",
                    rows,
                )
            logger.info("insert_df: inserted %d rows into '%s'.", len(rows), table)
            return len(rows)
        except (NetworkError, SocketTimeoutError) as exc:
            attempt += 1
            last_exc = exc
            logger.warning(
                "insert_df network error (attempt %d/%d): %s", attempt, retries, exc
            )

    raise RuntimeError(
        f"insert_df into '{table}' failed after {retries} retries"
    ) from last_exc


def ping() -> bool:
    """Return True if ClickHouse is reachable."""
    try:
        result = execute("SELECT 1")
        return result == [(1,)]
    except Exception as exc:  # noqa: BLE001
        logger.error("ClickHouse ping failed: %s", exc)
        return False
