"""
Silver loader — batch insert Polars DataFrames vào ClickHouse Silver tables.
Dùng INSERT INTO ... FORMAT JSONEachRow qua clickhouse_client.insert_df().
ReplacingMergeTree tự dedup khi OPTIMIZE hoặc FINAL query.
"""

from __future__ import annotations

import logging

import polars as pl

from src.db.clickhouse_client import insert_df

logger = logging.getLogger(__name__)

_INTRADAY_TABLE = "vn30.silver_intraday_ticks"
_OHLCV_TABLE = "vn30.silver_daily_ohlcv"
_INTRADAY_COLS = ["ticker", "match_time", "price", "volume", "action"]
_OHLCV_COLS = [
    "ticker", "trade_date", "open_price", "high_price",
    "low_price", "close_price", "total_volume", "total_value",
    "price_change", "price_change_pct",
]


def load_intraday_silver(df: pl.DataFrame) -> int:
    """
    Insert tick DataFrame vào silver_intraday_ticks.

    ReplacingMergeTree dedup theo ORDER KEY (ticker, match_time, price, volume) —
    insert lại cùng data là idempotent.

    Args:
        df: Polars DataFrame từ tick_transformer.transform_ticks().

    Returns:
        Số rows đã insert (0 nếu DataFrame rỗng).
    """
    if df.is_empty():
        logger.debug("load_intraday_silver: empty DataFrame, skipping.")
        return 0

    # Chuyển Datetime timezone-aware → naive UTC để clickhouse-driver nhận
    df_clean = _prepare_datetime_col(df, "match_time")

    rows = insert_df(_INTRADAY_TABLE, df_clean, column_names=_INTRADAY_COLS)
    logger.info("Silver intraday: inserted %d rows.", rows)
    return rows


def load_ohlcv_silver(df: pl.DataFrame) -> int:
    """
    Insert OHLCV DataFrame vào silver_daily_ohlcv.

    Args:
        df: Polars DataFrame từ ohlcv_transformer.transform_ohlcv().

    Returns:
        Số rows đã insert.
    """
    if df.is_empty():
        logger.debug("load_ohlcv_silver: empty DataFrame, skipping.")
        return 0

    # Polars Date → Python date objects for clickhouse-driver
    df_clean = df.with_columns(
        pl.col("trade_date").cast(pl.Utf8).alias("trade_date")
    )

    rows = insert_df(_OHLCV_TABLE, df_clean, column_names=_OHLCV_COLS)
    logger.info("Silver OHLCV: inserted %d rows.", rows)
    return rows


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _prepare_datetime_col(df: pl.DataFrame, col_name: str) -> pl.DataFrame:
    """
    Convert timezone-aware Datetime → naive UTC string.
    clickhouse-driver expects naive datetime objects.
    """
    if col_name not in df.columns:
        return df

    dtype = df[col_name].dtype
    if isinstance(dtype, pl.Datetime) and dtype.time_zone:
        # Convert to UTC then strip timezone
        df = df.with_columns(
            pl.col(col_name)
            .dt.convert_time_zone("UTC")
            .dt.replace_time_zone(None)
            .alias(col_name)
        )
    return df
