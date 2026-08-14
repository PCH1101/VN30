"""
OHLCV transformer — chuẩn hóa dữ liệu nến ngày chốt phiên.
Input : dict thô từ vnstock3 daily OHLCV API.
Output: polars.DataFrame sẵn sàng insert vào silver_daily_ohlcv.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)

_OHLCV_FIELD_MAP: dict[str, str] = {
    # Date
    "time":       "trade_date",
    "date":       "trade_date",
    "t":          "trade_date",
    # OHLCV
    "open":       "open_price",
    "high":       "high_price",
    "low":        "low_price",
    "close":      "close_price",
    "volume":     "total_volume",
    "value":      "total_value",
}

_OUTPUT_SCHEMA: dict[str, pl.DataType] = {
    "ticker":           pl.Utf8,
    "trade_date":       pl.Date,
    "open_price":       pl.Float64,
    "high_price":       pl.Float64,
    "low_price":        pl.Float64,
    "close_price":      pl.Float64,
    "total_volume":     pl.UInt64,
    "total_value":      pl.Float64,
    "price_change":     pl.Float64,
    "price_change_pct": pl.Float32,
}


def transform_ohlcv(
    raw_data: dict[str, list[dict[str, Any]]],
) -> pl.DataFrame:
    """
    Transform raw OHLCV dict thành Polars DataFrame sạch.

    Args:
        raw_data: Dict {ticker: [ohlcv_dicts]} từ daily_ohlcv extractor.

    Returns:
        Polars DataFrame với schema silver_daily_ohlcv.
    """
    if not raw_data:
        return _empty_ohlcv_df()

    frames: list[pl.DataFrame] = []

    for ticker, rows in raw_data.items():
        if not rows:
            continue
        df = _normalize_ohlcv(ticker, rows)
        if df is not None and not df.is_empty():
            frames.append(df)

    if not frames:
        return _empty_ohlcv_df()

    result = (
        pl.concat(frames, how="vertical")
        .sort(["ticker", "trade_date"])
    )

    logger.info("transform_ohlcv: %d rows from %d tickers.", len(result), len(raw_data))
    return result


def _normalize_ohlcv(ticker: str, rows: list[dict[str, Any]]) -> pl.DataFrame | None:
    """Normalize one ticker's OHLCV rows."""
    try:
        normalized: list[dict[str, Any]] = []
        for row in rows:
            mapped: dict[str, Any] = {"ticker": ticker}
            for raw_key, value in row.items():
                canonical = _OHLCV_FIELD_MAP.get(raw_key.lower(), raw_key)
                if canonical not in mapped:
                    mapped[canonical] = value
            normalized.append(mapped)

        df = pl.DataFrame(normalized)

        # trade_date
        if "trade_date" in df.columns:
            df = df.with_columns(
                pl.col("trade_date").cast(pl.Utf8)
                .str.slice(0, 10)
                .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                .alias("trade_date")
            )
        else:
            logger.warning("Ticker %s: missing trade_date.", ticker)
            return None

        # Numeric casts
        for col, dtype in [
            ("open_price",  pl.Float64),
            ("high_price",  pl.Float64),
            ("low_price",   pl.Float64),
            ("close_price", pl.Float64),
            ("total_value", pl.Float64),
        ]:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(dtype, strict=False))
            else:
                df = df.with_columns(pl.lit(0.0).cast(dtype).alias(col))

        if "total_volume" in df.columns:
            df = df.with_columns(pl.col("total_volume").cast(pl.UInt64, strict=False))
        else:
            df = df.with_columns(pl.lit(0).cast(pl.UInt64).alias("total_volume"))

        # Compute price_change & price_change_pct (within-ticker window)
        df = df.sort("trade_date").with_columns([
            (pl.col("close_price") - pl.col("close_price").shift(1))
            .fill_null(0.0).alias("price_change"),
            (
                (pl.col("close_price") - pl.col("close_price").shift(1))
                / pl.col("close_price").shift(1).replace(0.0, None)
                * 100
            ).fill_null(0.0).cast(pl.Float32).alias("price_change_pct"),
        ])

        return df.select(list(_OUTPUT_SCHEMA.keys()))

    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to normalize OHLCV for %s: %s", ticker, exc)
        return None


def _empty_ohlcv_df() -> pl.DataFrame:
    return pl.DataFrame(
        {col: pl.Series(col, [], dtype=dtype) for col, dtype in _OUTPUT_SCHEMA.items()}
    )
