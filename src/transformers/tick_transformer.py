"""
Tick transformer — làm sạch và chuẩn hóa dữ liệu khớp lệnh intraday.
Input : dict thô từ vnstock3 API.
Output: polars.DataFrame sẵn sàng insert vào silver_intraday_ticks.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import polars as pl

from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# ---------------------------------------------------------------------------
# Column mapping — vnstock3 API field → schema field
# Điều chỉnh nếu vnstock3 thay đổi tên cột.
# ---------------------------------------------------------------------------
_FIELD_MAP: dict[str, str] = {
    # Timestamp
    "time":          "match_time",
    "match_time":    "match_time",
    "t":             "match_time",
    # Price
    "price":         "price",
    "matchPrice":    "price",
    "p":             "price",
    # Volume
    "vol":           "volume",
    "volume":        "volume",
    "matchVolume":   "volume",
    "v":             "volume",
    # Side / Action
    "side":          "action",
    "action":        "action",
    "type":          "action",
    "cp":            "action",
    "match_type":    "action",
}

# Mapping raw side values → canonical Enum
_ACTION_MAP: dict[str, str] = {
    "b":    "BUY",
    "buy":  "BUY",
    "bu":   "BUY",
    "1":    "BUY",
    "s":    "SELL",
    "sell": "SELL",
    "sd":   "SELL",
    "2":    "SELL",
}

# Output schema (must match silver_intraday_ticks DDL)
_OUTPUT_SCHEMA: dict[str, pl.DataType] = {
    "ticker":     pl.Utf8,
    "match_time": pl.Datetime("us", "Asia/Ho_Chi_Minh"),
    "price":      pl.Float64,
    "volume":     pl.UInt64,
    "action":     pl.Utf8,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def transform_ticks(
    raw_data: dict[str, list[dict[str, Any]]],
    *,
    drop_zero: bool = True,
) -> pl.DataFrame:
    """
    Transform raw tick data dict thành Polars DataFrame sạch.

    Args:
        raw_data:   Dict {ticker: [raw_tick_dicts]} từ intraday extractor.
        drop_zero:  Nếu True, bỏ các row có price=0 hoặc volume=0 (lỗi API).

    Returns:
        Polars DataFrame với schema:
            ticker      : Utf8
            match_time  : Datetime (GMT+7)
            price       : Float64
            volume      : UInt64
            action      : Utf8 ("BUY" | "SELL" | "UNKNOWN")
    """
    if not raw_data:
        return _empty_df()

    frames: list[pl.DataFrame] = []

    for ticker, rows in raw_data.items():
        if not rows:
            continue

        df = _normalize_rows(ticker, rows)
        if df is not None and not df.is_empty():
            frames.append(df)

    if not frames:
        logger.debug("transform_ticks: all tickers returned empty data.")
        return _empty_df()

    result = pl.concat(frames, how="vertical")

    if drop_zero:
        before = len(result)
        result = result.filter(
            (pl.col("price") > 0) & (pl.col("volume") > 0)
        )
        dropped = before - len(result)
        if dropped:
            logger.debug("Dropped %d zero-price/volume rows.", dropped)

    logger.info(
        "transform_ticks: %d clean rows from %d tickers.",
        len(result), len(raw_data)
    )
    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _normalize_rows(ticker: str, rows: list[dict[str, Any]]) -> pl.DataFrame | None:
    """Normalize one ticker's raw rows to the output schema."""
    try:
        # Remap field names
        normalized: list[dict[str, Any]] = []
        for row in rows:
            mapped: dict[str, Any] = {"ticker": ticker}
            for raw_key, value in row.items():
                canonical = _FIELD_MAP.get(raw_key)
                if canonical and canonical not in mapped:
                    mapped[canonical] = value
            normalized.append(mapped)

        df = pl.DataFrame(normalized)

        # --- match_time ---
        df = _parse_match_time(df)

        # --- price ---
        if "price" in df.columns:
            df = df.with_columns(pl.col("price").cast(pl.Float64))
        else:
            logger.warning("Ticker %s: missing 'price' column.", ticker)
            return None

        # --- volume ---
        if "volume" in df.columns:
            df = df.with_columns(pl.col("volume").cast(pl.UInt64))
        else:
            logger.warning("Ticker %s: missing 'volume' column.", ticker)
            return None

        # --- action ---
        df = _normalize_action(df)

        # Select & reorder final columns
        return df.select(list(_OUTPUT_SCHEMA.keys()))

    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to normalize ticker %s: %s", ticker, exc)
        return None


def _parse_match_time(df: pl.DataFrame) -> pl.DataFrame:
    """Parse match_time — handles epoch int, epoch ms, and ISO string."""
    if "match_time" not in df.columns:
        # Fallback: use current time
        logger.debug("No match_time column — using current timestamp.")
        return df.with_columns(
            pl.lit(datetime.now(VN_TZ)).cast(pl.Datetime("us", "Asia/Ho_Chi_Minh")).alias("match_time")
        )

    col = df["match_time"]

    if col.dtype in (pl.Int64, pl.Int32, pl.UInt64):
        # Epoch seconds or milliseconds
        if col.max() > 1e12:  # milliseconds
            df = df.with_columns(
                (pl.col("match_time") * 1000)
                .cast(pl.Datetime("us"))
                .dt.convert_time_zone("Asia/Ho_Chi_Minh")
                .alias("match_time")
            )
        else:  # seconds
            df = df.with_columns(
                pl.from_epoch(pl.col("match_time"), time_unit="s")
                .dt.convert_time_zone("Asia/Ho_Chi_Minh")
                .alias("match_time")
            )
    elif col.dtype == pl.Utf8:
        df = df.with_columns(
            pl.col("match_time")
            .str.strptime(pl.Datetime("us"), format="%Y-%m-%d %H:%M:%S", strict=False)
            .dt.convert_time_zone("Asia/Ho_Chi_Minh")
            .alias("match_time")
        )

    return df


def _normalize_action(df: pl.DataFrame) -> pl.DataFrame:
    """Map raw action/side values → 'BUY' | 'SELL' | 'UNKNOWN'."""
    if "action" not in df.columns:
        return df.with_columns(pl.lit("UNKNOWN").alias("action"))

    return df.with_columns(
        pl.col("action")
        .cast(pl.Utf8)
        .str.to_lowercase()
        .str.strip_chars()
        .map_elements(lambda v: _ACTION_MAP.get(v, "UNKNOWN"), return_dtype=pl.Utf8)
        .alias("action")
    )


def _empty_df() -> pl.DataFrame:
    """Return an empty DataFrame with the correct schema."""
    return pl.DataFrame(
        {col: pl.Series(col, [], dtype=dtype) for col, dtype in _OUTPUT_SCHEMA.items()}
    )
