"""
VWAP (Volume Weighted Average Price) Calculator.

Công thức: VWAP = Sum(Price * Volume) / Sum(Volume) — lũy kế trong ngày.
Signal: Giá cắt từ dưới lên VWAP = BUY, từ trên xuống = SELL.

Dùng Polars scan để xử lý hiệu quả trên dữ liệu lớn.
"""

from __future__ import annotations

import logging
from datetime import date
from enum import Enum

import polars as pl

logger = logging.getLogger(__name__)


class VwapSignal(Enum):
    BUY = "BUY"       # Giá cắt lên trên VWAP (crossover up)
    SELL = "SELL"     # Giá cắt xuống dưới VWAP (crossover down)
    NONE = "NONE"     # Không có crossover


def compute_vwap(df: pl.DataFrame) -> pl.DataFrame:
    """
    Tính VWAP lũy kế trong ngày cho mỗi ticker.

    Args:
        df: Polars DataFrame với các cột:
            - ticker (Utf8)
            - match_time (Datetime)
            - price (Float64)
            - volume (UInt64)

    Returns:
        DataFrame gốc + thêm cột:
            - cum_pv    : lũy kế price * volume
            - cum_vol   : lũy kế volume
            - vwap      : VWAP tại mỗi tick
            - vwap_signal: 'BUY' | 'SELL' | 'NONE' — crossover tại điểm đó
    """
    if df.is_empty():
        return df.with_columns([
            pl.lit(0.0).alias("cum_pv"),
            pl.lit(0).cast(pl.UInt64).alias("cum_vol"),
            pl.lit(0.0).alias("vwap"),
            pl.lit("NONE").alias("vwap_signal"),
        ])

    result = (
        df
        .sort(["ticker", "match_time"])
        .with_columns([
            (pl.col("price") * pl.col("volume")).alias("_pv"),
        ])
        .with_columns([
            pl.col("_pv").cum_sum().over("ticker").alias("cum_pv"),
            pl.col("volume").cast(pl.Float64).cum_sum().over("ticker").alias("cum_vol"),
        ])
        .with_columns([
            (pl.col("cum_pv") / pl.col("cum_vol").replace(0.0, None))
            .fill_null(strategy="forward")
            .alias("vwap")
        ])
        .drop("_pv")
    )

    # Detect crossover: prev_price < prev_vwap AND curr_price >= curr_vwap → BUY
    result = result.with_columns([
        pl.col("price").shift(1).over("ticker").alias("_prev_price"),
        pl.col("vwap").shift(1).over("ticker").alias("_prev_vwap"),
    ]).with_columns([
        pl.when(
            (pl.col("_prev_price") < pl.col("_prev_vwap")) &
            (pl.col("price") >= pl.col("vwap"))
        ).then(pl.lit("BUY"))
        .when(
            (pl.col("_prev_price") > pl.col("_prev_vwap")) &
            (pl.col("price") <= pl.col("vwap"))
        ).then(pl.lit("SELL"))
        .otherwise(pl.lit("NONE"))
        .alias("vwap_signal")
    ]).drop(["_prev_price", "_prev_vwap"])

    crossovers = result.filter(pl.col("vwap_signal") != "NONE")
    logger.info(
        "compute_vwap: %d rows processed, %d crossover signals found.",
        len(result), len(crossovers)
    )
    return result


def compute_vwap_1min(df: pl.DataFrame) -> pl.DataFrame:
    """
    Tính VWAP trên nến 1 phút (từ gold_ohlcv_1min schema).

    Args:
        df: DataFrame với cột: ticker, window_start, close, volume

    Returns:
        DataFrame + cột vwap, vwap_signal.
    """
    if df.is_empty():
        return df

    return (
        df
        .sort(["ticker", "window_start"])
        .with_columns([
            (pl.col("close") * pl.col("volume")).alias("_pv"),
        ])
        .with_columns([
            pl.col("_pv").cum_sum().over("ticker").alias("cum_pv"),
            pl.col("volume").cast(pl.Float64).cum_sum().over("ticker").alias("cum_vol"),
        ])
        .with_columns([
            (pl.col("cum_pv") / pl.col("cum_vol").replace(0.0, None))
            .fill_null(strategy="forward")
            .alias("vwap")
        ])
        .drop("_pv")
        .with_columns([
            pl.col("close").shift(1).over("ticker").alias("_prev_close"),
            pl.col("vwap").shift(1).over("ticker").alias("_prev_vwap"),
        ])
        .with_columns([
            pl.when(
                (pl.col("_prev_close") < pl.col("_prev_vwap")) &
                (pl.col("close") >= pl.col("vwap"))
            ).then(pl.lit("BUY"))
            .when(
                (pl.col("_prev_close") > pl.col("_prev_vwap")) &
                (pl.col("close") <= pl.col("vwap"))
            ).then(pl.lit("SELL"))
            .otherwise(pl.lit("NONE"))
            .alias("vwap_signal")
        ])
        .drop(["_prev_close", "_prev_vwap"])
    )


def get_active_signals(df_with_vwap: pl.DataFrame) -> pl.DataFrame:
    """
    Lọc ra các rows có VWAP crossover signal (BUY hoặc SELL).

    Returns:
        Subset DataFrame chỉ chứa crossover rows, sắp xếp theo thời gian.
    """
    return (
        df_with_vwap
        .filter(pl.col("vwap_signal") != "NONE")
        .sort(["ticker", "match_time"] if "match_time" in df_with_vwap.columns
              else ["ticker", "window_start"])
    )
