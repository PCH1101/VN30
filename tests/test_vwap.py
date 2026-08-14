"""Unit tests cho src/analytics/vwap.py — không cần Docker."""

from datetime import datetime, timezone

import polars as pl

from src.analytics.vwap import VwapSignal, compute_vwap


def _tick_df(prices, volumes):
    n = len(prices)
    return pl.DataFrame({
        "ticker": ["ACB"] * n,
        "match_time": [datetime(2026, 8, 14, 9, 15, tzinfo=timezone.utc)] * n,
        "price": prices,
        "volume": volumes,
    })


def test_compute_vwap_empty():
    df = pl.DataFrame({
        "ticker": [],
        "match_time": [],
        "price": [],
        "volume": [],
    }, schema={
        "ticker": pl.Utf8,
        "match_time": pl.Datetime,
        "price": pl.Float64,
        "volume": pl.UInt64,
    })
    result = compute_vwap(df)
    assert result.is_empty()
    assert "vwap" in result.columns


def test_compute_vwap_accumulation():
    df = _tick_df([10.0, 20.0], [100, 100])
    result = compute_vwap(df).sort("match_time")
    vwap = result["vwap"].to_list()
    assert vwap == [10.0, 15.0]


def test_compute_vwap_buy_signal():
    df = _tick_df([20.0, 10.0, 15.0], [100, 100, 100])
    result = compute_vwap(df).sort("match_time")
    signals = result["vwap_signal"].to_list()
    assert VwapSignal.BUY.value in signals
