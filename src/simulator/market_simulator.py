"""
Market Data Simulator — generate synthetic OHLCV and intraday tick data for VN30.
Default scope: Entire month of July 2026 (2026-07-01 to 2026-07-31, 23 trading days).
"""

from __future__ import annotations

import logging
import random
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from src.config import settings

logger = logging.getLogger(__name__)
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

_SEED_PRICES: dict[str, float] = {
    "ACB": 24_500.0, "BCM": 68_000.0, "BID": 49_000.0, "BVH": 42_000.0, "CTG": 35_000.0,
    "FPT": 130_000.0, "GAS": 78_000.0, "GVR": 34_000.0, "HDB": 23_500.0, "HPG": 28_000.0,
    "MBB": 23_000.0, "MSN": 75_000.0, "MWG": 62_000.0, "PLX": 38_000.0, "POW": 11_500.0,
    "SAB": 56_000.0, "SHB": 11_000.0, "SSB": 22_000.0, "SSI": 36_000.0, "STB": 30_000.0,
    "TCB": 46_000.0, "TPB": 18_000.0, "VCB": 92_000.0, "VHM": 40_000.0, "VIB": 21_000.0,
    "VIC": 44_000.0, "VJC": 105_000.0, "VNM": 67_000.0, "VPB": 19_000.0, "VRE": 20_000.0,
}


def get_july_2026_trading_days(
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[date]:
    start = start_date or date(2026, 7, 1)
    end = end_date or date(2026, 7, 31)

    trading_days: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            trading_days.append(current)
        current += timedelta(days=1)

    return trading_days


def generate_simulated_ohlcv(
    tickers: list[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, list[dict[str, Any]]]:
    target_tickers = tickers or settings.vn30_tickers
    trading_days = get_july_2026_trading_days(start_date, end_date)

    data: dict[str, list[dict[str, Any]]] = {}

    for ticker in target_tickers:
        base_price = _SEED_PRICES.get(ticker, 30_000.0)
        rows: list[dict[str, Any]] = []
        current_close = base_price

        for t_date in trading_days:
            daily_change_pct = random.uniform(-0.025, 0.025)
            open_price = round(current_close * (1 + random.uniform(-0.005, 0.005)), -2)
            close_price = round(open_price * (1 + daily_change_pct), -2)
            high_price = max(open_price, close_price) + round(abs(open_price * random.uniform(0.002, 0.015)), -2)
            low_price = min(open_price, close_price) - round(abs(open_price * random.uniform(0.002, 0.015)), -2)

            volume = random.randint(500_000, 5_000_000)
            value = round(close_price * volume, 2)

            rows.append({
                "date": t_date.strftime("%Y-%m-%d"),
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
                "value": value,
            })
            current_close = close_price

        data[ticker] = rows

    return data


def generate_simulated_ticks(
    tickers: list[str] | None = None,
    target_date: date | None = None,
    num_ticks_per_session: int = 30,
) -> dict[str, list[dict[str, Any]]]:
    target_tickers = tickers or settings.vn30_tickers
    t_date = target_date or date(2026, 7, 1)

    data: dict[str, list[dict[str, Any]]] = {}

    sessions = [
        (datetime.combine(t_date, datetime.min.time().replace(hour=9, minute=15)),
         datetime.combine(t_date, datetime.min.time().replace(hour=11, minute=30))),
        (datetime.combine(t_date, datetime.min.time().replace(hour=13, minute=0)),
         datetime.combine(t_date, datetime.min.time().replace(hour=15, minute=0))),
    ]

    for ticker in target_tickers:
        base_price = _SEED_PRICES.get(ticker, 30_000.0)
        rows: list[dict[str, Any]] = []

        for start_dt, end_dt in sessions:
            delta_seconds = int((end_dt - start_dt).total_seconds())
            step = max(1, delta_seconds // num_ticks_per_session)

            curr_dt = start_dt
            curr_price = base_price
            while curr_dt <= end_dt:
                tick_change = random.choice([-100, -50, 0, 50, 100])
                curr_price = max(1_000.0, curr_price + tick_change)
                vol = random.randint(1_000, 50_000)
                side = random.choice(["b", "s"])

                rows.append({
                    "time": curr_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "price": curr_price,
                    "vol": vol,
                    "side": side,
                })
                curr_dt += timedelta(seconds=step + random.randint(-5, 5))

        data[ticker] = rows

    return data


def run_july_2026_simulation_pipeline(
    start_date: date | None = None,
    end_date: date | None = None,
    persist: bool = True,
) -> dict[str, Any]:
    from src.transformers.ohlcv_transformer import transform_ohlcv
    from src.transformers.tick_transformer import transform_ticks

    trading_days = get_july_2026_trading_days(start_date, end_date)
    raw_ohlcv = generate_simulated_ohlcv(start_date=trading_days[0], end_date=trading_days[-1])
    df_ohlcv = transform_ohlcv(raw_ohlcv)

    all_tick_frames = []
    for t_date in trading_days:
        raw_ticks = generate_simulated_ticks(target_date=t_date)
        df_tick = transform_ticks(raw_ticks)
        if not df_tick.is_empty():
            all_tick_frames.append(df_tick)

    import polars as pl
    df_all_ticks = pl.concat(all_tick_frames, how="vertical") if all_tick_frames else pl.DataFrame()

    return {
        "trading_days": len(trading_days),
        "ohlcv_rows": len(df_ohlcv),
        "tick_rows": len(df_all_ticks),
    }
