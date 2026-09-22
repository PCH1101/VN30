"""
Daily EOD (End of Day) Extractor & Aggregator for VN30.
Computes daily OHLCV from intraday ticks or fetches official daily bars from vnstock API.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from src.config import settings
from src.db.clickhouse_client import execute, insert_rows

logger = logging.getLogger(__name__)
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def aggregate_from_intraday_ticks(target_date: date | None = None) -> int:
    """
    Aggregate daily OHLCV from raw_intraday_ticks table in ClickHouse.

    Args:
        target_date: Date to aggregate (default: today in VN_TZ).

    Returns:
        Number of tickers aggregated and inserted into raw_daily_ohlcv.
    """
    t_date = target_date or datetime.now(VN_TZ).date()
    date_str = t_date.strftime("%Y-%m-%d")
    logger.info("Aggregating daily OHLCV from raw_intraday_ticks for trade_date: %s", date_str)

    query = """
    INSERT INTO vn30.raw_daily_ohlcv (
        ticker,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        total_volume,
        total_value
    )
    SELECT
        ticker,
        toDate(match_time) AS trade_date,
        argMin(price, match_time) AS open_price,
        max(price) AS high_price,
        min(price) AS low_price,
        argMax(price, match_time) AS close_price,
        sum(volume) AS total_volume,
        sum(price * volume * 1000) AS total_value
    FROM vn30.raw_intraday_ticks
    WHERE toDate(match_time) = %(target_date)s
      AND price > 0
      AND volume > 0
    GROUP BY
        ticker,
        toDate(match_time)
    """

    try:
        execute(query, {"target_date": date_str})
        
        # Check how many tickers were recorded
        check_query = """
        SELECT count() FROM vn30.raw_daily_ohlcv WHERE trade_date = %(target_date)s
        """
        res = execute(check_query, {"target_date": date_str})
        inserted_count = res[0][0] if res else 0
        logger.info("Successfully aggregated %d tickers for date %s", inserted_count, date_str)
        return inserted_count
    except Exception as exc:
        logger.error("Failed to aggregate intraday ticks for date %s: %s", date_str, exc)
        return 0


def fetch_and_insert_official_daily_bars(
    target_date: date | None = None,
    tickers: list[str] | None = None,
) -> int:
    """
    Fetch official daily OHLCV from vnstock API and insert into raw_daily_ohlcv.
    Useful for backfill or verifying official EOD settlement prices.

    Args:
        target_date: Specific trade date.
        tickers: List of ticker symbols (default: VN30).

    Returns:
        Number of daily records inserted.
    """
    t_date = target_date or datetime.now(VN_TZ).date()
    date_str = t_date.strftime("%Y-%m-%d")
    tickers = tickers or settings.vn30_tickers

    logger.info("Fetching official daily bars from vnstock API for date %s (%d tickers)...", date_str, len(tickers))

    try:
        try:
            from vnstock import Vnstock
        except ImportError:
            from vnstock3 import Vnstock  # type: ignore[import-untyped]

        source = settings.vnstock.source or "VCI"
        if source not in ("VCI", "KBS", "MSN", "FMP"):
            source = "VCI"

        records: list[dict[str, Any]] = []

        for symbol in tickers:
            try:
                stock = Vnstock(show_log=False).stock(symbol=symbol, source=source)
                df = stock.quote.history(start=date_str, end=date_str, interval="1D")
                if df is not None and not df.empty:
                    row = df.iloc[-1]
                    records.append({
                        "ticker": symbol.upper(),
                        "trade_date": t_date,
                        "open_price": float(row.get("open", 0.0)),
                        "high_price": float(row.get("high", 0.0)),
                        "low_price": float(row.get("low", 0.0)),
                        "close_price": float(row.get("close", 0.0)),
                        "total_volume": int(row.get("volume", 0)),
                        "total_value": float(row.get("value", 0.0) or (float(row.get("close", 0.0)) * int(row.get("volume", 0)))),
                    })
            except Exception as e:
                logger.warning("Could not fetch daily bar for %s on %s: %s", symbol, date_str, e)

        if records:
            inserted = insert_rows("vn30.raw_daily_ohlcv", records)
            logger.info("Inserted %d official daily bars into raw_daily_ohlcv.", inserted)
            return inserted

        logger.info("No records fetched from API for date %s.", date_str)
        return 0

    except Exception as exc:
        logger.error("Error fetching official daily bars: %s", exc)
        return 0


def run_eod_pipeline(target_date: date | None = None) -> int:
    """
    Main EOD pipeline runner:
    First attempts aggregation from raw_intraday_ticks.
    If no intraday ticks found, falls back to API fetch.
    """
    t_date = target_date or datetime.now(VN_TZ).date()
    count = aggregate_from_intraday_ticks(t_date)
    if count == 0:
        logger.info("No ticks in ClickHouse for %s — attempting API fetch fallback...", t_date)
        count = fetch_and_insert_official_daily_bars(t_date)
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    total = run_eod_pipeline()
    logger.info("EOD pipeline finished. Total records: %d", total)
