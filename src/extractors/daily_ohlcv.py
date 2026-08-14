"""
Daily OHLCV extractor — fetch nến ngày chốt phiên cho 30 mã VN30.
Chạy 1 lần/ngày lúc 15:30, sau khi phiên giao dịch đóng cửa.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta
from typing import Any

from src.config import settings

logger = logging.getLogger(__name__)


async def fetch_ohlcv_one(
    ticker: str,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    """
    Fetch OHLCV (nến ngày) cho 1 mã từ vnstock3.

    Args:
        ticker:     Mã cổ phiếu.
        start_date: Ngày bắt đầu (inclusive).
        end_date:   Ngày kết thúc (inclusive).

    Returns:
        List of OHLCV dicts, rỗng nếu lỗi.
    """
    try:
        # Lazy import: try vnstock first (v4+), fallback to vnstock3
        try:
            from vnstock import Vnstock  # type: ignore[import-untyped]
        except ImportError:
            from vnstock3 import Vnstock  # type: ignore[import-untyped]

        source = settings.vnstock.source or "KBS"
        if source not in ("KBS", "VCI", "MSN", "FMP"):
            source = "KBS"
        stock = Vnstock(
            show_log=settings.vnstock.show_log,
        ).stock(symbol=ticker, source=source)
        df = stock.quote.history(
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            interval="1D",
        )
        if df is None or df.empty:
            logger.debug("No OHLCV data for %s between %s and %s", ticker, start_date, end_date)
            return []

        return df.to_dict(orient="records")

    except ImportError:
        logger.error("vnstock3 not installed — cannot fetch OHLCV data.")
        return []
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to fetch OHLCV for %s: %s", ticker, exc)
        return []


async def fetch_all_vn30_ohlcv(
    target_date: date | None = None,
    tickers: list[str] | None = None,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    lookback_days: int = 1,
) -> dict[str, list[dict[str, Any]]]:
    """
    Fetch OHLCV cho toàn bộ VN30 cho ngày giao dịch gần nhất hoặc trong khoảng start_date..end_date.

    Args:
        target_date:   Ngày cần fetch (mặc định: hôm nay).
        tickers:       Override danh sách mã.
        start_date:    Ngày bắt đầu tùy chọn.
        end_date:      Ngày kết thúc tùy chọn.
        lookback_days: Số ngày lùi về để fetch nếu không truyền start_date.

    Returns:
        Dict {ticker: [ohlcv_dicts]}.
    """
    if end_date is None:
        end_date = target_date or date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=lookback_days)

    tickers = tickers or settings.vn30_tickers

    logger.info(
        "Fetching OHLCV for %d tickers from %s to %s...",
        len(tickers), start_date, end_date
    )

    tasks = [fetch_ohlcv_one(ticker, start_date, end_date) for ticker in tickers]
    results = await asyncio.gather(*tasks)

    data = {ticker: rows for ticker, rows in zip(tickers, results)}
    total_rows = sum(len(rows) for rows in data.values())
    logger.info("Fetched %d total OHLCV rows for %d tickers.", total_rows, len(tickers))
    return data

