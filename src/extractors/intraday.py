"""
Intraday tick extractor — fetch khớp lệnh realtime cho 30 mã VN30.
Sử dụng asyncio.gather() để cào song song, có jitter delay tránh rate limit.
"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, time as dt_time
from typing import Any

from src.config import settings

from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# ---------------------------------------------------------------------------
# Trading hour guard
# ---------------------------------------------------------------------------

def is_trading_hour(now: datetime | None = None) -> bool:
    """
    Kiểm tra thời điểm hiện tại có nằm trong giờ giao dịch không.
    Sessions: 09:15-11:30 và 13:00-15:00, Thứ 2 - Thứ 6 (GMT+7 Asia/Ho_Chi_Minh).
    """
    if now is None:
        now = datetime.now(VN_TZ)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=VN_TZ)
    else:
        now = now.astimezone(VN_TZ)

    # Cuối tuần → không giao dịch
    if now.weekday() >= 5:  # 5=Thứ 7, 6=Chủ nhật
        return False

    current = now.time()
    for start_str, end_str in settings.trading_sessions:
        h_s, m_s = map(int, start_str.split(":"))
        h_e, m_e = map(int, end_str.split(":"))
        if dt_time(h_s, m_s) <= current <= dt_time(h_e, m_e):
            return True
    return False


# ---------------------------------------------------------------------------
# Single-ticker fetch
# ---------------------------------------------------------------------------

async def _fetch_one_ticker(ticker: str, jitter_ms: int = 100) -> list[dict[str, Any]]:
    """
    Fetch intraday ticks cho 1 mã cổ phiếu từ vnstock3 API.

    Args:
        ticker:     Mã cổ phiếu, VD: 'HPG'.
        jitter_ms:  Max random delay (ms) để tránh burst request.

    Returns:
        List of raw tick dicts từ API (rỗng nếu lỗi / không có data).
    """
    # Jitter ngẫu nhiên tránh thundering herd
    await asyncio.sleep(random.randint(0, jitter_ms) / 1000)

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
        
        # Try stock.quote.intraday() first, fallback to price_depth()
        try:
            df = stock.quote.intraday()
        except AttributeError:
            df = stock.quote.price_depth()

        if df is None or df.empty:
            logger.debug("No intraday data for %s", ticker)
            return []

        records = df.to_dict(orient="records")
        logger.debug("Fetched %d ticks for %s", len(records), ticker)
        return records

    except ImportError:
        logger.error("vnstock3 not installed — cannot fetch intraday data.")
        return []
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to fetch intraday ticks for %s: %s", ticker, exc)
        return []


# ---------------------------------------------------------------------------
# Batch fetch — all 30 tickers in parallel
# ---------------------------------------------------------------------------

async def fetch_all_vn30_ticks(
    tickers: list[str] | None = None,
    *,
    check_trading_hour: bool = True,
    jitter_ms: int = 100,
) -> dict[str, list[dict[str, Any]]]:
    """
    Fetch intraday ticks cho toàn bộ rổ VN30 song song.

    Args:
        tickers:            Override danh sách mã (mặc định: settings.vn30_tickers).
        check_trading_hour: Nếu True, trả về {} nếu ngoài giờ giao dịch.
        jitter_ms:          Max random delay per request.

    Returns:
        Dict {ticker: [raw_tick_dicts]}.
        Trả về {} nếu ngoài giờ giao dịch (khi check_trading_hour=True).
    """
    if check_trading_hour and not is_trading_hour():
        logger.info("Outside trading hours — skipping intraday fetch.")
        return {}

    tickers = tickers or settings.vn30_tickers
    logger.info("Fetching intraday ticks for %d tickers...", len(tickers))

    tasks = [_fetch_one_ticker(ticker, jitter_ms) for ticker in tickers]
    results: list[list[dict]] = await asyncio.gather(*tasks)

    data = {ticker: rows for ticker, rows in zip(tickers, results)}
    total_ticks = sum(len(rows) for rows in data.values())
    logger.info("Fetched %d total ticks across %d tickers.", total_ticks, len(tickers))

    return data
