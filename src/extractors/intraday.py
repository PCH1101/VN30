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

# Global semaphore to limit concurrent requests to avoid Vietcap WAF rate limiting
_SEMAPHORE = asyncio.Semaphore(6)
_API_KEY_INITIALIZED = False


def _init_api_key_once():
    global _API_KEY_INITIALIZED
    if not _API_KEY_INITIALIZED:
        if settings.vnstock.api_key:
            try:
                import vnstock
                if hasattr(vnstock, "change_api_key"):
                    vnstock.change_api_key(settings.vnstock.api_key)
            except Exception as e:
                logger.debug("Failed to set vnstock api key: %s", e)
        _API_KEY_INITIALIZED = True


# ---------------------------------------------------------------------------
# Single-ticker fetch
# ---------------------------------------------------------------------------

def _fetch_one_ticker_sync(ticker: str) -> list[dict[str, Any]]:
    """Synchronous worker that calls modern vnstock API."""
    try:
        try:
            from vnstock.api.quote import Quote  # type: ignore[import-untyped]
            q = Quote(symbol=ticker, source="VCI")
            df = q.intraday()
        except ImportError:
            try:
                from vnstock import Vnstock  # type: ignore[import-untyped]
                df = Vnstock(show_log=False).stock(symbol=ticker, source="VCI").quote.intraday()
            except ImportError:
                from vnstock3 import Vnstock  # type: ignore[import-untyped]
                df = Vnstock(show_log=False).stock(symbol=ticker, source="VCI").quote.intraday()

        if df is None or df.empty:
            return []

        return df.to_dict(orient="records")

    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to fetch intraday ticks for %s: %s", ticker, exc)
        return []


async def _fetch_one_ticker(ticker: str, jitter_ms: int = 150, timeout_sec: float = 10.0) -> list[dict[str, Any]]:
    """
    Fetch intraday ticks cho 1 mã cổ phiếu từ vnstock API asynchronously using thread pool.
    """
    _init_api_key_once()
    await asyncio.sleep(random.randint(20, max(20, jitter_ms)) / 1000)

    async with _SEMAPHORE:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(_fetch_one_ticker_sync, ticker),
                timeout=timeout_sec,
            )
        except asyncio.TimeoutError:
            logger.warning("Timeout (>%.1fs) fetching ticks for %s", timeout_sec, ticker)
            return []
        except Exception as exc:  # noqa: BLE001
            logger.warning("Error fetching ticks for %s: %s", ticker, exc)
            return []


# ---------------------------------------------------------------------------
# Batch fetch — all 30 tickers in parallel
# ---------------------------------------------------------------------------

async def fetch_all_vn30_ticks(
    tickers: list[str] | None = None,
    *,
    check_trading_hour: bool = True,
    jitter_ms: int = 200,
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
    results: list[list[dict]] = await asyncio.gather(*tasks, return_exceptions=True)

    cleaned_results = []
    for res in results:
        if isinstance(res, list):
            cleaned_results.append(res)
        else:
            cleaned_results.append([])

    data = {ticker: rows for ticker, rows in zip(tickers, cleaned_results)}
    total_ticks = sum(len(rows) for rows in data.values())
    logger.info("Fetched %d total ticks across %d tickers.", total_ticks, len(tickers))

    return data
