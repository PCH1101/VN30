"""
Telegram alert module — phát hiện Volume Spike / Shark activity và gửi tin nhắn.

Điều kiện trigger:
    BUY  alert: vol_1min > N * MA20(vol) AND buy_ratio >= 70%
    SELL alert: vol_1min > N * MA20(vol) AND buy_ratio <= 30%

Data source: gold_ohlcv_1min (realtime, tổng hợp từ Materialized View).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import NamedTuple

import polars as pl

from src.config import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

class AlertSignal(NamedTuple):
    ticker: str
    alert_type: str          # "SHARK_BUY" | "SHARK_SELL" | "VOLUME_SPIKE"
    vol_1min: int
    ma20_vol: float
    buy_ratio: float
    window_start: datetime
    price: float             # close price of the spike candle


# ---------------------------------------------------------------------------
# Signal detection (pure — works on Polars DataFrame)
# ---------------------------------------------------------------------------

def detect_signals(df: pl.DataFrame) -> list[AlertSignal]:
    """
    Phát hiện volume spike signals từ tick DataFrame.

    Args:
        df: Polars DataFrame từ tick_transformer (silver_intraday_ticks schema).
            Cần các cột: ticker, match_time, price, volume, action.

    Returns:
        List of AlertSignal instances.
    """
    if df.is_empty():
        return []

    cfg = settings
    signals: list[AlertSignal] = []

    # Aggregate ticks → 1-minute candles per ticker
    candles = (
        df
        .sort("match_time")
        .group_by_dynamic("match_time", every="1m", group_by="ticker")
        .agg([
            pl.col("price").last().alias("close"),
            pl.col("volume").sum().alias("vol_1min"),
            pl.when(pl.col("action") == "BUY")
              .then(pl.col("volume"))
              .otherwise(0)
              .sum()
              .alias("buy_vol"),
        ])
        .with_columns(
            (pl.col("buy_vol") / pl.col("vol_1min").replace(0, None))
            .fill_null(0.5)
            .alias("buy_ratio")
        )
    )

    # Compute MA20 of volume per ticker (rolling window)
    candles = candles.sort(["ticker", "match_time"]).with_columns(
        pl.col("vol_1min")
          .rolling_mean(window_size=cfg.ma_volume_window)
          .over("ticker")
          .alias("ma20_vol")
    )

    # Filter: only rows where MA20 is available (>= 20 candles)
    candles = candles.filter(pl.col("ma20_vol").is_not_null())

    # Detect spikes
    spike_mask = pl.col("vol_1min") > (pl.col("ma20_vol") * cfg.volume_spike_multiplier)
    buy_mask   = pl.col("buy_ratio") >= cfg.buy_ratio_buy_threshold
    sell_mask  = pl.col("buy_ratio") <= cfg.buy_ratio_sell_threshold

    spikes = candles.filter(spike_mask)
    if spikes.is_empty():
        return []

    for row in spikes.iter_rows(named=True):
        buy_ratio = row["buy_ratio"]
        if buy_ratio >= cfg.buy_ratio_buy_threshold:
            alert_type = "SHARK_BUY"
        elif buy_ratio <= cfg.buy_ratio_sell_threshold:
            alert_type = "SHARK_SELL"
        else:
            alert_type = "VOLUME_SPIKE"

        signals.append(AlertSignal(
            ticker=row["ticker"],
            alert_type=alert_type,
            vol_1min=int(row["vol_1min"]),
            ma20_vol=float(row["ma20_vol"]),
            buy_ratio=float(buy_ratio),
            window_start=row["match_time"],
            price=float(row["close"]),
        ))

    logger.info("detect_signals: found %d signal(s).", len(signals))
    return signals


# ---------------------------------------------------------------------------
# Message formatter
# ---------------------------------------------------------------------------

def format_message(signal: AlertSignal) -> str:
    """Format AlertSignal thành Telegram message string (Markdown-safe)."""
    vol_k = signal.vol_1min / 1000
    ma_k  = signal.ma20_vol / 1000
    ratio_pct = signal.buy_ratio * 100
    multiplier = signal.vol_1min / signal.ma20_vol if signal.ma20_vol else 0
    time_str = signal.window_start.strftime("%H:%M")

    if signal.alert_type == "SHARK_BUY":
        icon = "[SHARK BUY]"
        direction = "Ca map gom hang!"
    elif signal.alert_type == "SHARK_SELL":
        icon = "[SHARK SELL]"
        direction = "Ca map xa hang!"
    else:
        icon = "[VOLUME SPIKE]"
        direction = "Dot bien khoi luong!"

    return (
        f"{icon} {signal.ticker} | {time_str}\n"
        f"{direction}\n"
        f"Gia: {signal.price:,.0f} VND\n"
        f"Vol 1min: {vol_k:.1f}K ({multiplier:.1f}x MA20)\n"
        f"MA20 Vol: {ma_k:.1f}K\n"
        f"Buy ratio: {ratio_pct:.1f}%"
    )


_CACHED_CHAT_ID: str | None = None
_AUTO_DETECT_ATTEMPTED: bool = False


def _get_auto_chat_id(token: str) -> str | None:
    """Tự động lấy chat_id của user gần nhất gửi tin nhắn /start tới bot (chỉ gọi 1 lần per process)."""
    global _CACHED_CHAT_ID, _AUTO_DETECT_ATTEMPTED  # noqa: PLW0603
    if _CACHED_CHAT_ID:
        return _CACHED_CHAT_ID
    if _AUTO_DETECT_ATTEMPTED:
        return None

    _AUTO_DETECT_ATTEMPTED = True

    import urllib.request
    import json

    url = f"https://api.telegram.org/bot{token}/getUpdates"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            results = data.get("result", [])
            if results:
                last_update = results[-1]
                chat = last_update.get("message", {}).get("chat", {})
                cid = chat.get("id")
                if cid:
                    _CACHED_CHAT_ID = str(cid)
                    logger.info("Telegram auto-detected chat_id: %s", _CACHED_CHAT_ID)
                    return _CACHED_CHAT_ID
    except Exception as exc:  # noqa: BLE001
        logger.warning("Telegram getUpdates auto-detection failed: %s", exc)

    return None


def send_telegram(message: str, chat_id: str | None = None) -> bool:
    """
    Gửi tin nhắn qua Telegram Bot API (synchronous HTTP).
    Tự động lấy chat_id từ settings hoặc getUpdates nếu không được truyền vào.

    Returns:
        True nếu gửi thành công, False nếu lỗi.
    """
    token = settings.telegram.token
    if not token:
        logger.warning("Telegram not configured (TELEGRAM_TOKEN missing).")
        return False

    target_chat_id = chat_id or settings.telegram.chat_id or _get_auto_chat_id(token)
    if not target_chat_id:
        logger.warning("Telegram chat_id missing and could not be auto-detected via getUpdates.")
        return False

    import urllib.request
    import json

    url     = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps({"chat_id": target_chat_id, "text": message}).encode("utf-8")
    req     = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            ok = resp.status == 200
            if ok:
                logger.info("Telegram message sent to chat %s.", target_chat_id)
            else:
                logger.warning("Telegram API returned status %d.", resp.status)
            return ok
    except Exception as exc:  # noqa: BLE001
        logger.error("Telegram send failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Alert log writer (ClickHouse audit trail)
# ---------------------------------------------------------------------------

def _log_alert(signal: AlertSignal, sent: bool) -> None:
    """Insert alert record into alert_log table (best-effort, no raise)."""
    try:
        from src.db.clickhouse_client import execute

        execute(
            "INSERT INTO vn30.alert_log "
            "(alert_time, ticker, alert_type, vol_1min, ma20_vol, buy_ratio, message_sent) "
            "VALUES",
            [{
                "alert_time":    datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")),
                "ticker":        signal.ticker,
                "alert_type":    signal.alert_type,
                "vol_1min":      signal.vol_1min,
                "ma20_vol":      signal.ma20_vol,
                "buy_ratio":     signal.buy_ratio,
                "message_sent":  1 if sent else 0,
            }],
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to log alert to ClickHouse: %s", exc)


# ---------------------------------------------------------------------------
# VWAP signal formatter
# ---------------------------------------------------------------------------

def format_vwap_message(ticker: str, signal: str, price: float, vwap: float, ts: datetime) -> str:
    """
    Format VWAP crossover signal thành Telegram message (ASCII-safe).

    Args:
        ticker: Mã cổ phiếu.
        signal: 'BUY' | 'SELL'.
        price:  Giá hiện tại.
        vwap:   VWAP tại điểm cắt.
        ts:     Thời điểm crossover.
    """
    direction = "cat len VWAP" if signal == "BUY" else "cat xuong VWAP"
    icon      = "[VWAP BUY]" if signal == "BUY" else "[VWAP SELL]"
    diff_pct  = ((price - vwap) / vwap * 100) if vwap else 0

    return (
        f"{icon} {ticker} | {ts.strftime('%H:%M')}\n"
        f"Gia {direction}\n"
        f"Gia: {price:,.0f} | VWAP: {vwap:,.0f}\n"
        f"Lenh: {'+' if signal == 'BUY' else '-'}{abs(diff_pct):.2f}%"
    )


# ---------------------------------------------------------------------------
# Public entry point — called from Airflow task
# ---------------------------------------------------------------------------

def check_and_alert(df: pl.DataFrame, max_alerts: int = 10) -> int:
    """
    Detect volume spike + VWAP crossover signals, gửi Telegram (tối đa max_alerts per batch).

    Args:
        df:         Polars DataFrame từ tick_transformer.
        max_alerts: Số lượng tin nhắn tối đa được phép gửi trong 1 batch.

    Returns:
        Số lượng alerts đã gửi.
    """
    sent_count = 0

    # --- UC1: Volume spike / Shark alerts ---
    signals = detect_signals(df)
    for signal in signals:
        if sent_count >= max_alerts:
            break
        msg  = format_message(signal)
        sent = send_telegram(msg)
        _log_alert(signal, sent)
        if sent:
            sent_count += 1

    # --- UC3: VWAP crossover alerts ---
    try:
        from src.analytics.vwap import compute_vwap, get_active_signals

        df_vwap   = compute_vwap(df)
        crossovers = get_active_signals(df_vwap)

        for row in crossovers.iter_rows(named=True):
            if sent_count >= max_alerts:
                break
            msg = format_vwap_message(
                ticker=row["ticker"],
                signal=row["vwap_signal"],
                price=float(row["price"]),
                vwap=float(row["vwap"]),
                ts=row["match_time"],
            )
            sent = send_telegram(msg)
            if sent:
                sent_count += 1
    except Exception as exc:  # noqa: BLE001
        logger.warning("VWAP alert check failed: %s", exc)

    return sent_count
