"""
Kafka Producer for VN30 intraday tick data.
Uses confluent-kafka to publish JSON messages.
Key = ticker to ensure per-symbol ordering.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from src.config import settings

logger = logging.getLogger(__name__)
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

_ACTION_MAP: dict[str, str] = {
    "b": "BUY",
    "buy": "BUY",
    "bu": "BUY",
    "1": "BUY",
    "s": "SELL",
    "sell": "SELL",
    "sd": "SELL",
    "2": "SELL",
}


def _delivery_report(err: Any, msg: Any) -> None:
    """Callback triggered by librdkafka upon successful delivery or failure."""
    if err is not None:
        logger.error("Message delivery failed: %s (topic: %s)", err, msg.topic())
    else:
        logger.debug(
            "Message delivered to %s [%d] at offset %d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def get_kafka_producer():
    """Create and return a confluent_kafka Producer instance."""
    from confluent_kafka import Producer

    conf = {
        "bootstrap.servers": settings.kafka.bootstrap_servers,
        "acks": settings.kafka.producer_acks,
        "retries": settings.kafka.producer_retries,
        "client.id": "vn30-python-producer",
    }
    return Producer(conf)


def _normalize_match_time(val: Any) -> str:
    """Normalize raw match time to 'YYYY-MM-DD HH:MM:SS' string format."""
    if isinstance(val, (datetime, date)):
        return val.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(val, str):
        val = val.strip()
        # If HH:MM:SS only, prepend today's date
        if len(val) == 8 and val.count(":") == 2:
            today_str = datetime.now(VN_TZ).strftime("%Y-%m-%d")
            return f"{today_str} {val}"
        return val
    return datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S")


# LRU / In-memory set to store recent tick fingerprints per ticker
_SEEN_TICKS: dict[str, set[str]] = {}
_MAX_SEEN_CACHE_SIZE = 5000


def produce_ticks(
    data: dict[str, list[dict[str, Any]]],
    topic: str | None = None,
) -> int:
    """
    Publish raw tick records for all tickers into Kafka.
    Only publishes new ticks that haven't been seen in previous cycles.

    Args:
        data: Dict {ticker: [raw_tick_dicts]}.
        topic: Kafka topic name (default: settings.kafka.topic_intraday).

    Returns:
        Total number of NEW messages produced.
    """
    target_topic = topic or settings.kafka.topic_intraday
    producer = get_kafka_producer()
    published_count = 0

    for ticker, rows in data.items():
        if not rows:
            continue

        sym = str(ticker).upper()
        if sym not in _SEEN_TICKS:
            _SEEN_TICKS[sym] = set()
        seen_set = _SEEN_TICKS[sym]

        for r in rows:
            # Extract and normalize fields
            raw_time = r.get("time") or r.get("match_time") or r.get("t")
            match_time_str = _normalize_match_time(raw_time)

            raw_price = r.get("price") or r.get("matchPrice") or r.get("p") or 0.0
            raw_vol = r.get("vol") or r.get("volume") or r.get("matchVolume") or r.get("v") or 0
            raw_action = r.get("side") or r.get("action") or r.get("type") or r.get("cp") or r.get("match_type") or ""

            try:
                price = float(raw_price)
                volume = int(raw_vol)
            except (ValueError, TypeError):
                continue

            if price <= 0 or volume <= 0:
                continue

            action_normalized = _ACTION_MAP.get(str(raw_action).strip().lower(), "UNKNOWN")

            # Create unique fingerprint for deduplication
            fingerprint = f"{match_time_str}_{price}_{volume}_{action_normalized}"
            if fingerprint in seen_set:
                continue

            seen_set.add(fingerprint)
            # Prevent memory growth by keeping cache bounded
            if len(seen_set) > _MAX_SEEN_CACHE_SIZE:
                # Remove oldest elements if cache grows too large
                seen_set.clear()
                seen_set.add(fingerprint)

            payload = {
                "ticker": sym,
                "match_time": match_time_str,
                "price": price,
                "volume": volume,
                "action": action_normalized,
            }

            key_bytes = sym.encode("utf-8")
            val_bytes = json.dumps(payload).encode("utf-8")

            producer.produce(
                topic=target_topic,
                key=key_bytes,
                value=val_bytes,
                on_delivery=_delivery_report,
            )
            published_count += 1

            if published_count % 1000 == 0:
                producer.poll(0)

    # Flush all remaining messages
    producer.flush(timeout=5)
    if published_count > 0:
        logger.info("Published %d NEW tick messages to Kafka topic '%s'.", published_count, target_topic)
    return published_count


def main():
    import argparse
    import os
    import signal
    import time
    
    default_interval = int(os.getenv("PRODUCER_INTERVAL", "15"))
    default_check_trading = os.getenv("PRODUCER_CHECK_TRADING_HOUR", "false").lower() in ("true", "1", "yes")

    parser = argparse.ArgumentParser(description="VN30 Realtime Tick Producer")
    parser.add_argument("--loop", action="store_true", default=os.getenv("PRODUCER_LOOP", "false").lower() in ("true", "1", "yes"), help="Run continuously in a loop")
    parser.add_argument("--interval", type=int, default=default_interval, help="Interval in seconds between fetch cycles in loop mode")
    parser.add_argument("--check-trading-hour", action="store_true", default=default_check_trading, help="Only fetch during trading hours")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    from src.extractors.intraday import fetch_all_vn30_ticks

    running = True

    def _shutdown_handler(signum, frame):
        nonlocal running
        logger.info("Received termination signal (%s). Stopping producer gracefully...", signum)
        running = False

    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)

    if args.loop:
        all_tickers = settings.vn30_tickers
        half = len(all_tickers) // 2
        groups = [all_tickers[:half], all_tickers[half:]]
        group_idx = 0

        logger.info(
            "Starting continuous realtime streaming producer (interval: %ds, split: 2 groups of %d tickers, check_trading_hour: %s)...",
            args.interval,
            half,
            args.check_trading_hour,
        )
        while running:
            try:
                current_batch = groups[group_idx]
                group_idx = (group_idx + 1) % len(groups)
                
                logger.info("Fetching tick batch (%d tickers: %s...)", len(current_batch), ", ".join(current_batch[:4]))
                raw_ticks = asyncio.run(fetch_all_vn30_ticks(tickers=current_batch, check_trading_hour=args.check_trading_hour))
                total_fetched = sum(len(v) for v in raw_ticks.values())
                if total_fetched > 0:
                    count = produce_ticks(raw_ticks)
                    logger.info("Cycle complete: %d messages produced. Next cycle in %ds...", count, args.interval)
                else:
                    logger.info("No ticks returned in this cycle. Next cycle in %ds...", args.interval)
            except Exception as e:
                logger.error("Error during produce cycle: %s", e)
            except BaseException as be:
                logger.warning("Interrupted during produce cycle: %s", be)

            # Sleep in small increments to respond quickly to termination signal
            for _ in range(args.interval):
                if not running:
                    break
                time.sleep(1)
        logger.info("Realtime producer stopped.")
    else:
        logger.info("Starting single intraday fetch & Kafka produce...")
        raw_ticks = asyncio.run(fetch_all_vn30_ticks(check_trading_hour=args.check_trading_hour))
        count = produce_ticks(raw_ticks)
        logger.info("Done. %d messages produced.", count)


if __name__ == "__main__":
    main()


