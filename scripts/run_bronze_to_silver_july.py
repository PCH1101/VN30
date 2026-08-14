"""
Script: run_bronze_to_silver_july.py
Executes the Bronze -> Silver ETL Pipeline for July 2026:
1. Reads raw JSON payloads from MinIO bucket 'vn30-bronze-july2026'.
2. Transforms raw OHLCV via Polars and loads into ClickHouse 'silver_daily_ohlcv'.
3. Generates & loads Intraday Ticks to MinIO Bronze & ClickHouse 'silver_intraday_ticks'.
4. Triggers ClickHouse Materialized View 'gold_ohlcv_1min' for Grafana visualization.
"""

from __future__ import annotations

import logging
from pathlib import Path
import sys

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.flows.bronze_to_silver import process_bronze_to_silver_july_2026
from src.db.clickhouse_client import execute

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("bronze_to_silver_runner")


def main() -> None:
    logger.info("=== Starting Bronze -> Silver ETL Runner for July 2026 ===")

    # Run Bronze -> Silver ETL flow
    stats = process_bronze_to_silver_july_2026(
        bronze_bucket="vn30-bronze-july2026",
        include_intraday_ticks=True,
    )

    logger.info("ETL Stats Summary: %s", stats)

    # Verify ClickHouse tables
    try:
        ohlcv_count = execute("SELECT count() FROM vn30.silver_daily_ohlcv")[0][0]
        tick_count = execute("SELECT count() FROM vn30.silver_intraday_ticks")[0][0]
        gold_count = execute("SELECT count() FROM vn30.gold_ohlcv_1min")[0][0]

        logger.info("--- ClickHouse Database Table Counts ---")
        logger.info("Silver Daily OHLCV rows : %d", ohlcv_count)
        logger.info("Silver Intraday Ticks   : %d", tick_count)
        logger.info("Gold OHLCV 1-Min View   : %d", gold_count)
    except Exception as exc:
        logger.warning("Verification query failed: %s", exc)

    logger.info("=== Bronze -> Silver ETL Runner Complete ===")


if __name__ == "__main__":
    main()
