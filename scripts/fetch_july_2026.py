"""
Script: fetch_july_2026.py
Extract real market OHLCV data for entire month of July 2026 (2026-07-01 to 2026-07-31)
from vnstock, transform using Polars, save Bronze to MinIO, and load Silver into ClickHouse.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date
from pathlib import Path
import sys

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.extractors.daily_ohlcv import fetch_all_vn30_ohlcv
from src.transformers.ohlcv_transformer import transform_ohlcv
from src.loaders.bronze_loader import save_ohlcv_bronze, save_july_2026_daily_bronze
from src.loaders.silver_loader import load_ohlcv_silver

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("july_2026_etl")


async def main() -> None:
    start_date = date(2026, 7, 1)
    end_date = date(2026, 7, 31)

    logger.info("=== Starting Real VN30 July 2026 Data ETL (%s to %s) ===", start_date, end_date)

    # 1. Extract real OHLCV data via vnstock
    raw_data = await fetch_all_vn30_ohlcv(start_date=start_date, end_date=end_date)
    total_raw_rows = sum(len(v) for v in raw_data.values())
    logger.info("Extracted %d raw OHLCV records across %d tickers.", total_raw_rows, len(raw_data))

    if total_raw_rows == 0:
        logger.warning("No data returned from vnstock for July 2026.")
        return

    # 2. Save Bronze layer (MinIO S3) - both standard bucket and dedicated July 2026 bucket
    try:
        save_ohlcv_bronze(raw_data)
        daily_count = save_july_2026_daily_bronze(raw_data, bucket="vn30-bronze-july2026")
        logger.info("Bronze OHLCV saved to MinIO bucket 'vn30-bronze-july2026' (%d daily objects).", daily_count)
    except Exception as exc:
        logger.warning("Failed to save Bronze layer: %s", exc)

    # 3. Transform using Polars
    df = transform_ohlcv(raw_data)
    logger.info("Transformed %d clean OHLCV records.", len(df))

    # 4. Load Silver layer (ClickHouse)
    try:
        inserted_rows = load_ohlcv_silver(df)
        logger.info("Silver OHLCV: Inserted %d rows into ClickHouse.", inserted_rows)
    except Exception as exc:
        logger.warning("Failed to save Silver layer: %s", exc)

    logger.info("=== July 2026 Real Data ETL Complete ===")



if __name__ == "__main__":
    asyncio.run(main())
