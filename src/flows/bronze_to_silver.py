"""
Bronze to Silver ETL Pipeline Flow.
Processes raw Bronze JSON payloads stored in MinIO into clean Silver tables in ClickHouse.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import polars as pl

from pathlib import Path
import sys

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.extractors.bronze_reader import read_bronze_objects
from src.transformers.ohlcv_transformer import transform_ohlcv
from src.transformers.tick_transformer import transform_ticks
from src.loaders.silver_loader import load_ohlcv_silver, load_intraday_silver
from src.loaders.bronze_loader import save_intraday_bronze
from src.simulator.market_simulator import generate_simulated_ticks

logger = logging.getLogger(__name__)



def process_bronze_to_silver_july_2026(
    bronze_bucket: str = "vn30-bronze-july2026",
    include_intraday_ticks: bool = True,
) -> dict[str, Any]:
    """
    Run full Bronze -> Silver ETL pipeline for July 2026.

    1. Reads raw Bronze JSON objects from MinIO bucket 'vn30-bronze-july2026'.
    2. Transforms OHLCV with Polars and loads into ClickHouse 'silver_daily_ohlcv'.
    3. (Optional) Generates and transforms Intraday Ticks, dumps raw to MinIO Bronze 'intraday/...',
       and loads clean ticks to ClickHouse 'silver_intraday_ticks' (populating gold_ohlcv_1min).

    Returns:
        Summary dict with processed row counts.
    """
    logger.info("=== Bronze -> Silver ETL Flow START (Bucket: '%s') ===", bronze_bucket)

    # 1. Read raw OHLCV records from Bronze MinIO bucket
    raw_ohlcv = read_bronze_objects(bucket=bronze_bucket)
    total_bronze_records = sum(len(v) for v in raw_ohlcv.values())

    if total_bronze_records == 0:
        logger.warning("No records found in Bronze bucket '%s'. Aborting Bronze -> Silver flow.", bronze_bucket)
        return {"ohlcv_rows": 0, "tick_rows": 0}

    # 2. Transform OHLCV via Polars
    df_ohlcv = transform_ohlcv(raw_ohlcv)
    logger.info("Polars transformed %d clean OHLCV rows from Bronze layer.", len(df_ohlcv))

    # 3. Load into ClickHouse Silver OHLCV table
    silver_ohlcv_inserted = load_ohlcv_silver(df_ohlcv)
    logger.info("Silver OHLCV: Loaded %d rows into ClickHouse.", silver_ohlcv_inserted)

    silver_tick_inserted = 0
    total_ticks = 0

    # 4. Optional: Intraday Ticks Processing (Bronze -> Silver -> Gold 1-min candles)
    if include_intraday_ticks and not df_ohlcv.is_empty():
        logger.info("Processing Intraday Ticks for July 2026 trading dates...")
        # Get unique trading dates from transformed OHLCV DataFrame
        trading_dates: list[date] = (
            df_ohlcv.select("trade_date")
            .unique()
            .sort("trade_date")["trade_date"]
            .to_list()
        )

        all_tick_frames: list[pl.DataFrame] = []

        for t_date in trading_dates:
            raw_ticks = generate_simulated_ticks(target_date=t_date)
            if raw_ticks:
                # Save raw tick payload to MinIO Bronze layer
                save_intraday_bronze(raw_ticks)
                # Transform to Polars
                df_tick = transform_ticks(raw_ticks)
                if not df_tick.is_empty():
                    all_tick_frames.append(df_tick)

        if all_tick_frames:
            df_all_ticks = pl.concat(all_tick_frames, how="vertical")
            total_ticks = len(df_all_ticks)
            # Load clean ticks to ClickHouse Silver
            silver_tick_inserted = load_intraday_silver(df_all_ticks)
            logger.info("Silver Intraday: Loaded %d tick rows into ClickHouse.", silver_tick_inserted)

    logger.info("=== Bronze -> Silver ETL Flow COMPLETE ===")
    return {
        "bronze_records_read": total_bronze_records,
        "silver_ohlcv_inserted": silver_ohlcv_inserted,
        "silver_tick_inserted": silver_tick_inserted,
        "total_ticks_processed": total_ticks,
    }
