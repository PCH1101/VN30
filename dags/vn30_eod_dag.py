"""
Airflow DAG — VN30 End-of-Day (EOD) OHLCV ETL & Backfill Reconcile.
Schedule: 30 15 * * 1-5  (15:30 Thứ 2 - Thứ 6)
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
import sys

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

default_args = {
    "owner": "vn30-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=60),
}


@dag(
    dag_id="vn30_eod_dag",
    default_args=default_args,
    description="VN30 EOD OHLCV ETL + Backfill reconcile lúc 15:30 T2-T6",
    schedule_interval="30 15 * * 1-5",
    start_date=datetime(2024, 1, 1, tzinfo=VN_TZ),
    catchup=False,
    tags=["vn30", "eod", "production"],
)
def vn30_eod_pipeline():

    @task(task_id="run_eod_etl")
    def run_eod_etl_task(target_date_str: str | None = None) -> None:
        from src.extractors.daily_ohlcv import fetch_all_vn30_ohlcv
        from src.transformers.ohlcv_transformer import transform_ohlcv
        from src.loaders.bronze_loader import save_ohlcv_bronze, save_intraday_bronze
        from src.loaders.silver_loader import load_ohlcv_silver, load_intraday_silver
        from src.extractors.intraday import fetch_all_vn30_ticks
        from src.transformers.tick_transformer import transform_ticks
        from src.config import settings

        target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date() if target_date_str else datetime.now(VN_TZ).date()
        logger.info("=== EOD Flow START for %s ===", target_date)

        # 1. Extract OHLCV
        raw_data = asyncio.run(fetch_all_vn30_ohlcv(target_date=target_date))
        total_rows = sum(len(v) for v in raw_data.values())
        if total_rows == 0:
            logger.warning("No OHLCV data returned across all tickers — aborting flow.")
            return

        logger.info("Extracted %d OHLCV rows for %s.", total_rows, target_date)

        # 2. Bronze OHLCV
        save_ohlcv_bronze(raw_data)

        # 3. Transform & Silver OHLCV
        df = transform_ohlcv(raw_data)
        if not df.is_empty():
            rows = load_ohlcv_silver(df)
            logger.info("Silver OHLCV: inserted %d rows into ClickHouse.", rows)
        else:
            logger.info("Silver OHLCV: 0 rows to insert.")

        # 4. Reconcile intraday backfill nếu thiếu
        try:
            from src.db.clickhouse_client import execute
            date_str = target_date.strftime("%Y-%m-%d")
            result = execute(
                f"""
                SELECT toStartOfInterval(match_time, INTERVAL 2 MINUTE) AS window,
                       count() AS cnt
                FROM {settings.clickhouse.database}.silver_intraday_ticks
                WHERE toDate(match_time) = '{date_str}'
                GROUP BY window
                ORDER BY window
                """
            )
            existing_windows = {row[0] for row in result}

            # Expected 2-minute windows during trading hours
            expected_windows = []
            for start_h, start_m, end_h, end_m in [(9, 15, 11, 30), (13, 0, 15, 0)]:
                cur = datetime.combine(target_date, datetime.min.time().replace(hour=start_h, minute=start_m))
                end = datetime.combine(target_date, datetime.min.time().replace(hour=end_h, minute=end_m))
                while cur <= end:
                    expected_windows.append(cur)
                    cur += timedelta(minutes=2)

            missing = [w for w in expected_windows if w not in existing_windows]
            if missing:
                logger.warning("Reconcile: %d missing windows detected for %s. Backfilling...", len(missing), date_str)
                backfill_data = asyncio.run(fetch_all_vn30_ticks(check_trading_hour=False))
                total_backfill = sum(len(v) for v in backfill_data.values())
                if total_backfill > 0:
                    save_intraday_bronze(backfill_data, timestamp=datetime.now(VN_TZ))
                    df_backfill = transform_ticks(backfill_data)
                    bf_rows = load_intraday_silver(df_backfill)
                    logger.info("Backfill complete: inserted %d rows.", bf_rows)
            else:
                logger.info("Reconcile: no missing windows for %s.", date_str)
        except Exception as exc:
            logger.warning("Reconcile failed: %s", exc)

        logger.info("=== EOD Flow COMPLETE ===")

    run_eod_etl_task()


vn30_eod_dag = vn30_eod_pipeline()
