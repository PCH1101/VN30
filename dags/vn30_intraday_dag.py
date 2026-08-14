"""
Airflow DAG — VN30 Intraday Tick ETL (Micro-batch mỗi 2 phút).
Schedule: */2 9-11,13-15 * * 1-5 (Chỉ Thứ 2 - Thứ 6)
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
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
    "retry_delay": timedelta(seconds=15),
}


@dag(
    dag_id="vn30_intraday_dag",
    default_args=default_args,
    description="VN30 intraday tick ETL micro-batch chạy mỗi 2 phút trong giờ giao dịch",
    schedule_interval="*/2 9-11,13-15 * * 1-5",
    start_date=datetime(2024, 1, 1, tzinfo=VN_TZ),
    catchup=False,
    tags=["vn30", "intraday", "production"],
)
def vn30_intraday_pipeline():

    @task(task_id="run_intraday_etl")
    def run_intraday_etl_task(**context) -> None:
        from src.extractors.intraday import fetch_all_vn30_ticks, is_trading_hour
        from src.transformers.tick_transformer import transform_ticks
        from src.loaders.bronze_loader import save_intraday_bronze
        from src.loaders.silver_loader import load_intraday_silver

        # 1. Guard Check (cho phép override qua Airflow conf: {"force_run": true})
        dag_run = context.get("dag_run")
        force_run = bool(dag_run and dag_run.conf and dag_run.conf.get("force_run"))

        if not force_run and not is_trading_hour():
            logger.info("Outside trading hours — skipping intraday run. (Pass conf={'force_run': true} to override)")
            return

        logger.info("=== Intraday Flow START ===")

        # 2. Extract
        raw_data = asyncio.run(fetch_all_vn30_ticks(check_trading_hour=False))
        total_ticks = sum(len(v) for v in raw_data.values())
        if total_ticks == 0:
            logger.warning("No ticks fetched across all tickers — aborting flow.")
            return

        logger.info("Extracted %d ticks across %d tickers.", total_ticks, len(raw_data))

        # 3. Bronze Loader
        uploaded = save_intraday_bronze(raw_data)
        logger.info("Bronze: %d files uploaded to MinIO.", len(uploaded))

        # 4. Transform
        df = transform_ticks(raw_data)
        logger.info("Transformed %d clean tick rows.", len(df))

        # 5. Silver Loader
        if not df.is_empty():
            rows = load_intraday_silver(df)
            logger.info("Silver: inserted %d rows into ClickHouse.", rows)

            # 6. Telegram Alert
            try:
                from src.alerts.telegram_alert import check_and_alert
                check_and_alert(df)
            except Exception as exc:
                logger.warning("Telegram alert check failed: %s", exc)
        else:
            logger.info("Silver: 0 rows to insert.")

        logger.info("=== Intraday Flow COMPLETE ===")

    run_intraday_etl_task()


vn30_intraday_dag = vn30_intraday_pipeline()
