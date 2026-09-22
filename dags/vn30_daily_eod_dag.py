"""
Airflow DAG — VN30 Daily End-of-Day (EOD) Pipeline.
Runs once every trading day at 15:15 (Monday - Friday GMT+7).
Aggregates daily OHLCV, runs dbt models, and executes data quality tests.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

default_args = {
    "owner": "vn30-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="vn30_daily_eod_dag",
    default_args=default_args,
    description="VN30 EOD Pipeline — Aggregate Daily OHLCV -> dbt transform -> dbt test",
    schedule_interval="15 15 * * 1-5",
    start_date=datetime(2026, 1, 1, tzinfo=VN_TZ),
    catchup=False,
    tags=["vn30", "eod", "daily", "batch"],
)
def vn30_daily_eod_pipeline():

    @task(task_id="aggregate_daily_ohlcv")
    def aggregate_daily_ohlcv_task(**context) -> None:
        from src.extractors.daily_eod import run_eod_pipeline
        
        logger.info("=== Running VN30 EOD Daily OHLCV Aggregation ===")
        total = run_eod_pipeline()
        logger.info("EOD Aggregation completed. %d records updated in raw_daily_ohlcv.", total)

    dbt_run = BashOperator(
        task_id="dbt_run_transformations",
        bash_command="cd /opt/airflow/transform && DBT_LOG_PATH=/tmp/dbt_logs DBT_TARGET_PATH=/tmp/dbt_target dbt run --profiles-dir . --project-dir . --log-path /tmp/dbt_logs",
    )

    dbt_test = BashOperator(
        task_id="dbt_test_transformations",
        bash_command="cd /opt/airflow/transform && DBT_LOG_PATH=/tmp/dbt_logs DBT_TARGET_PATH=/tmp/dbt_target dbt test --profiles-dir . --project-dir . --log-path /tmp/dbt_logs",
    )

    aggregate_daily_ohlcv_task() >> dbt_run >> dbt_test


vn30_daily_eod_dag = vn30_daily_eod_pipeline()
