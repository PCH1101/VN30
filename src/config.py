"""
Application settings loaded from environment variables via pydantic-settings.
All credentials must be in .env — never hardcoded.
"""

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class ClickHouseSettings(BaseSettings):
    host: str = "localhost"
    port: int = 9000
    http_port: int = 8123
    database: str = "vn30"
    user: str = "vn30_user"
    password: str = "123456"

    model_config = SettingsConfigDict(env_prefix="CLICKHOUSE_")


class KafkaSettings(BaseSettings):
    bootstrap_servers: str = "localhost:9092"
    topic_intraday: str = "vn30.intraday.ticks"
    producer_acks: str = "1"
    producer_retries: int = 3

    model_config = SettingsConfigDict(env_prefix="KAFKA_")


class AirflowSettings(BaseSettings):
    webserver_url: str = "http://localhost:8088"

    model_config = SettingsConfigDict(env_prefix="AIRFLOW_")


class VnstockSettings(BaseSettings):
    source: str = "VCI"
    api_key: str = ""
    timeout: int = 30
    show_log: bool = False

    model_config = SettingsConfigDict(env_prefix="VNSTOCK_")


class Settings(BaseSettings):
    # Sub-settings (nested via env prefix)
    clickhouse: ClickHouseSettings = ClickHouseSettings()
    kafka: KafkaSettings = KafkaSettings()
    airflow: AirflowSettings = AirflowSettings()
    vnstock: VnstockSettings = VnstockSettings()

    # VN30 ticker universe
    vn30_tickers: list[str] = [
        "ACB", "BCM", "BID", "BVH", "CTG",
        "FPT", "GAS", "GVR", "HDB", "HPG",
        "MBB", "MSN", "MWG", "PLX", "POW",
        "SAB", "SHB", "SSB", "SSI", "STB",
        "TCB", "TPB", "VCB", "VHM", "VIB",
        "VIC", "VJC", "VNM", "VPB", "VRE",
    ]

    # Trading hours (Vietnam GMT+7)
    trading_sessions: list[tuple[str, str]] = [
        ("09:15", "11:30"),
        ("13:00", "15:00"),
    ]

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached Settings instance (loaded once at startup)."""
    return Settings()


# Convenience shortcut
settings = get_settings()
