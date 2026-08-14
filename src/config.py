"""
Application settings loaded from environment variables via pydantic-settings.
All credentials must be in .env — never hardcoded.
"""

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class ClickHouseSettings(BaseSettings):
    host: str = "localhost"
    port: int = 9000
    database: str = "vn30"
    user: str = "vn30_user"
    password: str = "123456"

    model_config = SettingsConfigDict(env_prefix="CLICKHOUSE_")


class MinIOSettings(BaseSettings):
    endpoint: str = "localhost:9001"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin123"
    secure: bool = False
    bucket_bronze: str = "vn30-bronze"

    model_config = SettingsConfigDict(env_prefix="MINIO_")


class TelegramSettings(BaseSettings):
    token: str = ""
    chat_id: str = ""

    model_config = SettingsConfigDict(env_prefix="TELEGRAM_")


class AirflowSettings(BaseSettings):
    webserver_url: str = "http://localhost:8088"

    model_config = SettingsConfigDict(env_prefix="AIRFLOW_")


class VnstockSettings(BaseSettings):
    source: str = "TCBS"
    api_key: str = ""
    timeout: int = 30
    show_log: bool = False

    model_config = SettingsConfigDict(env_prefix="VNSTOCK_")


class Settings(BaseSettings):
    # Sub-settings (nested via env prefix)
    clickhouse: ClickHouseSettings = ClickHouseSettings()
    minio: MinIOSettings = MinIOSettings()
    telegram: TelegramSettings = TelegramSettings()
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

    # Alert thresholds
    volume_spike_multiplier: float = 3.0      # Vol > N * MA20(Vol)
    buy_ratio_buy_threshold: float = 0.70     # >= 70% buy = bullish alert
    buy_ratio_sell_threshold: float = 0.30    # <= 30% buy = bearish alert
    ma_volume_window: int = 20                # MA period for volume baseline

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
