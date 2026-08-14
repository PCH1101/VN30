"""Unit tests cho src/config.py — không cần Docker."""

from src.config import get_settings


def test_settings_loads_vn30_tickers():
    settings = get_settings()
    assert len(settings.vn30_tickers) == 30
    assert "ACB" in settings.vn30_tickers
    assert "VRE" in settings.vn30_tickers


def test_settings_defaults():
    settings = get_settings()
    assert settings.clickhouse.database == "vn30"
    assert settings.minio.bucket_bronze == "vn30-bronze"
    assert settings.volume_spike_multiplier == 3.0
