-- =============================================================================
-- VN30 ETL Pipeline — ClickHouse DDL Schema
-- Kiến trúc: Medallion (Bronze → Silver → Gold) + Star Schema
-- Engine conventions:
--   ReplacingMergeTree  → dedup on insert (idempotent upserts)
--   SummingMergeTree    → aggregate sums (used via Materialized View)
-- =============================================================================

-- Tạo database nếu chưa tồn tại (Docker entrypoint đã tạo nhưng phòng hờ)
CREATE DATABASE IF NOT EXISTS vn30;

USE vn30;

-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

-- dim_stocks: Danh mục 30 mã cổ phiếu VN30
CREATE TABLE IF NOT EXISTS dim_stocks (
    stock_key     String,           -- PK: mã cổ phiếu, VD: 'HPG', 'VCB'
    company_name  String,
    exchange      String DEFAULT 'HOSE',
    sector_name   String,           -- VD: 'Vật liệu', 'Tài chính'
    industry_name String,           -- VD: 'Thép', 'Ngân hàng'
    is_vn30       UInt8  DEFAULT 1, -- 1=TRUE, 0=FALSE
    updated_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY stock_key;

-- dim_date: Chiều thời gian — chứa thông tin ngày giao dịch
CREATE TABLE IF NOT EXISTS dim_date (
    date_key       Date,            -- PK: ngày, VD: '2026-08-13'
    day_of_week    UInt8,           -- 1=Thứ 2 ... 7=Chủ nhật
    week_of_year   UInt8,
    month_number   UInt8,
    year_number    UInt16,
    is_trading_day UInt8 DEFAULT 1, -- 1=ngày giao dịch, 0=nghỉ/lễ
    holiday_name   String DEFAULT ''
)
ENGINE = ReplacingMergeTree()
ORDER BY date_key;

-- Pre-populate dim_date 2024-2030 sẽ được seed riêng

-- =============================================================================
-- BRONZE LAYER (Raw staging — lưu payload gốc để audit/replay)
-- =============================================================================

CREATE TABLE IF NOT EXISTS bronze_intraday_raw (
    ingested_at  DateTime,          -- Thời điểm cào dữ liệu
    ticker       LowCardinality(String),
    raw_payload  String             -- JSON gốc từ vnstock3 API
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ticker, ingested_at)
TTL ingested_at + INTERVAL 90 DAY;  -- Tự xóa Bronze > 90 ngày

-- =============================================================================
-- SILVER LAYER (Cleaned & deduplicated)
-- =============================================================================

-- silver_intraday_ticks: Tick-by-tick khớp lệnh trong ngày
CREATE TABLE IF NOT EXISTS silver_intraday_ticks (
    ticker      LowCardinality(String),
    match_time  DateTime,           -- Thời điểm khớp lệnh (GMT+7)
    price       Float64,            -- Giá khớp (VND)
    volume      UInt64,             -- Khối lượng khớp
    action      Enum8(
                    'BUY'     = 1,
                    'SELL'    = 2,
                    'UNKNOWN' = 0
                )
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMMDD(match_time)
ORDER BY (ticker, match_time, price, volume)
SETTINGS index_granularity = 8192;

-- silver_daily_ohlcv: Nến ngày chốt phiên
CREATE TABLE IF NOT EXISTS silver_daily_ohlcv (
    ticker       LowCardinality(String),
    trade_date   Date,
    open_price   Float64,
    high_price   Float64,
    low_price    Float64,
    close_price  Float64,
    total_volume UInt64,
    total_value  Float64,           -- Giá trị giao dịch (VND)
    -- Computed fields
    price_change       Float64 DEFAULT 0,    -- Thay đổi tuyệt đối
    price_change_pct   Float32 DEFAULT 0     -- Thay đổi % so với phiên trước
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(trade_date)
ORDER BY (ticker, trade_date);

-- =============================================================================
-- GOLD LAYER (Aggregated analytics — auto-maintained by Materialized Views)
-- =============================================================================

-- gold_ohlcv_1min: Nến 1 phút — aggregate từ silver_intraday_ticks
-- Step 1: Backing table (SummingMergeTree sums các cột UInt64/Float64)
CREATE TABLE IF NOT EXISTS gold_ohlcv_1min (
    ticker       LowCardinality(String),
    window_start DateTime,          -- Mốc đầu mỗi phút
    open         Float64,
    high         Float64,
    low          Float64,
    close        Float64,
    volume       UInt64,
    buy_volume   UInt64,
    sell_volume  UInt64,
    tick_count   UInt32             -- Số lượng tick trong nến
)
ENGINE = SummingMergeTree((volume, buy_volume, sell_volume, tick_count))
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (ticker, window_start);

-- Step 2: Materialized View tự populate gold_ohlcv_1min mỗi khi silver có INSERT mới
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_silver_to_gold_1min
TO gold_ohlcv_1min
AS
SELECT
    ticker,
    toStartOfMinute(match_time)  AS window_start,
    argMin(price, match_time)    AS open,
    max(price)                   AS high,
    min(price)                   AS low,
    argMax(price, match_time)    AS close,
    sum(volume)                  AS volume,
    sumIf(volume, action = 'BUY')   AS buy_volume,
    sumIf(volume, action = 'SELL')  AS sell_volume,
    toUInt32(count())            AS tick_count
FROM silver_intraday_ticks
GROUP BY ticker, toStartOfMinute(match_time);


-- gold_vn30_daily_money_flow: Dòng tiền ròng toàn rổ theo ngày
CREATE TABLE IF NOT EXISTS gold_vn30_daily_money_flow (
    trade_date      Date,
    ticker          LowCardinality(String),
    net_buy_volume  Int64,          -- buy_volume - sell_volume (có thể âm)
    net_buy_value   Float64,        -- Net value (VND)
    buy_ratio       Float32,        -- buy_vol / total_vol
    total_volume    UInt64,
    computed_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(trade_date)
ORDER BY (trade_date, ticker);

-- =============================================================================
-- ALERT LOG (Audit trail cho Telegram alerts)
-- =============================================================================

CREATE TABLE IF NOT EXISTS alert_log (
    alert_time   DateTime DEFAULT now(),
    ticker       LowCardinality(String),
    alert_type   Enum8('SHARK_BUY' = 1, 'SHARK_SELL' = 2, 'VOLUME_SPIKE' = 3),
    vol_1min     UInt64,
    ma20_vol     Float64,
    buy_ratio    Float32,
    message_sent UInt8 DEFAULT 0   -- 1 nếu Telegram đã nhận
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(alert_time)
ORDER BY (alert_time, ticker)
TTL alert_time + INTERVAL 365 DAY;
