-- =============================================================================
-- VN30 ETL Pipeline — ClickHouse DDL Schema
-- Tech Stack: vnstock -> Kafka -> ClickHouse (Kafka Engine + MV) -> dbt -> Grafana
-- =============================================================================

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

-- =============================================================================
-- RAW INGESTION LAYER (ClickHouse Native Kafka Ingestion)
-- =============================================================================

-- raw_intraday_ticks: Bảng lưu trữ Raw tick records từ Kafka
CREATE TABLE IF NOT EXISTS raw_intraday_ticks (
    ticker      LowCardinality(String),
    match_time  DateTime64(3, 'Asia/Ho_Chi_Minh'),
    price       Float64,
    volume      UInt64,
    action      LowCardinality(String)  -- 'BUY' | 'SELL' | 'UNKNOWN'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(match_time)
ORDER BY (ticker, match_time)
TTL toDateTime(match_time) + INTERVAL 90 DAY;

-- Kafka Engine Stream: Kết nối trực tiếp và consume topic vn30.intraday.ticks
CREATE TABLE IF NOT EXISTS kafka_intraday_ticks_stream (
    ticker     LowCardinality(String),
    match_time DateTime64(3, 'Asia/Ho_Chi_Minh'),
    price      Float64,
    volume     UInt64,
    action     LowCardinality(String)
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'vn30.intraday.ticks',
    kafka_group_name = 'clickhouse-realtime-consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_poll_timeout_ms = 500,
    kafka_max_block_size = 1048576;

-- Materialized View: Tự động forward các bản ghi từ Kafka Stream vào bảng raw_intraday_ticks
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kafka_intraday_to_raw
TO raw_intraday_ticks
AS SELECT
    ticker,
    match_time,
    price,
    volume,
    action
FROM kafka_intraday_ticks_stream;


