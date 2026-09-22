{{ config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by=['ticker']
) }}

WITH candles_agg AS (
    SELECT
        ticker,
        argMin(open, window_start)  AS open_price,
        argMax(close, window_start) AS last_price,
        sum(total_volume)           AS total_intraday_volume,
        max(window_start)           AS latest_timestamp
    FROM {{ ref('int_ohlcv_1min') }}
    GROUP BY ticker
)

SELECT
    c.ticker,
    s.company_name,
    s.sector_name,
    s.industry_name,
    c.last_price,
    c.total_intraday_volume AS volume,
    c.open_price AS reference_price,
    CASE
        WHEN c.open_price > 0 THEN round((c.last_price - c.open_price) / c.open_price * 100, 2)
        ELSE 0.0
    END AS change_pct,
    c.latest_timestamp AS updated_at
FROM candles_agg c
LEFT JOIN {{ source('vn30_raw', 'dim_stocks') }} s ON c.ticker = s.stock_key

