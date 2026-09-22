{{ config(materialized='view') }}

WITH raw_source AS (
    SELECT
        ticker,
        match_time,
        price,
        volume,
        action
    FROM {{ source('vn30_raw', 'raw_intraday_ticks') }}
    WHERE price > 0 AND volume > 0
),

deduped AS (
    SELECT
        ticker,
        match_time,
        price,
        volume,
        action,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, match_time, price, volume, action
            ORDER BY match_time
        ) AS row_num
    FROM raw_source
)

SELECT
    ticker,
    match_time,
    price,
    volume,
    action
FROM deduped
WHERE row_num = 1
