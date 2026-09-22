{{ config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by=['ticker', 'window_start'],
    partition_by='toYYYYMMDD(window_start)'
) }}

SELECT
    ticker,
    toStartOfMinute(match_time) AS window_start,
    argMin(price, match_time)   AS open,
    max(price)                  AS high,
    min(price)                  AS low,
    argMax(price, match_time)   AS close,
    sum(volume)                 AS total_volume,
    sumIf(volume, action = 'BUY')  AS buy_volume,
    sumIf(volume, action = 'SELL') AS sell_volume,
    count()                     AS tick_count
FROM {{ ref('stg_intraday_ticks') }}
GROUP BY
    ticker,
    toStartOfMinute(match_time)

