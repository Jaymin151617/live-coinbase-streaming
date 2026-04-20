-- Generate price vs rolling VWAP (Volume Weighted Average Price)
-- for the last 30 minutes, aligned on timestamp

WITH price_data AS (
    /* Fetch price data (ticker snapshots) */
    SELECT
        ticker_ts_utc AS ts_utc,
        price
    FROM coinbase.ticker_snapshot
    WHERE
        product_id = {{product_id}}
        AND ticker_ts_utc >= NOW() - INTERVAL '30 minutes'
),
vwap_data AS (
    /* Compute rolling VWAP using window functions */
    SELECT
        trade_time_utc AS ts_utc,
        SUM(notional_value) OVER (ORDER BY trade_time_utc)
        /
        NULLIF(
            SUM(total_volume) OVER (ORDER BY trade_time_utc),
            0
        ) AS rolling_vwap
    FROM coinbase.market_trades_agg
    WHERE
        product_id = {{product_id}}
        AND trade_time_utc >= NOW() - INTERVAL '30 minutes'
)
/* Join price with VWAP on timestamp */
SELECT
    TO_CHAR(
        p.ts_utc AT TIME ZONE 'Asia/Kolkata',
        'HH24:MI'
    ) AS time_ist,
    p.price,
    v.rolling_vwap
FROM price_data p
LEFT JOIN vwap_data v
    ON p.ts_utc = v.ts_utc
ORDER BY p.ts_utc;