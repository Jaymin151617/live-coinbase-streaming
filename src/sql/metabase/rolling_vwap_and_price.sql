-- Generate price vs rolling VWAP (Volume Weighted Average Price)
-- for the last 30 minutes, aligned on timestamp

WITH price_data AS (
    -- Step 1: Fetch price data (ticker snapshots)
    SELECT
        ticker_ts_utc AS ts_utc,   -- Timestamp (UTC)
        price                        -- Latest traded price
    FROM
        coinbase.ticker_snapshot
    WHERE
        product_id = {{product_id}}  -- Selected trading pair (Metabase variable)
        AND ticker_ts_utc >= NOW() - INTERVAL '30 minutes'  -- Last 30 minutes
),

vwap_data AS (
    -- Step 2: Compute rolling VWAP using window functions
    SELECT
        trade_time_utc AS ts_utc,   -- Timestamp (UTC)

        -- Rolling VWAP = cumulative notional / cumulative volume
        SUM(notional_value) OVER (ORDER BY trade_time_utc)
        /
        NULLIF(
            SUM(total_volume) OVER (ORDER BY trade_time_utc),
            0
        ) AS rolling_vwap
    FROM
        coinbase.market_trades_agg
    WHERE
        product_id = {{product_id}}  -- Selected trading pair
        AND trade_time_utc >= NOW() - INTERVAL '30 minutes'  -- Last 30 minutes
)

-- Step 3: Join price with VWAP on timestamp
SELECT
    TO_CHAR(
        p.ts_utc AT TIME ZONE 'Asia/Kolkata',
        'HH24:MI'
    ) AS time_ist,        -- Convert UTC → IST for display

    p.price,              -- Actual market price
    v.rolling_vwap        -- Rolling VWAP value

FROM
    price_data p
LEFT JOIN
    vwap_data v
    ON p.ts_utc = v.ts_utc   -- Join on exact timestamp alignment

ORDER BY
    p.ts_utc;                -- Ensure chronological order