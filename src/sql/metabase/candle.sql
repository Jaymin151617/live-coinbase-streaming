-- Fetch the latest candle (most recent price snapshot) for a given product
SELECT
    open,        -- Opening price of the latest candle
    high,        -- Highest price during the candle interval
    low,         -- Lowest price during the candle interval
    close,       -- Closing price of the candle
    avg_price,   -- Average traded price within the candle
    range        -- Price range (high - low)
FROM
    coinbase.candles_snapshot   -- Table storing candle (OHLC) snapshots
WHERE
    product_id = {{product_id}} -- Filter for selected trading pair (Metabase variable)
ORDER BY
    candle_ts_utc DESC          -- Sort by timestamp (latest first)
LIMIT 1                         -- Return only the most recent candle
;