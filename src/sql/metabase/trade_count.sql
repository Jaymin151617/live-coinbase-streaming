-- Calculate total number of trades over the last 30 minutes
SELECT
    SUM(trade_count) AS trade_count_30m  -- Aggregate total trades executed
FROM
    coinbase.market_trades_agg           -- Pre-aggregated trades table
WHERE
    product_id = {{product_id}}          -- Filter for selected trading pair (Metabase variable)
    AND trade_time_utc >= NOW() - INTERVAL '30 minutes'  -- Only include last 30 mins of data
;