-- Calculate total buy volume over the last 30 minutes
SELECT
    SUM(buy_volume) AS buy_volume_30m  -- Aggregate total buy volume
FROM
    coinbase.market_trades_agg         -- Pre-aggregated trades table
WHERE
    product_id = {{product_id}}        -- Filter for selected trading pair (Metabase variable)
    AND trade_time_utc >= NOW() - INTERVAL '30 minutes'  -- Only include last 30 mins of data
;