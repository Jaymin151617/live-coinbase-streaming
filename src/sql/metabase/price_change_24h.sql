-- Fetch the latest 24-hour price percentage change for a given product
SELECT
    price_pct_chg_24h   -- Percentage change in price over the past 24 hours
FROM
    coinbase.ticker_snapshot   -- Table storing real-time ticker updates
WHERE
    product_id = {{product_id}}  -- Filter for selected trading pair (Metabase variable)
ORDER BY
    ticker_ts_utc DESC           -- Sort by timestamp (latest first)
LIMIT 1                          -- Return only the most recent snapshot
;