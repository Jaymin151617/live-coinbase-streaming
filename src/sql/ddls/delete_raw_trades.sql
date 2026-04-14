CREATE OR REPLACE FUNCTION coinbase.cleanup_raw_trades()
RETURNS void AS $$
BEGIN
    WITH to_delete AS (
        SELECT trade_id
        FROM coinbase.raw_market_trades
        WHERE trade_time_utc < NOW() - INTERVAL '1 hour'
        LIMIT 5000
    )
    DELETE FROM coinbase.raw_market_trades
    WHERE trade_id IN (SELECT trade_id FROM to_delete);
END;
$$ LANGUAGE plpgsql;