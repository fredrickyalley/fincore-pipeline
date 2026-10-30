-- What the local pipeline produced. Used by `run results`.
\echo '== Row counts (raw landing tables and marts) =='
SELECT
    (SELECT count(*) FROM raw.fincore_trades)                 AS raw_trades,
    (SELECT count(*) FROM raw.fincore_market_data)            AS raw_prices,
    (SELECT count(*) FROM raw.fincore_portfolio_positions)    AS raw_positions,
    (SELECT count(*) FROM analytics_marts.fct_daily_trades)   AS fct_daily_trades,
    (SELECT count(*) FROM analytics_marts.dim_instrument)     AS dim_instrument;

\echo '== agg_portfolio_pnl (one row per portfolio per date) =='
SELECT portfolio_id, pnl_date, total_pnl, trade_count, unresolvable_trade_count,
       position_count, total_market_value_usd, limit_breach_count
FROM analytics_marts.agg_portfolio_pnl
ORDER BY pnl_date, portfolio_id;
