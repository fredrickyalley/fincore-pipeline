-- agg_portfolio_pnl.total_pnl must equal the exact sum of fct_daily_trades.realized_pnl_usd
-- for the same portfolio and date. Any returned row is a failure. Decimal math, no tolerance:
-- a tolerance is how a precision bug hides for three days.

with expected as (

    select
        portfolio_id,
        trade_date as pnl_date,
        coalesce(sum(realized_pnl_usd), 0)::decimal(38, 6) as expected_total_pnl
    from {{ ref('fct_daily_trades') }}
    group by 1, 2

),

actual as (

    select portfolio_id, pnl_date, total_pnl
    from {{ ref('agg_portfolio_pnl') }}

)

select
    coalesce(actual.portfolio_id, expected.portfolio_id) as portfolio_id,
    coalesce(actual.pnl_date, expected.pnl_date)         as pnl_date,
    expected.expected_total_pnl,
    actual.total_pnl
from expected
full outer join actual
    on expected.portfolio_id = actual.portfolio_id
   and expected.pnl_date = actual.pnl_date
where actual.portfolio_id is null
   or (expected.portfolio_id is null and actual.total_pnl <> 0)
   or actual.total_pnl <> expected.expected_total_pnl
