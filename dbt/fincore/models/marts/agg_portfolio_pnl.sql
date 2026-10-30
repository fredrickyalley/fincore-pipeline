{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='pnl_date',
        sort=['pnl_date', 'portfolio_id'],
    )
}}

-- grain: one row per portfolio per date.
-- total_pnl comes only from fct_daily_trades so the reconciliation test can be exact.

with trade_pnl as (

    select
        portfolio_id,
        trade_date                                      as pnl_date,
        sum(realized_pnl_usd)                           as total_pnl,
        count(*)                                        as trade_count,
        sum(case when is_pnl_unresolvable then 1 else 0 end) as unresolvable_trade_count
    from {{ ref('fct_daily_trades') }}
    {% if is_incremental() %}
    where trade_date = '{{ var("run_date") }}'::date
    {% endif %}
    group by 1, 2

),

positions as (

    select
        portfolio_id,
        as_of_date                                      as pnl_date,
        count(*)                                        as position_count,
        sum(market_value_usd)                           as total_market_value_usd,
        sum(unrealized_pnl_usd)                         as total_unrealized_pnl_usd,
        sum(case when is_position_limit_breached then 1 else 0 end) as limit_breach_count
    from {{ ref('stg_portfolio_positions') }}
    {% if is_incremental() %}
    where as_of_date = '{{ var("run_date") }}'::date
    {% endif %}
    group by 1, 2

)

select
    coalesce(trade_pnl.portfolio_id, positions.portfolio_id)   as portfolio_id,
    coalesce(trade_pnl.pnl_date, positions.pnl_date)           as pnl_date,
    coalesce(trade_pnl.total_pnl, 0)::decimal(38, 6)           as total_pnl,
    coalesce(trade_pnl.trade_count, 0)                         as trade_count,
    coalesce(trade_pnl.unresolvable_trade_count, 0)            as unresolvable_trade_count,
    coalesce(positions.position_count, 0)                      as position_count,
    coalesce(positions.total_market_value_usd, 0)::decimal(38, 6)   as total_market_value_usd,
    coalesce(positions.total_unrealized_pnl_usd, 0)::decimal(38, 6) as total_unrealized_pnl_usd,
    coalesce(positions.limit_breach_count, 0)                  as limit_breach_count
from trade_pnl
full outer join positions
    on trade_pnl.portfolio_id = positions.portfolio_id
   and trade_pnl.pnl_date = positions.pnl_date
