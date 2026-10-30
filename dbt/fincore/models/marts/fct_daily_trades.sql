{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='trade_date',
        dist='portfolio_id',
        sort=['trade_date', 'portfolio_id'],
    )
}}

-- grain: one row per trade (order_id). Re-running a date replaces that date's rows.

select
    order_id,
    client_order_id,
    portfolio_id,
    symbol,
    side,
    quantity,
    price,
    currency,
    transact_time,
    trade_date,
    close_price,
    close_currency,
    realized_pnl_usd,
    is_pnl_unresolvable,
    unresolvable_reason,
    etl_loaded_at
from {{ ref('stg_trades') }}
{% if is_incremental() %}
where trade_date = '{{ var("run_date") }}'::date
{% endif %}
