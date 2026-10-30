with source as (

    select * from {{ source('raw', 'fincore_portfolio_positions') }}

)

select
    cast(portfolio_id as varchar(64))               as portfolio_id,
    cast(upper(trim(symbol)) as varchar(32))        as symbol,
    cast(as_of_date as date)                        as as_of_date,
    cast(position_quantity as decimal(18, 6))       as position_quantity,
    cast(average_cost as decimal(18, 8))            as average_cost,
    cast(close_price as decimal(18, 8))             as close_price,
    cast(price_currency as char(3))                 as price_currency,
    cast(fx_rate_to_usd as decimal(18, 12))         as fx_rate_to_usd,
    cast(coalesce(market_value_usd, 0) as decimal(38, 6))   as market_value_usd,
    cast(coalesce(unrealized_pnl_usd, 0) as decimal(38, 6)) as unrealized_pnl_usd,
    portfolio_manager,
    cast(max_position_value_usd as decimal(38, 6))  as max_position_value_usd,
    cast(max_var_usd as decimal(38, 6))             as max_var_usd,
    cast(max_single_name_pct as decimal(9, 4))      as max_single_name_pct,
    coalesce(position_limit_breached, false)        as is_position_limit_breached,
    etl_loaded_at
from source
