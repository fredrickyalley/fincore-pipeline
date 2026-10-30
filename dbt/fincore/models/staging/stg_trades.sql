with source as (

    select * from {{ source('raw', 'fincore_trades') }}

)

select
    cast(order_id as varchar(64))                         as order_id,
    cast(client_order_id as varchar(64))                  as client_order_id,
    cast(portfolio_id as varchar(64))                     as portfolio_id,
    cast(upper(trim(symbol)) as varchar(32))              as symbol,
    cast(side as varchar(4))                              as side,
    cast(quantity as decimal(18, 6))                      as quantity,
    cast(price as decimal(18, 8))                         as price,
    cast(coalesce(currency, 'USD') as char(3))            as currency,
    cast(transact_time as timestamp)                      as transact_time,
    cast(trade_date as date)                              as trade_date,
    cast(close_price as decimal(18, 8))                   as close_price,
    cast(close_currency as char(3))                       as close_currency,
    cast(realized_pnl_usd as decimal(38, 6))              as realized_pnl_usd,
    coalesce(pnl_unresolvable, false)                     as is_pnl_unresolvable,
    unresolvable_reason,
    etl_loaded_at
from source
