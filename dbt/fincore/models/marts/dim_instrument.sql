-- grain: one row per symbol. Static attributes from the seed, latest price facts from
-- market data. Symbols seen in market data but missing from the seed still get a row.

with ranked_prices as (

    select
        symbol,
        asset_class,
        currency,
        close_price,
        price_date,
        row_number() over (partition by symbol order by price_date desc) as recency
    from {{ ref('stg_market_data') }}

),

latest_price as (

    select * from ranked_prices where recency = 1

),

reference as (

    select * from {{ ref('instrument_metadata') }}

)

select
    coalesce(reference.symbol, latest_price.symbol)             as symbol,
    reference.instrument_name,
    coalesce(reference.asset_class, latest_price.asset_class)   as asset_class,
    coalesce(reference.currency, latest_price.currency)         as currency,
    reference.exchange,
    reference.sector,
    reference.isin,
    latest_price.close_price                                    as last_close_price,
    latest_price.price_date                                     as last_price_date,
    reference.symbol is not null                                as has_reference_data
from reference
full outer join latest_price
    on reference.symbol = latest_price.symbol
