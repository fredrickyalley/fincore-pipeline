with source as (

    select * from {{ source('raw', 'fincore_market_data') }}

)

select
    cast(upper(trim(symbol)) as varchar(32))    as symbol,
    cast(upper(asset_class) as varchar(32))     as asset_class,
    cast(coalesce(currency, 'USD') as char(3))  as currency,
    cast(close_price as decimal(18, 8))         as close_price,
    cast(price_date as date)                    as price_date,
    etl_loaded_at
from source
-- the ETL already dead-letters bad prices; this is defence in depth, enforced again by a test
where close_price > 0
