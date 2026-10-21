-- Staging model for retail market food commodity prices
-- Reads from silver Delta table s3://silver/food_prices
with source as (
    select * from {{ source('silver', 'food_prices') }}
),

renamed as (
    select
        cast(date as date) as price_date,
        trim(market) as market,
        trim(commodity) as commodity,
        cast(price as double) as price_pkr,
        upper(trim(unit)) as unit
    from source
    where price is not null
      and price > 0
)

select * from renamed
