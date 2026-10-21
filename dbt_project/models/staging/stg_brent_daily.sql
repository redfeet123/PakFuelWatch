-- Staging model for Brent crude daily prices
-- Reads from silver Delta table s3://silver/brent_daily
with source as (
    select * from {{ source('silver', 'brent_daily') }}
),

renamed as (
    select
        cast(date as date) as price_date,
        cast(price_usd as double) as brent_price_usd,
        lower(trim(source)) as source
    from source
    where price_usd is not null
      and price_usd > 0
)

select * from renamed
