-- Staging model for fuel prices (Petrol and Diesel)
-- Reads from silver Delta table s3://silver/fuel_prices
with source as (
    select * from {{ source('silver', 'fuel_prices') }}
),

renamed as (
    select
        cast(effective_date as date) as effective_date,
        trim(product) as product,
        cast(price_pkr_per_litre as double) as price_pkr_per_litre,
        lower(trim(source)) as source,
        lower(trim(confidence)) as confidence
    from source
    where price_pkr_per_litre is not null
      and price_pkr_per_litre > 0
)

select * from renamed
