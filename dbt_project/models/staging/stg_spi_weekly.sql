-- Staging model for Sensitive Price Indicator (SPI) weekly basket items
-- Reads from silver Delta table s3://silver/spi_weekly
with source as (
    select * from {{ source('silver', 'spi_weekly') }}
),

renamed as (
    select
        cast(week_end as date) as week_end_date,
        trim(item) as item,
        cast(price_pkr as double) as price_pkr,
        upper(trim(unit)) as unit
    from source
    where price_pkr is not null
      and price_pkr > 0
)

select * from renamed
