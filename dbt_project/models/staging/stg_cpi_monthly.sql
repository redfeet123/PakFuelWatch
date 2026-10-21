-- Staging model for monthly Consumer Price Index (CPI) and YoY inflation
-- Reads from silver Delta table s3://silver/cpi_monthly
with source as (
    select * from {{ source('silver', 'cpi_monthly') }}
),

renamed as (
    select
        trim(month) as month,
        trim(category) as category,
        cast(index_value as double) as cpi_index,
        cast(yoy_pct as double) as yoy_inflation_pct
    from source
    where index_value is not null
)

select * from renamed
