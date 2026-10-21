-- Staging model for daily foreign exchange rates (USD/PKR)
-- Reads from silver Delta table s3://silver/fx_daily
with source as (
    select * from {{ source('silver', 'fx_daily') }}
),

renamed as (
    select
        cast(date as date) as rate_date,
        upper(trim(pair)) as currency_pair,
        cast(rate as double) as usd_pkr_rate,
        lower(trim(source)) as source
    from source
    where rate is not null
      and rate > 0
)

select * from renamed
