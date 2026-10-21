-- Mart: fuel_price_timeline
-- Combines fuel price revisions with nearest prior Brent crude and USD/PKR exchange rates.
-- Computes absolute and percentage price changes per revision.

with fuel as (
    select
        effective_date,
        product,
        price_pkr_per_litre,
        source,
        confidence
    from {{ ref('stg_fuel_prices') }}
),

fuel_deduped as (
    -- Deduplicate if multiple source entries exist for the same date and product
    select
        effective_date,
        product,
        price_pkr_per_litre,
        source,
        confidence,
        row_number() over (
            partition by effective_date, product
            order by
                case confidence
                    when 'high' then 1
                    when 'medium' then 2
                    when 'low' then 3
                    else 4
                end,
                price_pkr_per_litre desc
        ) as rn
    from fuel
),

fuel_revisions as (
    select
        effective_date,
        product,
        price_pkr_per_litre,
        source,
        confidence,
        lag(price_pkr_per_litre) over (
            partition by product
            order by effective_date
        ) as prev_fuel_price_pkr,
        lag(effective_date) over (
            partition by product
            order by effective_date
        ) as prev_effective_date
    from fuel_deduped
    where rn = 1
),

brent as (
    select
        price_date,
        brent_price_usd
    from {{ ref('stg_brent_daily') }}
),

fx as (
    select
        rate_date,
        usd_pkr_rate
    from {{ ref('stg_fx_daily') }}
),

-- Join nearest prior Brent crude spot price
fuel_with_brent as (
    select
        fr.*,
        b.brent_price_usd as nearest_brent_usd,
        b.price_date as brent_price_date,
        row_number() over (
            partition by fr.effective_date, fr.product
            order by b.price_date desc
        ) as brent_rn
    from fuel_revisions fr
    left join brent b
        on b.price_date <= fr.effective_date
),

-- Join nearest prior USD/PKR exchange rate
fuel_with_fx as (
    select
        fwb.*,
        fx.usd_pkr_rate as nearest_usd_pkr,
        fx.rate_date as fx_rate_date,
        row_number() over (
            partition by fwb.effective_date, fwb.product
            order by fx.rate_date desc
        ) as fx_rn
    from fuel_with_brent fwb
    left join fx
        on fx.rate_date <= fwb.effective_date
    where fwb.brent_rn = 1 or fwb.brent_rn is null
),

final as (
    select
        effective_date,
        product,
        price_pkr_per_litre,
        prev_fuel_price_pkr,
        round(price_pkr_per_litre - prev_fuel_price_pkr, 2) as fuel_price_change_pkr,
        round(
            case
                when prev_fuel_price_pkr > 0
                then ((price_pkr_per_litre - prev_fuel_price_pkr) / prev_fuel_price_pkr) * 100.0
                else null
            end,
            2
        ) as fuel_price_change_pct,
        nearest_brent_usd,
        brent_price_date,
        nearest_usd_pkr,
        fx_rate_date,
        round(nearest_brent_usd * nearest_usd_pkr, 2) as landed_crude_pkr_per_bbl,
        source,
        confidence
    from fuel_with_fx
    where fx_rn = 1 or fx_rn is null
)

select * from final
order by product, effective_date
