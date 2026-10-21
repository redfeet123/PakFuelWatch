-- Mart: price_change_signal
-- Predicts direction (UP, DOWN, FLAT) for the upcoming fortnightly fuel price revision.
--
-- FORMULA AND RATIONALE:
-- 1. Landed Crude Import Cost in PKR per barrel = Brent (USD) * USD/PKR Exchange Rate.
-- 2. Landed Cost Change (%) = ((Current Landed PKR - Revision Landed PKR) / Revision Landed PKR) * 100.
-- 3. Decision Rule:
--    - If Landed Cost Change > +1.5%  ==> 'UP'   (cost increase pressure)
--    - If Landed Cost Change < -1.5%  ==> 'DOWN' (cost reduction opportunity)
--    - Otherwise                     ==> 'FLAT' (stable within standard pricing buffer)

with latest_fuel as (
    select
        product,
        effective_date as last_revision_date,
        price_pkr_per_litre as current_fuel_price_pkr,
        row_number() over (partition by product order by effective_date desc) as rn
    from {{ ref('stg_fuel_prices') }}
),

latest_fuel_products as (
    select * from latest_fuel where rn = 1
),

brent_revision as (
    select
        lfp.product,
        b.brent_price_usd as brent_at_revision_usd,
        b.price_date as brent_revision_date,
        row_number() over (partition by lfp.product order by b.price_date desc) as rn
    from latest_fuel_products lfp
    left join {{ ref('stg_brent_daily') }} b
        on b.price_date <= lfp.last_revision_date
),

fx_revision as (
    select
        lfp.product,
        f.usd_pkr_rate as fx_at_revision,
        f.rate_date as fx_revision_date,
        row_number() over (partition by lfp.product order by f.rate_date desc) as rn
    from latest_fuel_products lfp
    left join {{ ref('stg_fx_daily') }} f
        on f.rate_date <= lfp.last_revision_date
),

brent_latest as (
    select
        brent_price_usd as brent_latest_usd,
        price_date as brent_latest_date
    from {{ ref('stg_brent_daily') }}
    order by price_date desc
    limit 1
),

fx_latest as (
    select
        usd_pkr_rate as fx_latest,
        rate_date as fx_latest_date
    from {{ ref('stg_fx_daily') }}
    order by rate_date desc
    limit 1
),

combined as (
    select
        lfp.product,
        lfp.last_revision_date,
        lfp.current_fuel_price_pkr,
        br.brent_at_revision_usd,
        bl.brent_latest_usd,
        bl.brent_latest_date,
        round(
            ((bl.brent_latest_usd - br.brent_at_revision_usd) / nullif(br.brent_at_revision_usd, 0)) * 100.0,
            2
        ) as brent_pct_change,
        fr.fx_at_revision,
        fl.fx_latest,
        fl.fx_latest_date,
        round(
            ((fl.fx_latest - fr.fx_at_revision) / nullif(fr.fx_at_revision, 0)) * 100.0,
            2
        ) as fx_pct_change,
        round(br.brent_at_revision_usd * fr.fx_at_revision, 2) as landed_crude_at_revision_pkr,
        round(bl.brent_latest_usd * fl.fx_latest, 2) as landed_crude_latest_pkr,
        round(
            (((bl.brent_latest_usd * fl.fx_latest) - (br.brent_at_revision_usd * fr.fx_at_revision))
            / nullif(br.brent_at_revision_usd * fr.fx_at_revision, 0)) * 100.0,
            2
        ) as landed_cost_pct_change
    from latest_fuel_products lfp
    left join brent_revision br
        on br.product = lfp.product and br.rn = 1
    left join fx_revision fr
        on fr.product = lfp.product and fr.rn = 1
    cross join brent_latest bl
    cross join fx_latest fl
),

signals as (
    select
        product,
        last_revision_date,
        current_fuel_price_pkr,
        brent_at_revision_usd,
        brent_latest_usd,
        brent_pct_change,
        fx_at_revision,
        fx_latest,
        fx_pct_change,
        landed_crude_at_revision_pkr,
        landed_crude_latest_pkr,
        landed_cost_pct_change,
        case
            when landed_cost_pct_change > 1.5 then 'UP'
            when landed_cost_pct_change < -1.5 then 'DOWN'
            else 'FLAT'
        end as predicted_signal,
        case
            when landed_cost_pct_change > 1.5 then
                'Landed crude cost increased by ' || landed_cost_pct_change || '% (Brent: ' || brent_pct_change || '%, FX: ' || fx_pct_change || '%) exceeding the +1.5% upward revision threshold.'
            when landed_cost_pct_change < -1.5 then
                'Landed crude cost decreased by ' || landed_cost_pct_change || '% (Brent: ' || brent_pct_change || '%, FX: ' || fx_pct_change || '%) exceeding the -1.5% downward revision threshold.'
            else
                'Landed crude cost changed by ' || coalesce(cast(landed_cost_pct_change as varchar), '0') || '% within the +/-1.5% price stability buffer.'
        end as signal_rationale,
        current_date as evaluated_at
    from combined
)

select * from signals
order by product
