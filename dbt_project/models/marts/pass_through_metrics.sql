-- Mart: pass_through_metrics
-- Analyzes the pass-through of Brent crude and USD/PKR exchange rate changes into
-- domestic retail fuel prices, essential SPI items, and food prices across 0 to 8 week lags.

with lags as (
    -- Integer lags 0 through 8 weeks
    select unnest(range(0, 9)) as lag_weeks
),

drivers_list as (
    select 'Brent Crude (USD)' as driver_name
    union all
    select 'USD/PKR Exchange Rate' as driver_name
    union all
    select 'Landed Crude (PKR)' as driver_name
),

-- Weekly aggregated Brent crude
brent_weekly as (
    select
        date_trunc('week', price_date) as week_start,
        avg(brent_price_usd) as brent_usd
    from {{ ref('stg_brent_daily') }}
    group by 1
),

-- Weekly aggregated USD/PKR exchange rate
fx_weekly as (
    select
        date_trunc('week', rate_date) as week_start,
        avg(usd_pkr_rate) as usd_pkr
    from {{ ref('stg_fx_daily') }}
    group by 1
),

-- Combined weekly macro drivers and week-over-week percentage changes
drivers_weekly as (
    select
        coalesce(b.week_start, f.week_start) as week_start,
        b.brent_usd,
        f.usd_pkr,
        (b.brent_usd * f.usd_pkr) as landed_crude_pkr,
        -- Week-over-week changes
        round(
            (b.brent_usd - lag(b.brent_usd) over (order by coalesce(b.week_start, f.week_start)))
            / nullif(lag(b.brent_usd) over (order by coalesce(b.week_start, f.week_start)), 0) * 100.0,
            2
        ) as brent_pct_change,
        round(
            (f.usd_pkr - lag(f.usd_pkr) over (order by coalesce(b.week_start, f.week_start)))
            / nullif(lag(f.usd_pkr) over (order by coalesce(b.week_start, f.week_start)), 0) * 100.0,
            2
        ) as fx_pct_change,
        round(
            ((b.brent_usd * f.usd_pkr) - lag(b.brent_usd * f.usd_pkr) over (order by coalesce(b.week_start, f.week_start)))
            / nullif(lag(b.brent_usd * f.usd_pkr) over (order by coalesce(b.week_start, f.week_start)), 0) * 100.0,
            2
        ) as landed_crude_pct_change
    from brent_weekly b
    full outer join fx_weekly f
        on b.week_start = f.week_start
),

-- Unpivot drivers for uniform correlation calculation
unpivoted_drivers as (
    select week_start, 'Brent Crude (USD)' as driver_name, brent_pct_change as driver_pct_change from drivers_weekly where brent_pct_change is not null
    union all
    select week_start, 'USD/PKR Exchange Rate' as driver_name, fx_pct_change as driver_pct_change from drivers_weekly where fx_pct_change is not null
    union all
    select week_start, 'Landed Crude (PKR)' as driver_name, landed_crude_pct_change as driver_pct_change from drivers_weekly where landed_crude_pct_change is not null
),

-- 1. Weekly target: Fuel Prices
fuel_weekly as (
    select
        date_trunc('week', effective_date) as week_start,
        product as item_name,
        'Fuel Prices' as target_category,
        avg(price_pkr_per_litre) as price_val
    from {{ ref('stg_fuel_prices') }}
    group by 1, 2
),

-- 2. Weekly target: SPI Essential Basket Items
spi_weekly_items as (
    select
        date_trunc('week', week_end_date) as week_start,
        item as item_name,
        'SPI Basket' as target_category,
        avg(price_pkr) as price_val
    from {{ ref('stg_spi_weekly') }}
    group by 1, 2
),

-- 3. Weekly target: Food Commodity Prices
food_weekly as (
    select
        date_trunc('week', price_date) as week_start,
        commodity as item_name,
        'Food Prices' as target_category,
        avg(price_pkr) as price_val
    from {{ ref('stg_food_prices') }}
    group by 1, 2
),

-- Union all targets with weekly percentage changes
all_targets as (
    select
        week_start,
        target_category,
        item_name,
        price_val,
        round(
            (price_val - lag(price_val) over (partition by target_category, item_name order by week_start))
            / nullif(lag(price_val) over (partition by target_category, item_name order by week_start), 0) * 100.0,
            2
        ) as target_pct_change
    from (
        select * from fuel_weekly
        union all
        select * from spi_weekly_items
        union all
        select * from food_weekly
    )
),

target_items as (
    select distinct target_category, item_name from all_targets
),

-- Cross-product grid of all target items, drivers, and lag weeks (0-8)
grid as (
    select
        ti.target_category,
        ti.item_name,
        dl.driver_name,
        l.lag_weeks
    from target_items ti
    cross join drivers_list dl
    cross join lags l
),

-- Paired observations matching target week with driver at lag_weeks
pairs as (
    select
        t.target_category,
        t.item_name,
        d.driver_name,
        l.lag_weeks,
        t.target_pct_change,
        d.driver_pct_change
    from all_targets t
    cross join lags l
    join unpivoted_drivers d
        on d.week_start = t.week_start - (l.lag_weeks * interval '7 days')
    where t.target_pct_change is not null
      and d.driver_pct_change is not null
),

aggregated_pairs as (
    select
        target_category,
        item_name,
        driver_name,
        lag_weeks,
        count(*) as sample_size,
        round(corr(driver_pct_change, target_pct_change), 4) as correlation,
        round(avg(driver_pct_change), 2) as avg_driver_pct_change,
        round(avg(target_pct_change), 2) as avg_target_pct_change
    from pairs
    group by 1, 2, 3, 4
),

final as (
    select
        g.target_category,
        g.item_name,
        g.driver_name,
        g.lag_weeks,
        coalesce(ap.sample_size, 0) as sample_size,
        ap.correlation,
        ap.avg_driver_pct_change,
        ap.avg_target_pct_change
    from grid g
    left join aggregated_pairs ap
        on g.target_category = ap.target_category
       and g.item_name = ap.item_name
       and g.driver_name = ap.driver_name
       and g.lag_weeks = ap.lag_weeks
)

select * from final
order by target_category, item_name, driver_name, lag_weeks
