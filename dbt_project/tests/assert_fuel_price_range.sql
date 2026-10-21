-- Test: assert_fuel_price_range
-- Fuel retail prices in Pakistan must fall within realistic bounds: 50.0 to 1000.0 PKR/litre.
-- Returns any records violating this range (test fails if any row is returned).

select
    effective_date,
    product,
    price_pkr_per_litre
from {{ ref('stg_fuel_prices') }}
where price_pkr_per_litre < 50.0
   or price_pkr_per_litre > 1000.0
