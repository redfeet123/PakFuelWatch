"""PyArrow schemas for PakFuelWatch Silver Delta tables."""

from typing import Dict

import pyarrow as pa

# 1. brent_daily(date, price_usd, source)
BRENT_DAILY_SCHEMA = pa.schema(
    [
        pa.field("date", pa.date32(), nullable=False),
        pa.field("price_usd", pa.float64(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
    ]
)

# 2. fx_daily(date, pair, rate, source)
FX_DAILY_SCHEMA = pa.schema(
    [
        pa.field("date", pa.date32(), nullable=False),
        pa.field("pair", pa.string(), nullable=False),
        pa.field("rate", pa.float64(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
    ]
)

# 3. fuel_prices(effective_date, product, price_pkr_per_litre, source, confidence)
FUEL_PRICES_SCHEMA = pa.schema(
    [
        pa.field("effective_date", pa.date32(), nullable=False),
        pa.field("product", pa.string(), nullable=False),
        pa.field("price_pkr_per_litre", pa.float64(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("confidence", pa.string(), nullable=False),
    ]
)

# 4. cpi_monthly(month, category, index_value, yoy_pct)
CPI_MONTHLY_SCHEMA = pa.schema(
    [
        pa.field("month", pa.string(), nullable=False),
        pa.field("category", pa.string(), nullable=False),
        pa.field("index_value", pa.float64(), nullable=False),
        pa.field("yoy_pct", pa.float64(), nullable=True),
    ]
)

# 5. spi_weekly(week_end, item, price_pkr, unit)
SPI_WEEKLY_SCHEMA = pa.schema(
    [
        pa.field("week_end", pa.date32(), nullable=False),
        pa.field("item", pa.string(), nullable=False),
        pa.field("price_pkr", pa.float64(), nullable=False),
        pa.field("unit", pa.string(), nullable=False),
    ]
)

# 6. food_prices(date, market, commodity, price, unit)
FOOD_PRICES_SCHEMA = pa.schema(
    [
        pa.field("date", pa.date32(), nullable=False),
        pa.field("market", pa.string(), nullable=False),
        pa.field("commodity", pa.string(), nullable=False),
        pa.field("price", pa.float64(), nullable=False),
        pa.field("unit", pa.string(), nullable=False),
    ]
)

# Registry mapping silver table names to their PyArrow schema
SILVER_SCHEMAS: Dict[str, pa.Schema] = {
    "brent_daily": BRENT_DAILY_SCHEMA,
    "fx_daily": FX_DAILY_SCHEMA,
    "fuel_prices": FUEL_PRICES_SCHEMA,
    "cpi_monthly": CPI_MONTHLY_SCHEMA,
    "spi_weekly": SPI_WEEKLY_SCHEMA,
    "food_prices": FOOD_PRICES_SCHEMA,
}
