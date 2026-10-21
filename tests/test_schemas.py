"""Tests for Silver table PyArrow schemas."""

import pyarrow as pa

from ingestion.core.schemas import (
    BRENT_DAILY_SCHEMA,
    CPI_MONTHLY_SCHEMA,
    FOOD_PRICES_SCHEMA,
    FUEL_PRICE_CONFLICTS_SCHEMA,
    FUEL_PRICES_SCHEMA,
    FX_DAILY_SCHEMA,
    SILVER_SCHEMAS,
    SPI_WEEKLY_SCHEMA,
)


def test_silver_schemas_registry() -> None:
    """Validate that all six silver schemas are registered in SILVER_SCHEMAS."""
    expected_tables = {
        "brent_daily",
        "fx_daily",
        "fuel_prices",
        "cpi_monthly",
        "spi_weekly",
        "food_prices",
    }
    assert set(SILVER_SCHEMAS.keys()) == expected_tables


def test_brent_daily_schema_fields() -> None:
    """Validate fields and types for brent_daily schema."""
    assert BRENT_DAILY_SCHEMA.names == ["date", "price_usd", "source"]
    assert BRENT_DAILY_SCHEMA.field("date").type == pa.date32()
    assert BRENT_DAILY_SCHEMA.field("price_usd").type == pa.float64()
    assert BRENT_DAILY_SCHEMA.field("source").type == pa.string()


def test_fx_daily_schema_fields() -> None:
    """Validate fields and types for fx_daily schema."""
    assert FX_DAILY_SCHEMA.names == ["date", "pair", "rate", "source"]
    assert FX_DAILY_SCHEMA.field("date").type == pa.date32()
    assert FX_DAILY_SCHEMA.field("pair").type == pa.string()
    assert FX_DAILY_SCHEMA.field("rate").type == pa.float64()
    assert FX_DAILY_SCHEMA.field("source").type == pa.string()


def test_fuel_prices_schema_fields() -> None:
    """Validate fields and types for fuel_prices schema."""
    assert FUEL_PRICES_SCHEMA.names == [
        "effective_date",
        "product",
        "price_pkr_per_litre",
        "source",
        "confidence",
    ]
    assert FUEL_PRICES_SCHEMA.field("effective_date").type == pa.date32()
    assert FUEL_PRICES_SCHEMA.field("price_pkr_per_litre").type == pa.float64()


def test_cpi_monthly_schema_fields() -> None:
    """Validate fields and types for cpi_monthly schema."""
    assert CPI_MONTHLY_SCHEMA.names == ["month", "category", "index_value", "yoy_pct"]
    assert CPI_MONTHLY_SCHEMA.field("month").type == pa.string()
    assert CPI_MONTHLY_SCHEMA.field("index_value").type == pa.float64()


def test_spi_weekly_schema_fields() -> None:
    """Validate fields and types for spi_weekly schema."""
    assert SPI_WEEKLY_SCHEMA.names == ["week_end", "item", "price_pkr", "unit"]
    assert SPI_WEEKLY_SCHEMA.field("week_end").type == pa.date32()
    assert SPI_WEEKLY_SCHEMA.field("price_pkr").type == pa.float64()


def test_food_prices_schema_fields() -> None:
    """Validate fields and types for food_prices schema."""
    assert FOOD_PRICES_SCHEMA.names == ["date", "market", "commodity", "price", "unit"]
    assert FOOD_PRICES_SCHEMA.field("date").type == pa.date32()
    assert FOOD_PRICES_SCHEMA.field("price").type == pa.float64()


def test_fuel_price_conflicts_schema_fields() -> None:
    """Validate fields and types for fuel_price_conflicts schema."""
    assert FUEL_PRICE_CONFLICTS_SCHEMA.names == [
        "effective_date",
        "product",
        "source_a",
        "price_a",
        "source_b",
        "price_b",
        "pct_difference",
        "detected_at",
    ]
    assert FUEL_PRICE_CONFLICTS_SCHEMA.field("effective_date").type == pa.date32()
    assert FUEL_PRICE_CONFLICTS_SCHEMA.field("price_a").type == pa.float64()
    assert FUEL_PRICE_CONFLICTS_SCHEMA.field("pct_difference").type == pa.float64()
    assert FUEL_PRICE_CONFLICTS_SCHEMA.field("detected_at").type == pa.string()

