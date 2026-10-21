"""Core ingestion utilities for PakFuelWatch."""

from ingestion.core.delta_io import read_delta, read_delta_pandas, write_delta_merge
from ingestion.core.http_client import HttpClient, get_http_client
from ingestion.core.schemas import (
    BRENT_DAILY_SCHEMA,
    CPI_MONTHLY_SCHEMA,
    FOOD_PRICES_SCHEMA,
    FUEL_PRICES_SCHEMA,
    FX_DAILY_SCHEMA,
    SILVER_SCHEMAS,
    SPI_WEEKLY_SCHEMA,
)
from ingestion.core.storage import StorageClient, get_raw, list_raw, put_raw

__all__ = [
    "StorageClient",
    "put_raw",
    "get_raw",
    "list_raw",
    "HttpClient",
    "get_http_client",
    "write_delta_merge",
    "read_delta",
    "read_delta_pandas",
    "BRENT_DAILY_SCHEMA",
    "FX_DAILY_SCHEMA",
    "FUEL_PRICES_SCHEMA",
    "CPI_MONTHLY_SCHEMA",
    "SPI_WEEKLY_SCHEMA",
    "FOOD_PRICES_SCHEMA",
    "SILVER_SCHEMAS",
]
