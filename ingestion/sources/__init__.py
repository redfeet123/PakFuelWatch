"""Source ingestion modules for Bronze layer."""

from ingestion.sources.brent import (
    fetch_brent,
    fetch_brent_backfill,
    fetch_brent_incremental,
)
from ingestion.sources.fx import (
    fetch_fx,
    fetch_fx_backfill,
    fetch_fx_incremental,
)
from ingestion.sources.hdx_food import (
    discover_hdx_csv_resource,
    fetch_hdx_food_prices,
)
from ingestion.sources.worldbank import (
    fetch_worldbank_all,
    fetch_worldbank_indicator,
)

__all__ = [
    "fetch_brent",
    "fetch_brent_incremental",
    "fetch_brent_backfill",
    "fetch_fx",
    "fetch_fx_incremental",
    "fetch_fx_backfill",
    "fetch_worldbank_indicator",
    "fetch_worldbank_all",
    "discover_hdx_csv_resource",
    "fetch_hdx_food_prices",
]
