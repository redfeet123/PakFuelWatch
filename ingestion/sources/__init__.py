"""Source ingestion modules for Bronze layer."""

from ingestion.sources.brent import (
    fetch_brent,
    fetch_brent_backfill,
    fetch_brent_incremental,
)
from ingestion.sources.fuel import (
    detect_fuel_price_gaps,
    fetch_news_fuel_prices,
    fetch_pso_fuel_prices,
    load_fuel_seed_history,
    reconcile_fuel_prices,
    transform_fuel_news,
    transform_fuel_pso,
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
from ingestion.sources.pbs import (
    discover_pbs_release_links,
    fetch_pbs_backfill,
    fetch_pbs_cpi_latest,
    fetch_pbs_spi_latest,
    transform_pbs_cpi,
    transform_pbs_spi,
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
    "discover_pbs_release_links",
    "fetch_pbs_spi_latest",
    "fetch_pbs_cpi_latest",
    "fetch_pbs_backfill",
    "transform_pbs_spi",
    "transform_pbs_cpi",
    "fetch_pso_fuel_prices",
    "fetch_news_fuel_prices",
    "transform_fuel_pso",
    "transform_fuel_news",
    "load_fuel_seed_history",
    "reconcile_fuel_prices",
    "detect_fuel_price_gaps",
]

