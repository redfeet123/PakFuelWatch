"""Silver layer Dagster assets for cleaning and Delta Lake loading."""

import logging
from typing import Any, Dict

from dagster import (
    AssetExecutionContext,
    AssetSelection,
    Output,
    asset,
    define_asset_job,
)

from dagster_project.assets.bronze import (
    bronze_brent,
    bronze_fx,
    bronze_hdx_food,
    bronze_worldbank,
)
from ingestion.silver.brent import transform_brent
from ingestion.silver.cpi_monthly import transform_cpi_monthly
from ingestion.silver.food_prices import transform_food_prices
from ingestion.silver.fx import transform_fx

logger = logging.getLogger("pakfuelwatch.dagster.silver")


@asset(
    group_name="silver",
    compute_kind="delta",
    deps=[bronze_brent],
    description="Clean, validate, and merge raw Brent crude prices into Silver Delta.",
)
def silver_brent(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """Transform raw Bronze Brent data into typed Silver brent_daily Delta table."""
    context.log.info("Starting silver_brent transformation...")
    result = transform_brent()
    context.log.info(
        "Completed silver_brent: %d read, %d rejected, %d written to %s",
        result.read_count,
        result.rejected_count,
        result.written_count,
        result.table_uri,
    )
    return Output(
        value={
            "table_name": result.table_name,
            "read_count": result.read_count,
            "rejected_count": result.rejected_count,
            "written_count": result.written_count,
            "table_uri": result.table_uri,
            "rejects_uri": result.rejects_uri,
        },
        metadata={
            "read_count": result.read_count,
            "rejected_count": result.rejected_count,
            "written_count": result.written_count,
            "table_uri": result.table_uri,
            "rejects_uri": result.rejects_uri or "none",
        },
    )


@asset(
    group_name="silver",
    compute_kind="delta",
    deps=[bronze_fx],
    description="Clean, validate, and merge raw USD/PKR FX rates into Silver fx_daily Delta table.",
)
def silver_fx(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """Transform raw Bronze FX data into typed Silver fx_daily Delta table."""
    context.log.info("Starting silver_fx transformation...")
    result = transform_fx()
    context.log.info(
        "Completed silver_fx: %d read, %d rejected, %d written to %s",
        result.read_count,
        result.rejected_count,
        result.written_count,
        result.table_uri,
    )
    return Output(
        value={
            "table_name": result.table_name,
            "read_count": result.read_count,
            "rejected_count": result.rejected_count,
            "written_count": result.written_count,
            "table_uri": result.table_uri,
            "rejects_uri": result.rejects_uri,
        },
        metadata={
            "read_count": result.read_count,
            "rejected_count": result.rejected_count,
            "written_count": result.written_count,
            "table_uri": result.table_uri,
            "rejects_uri": result.rejects_uri or "none",
        },
    )


@asset(
    group_name="silver",
    compute_kind="delta",
    deps=[bronze_hdx_food],
    description="Clean, validate, and merge raw WFP food prices into Silver Delta.",
)
def silver_food_prices(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """Transform raw Bronze HDX WFP Food Prices into typed Silver food_prices Delta table."""
    context.log.info("Starting silver_food_prices transformation...")
    result = transform_food_prices()
    context.log.info(
        "Completed silver_food_prices: %d read, %d rejected, %d written to %s",
        result.read_count,
        result.rejected_count,
        result.written_count,
        result.table_uri,
    )
    return Output(
        value={
            "table_name": result.table_name,
            "read_count": result.read_count,
            "rejected_count": result.rejected_count,
            "written_count": result.written_count,
            "table_uri": result.table_uri,
            "rejects_uri": result.rejects_uri,
        },
        metadata={
            "read_count": result.read_count,
            "rejected_count": result.rejected_count,
            "written_count": result.written_count,
            "table_uri": result.table_uri,
            "rejects_uri": result.rejects_uri or "none",
        },
    )


@asset(
    group_name="silver",
    compute_kind="delta",
    deps=[bronze_worldbank],
    description="Clean, validate, and merge raw World Bank CPI into Silver Delta.",
)
def silver_cpi_monthly(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """Transform raw Bronze World Bank CPI data into typed Silver cpi_monthly Delta table."""
    context.log.info("Starting silver_cpi_monthly transformation...")
    result = transform_cpi_monthly()
    context.log.info(
        "Completed silver_cpi_monthly: %d read, %d rejected, %d written to %s",
        result.read_count,
        result.rejected_count,
        result.written_count,
        result.table_uri,
    )
    return Output(
        value={
            "table_name": result.table_name,
            "read_count": result.read_count,
            "rejected_count": result.rejected_count,
            "written_count": result.written_count,
            "table_uri": result.table_uri,
            "rejects_uri": result.rejects_uri,
        },
        metadata={
            "read_count": result.read_count,
            "rejected_count": result.rejected_count,
            "written_count": result.written_count,
            "table_uri": result.table_uri,
            "rejects_uri": result.rejects_uri or "none",
        },
    )


silver_assets = [
    silver_brent,
    silver_fx,
    silver_food_prices,
    silver_cpi_monthly,
]

silver_all_job = define_asset_job(
    name="silver_all_job",
    selection=AssetSelection.groups("silver"),
    description="Job executing all Silver Delta transformations.",
)

silver_jobs = [
    silver_all_job,
]
