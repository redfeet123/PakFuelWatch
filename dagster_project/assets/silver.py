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
    bronze_fuel_news,
    bronze_fuel_pso,
    bronze_fx,
    bronze_hdx_food,
    bronze_pbs_cpi,
    bronze_pbs_spi,
    bronze_worldbank,
)
from ingestion.silver.brent import transform_brent
from ingestion.silver.cpi_monthly import transform_cpi_monthly
from ingestion.silver.food_prices import transform_food_prices
from ingestion.silver.fx import transform_fx
from ingestion.sources.fuel import (
    detect_fuel_price_gaps,
    load_fuel_seed_history,
    reconcile_fuel_prices,
    transform_fuel_news,
    transform_fuel_pso,
)
from ingestion.sources.pbs import (
    transform_pbs_cpi,
    transform_pbs_spi,
)

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


@asset(
    group_name="silver",
    compute_kind="delta",
    deps=[bronze_pbs_spi],
    description="Parse PBS weekly SPI release and merge into Silver spi_weekly Delta table.",
)
def silver_spi_weekly(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """Transform raw Bronze PBS SPI release into typed Silver spi_weekly Delta table."""
    context.log.info("Starting silver_spi_weekly transformation...")
    result = transform_pbs_spi()
    context.log.info(
        "Completed silver_spi_weekly: %d read, %d rejected, %d written to %s",
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
    deps=[bronze_pbs_cpi],
    description="Parse PBS monthly CPI release and merge into Silver cpi_monthly Delta table.",
)
def silver_cpi_monthly_pbs(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """Transform raw Bronze PBS CPI release into typed Silver cpi_monthly Delta table."""
    context.log.info("Starting silver_cpi_monthly_pbs transformation...")
    result = transform_pbs_cpi()
    context.log.info(
        "Completed silver_cpi_monthly_pbs: %d read, %d rejected, %d written to %s",
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
    deps=[bronze_fuel_pso, bronze_fuel_news],
    description="Transform, reconcile, and gap-check fuel prices into Silver Delta table.",
)
def silver_fuel_prices(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """Transform fuel prices from PSO and News, reconcile sources, and detect fortnight gaps."""
    context.log.info("Starting silver_fuel_prices processing...")

    # Transform both sources
    res_pso = transform_fuel_pso()
    res_news = transform_fuel_news()

    # Reconcile sources
    agreed, conflicts = reconcile_fuel_prices()
    context.log.info("Fuel reconciliation: %d agreed, %d conflicts", agreed, conflicts)

    # Detect fortnight gaps
    gaps = detect_fuel_price_gaps()
    if gaps:
        context.log.warning("Detected %d missing fortnights in fuel prices: %s", len(gaps), gaps)

    total_read = res_pso.read_count + res_news.read_count
    total_written = res_pso.written_count + res_news.written_count

    return Output(
        value={
            "total_read": total_read,
            "total_written": total_written,
            "reconciled_agreements": agreed,
            "conflicts_count": conflicts,
            "gaps_count": len(gaps),
        },
        metadata={
            "total_read": total_read,
            "total_written": total_written,
            "reconciled_agreements": agreed,
            "conflicts_count": conflicts,
            "gaps_count": len(gaps),
        },
    )


@asset(
    group_name="silver",
    compute_kind="delta",
    description="Load historical fuel prices from seed CSV into Silver fuel_prices table.",
)
def silver_fuel_seed_backfill(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """Load historical fuel prices from seed CSV."""
    context.log.info("Starting silver_fuel_seed_backfill from CSV...")
    res = load_fuel_seed_history()
    context.log.info("Completed silver_fuel_seed_backfill: %d merged", res.written_count)
    return Output(
        value={"written_count": res.written_count},
        metadata={"written_count": res.written_count, "table_uri": res.table_uri},
    )


silver_assets = [
    silver_brent,
    silver_fx,
    silver_food_prices,
    silver_cpi_monthly,
    silver_spi_weekly,
    silver_cpi_monthly_pbs,
    silver_fuel_prices,
    silver_fuel_seed_backfill,
]

silver_all_job = define_asset_job(
    name="silver_all_job",
    selection=AssetSelection.groups("silver"),
    description="Job executing all Silver Delta transformations.",
)

silver_jobs = [
    silver_all_job,
]


