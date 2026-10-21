"""Bronze layer Dagster assets, jobs, and schedules for raw data ingestion."""

import logging
from typing import Any, Dict, List

from dagster import (
    AssetExecutionContext,
    Output,
    ScheduleDefinition,
    asset,
    define_asset_job,
    job,
    op,
)

from ingestion.sources.brent import (
    fetch_brent_backfill,
    fetch_brent_incremental,
)
from ingestion.sources.fx import (
    fetch_fx_backfill,
    fetch_fx_incremental,
)
from ingestion.sources.hdx_food import (
    fetch_hdx_food_prices,
)
from ingestion.sources.worldbank import (
    fetch_worldbank_all,
)

logger = logging.getLogger("pakfuelwatch.dagster.bronze")


# =====================================================================
# Assets (Bronze Raw Ingestion)
# =====================================================================


@asset(
    group_name="bronze",
    compute_kind="python",
    description="Ingest daily Brent crude prices into Bronze storage (FRED with EIA fallback).",
)
def bronze_brent(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """Fetch recent Brent crude prices and write immutable raw payload to Bronze."""
    context.log.info("Starting bronze_brent incremental ingestion...")
    uri, was_written = fetch_brent_incremental(days=30)
    context.log.info("Completed bronze_brent: %s (new_write=%s)", uri, was_written)
    return Output(
        value={"uri": uri, "written": was_written},
        metadata={"s3_uri": uri, "was_written": was_written, "source": "brent"},
    )


@asset(
    group_name="bronze",
    compute_kind="python",
    description="Ingest daily USD/PKR FX rates into Bronze storage (SBP with fallback).",
)
def bronze_fx(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """Fetch recent USD/PKR rates and write immutable raw payload to Bronze."""
    context.log.info("Starting bronze_fx incremental ingestion...")
    uri, was_written = fetch_fx_incremental(days=30)
    context.log.info("Completed bronze_fx: %s (new_write=%s)", uri, was_written)
    return Output(
        value={"uri": uri, "written": was_written},
        metadata={"s3_uri": uri, "was_written": was_written, "source": "fx"},
    )


@asset(
    group_name="bronze",
    compute_kind="python",
    description="Ingest monthly World Bank economic indicators for Pakistan into Bronze.",
)
def bronze_worldbank(context: AssetExecutionContext) -> Output[List[Dict[str, Any]]]:
    """Fetch World Bank indicators (CPI, fuel imports) and write raw payloads to Bronze."""
    context.log.info("Starting bronze_worldbank monthly ingestion...")
    results = fetch_worldbank_all(is_backfill=False)
    summary = [{"uri": uri, "written": written} for uri, written in results]
    context.log.info("Completed bronze_worldbank for %d indicators", len(results))
    return Output(
        value=summary,
        metadata={"indicator_count": len(results), "source": "worldbank"},
    )


@asset(
    group_name="bronze",
    compute_kind="python",
    description="Ingest weekly WFP Food Prices for Pakistan from HDX into Bronze.",
)
def bronze_hdx_food(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """Discover current CSV from HDX API and write raw bytes to Bronze."""
    context.log.info("Starting bronze_hdx_food ingestion...")
    uri, was_written = fetch_hdx_food_prices(is_backfill=False)
    context.log.info("Completed bronze_hdx_food: %s (new_write=%s)", uri, was_written)
    return Output(
        value={"uri": uri, "written": was_written},
        metadata={"s3_uri": uri, "was_written": was_written, "source": "hdx_food"},
    )


bronze_assets = [bronze_brent, bronze_fx, bronze_worldbank, bronze_hdx_food]


# =====================================================================
# Scheduled Asset Jobs
# =====================================================================

brent_daily_job = define_asset_job(
    name="brent_daily_job",
    selection=["bronze_brent"],
    description="Job executing daily incremental Brent crude price ingestion.",
)

fx_daily_job = define_asset_job(
    name="fx_daily_job",
    selection=["bronze_fx"],
    description="Job executing daily USD/PKR FX rate ingestion.",
)

worldbank_monthly_job = define_asset_job(
    name="worldbank_monthly_job",
    selection=["bronze_worldbank"],
    description="Job executing monthly World Bank Pakistan indicators ingestion.",
)

hdx_weekly_job = define_asset_job(
    name="hdx_weekly_job",
    selection=["bronze_hdx_food"],
    description="Job executing weekly WFP Pakistan food prices ingestion from HDX.",
)


# =====================================================================
# Backfill Ops & Jobs
# =====================================================================


@op(description="Execute full historical backfill of Brent crude oil prices.")
def op_brent_backfill() -> str:
    """Op executing full historical backfill of Brent crude oil prices."""
    uri, _ = fetch_brent_backfill()
    return uri


@op(description="Execute historical backfill of USD/PKR exchange rates.")
def op_fx_backfill() -> str:
    """Op executing historical backfill of USD/PKR exchange rates."""
    uri, _ = fetch_fx_backfill()
    return uri


@op(description="Execute full historical backfill of World Bank Pakistan indicators.")
def op_worldbank_backfill() -> int:
    """Op executing full historical backfill of World Bank indicators."""
    results = fetch_worldbank_all(is_backfill=True)
    return len(results)


@op(description="Execute historical backfill of HDX WFP Food Prices.")
def op_hdx_backfill() -> str:
    """Op executing historical backfill of HDX WFP Food Prices."""
    uri, _ = fetch_hdx_food_prices(is_backfill=True)
    return uri


@job(description="Backfill job for Brent crude historical series from inception.")
def brent_backfill_job() -> None:
    """Job to backfill complete Brent history."""
    op_brent_backfill()


@job(description="Backfill job for USD/PKR daily exchange rates history.")
def fx_backfill_job() -> None:
    """Job to backfill complete FX rate history."""
    op_fx_backfill()


@job(description="Backfill job for World Bank Pakistan historical economic indicators.")
def worldbank_backfill_job() -> None:
    """Job to backfill complete World Bank indicators history."""
    op_worldbank_backfill()


@job(description="Backfill job for HDX WFP Food Prices for Pakistan.")
def hdx_backfill_job() -> None:
    """Job to backfill HDX food prices dataset."""
    op_hdx_backfill()


bronze_jobs = [
    brent_daily_job,
    fx_daily_job,
    worldbank_monthly_job,
    hdx_weekly_job,
    brent_backfill_job,
    fx_backfill_job,
    worldbank_backfill_job,
    hdx_backfill_job,
]


# =====================================================================
# Schedules
# =====================================================================

# Brent daily at 06:00 UTC
brent_daily_schedule = ScheduleDefinition(
    name="brent_daily_schedule",
    job=brent_daily_job,
    cron_schedule="0 6 * * *",
    description="Daily schedule fetching Brent crude prices at 06:00 UTC",
)

# FX daily at 18:00 UTC (closing rate)
fx_daily_schedule = ScheduleDefinition(
    name="fx_daily_schedule",
    job=fx_daily_job,
    cron_schedule="0 18 * * *",
    description="Daily schedule fetching USD/PKR rates at 18:00 UTC",
)

# World Bank monthly on 1st of month at 02:00 UTC
worldbank_monthly_schedule = ScheduleDefinition(
    name="worldbank_monthly_schedule",
    job=worldbank_monthly_job,
    cron_schedule="0 2 1 * *",
    description="Monthly schedule fetching World Bank indicators on 1st of month at 02:00 UTC",
)

# HDX weekly on Monday at 03:00 UTC
hdx_weekly_schedule = ScheduleDefinition(
    name="hdx_weekly_schedule",
    job=hdx_weekly_job,
    cron_schedule="0 3 * * 1",
    description="Weekly schedule fetching HDX food prices every Monday at 03:00 UTC",
)

bronze_schedules = [
    brent_daily_schedule,
    fx_daily_schedule,
    worldbank_monthly_schedule,
    hdx_weekly_schedule,
]
