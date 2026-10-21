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
from ingestion.sources.fuel import (
    fetch_news_fuel_prices,
    fetch_pso_fuel_prices,
)
from ingestion.sources.fx import (
    fetch_fx_backfill,
    fetch_fx_incremental,
)
from ingestion.sources.hdx_food import (
    fetch_hdx_food_prices,
)
from ingestion.sources.pbs import (
    fetch_pbs_backfill,
    fetch_pbs_cpi_latest,
    fetch_pbs_spi_latest,
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


@asset(
    group_name="bronze",
    compute_kind="patchright",
    description="Scrape weekly Sensitive Price Indicator (SPI) release from PBS.",
)
def bronze_pbs_spi(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """Fetch latest weekly SPI release from Pakistan Bureau of Statistics."""
    context.log.info("Starting bronze_pbs_spi browser ingestion...")
    uri, was_written = fetch_pbs_spi_latest()
    context.log.info("Completed bronze_pbs_spi: %s (new_write=%s)", uri, was_written)
    return Output(
        value={"uri": uri, "written": was_written},
        metadata={"s3_uri": uri, "was_written": was_written, "source": "pbs_spi"},
    )


@asset(
    group_name="bronze",
    compute_kind="patchright",
    description="Scrape monthly Consumer Price Index (CPI) release from PBS.",
)
def bronze_pbs_cpi(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """Fetch latest monthly CPI release from Pakistan Bureau of Statistics."""
    context.log.info("Starting bronze_pbs_cpi browser ingestion...")
    uri, was_written = fetch_pbs_cpi_latest()
    context.log.info("Completed bronze_pbs_cpi: %s (new_write=%s)", uri, was_written)
    return Output(
        value={"uri": uri, "written": was_written},
        metadata={"s3_uri": uri, "was_written": was_written, "source": "pbs_cpi"},
    )


@asset(
    group_name="bronze",
    compute_kind="patchright",
    description="Scrape official fuel price notification page (PSO) into Bronze.",
)
def bronze_fuel_pso(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """Scrape official fuel price notification page saving HTML and screenshot to Bronze."""
    context.log.info("Starting bronze_fuel_pso browser scraping...")
    html_uri, png_uri = fetch_pso_fuel_prices()
    context.log.info("Completed bronze_fuel_pso: HTML=%s, PNG=%s", html_uri, png_uri)
    return Output(
        value={"html_uri": html_uri, "png_uri": png_uri},
        metadata={"html_uri": html_uri, "png_uri": png_uri, "source": "fuel_pso"},
    )


@asset(
    group_name="bronze",
    compute_kind="patchright",
    description="Scrape fuel price announcement news coverage into Bronze.",
)
def bronze_fuel_news(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """Scrape news announcement coverage saving HTML and screenshot to Bronze."""
    context.log.info("Starting bronze_fuel_news browser scraping...")
    html_uri, png_uri = fetch_news_fuel_prices()
    context.log.info("Completed bronze_fuel_news: HTML=%s, PNG=%s", html_uri, png_uri)
    return Output(
        value={"html_uri": html_uri, "png_uri": png_uri},
        metadata={"html_uri": html_uri, "png_uri": png_uri, "source": "fuel_news"},
    )


bronze_assets = [
    bronze_brent,
    bronze_fx,
    bronze_worldbank,
    bronze_hdx_food,
    bronze_pbs_spi,
    bronze_pbs_cpi,
    bronze_fuel_pso,
    bronze_fuel_news,
]


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


@op(description="Execute historical backfill of PBS releases.")
def op_pbs_backfill() -> int:
    """Op executing historical backfill of PBS releases."""
    results = fetch_pbs_backfill(months=12)
    return len(results)


@job(description="Backfill job for PBS historical releases (at least 12 months).")
def pbs_backfill_job() -> None:
    """Job to backfill PBS historical releases."""
    op_pbs_backfill()


pbs_spi_weekly_job = define_asset_job(
    name="pbs_spi_weekly_job",
    selection=["bronze_pbs_spi"],
    description="Job executing weekly PBS Sensitive Price Indicator (SPI) scraping.",
)

pbs_cpi_monthly_job = define_asset_job(
    name="pbs_cpi_monthly_job",
    selection=["bronze_pbs_cpi"],
    description="Job executing monthly PBS Consumer Price Index (CPI) scraping.",
)

fuel_scrape_job = define_asset_job(
    name="fuel_scrape_job",
    selection=["bronze_fuel_pso", "bronze_fuel_news"],
    description="Job scraping fuel price announcements from official and news sources.",
)

bronze_jobs = [
    brent_daily_job,
    fx_daily_job,
    worldbank_monthly_job,
    hdx_weekly_job,
    pbs_spi_weekly_job,
    pbs_cpi_monthly_job,
    fuel_scrape_job,
    brent_backfill_job,
    fx_backfill_job,
    worldbank_backfill_job,
    hdx_backfill_job,
    pbs_backfill_job,
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

# PBS SPI weekly on Friday at 12:00 UTC
pbs_spi_weekly_schedule = ScheduleDefinition(
    name="pbs_spi_weekly_schedule",
    job=pbs_spi_weekly_job,
    cron_schedule="0 12 * * 5",
    description="Weekly schedule fetching PBS SPI release on Friday at 12:00 UTC",
)

# PBS CPI monthly on 2nd of month at 08:00 UTC
pbs_cpi_monthly_schedule = ScheduleDefinition(
    name="pbs_cpi_monthly_schedule",
    job=pbs_cpi_monthly_job,
    cron_schedule="0 8 2 * *",
    description="Monthly schedule fetching PBS CPI release on 2nd of month at 08:00 UTC",
)

# Fuel revision window: every 6 hours around the 15th and month-end
fuel_revision_window_schedule = ScheduleDefinition(
    name="fuel_revision_window_schedule",
    job=fuel_scrape_job,
    cron_schedule="0 */6 14,15,16,28,29,30,31 * *",
    description="Scrape fuel prices every 6 hours during revision windows (14-16 & 28-31)",
)

# Fuel daily otherwise
fuel_daily_schedule = ScheduleDefinition(
    name="fuel_daily_schedule",
    job=fuel_scrape_job,
    cron_schedule="0 6 1-13,17-27 * *",
    description="Scrape fuel prices daily at 06:00 UTC during standard non-revision days",
)

bronze_schedules = [
    brent_daily_schedule,
    fx_daily_schedule,
    worldbank_monthly_schedule,
    hdx_weekly_schedule,
    pbs_spi_weekly_schedule,
    pbs_cpi_monthly_schedule,
    fuel_revision_window_schedule,
    fuel_daily_schedule,
]

