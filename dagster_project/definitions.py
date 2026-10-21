"""Dagster definitions entrypoint for PakFuelWatch."""

import logging

from dagster import Definitions, asset

from dagster_project.assets.bronze import (
    bronze_assets,
    bronze_jobs,
    bronze_schedules,
)
from dagster_project.assets.dbt import (
    dbt_assets_list,
    dbt_jobs,
    dbt_resource,
)
from dagster_project.assets.silver import (
    silver_assets,
    silver_jobs,
)

logger = logging.getLogger("pakfuelwatch.dagster")


@asset(
    description="Placeholder smoke asset to verify Dagster infrastructure and UI loading.",
    group_name="monitoring",
)
def placeholder_smoke_asset() -> str:
    """Execute placeholder asset and return a confirmation message.

    Returns:
        Status message indicating successful execution.
    """
    logger.info("Executing placeholder smoke asset...")
    return "PakFuelWatch Dagster infrastructure is up and running"


defs = Definitions(
    assets=[placeholder_smoke_asset, *bronze_assets, *silver_assets, *dbt_assets_list],
    jobs=[*bronze_jobs, *silver_jobs, *dbt_jobs],
    schedules=bronze_schedules,
    resources={"dbt": dbt_resource},
)

