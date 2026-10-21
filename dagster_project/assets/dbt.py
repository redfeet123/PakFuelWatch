"""Dagster integration for dbt-duckdb Gold layer models."""

import os
import shutil
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Optional

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    define_asset_job,
)
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    dbt_assets,
)

from ingestion.config import get_settings

DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "..", "dbt_project").resolve()
DBT_PROFILES_DIR = DBT_PROJECT_DIR
MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"

settings = get_settings()
dbt_executable = shutil.which("dbt") or str(Path(sys.executable).parent / "dbt")

# Set object storage environment variables for DuckDB and S3
os.environ["MINIO_ENDPOINT"] = settings.minio_endpoint
os.environ["MINIO_ACCESS_KEY"] = settings.minio_access_key
os.environ["MINIO_SECRET_KEY"] = settings.minio_secret_key
os.environ["AWS_ACCESS_KEY_ID"] = settings.minio_access_key
os.environ["AWS_SECRET_ACCESS_KEY"] = settings.minio_secret_key
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_ENDPOINT_URL_S3"] = settings.minio_endpoint
os.environ["AWS_ENDPOINT_URL"] = settings.minio_endpoint
os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
os.environ["AWS_ALLOW_HTTP"] = "true"

dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROFILES_DIR),
    dbt_executable=dbt_executable,
)

# Parse manifest if not already generated
if not MANIFEST_PATH.exists():
    dbt_resource.cli(["parse"]).wait()


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    """Custom translator mapping dbt sources to upstream Dagster Silver assets."""

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        if dbt_resource_props.get("resource_type") == "source":
            name = dbt_resource_props.get("name")
            mapping = {
                "fuel_prices": "silver_fuel_prices",
                "brent_daily": "silver_brent",
                "fx_daily": "silver_fx",
                "food_prices": "silver_food_prices",
                "spi_weekly": "silver_spi_weekly",
                "cpi_monthly": "silver_cpi_monthly",
            }
            return AssetKey(mapping.get(name, f"silver_{name}"))
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        fqn = dbt_resource_props.get("fqn", [])
        if "marts" in fqn:
            return "gold"
        if "staging" in fqn:
            return "staging"
        return "gold"


@dbt_assets(
    manifest=MANIFEST_PATH,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
)
def dbt_gold_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Execute dbt build for staging and gold mart models."""
    yield from dbt.cli(["build"], context=context).stream()


dbt_models_job = define_asset_job(
    name="dbt_models_job",
    selection=AssetSelection.groups("gold", "staging"),
    description="Job executing dbt models for staging and gold layers.",
)

dbt_assets_list = [dbt_gold_assets]
dbt_jobs = [dbt_models_job]
