"""Tests for Dagster Bronze assets, jobs, and schedules."""

from unittest.mock import patch

from dagster import build_asset_context

from dagster_project.assets.bronze import (
    bronze_brent,
    bronze_fx,
    bronze_hdx_food,
    bronze_worldbank,
)
from dagster_project.definitions import defs


def test_dagster_bronze_definitions_registration() -> None:
    """Verify that all bronze assets, jobs, and schedules are loaded into Definitions."""
    # Check assets
    asset_keys = {str(k.path[0]) for k in defs.resolve_asset_graph().get_all_asset_keys()}
    assert "bronze_brent" in asset_keys
    assert "bronze_fx" in asset_keys
    assert "bronze_worldbank" in asset_keys
    assert "bronze_hdx_food" in asset_keys

    # Check jobs
    job_names = {job.name for job in defs.resolve_all_job_defs()}
    expected_jobs = {
        "brent_daily_job",
        "fx_daily_job",
        "worldbank_monthly_job",
        "hdx_weekly_job",
        "brent_backfill_job",
        "fx_backfill_job",
        "worldbank_backfill_job",
        "hdx_backfill_job",
    }
    assert expected_jobs.issubset(job_names)

    # Check schedules
    schedule_names = {schedule.name for schedule in (defs.schedules or [])}
    expected_schedules = {
        "brent_daily_schedule",
        "fx_daily_schedule",
        "worldbank_monthly_schedule",
        "hdx_weekly_schedule",
    }
    assert expected_schedules.issubset(schedule_names)


def test_bronze_brent_asset_execution() -> None:
    """Test bronze_brent asset execution with mocked fetch function."""
    with patch(
        "dagster_project.assets.bronze.fetch_brent_incremental",
        return_value=("s3://bronze/brent/test.json", True),
    ):
        context = build_asset_context()
        output = bronze_brent(context)
        assert output.value["uri"] == "s3://bronze/brent/test.json"
        assert output.value["written"] is True


def test_bronze_fx_asset_execution() -> None:
    """Test bronze_fx asset execution with mocked fetch function."""
    with patch(
        "dagster_project.assets.bronze.fetch_fx_incremental",
        return_value=("s3://bronze/fx/test.json", True),
    ):
        context = build_asset_context()
        output = bronze_fx(context)
        assert output.value["uri"] == "s3://bronze/fx/test.json"
        assert output.value["written"] is True


def test_bronze_worldbank_asset_execution() -> None:
    """Test bronze_worldbank asset execution with mocked fetch function."""
    with patch(
        "dagster_project.assets.bronze.fetch_worldbank_all",
        return_value=[("s3://bronze/worldbank/cpi.json", True)],
    ):
        context = build_asset_context()
        output = bronze_worldbank(context)
        assert len(output.value) == 1
        assert output.value[0]["uri"] == "s3://bronze/worldbank/cpi.json"


def test_bronze_hdx_food_asset_execution() -> None:
    """Test bronze_hdx_food asset execution with mocked fetch function."""
    with patch(
        "dagster_project.assets.bronze.fetch_hdx_food_prices",
        return_value=("s3://bronze/hdx_food/prices.csv", True),
    ):
        context = build_asset_context()
        output = bronze_hdx_food(context)
        assert output.value["uri"] == "s3://bronze/hdx_food/prices.csv"
        assert output.value["written"] is True
