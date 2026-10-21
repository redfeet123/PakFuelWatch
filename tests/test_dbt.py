"""Unit tests for dbt-duckdb models, marts, and Dagster-dbt integration."""

import json
from pathlib import Path

import duckdb
from dagster import AssetKey

from dagster_project.definitions import defs

DBT_DIR = Path(__file__).parent.parent / "dbt_project"
MANIFEST_PATH = DBT_DIR / "target" / "manifest.json"
DUCKDB_PATH = Path("data/gold.duckdb")


def test_dbt_models_registered_in_dagster() -> None:
    """Verify that all 9 dbt models are registered in Dagster asset graph downstream of silver."""
    graph = defs.resolve_asset_graph()

    expected_staging = [
        ("stg_fuel_prices", "silver_fuel_prices"),
        ("stg_brent_daily", "silver_brent"),
        ("stg_fx_daily", "silver_fx"),
        ("stg_food_prices", "silver_food_prices"),
        ("stg_spi_weekly", "silver_spi_weekly"),
        ("stg_cpi_monthly", "silver_cpi_monthly"),
    ]

    for model_name, expected_parent in expected_staging:
        node = graph.get(AssetKey(model_name))
        assert node is not None, f"Expected asset {model_name} in Dagster graph"
        parent_keys = [p.key.to_user_string() for p in graph.get_parents(node)]
        assert (
            expected_parent in parent_keys
        ), f"Expected {expected_parent} to be upstream of {model_name}, got {parent_keys}"

    # Verify marts
    for mart in ["fuel_price_timeline", "pass_through_metrics", "price_change_signal"]:
        node = graph.get(AssetKey(mart))
        assert node is not None, f"Expected mart {mart} in Dagster graph"
        parents = [p.key.to_user_string() for p in graph.get_parents(node)]
        assert any(
            p.startswith("stg_") for p in parents
        ), f"Expected staging models upstream of {mart}, got {parents}"

    # Verify dbt job definition
    job_names = {job.name for job in defs.resolve_all_job_defs()}
    assert "dbt_models_job" in job_names


def test_dbt_manifest_and_models_specs() -> None:
    """Verify dbt manifest artifact contains all 9 models and configured documentation."""
    assert MANIFEST_PATH.exists(), f"Expected manifest file at {MANIFEST_PATH}"
    data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))

    nodes = data.get("nodes", {})
    model_names = {
        details["name"] for key, details in nodes.items() if details.get("resource_type") == "model"
    }

    expected_models = {
        "stg_fuel_prices",
        "stg_brent_daily",
        "stg_fx_daily",
        "stg_food_prices",
        "stg_spi_weekly",
        "stg_cpi_monthly",
        "fuel_price_timeline",
        "pass_through_metrics",
        "price_change_signal",
    }
    assert expected_models.issubset(model_names)

    # Verify formula documentation on price_change_signal
    signal_node = next(
        details for key, details in nodes.items() if details.get("name") == "price_change_signal"
    )
    description = signal_node.get("description", "")
    assert "FORMULA SPECIFICATION" in description
    assert "+1.5%" in description


def test_marts_query_content_and_range() -> None:
    """Verify DuckDB marts table data content, price ranges, and signal outputs."""
    if not DUCKDB_PATH.exists():
        return

    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    # 1. Fuel price timeline
    df_timeline = con.execute("SELECT * FROM fuel_price_timeline").df()
    assert not df_timeline.empty
    assert set(df_timeline["product"].unique()).issubset({"Petrol", "Diesel"})
    # Check bounds (between 50 and 1000 PKR)
    assert (df_timeline["price_pkr_per_litre"] >= 50.0).all()
    assert (df_timeline["price_pkr_per_litre"] <= 1000.0).all()

    # 2. Price change signal
    df_signal = con.execute("SELECT * FROM price_change_signal").df()
    assert len(df_signal) == 2
    assert set(df_signal["predicted_signal"].unique()).issubset({"UP", "DOWN", "FLAT"})
    assert all(df_signal["signal_rationale"].str.len() > 10)

    # 3. Pass through metrics
    df_metrics = con.execute("SELECT * FROM pass_through_metrics").df()
    assert not df_metrics.empty
    assert set(df_metrics["lag_weeks"].unique()) == set(range(9))
    assert (df_metrics["sample_size"] >= 0).all()
