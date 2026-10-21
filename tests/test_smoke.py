"""Smoke test suite validating PakFuelWatch scaffolding and core utilities."""

from ingestion.config import Settings, get_settings
from ingestion.logging_setup import setup_logging


def test_settings_initialization() -> None:
    """Verify that Settings loads with expected defaults and properties."""
    settings = get_settings()
    assert isinstance(settings, Settings)
    assert settings.minio_endpoint == "http://localhost:9000"
    assert settings.postgres_port == 5432
    assert "postgresql://" in settings.postgres_connection_string


def test_logging_setup() -> None:
    """Verify that structured logger initializes and emits logs properly."""
    logger = setup_logging(logger_name="test_smoke", log_level="DEBUG", as_json=True)
    assert logger.name == "test_smoke"
    assert len(logger.handlers) >= 1
    logger.info("Smoke test structured log message executed successfully.")


def test_dagster_definitions_and_asset() -> None:
    """Verify that Dagster definitions and placeholder asset load properly."""
    from dagster_project.definitions import defs, placeholder_smoke_asset

    assert defs is not None
    # Execute the placeholder asset directly
    result = placeholder_smoke_asset()
    assert "PakFuelWatch" in result
