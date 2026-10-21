"""Tests for Bronze layer source ingestion modules using saved fixtures."""

from pathlib import Path
from unittest.mock import patch

import responses
from moto import mock_aws

from ingestion.config import Settings
from ingestion.core.http_client import HttpClient
from ingestion.core.storage import StorageClient
from ingestion.sources.brent import (
    EIA_BASE_URL,
    FRED_BASE_URL,
    fetch_brent_backfill,
    fetch_brent_incremental,
)
from ingestion.sources.fx import (
    FRANKFURTER_BASE_URL,
    SBP_EASYDATA_URL,
    fetch_fx_backfill,
    fetch_fx_incremental,
)
from ingestion.sources.hdx_food import (
    HDX_PACKAGE_SHOW_URL,
    discover_hdx_csv_resource,
    fetch_hdx_food_prices,
)
from ingestion.sources.worldbank import (
    WORLDBANK_BASE_URL,
    fetch_worldbank_all,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _read_fixture(filename: str) -> bytes:
    """Read saved fixture bytes."""
    return (FIXTURES_DIR / filename).read_bytes()


@mock_aws
@responses.activate
def test_brent_ingestion_fred_success() -> None:
    """Test Brent crude ingestion via FRED primary provider."""
    fred_sample = _read_fixture("fred_brent_sample.json")
    responses.add(responses.GET, FRED_BASE_URL, body=fred_sample, status=200)

    storage = StorageClient(
        endpoint_url="https://s3.amazonaws.com",
        access_key="test",
        secret_key="test",
        bucket="bronze",
    )
    http_client = HttpClient(min_delay_seconds=0.0)

    test_settings = Settings(fred_api_key="mock-fred-key", eia_api_key="mock-eia-key")
    with patch("ingestion.sources.brent.get_settings", return_value=test_settings):
        uri, written = fetch_brent_incremental(
            days=7,
            http_client=http_client,
            storage_client=storage,
        )

    assert written is True
    assert "s3://bronze/brent/" in uri
    assert "incremental" in uri


@mock_aws
@responses.activate
def test_brent_ingestion_eia_fallback() -> None:
    """Test Brent crude ingestion fallback to EIA when FRED fails."""
    responses.add(responses.GET, FRED_BASE_URL, status=500)
    eia_sample = _read_fixture("eia_brent_sample.json")
    responses.add(responses.GET, EIA_BASE_URL, body=eia_sample, status=200)

    storage = StorageClient(
        endpoint_url="https://s3.amazonaws.com",
        access_key="test",
        secret_key="test",
        bucket="bronze",
    )
    http_client = HttpClient(min_delay_seconds=0.0, max_retries=1)

    test_settings = Settings(fred_api_key="mock-fred-key", eia_api_key="mock-eia-key")
    with patch("ingestion.sources.brent.get_settings", return_value=test_settings):
        uri, written = fetch_brent_backfill(
            start_date="2020-01-01",
            http_client=http_client,
            storage_client=storage,
        )

    assert written is True
    assert "s3://bronze/brent/" in uri
    assert "backfill" in uri


@mock_aws
@responses.activate
def test_fx_ingestion_sbp_success() -> None:
    """Test USD/PKR ingestion via primary SBP provider."""
    sbp_sample = _read_fixture("sbp_fx_sample.json")
    responses.add(responses.GET, SBP_EASYDATA_URL, body=sbp_sample, status=200)

    storage = StorageClient(
        endpoint_url="https://s3.amazonaws.com",
        access_key="test",
        secret_key="test",
        bucket="bronze",
    )
    http_client = HttpClient(min_delay_seconds=0.0)

    test_settings = Settings(sbp_api_key="mock-sbp-key", fx_provider="sbp")
    with patch("ingestion.sources.fx.get_settings", return_value=test_settings):
        uri, written = fetch_fx_incremental(
            days=5,
            http_client=http_client,
            storage_client=storage,
        )

    assert written is True
    assert "s3://bronze/fx/" in uri


@mock_aws
@responses.activate
def test_fx_ingestion_frankfurter_fallback() -> None:
    """Test USD/PKR ingestion fallback to Frankfurter when SBP fails."""
    responses.add(responses.GET, SBP_EASYDATA_URL, status=500)
    frankfurter_sample = _read_fixture("frankfurter_fx_sample.json")
    responses.add(
        responses.GET,
        f"{FRANKFURTER_BASE_URL}/2024-01-01..2024-01-05",
        body=frankfurter_sample,
        status=200,
    )

    storage = StorageClient(
        endpoint_url="https://s3.amazonaws.com",
        access_key="test",
        secret_key="test",
        bucket="bronze",
    )
    http_client = HttpClient(min_delay_seconds=0.0, max_retries=1)

    test_settings = Settings(sbp_api_key="mock-sbp-key", fx_provider="sbp")
    with patch("ingestion.sources.fx.get_settings", return_value=test_settings):
        uri, written = fetch_fx_backfill(
            start_date="2024-01-01",
            end_date="2024-01-05",
            http_client=http_client,
            storage_client=storage,
        )

    assert written is True
    assert "s3://bronze/fx/" in uri


@mock_aws
@responses.activate
def test_worldbank_ingestion_all() -> None:
    """Test World Bank indicators ingestion for Pakistan."""
    cpi_sample = _read_fixture("worldbank_cpi_sample.json")
    fuel_sample = _read_fixture("worldbank_fuel_sample.json")
    index_sample = _read_fixture("worldbank_index_sample.json")

    responses.add(
        responses.GET,
        f"{WORLDBANK_BASE_URL}/FP.CPI.TOTL.ZG",
        body=cpi_sample,
        status=200,
    )
    responses.add(
        responses.GET,
        f"{WORLDBANK_BASE_URL}/TM.VAL.FUEL.ZS.UN",
        body=fuel_sample,
        status=200,
    )
    responses.add(
        responses.GET,
        f"{WORLDBANK_BASE_URL}/FP.CPI.TOTL",
        body=index_sample,
        status=200,
    )

    storage = StorageClient(
        endpoint_url="https://s3.amazonaws.com",
        access_key="test",
        secret_key="test",
        bucket="bronze",
    )
    http_client = HttpClient(min_delay_seconds=0.0)

    results = fetch_worldbank_all(
        date_range="2020:2023",
        http_client=http_client,
        storage_client=storage,
    )

    assert len(results) == 3
    for uri, written in results:
        assert written is True
        assert "s3://bronze/worldbank/" in uri


@mock_aws
@responses.activate
def test_hdx_food_prices_ingestion() -> None:
    """Test HDX WFP food prices dynamic resource discovery and CSV ingestion."""
    package_sample = _read_fixture("hdx_package_sample.json")
    csv_sample = _read_fixture("hdx_food_prices_sample.csv")

    responses.add(
        responses.GET,
        HDX_PACKAGE_SHOW_URL,
        body=package_sample,
        status=200,
    )
    download_url = (
        "https://data.humdata.org/dataset/pakistan-food-prices/download/"
        "wfp_food_prices_pakistan.csv"
    )
    responses.add(responses.GET, download_url, body=csv_sample, status=200)

    http_client = HttpClient(min_delay_seconds=0.0)

    # 1. Verify dynamic resource discovery
    resource = discover_hdx_csv_resource(http_client=http_client)
    assert resource["download_url"] == download_url
    assert resource["resource_id"] == "res-data-123"

    # 2. Verify ingestion
    storage = StorageClient(
        endpoint_url="https://s3.amazonaws.com",
        access_key="test",
        secret_key="test",
        bucket="bronze",
    )

    uri, written = fetch_hdx_food_prices(
        http_client=http_client,
        storage_client=storage,
    )

    assert written is True
    assert "s3://bronze/hdx_food/" in uri
    assert uri.endswith(".csv")
