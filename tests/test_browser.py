"""Unit tests for browser scraping foundation (Patchright, selectors, rate limits, robots.txt)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import responses
from moto import mock_aws

from ingestion.core.browser import (
    download_file,
    get_selector,
    is_url_allowed_by_robots,
    load_selectors,
    polite_goto,
    save_page,
)
from ingestion.core.storage import StorageClient


def test_load_selectors_and_get_selector() -> None:
    """Verify loading selectors from selectors.yaml."""
    selectors = load_selectors()
    assert "pbs" in selectors
    assert "spi" in selectors["pbs"]
    assert "cpi" in selectors["pbs"]

    # Verify get_selector returns string
    rel_selector = get_selector("pbs", "spi", "release_links")
    assert "href" in rel_selector

    # Fallback default
    custom_sel = get_selector("pbs", "spi", "non_existent", default=".fallback")
    assert custom_sel == ".fallback"

    with pytest.raises(KeyError, match="Selector not found"):
        get_selector("pbs", "spi", "non_existent")


@responses.activate
def test_robots_txt_allowed_and_disallowed() -> None:
    """Verify robots.txt compliance logic."""
    disallow_robots = "User-agent: *\nDisallow: /private/\nDisallow: /admin/\n"
    responses.add(
        responses.GET,
        "https://example.com/robots.txt",
        body=disallow_robots,
        status=200,
    )

    # Allowed URL
    assert is_url_allowed_by_robots("https://example.com/cpi") is True
    # Disallowed URL
    assert is_url_allowed_by_robots("https://example.com/private/secret") is False


def test_polite_goto_enforces_robots_permission() -> None:
    """Verify polite_goto raises PermissionError when robots.txt disallows."""
    mock_page = MagicMock()
    with patch("ingestion.core.browser.is_url_allowed_by_robots", return_value=False):
        with pytest.raises(PermissionError, match="disallowed by robots.txt"):
            polite_goto(mock_page, "https://example.com/forbidden", check_robots=True)


def test_polite_goto_rate_limiting_and_success() -> None:
    """Verify polite_goto sleeps to enforce minimum delay between host requests."""
    mock_page = MagicMock()

    with patch("ingestion.core.browser.is_url_allowed_by_robots", return_value=True):
        with patch("time.sleep") as mock_sleep:
            # First navigation sets timestamp
            polite_goto(mock_page, "https://test-host.org/page1", min_delay=2.0)
            # Immediate second navigation to same host must trigger rate-limit sleep
            polite_goto(mock_page, "https://test-host.org/page2", min_delay=2.0)

            assert mock_sleep.called
            sleep_arg = mock_sleep.call_args[0][0]
            assert 0.0 < sleep_arg <= 2.0


@mock_aws
def test_save_page_stores_html_and_screenshot() -> None:
    """Verify save_page captures HTML and screenshot and stores to Bronze with metadata."""
    storage = StorageClient(
        endpoint_url="https://s3.amazonaws.com",
        access_key="test",
        secret_key="test",
        bucket="bronze",
    )

    mock_page = MagicMock()
    mock_page.url = "https://www.pbs.gov.pk/cpi"
    mock_page.title.return_value = "PBS CPI Page"
    mock_page.content.return_value = "<html><body><h1>CPI</h1></body></html>"
    mock_page.screenshot.return_value = b"\x89PNG\r\n\x1a\nfake-screenshot-data"

    html_uri, png_uri = save_page(
        source="pbs_cpi",
        page=mock_page,
        storage_client=storage,
        fetch_date="2024-01-15",
    )

    assert "s3://bronze/pbs_cpi/2024-01-15/" in html_uri
    assert html_uri.endswith(".html")
    assert "s3://bronze/pbs_cpi/2024-01-15/" in png_uri
    assert png_uri.endswith(".png")

    # Verify content stored
    raw_html, sidecar = storage.get_raw("pbs_cpi", "2024-01-15", Path(html_uri).name)
    assert b"<h1>CPI</h1>" in raw_html
    assert sidecar["page_title"] == "PBS CPI Page"


@mock_aws
@responses.activate
def test_download_file_direct_url() -> None:
    """Verify download_file downloads binary file and stores into Bronze."""
    storage = StorageClient(
        endpoint_url="https://s3.amazonaws.com",
        access_key="test",
        secret_key="test",
        bucket="bronze",
    )

    file_content = b"fake-excel-or-pdf-bytes"
    test_url = "https://www.pbs.gov.pk/sites/default/files/spi_report.xlsx"
    responses.add(
        responses.GET,
        test_url,
        body=file_content,
        status=200,
        headers={
            "content-type": (
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        },
    )

    mock_page = MagicMock()
    uri, written = download_file(
        page=mock_page,
        selector_or_url=test_url,
        source="pbs_spi",
        storage_client=storage,
        fetch_date="2024-01-15",
    )

    assert written is True
    assert "s3://bronze/pbs_spi/2024-01-15/spi_report.xlsx" == uri
