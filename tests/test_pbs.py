"""Unit tests for Pakistan Bureau of Statistics (PBS) SPI and CPI source."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from dagster import AssetKey

from dagster_project.definitions import defs
from ingestion.core.delta_io import read_delta, read_delta_pandas
from ingestion.core.schemas import CPI_MONTHLY_SCHEMA, SPI_WEEKLY_SCHEMA
from ingestion.sources.pbs import (
    discover_pbs_release_links,
    fetch_pbs_backfill,
    parse_cpi_excel,
    parse_spi_excel,
    transform_pbs_cpi,
    transform_pbs_spi,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _read_fixture(filename: str) -> bytes:
    """Read saved fixture bytes."""
    return (FIXTURES_DIR / filename).read_bytes()


def test_discover_pbs_release_links_from_html() -> None:
    """Verify discovery of release links from saved HTML page."""
    html_content = _read_fixture("pbs_spi_page.html").decode("utf-8")

    # Mock Patchright Page and locator elements
    mock_el1 = MagicMock()
    mock_el1.get_attribute.return_value = "/sites/default/files/spi_release_20240111.xlsx"
    mock_el1.inner_text.return_value = "Download SPI Excel (XLSX)"

    mock_el2 = MagicMock()
    mock_el2.get_attribute.return_value = "/sites/default/files/spi_release_20240104.pdf"
    mock_el2.inner_text.return_value = "Download SPI PDF"

    mock_page = MagicMock()
    mock_page.url = "https://www.pbs.gov.pk/cpi"
    mock_page.content.return_value = html_content
    mock_page.locator.return_value.all.return_value = [mock_el1, mock_el2]

    links = discover_pbs_release_links(mock_page, section="spi")
    assert len(links) == 2
    assert links[0]["file_type"] == "xlsx"
    assert "https://www.pbs.gov.pk/sites/default/files/spi_release_20240111.xlsx" == links[0]["url"]
    assert links[1]["file_type"] == "pdf"


def test_parse_spi_excel_sample() -> None:
    """Verify openpyxl parsing of sample SPI Excel file."""
    excel_bytes = _read_fixture("pbs_spi_sample.xlsx")
    df = parse_spi_excel(excel_bytes, "pbs_spi_sample.xlsx")

    assert len(df) == 3
    assert "Wheat Flour Bag" in df["raw_item"].tolist()
    assert 2800.0 in df["raw_price"].tolist()


def test_transform_pbs_spi_success(tmp_path: Path) -> None:
    """Verify transform_pbs_spi cleans, types, and writes to spi_weekly Delta table."""
    excel_bytes = _read_fixture("pbs_spi_sample.xlsx")
    silver_base = str(tmp_path / "silver")
    rejects_base = str(tmp_path / "rejects")

    result = transform_pbs_spi(
        file_bytes=excel_bytes,
        filename="pbs_spi_sample.xlsx",
        silver_base_uri=silver_base,
        rejects_base_uri=rejects_base,
    )

    assert result.table_name == "spi_weekly"
    assert result.read_count == 3
    assert result.rejected_count == 0
    assert result.written_count == 3

    # Check Delta table schema and rows
    dt = read_delta(f"{silver_base}/spi_weekly")
    assert dt.schema == SPI_WEEKLY_SCHEMA
    df = read_delta_pandas(f"{silver_base}/spi_weekly")
    assert len(df) == 3
    items = set(df["item"].tolist())
    assert "Wheat Flour Bag" in items
    assert "Sugar" in items


def test_transform_pbs_spi_quarantine_on_corrupt_file(tmp_path: Path) -> None:
    """Verify corrupt file is quarantined without crashing the pipeline."""
    corrupt_bytes = b"This is a corrupt non-excel binary payload"
    silver_base = str(tmp_path / "silver")
    quarantine_base = str(tmp_path / "quarantine")

    result = transform_pbs_spi(
        file_bytes=corrupt_bytes,
        filename="corrupt_spi.xlsx",
        silver_base_uri=silver_base,
        quarantine_base_uri=quarantine_base,
    )

    # Must NOT crash, returns 0 written
    assert result.written_count == 0

    # Quarantined file and sidecar metadata must exist
    quarantined_file = tmp_path / "quarantine" / "pbs_spi" / "corrupt_spi.xlsx"
    assert quarantined_file.exists()
    assert quarantined_file.read_bytes() == corrupt_bytes

    meta_file = tmp_path / "quarantine" / "pbs_spi" / "corrupt_spi.xlsx.metadata.json"
    assert meta_file.exists()
    assert "error_message" in meta_file.read_text(encoding="utf-8")


def test_parse_cpi_excel_sample() -> None:
    """Verify openpyxl parsing of sample CPI Excel file."""
    excel_bytes = _read_fixture("pbs_cpi_sample.xlsx")
    df = parse_cpi_excel(excel_bytes, "pbs_cpi_sample.xlsx")

    assert len(df) == 3
    assert "General Headline CPI" in df["category"].tolist()


def test_transform_pbs_cpi_success(tmp_path: Path) -> None:
    """Verify transform_pbs_cpi cleans, types, and writes to cpi_monthly Delta table."""
    excel_bytes = _read_fixture("pbs_cpi_sample.xlsx")
    silver_base = str(tmp_path / "silver")
    rejects_base = str(tmp_path / "rejects")

    result = transform_pbs_cpi(
        file_bytes=excel_bytes,
        filename="pbs_cpi_sample.xlsx",
        silver_base_uri=silver_base,
        rejects_base_uri=rejects_base,
    )

    assert result.table_name == "cpi_monthly"
    assert result.read_count == 3
    assert result.written_count == 3

    dt = read_delta(f"{silver_base}/cpi_monthly")
    assert dt.schema == CPI_MONTHLY_SCHEMA
    df = read_delta_pandas(f"{silver_base}/cpi_monthly")
    assert len(df) == 3
    assert "General Headline Cpi" in df["category"].tolist()


def test_transform_pbs_cpi_quarantine_on_corrupt_file(tmp_path: Path) -> None:
    """Verify corrupt CPI file is quarantined without crashing."""
    corrupt_bytes = b"corrupted-pdf-or-excel-bytes"
    silver_base = str(tmp_path / "silver")
    quarantine_base = str(tmp_path / "quarantine")

    result = transform_pbs_cpi(
        file_bytes=corrupt_bytes,
        filename="corrupt_cpi.pdf",
        silver_base_uri=silver_base,
        quarantine_base_uri=quarantine_base,
    )

    assert result.written_count == 0
    quarantined_file = tmp_path / "quarantine" / "pbs_cpi" / "corrupt_cpi.pdf"
    assert quarantined_file.exists()
    assert quarantined_file.read_bytes() == corrupt_bytes


def test_fetch_pbs_backfill() -> None:
    """Verify backfill function walks back multiple releases."""
    mock_ctx = MagicMock()
    mock_page = MagicMock()
    mock_ctx.new_page.return_value = mock_page

    with patch("ingestion.sources.pbs.polite_goto"):
        with patch("ingestion.sources.pbs.save_page"):
            with patch(
                "ingestion.sources.pbs.discover_pbs_release_links",
                return_value=[
                    {"url": "https://pbs.gov.pk/f1.xlsx", "file_type": "xlsx"},
                    {"url": "https://pbs.gov.pk/f2.xlsx", "file_type": "xlsx"},
                ],
            ):
                with patch(
                    "ingestion.sources.pbs.download_file",
                    return_value=("s3://bronze/pbs_spi/f1.xlsx", True),
                ) as mock_dl:
                    res = fetch_pbs_backfill(
                        months=12,
                        sections=["spi"],
                        browser_context=mock_ctx,
                    )
                    assert len(res) == 2
                    assert mock_dl.call_count == 2


def test_dagster_pbs_assets_and_dependencies() -> None:
    """Verify PBS Dagster assets and dependency relationships."""
    asset_graph = defs.resolve_asset_graph()
    all_keys = {str(k.path[0]) for k in asset_graph.get_all_asset_keys()}

    expected = {"bronze_pbs_spi", "bronze_pbs_cpi", "silver_spi_weekly", "silver_cpi_monthly_pbs"}
    for k in expected:
        assert k in all_keys

    # Check dependency edges
    assert AssetKey(["bronze_pbs_spi"]) in asset_graph.get(
        AssetKey(["silver_spi_weekly"])
    ).parent_keys
    assert AssetKey(["bronze_pbs_cpi"]) in asset_graph.get(
        AssetKey(["silver_cpi_monthly_pbs"])
    ).parent_keys
