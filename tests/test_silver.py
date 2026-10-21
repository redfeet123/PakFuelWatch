"""Unit tests for Silver transformations and rejects handling using saved fixtures."""

from pathlib import Path
from unittest.mock import patch

from dagster import AssetKey, build_asset_context
from deltalake import DeltaTable

from dagster_project.assets.silver import (
    silver_brent,
    silver_cpi_monthly,
    silver_food_prices,
    silver_fx,
)
from dagster_project.definitions import defs
from ingestion.core.delta_io import read_delta, read_delta_pandas
from ingestion.core.schemas import (
    BRENT_DAILY_SCHEMA,
    CPI_MONTHLY_SCHEMA,
    FOOD_PRICES_SCHEMA,
    FX_DAILY_SCHEMA,
)
from ingestion.silver.brent import transform_brent
from ingestion.silver.cpi_monthly import transform_cpi_monthly
from ingestion.silver.food_prices import transform_food_prices
from ingestion.silver.fx import transform_fx
from ingestion.silver.utils import TransformationResult

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _read_fixture(filename: str) -> bytes:
    """Read saved fixture bytes."""
    return (FIXTURES_DIR / filename).read_bytes()


def test_brent_silver_transformation(tmp_path: Path) -> None:
    """Test standard Brent crude transformation into Silver Delta table."""
    raw_bytes = _read_fixture("fred_brent_sample.json")
    silver_base = str(tmp_path / "silver")
    rejects_base = str(tmp_path / "rejects")

    result = transform_brent(
        raw_bytes=raw_bytes,
        silver_base_uri=silver_base,
        rejects_base_uri=rejects_base,
    )

    assert result.table_name == "brent_daily"
    assert result.read_count == 3
    assert result.rejected_count == 0
    assert result.written_count == 3

    # Verify Delta table contents and PyArrow schema
    dt = read_delta(f"{silver_base}/brent_daily")
    assert dt.schema == BRENT_DAILY_SCHEMA
    df = read_delta_pandas(f"{silver_base}/brent_daily")
    assert len(df) == 3
    assert set(df["source"].unique()) == {"fred"}


def test_brent_edge_cases_duplicates_and_missing(tmp_path: Path) -> None:
    """Test Brent crude rejects bad rows and deduplicates identical dates."""
    raw_bytes = _read_fixture("brent_edge_cases.json")
    silver_base = str(tmp_path / "silver")
    rejects_base = str(tmp_path / "rejects")

    result = transform_brent(
        raw_bytes=raw_bytes,
        silver_base_uri=silver_base,
        rejects_base_uri=rejects_base,
    )

    assert result.read_count == 5
    assert result.rejected_count == 3
    assert result.written_count == 1  # Deduplicated from 2 valid records with same date

    # Verify rejects table
    rejects_dt = DeltaTable(f"{rejects_base}/brent_daily")
    rejects_df = rejects_dt.to_pandas()
    assert len(rejects_df) == 3
    reasons = set(rejects_df["reason"].tolist())
    assert "missing_price" in reasons
    assert "non_positive_price" in reasons
    assert "missing_date" in reasons


def test_fx_silver_transformation(tmp_path: Path) -> None:
    """Test FX daily transformation from SBP and Frankfurter fixtures."""
    sbp_bytes = _read_fixture("sbp_fx_sample.json")
    silver_base = str(tmp_path / "silver")
    rejects_base = str(tmp_path / "rejects")

    # Ingest SBP
    result_sbp = transform_fx(
        raw_bytes=sbp_bytes,
        silver_base_uri=silver_base,
        rejects_base_uri=rejects_base,
    )
    assert result_sbp.read_count == 3
    assert result_sbp.rejected_count == 0
    assert result_sbp.written_count == 3

    # Ingest Frankfurter
    frankfurter_bytes = _read_fixture("frankfurter_fx_sample.json")
    result_fra = transform_fx(
        raw_bytes=frankfurter_bytes,
        silver_base_uri=silver_base,
        rejects_base_uri=rejects_base,
    )
    assert result_fra.read_count == 3
    assert result_fra.rejected_count == 0
    assert result_fra.written_count == 3

    # Verify combined Delta table schema and rows
    dt = read_delta(f"{silver_base}/fx_daily")
    assert dt.schema == FX_DAILY_SCHEMA
    df = read_delta_pandas(f"{silver_base}/fx_daily")
    # 3 from sbp + 3 from frankfurter (different sources)
    assert len(df) == 6


def test_fx_edge_cases_duplicates_and_missing(tmp_path: Path) -> None:
    """Test FX rejects missing values and deduplicates duplicate rows."""
    raw_bytes = _read_fixture("fx_edge_cases.json")
    silver_base = str(tmp_path / "silver")
    rejects_base = str(tmp_path / "rejects")

    result = transform_fx(
        raw_bytes=raw_bytes,
        silver_base_uri=silver_base,
        rejects_base_uri=rejects_base,
    )

    assert result.read_count == 5
    assert result.rejected_count == 3
    assert result.written_count == 1  # 2 valid rows on 2024-01-02 deduplicated to 1

    # Verify rejects table has reason column and expected reasons
    rejects_dt = DeltaTable(f"{rejects_base}/fx_daily")
    rejects_df = rejects_dt.to_pandas()
    assert len(rejects_df) == 3
    reasons = set(rejects_df["reason"].tolist())
    assert "missing_rate" in reasons
    assert "missing_date" in reasons
    assert "non_positive_rate" in reasons


def test_food_prices_silver_transformation(tmp_path: Path) -> None:
    """Test WFP food prices transformation into Silver Delta table."""
    raw_bytes = _read_fixture("hdx_food_prices_sample.csv")
    silver_base = str(tmp_path / "silver")
    rejects_base = str(tmp_path / "rejects")

    result = transform_food_prices(
        raw_bytes=raw_bytes,
        silver_base_uri=silver_base,
        rejects_base_uri=rejects_base,
    )

    assert result.table_name == "food_prices"
    assert result.read_count == 3
    assert result.rejected_count == 0
    assert result.written_count == 3

    # Verify schema
    dt = read_delta(f"{silver_base}/food_prices")
    assert dt.schema == FOOD_PRICES_SCHEMA
    df = read_delta_pandas(f"{silver_base}/food_prices")
    assert len(df) == 3
    assert set(df["unit"].unique()) == {"KG"}


def test_food_prices_edge_cases_duplicates_missing_unit_mismatch(tmp_path: Path) -> None:
    """Test food prices handling of duplicates, missing values, and unit mismatch."""
    raw_bytes = _read_fixture("food_prices_edge_cases.csv")
    silver_base = str(tmp_path / "silver")
    rejects_base = str(tmp_path / "rejects")

    result = transform_food_prices(
        raw_bytes=raw_bytes,
        silver_base_uri=silver_base,
        rejects_base_uri=rejects_base,
    )

    # 6 rows in fixture (HXL row is stripped)
    assert result.read_count == 6
    assert result.rejected_count == 4
    assert result.written_count == 1  # Two identical rows deduplicated to 1

    # Verify rejects table contains unit_mismatch and missing values
    rejects_dt = DeltaTable(f"{rejects_base}/food_prices")
    rejects_df = rejects_dt.to_pandas()
    assert len(rejects_df) == 4
    reasons = set(rejects_df["reason"].tolist())
    assert "unit_mismatch" in reasons
    assert "missing_price" in reasons
    assert "missing_date" in reasons
    assert "non_positive_price" in reasons


def test_cpi_monthly_silver_transformation(tmp_path: Path) -> None:
    """Test World Bank CPI data transformation into Silver Delta table."""
    raw_bytes = _read_fixture("worldbank_index_sample.json")
    silver_base = str(tmp_path / "silver")
    rejects_base = str(tmp_path / "rejects")

    result = transform_cpi_monthly(
        raw_bytes=raw_bytes,
        silver_base_uri=silver_base,
        rejects_base_uri=rejects_base,
    )

    assert result.table_name == "cpi_monthly"
    assert result.read_count == 2
    assert result.rejected_count == 0
    assert result.written_count == 2

    dt = read_delta(f"{silver_base}/cpi_monthly")
    assert dt.schema == CPI_MONTHLY_SCHEMA
    df = read_delta_pandas(f"{silver_base}/cpi_monthly")
    assert len(df) == 2
    assert set(df["month"].tolist()) == {"2023-01", "2022-01"}
    assert set(df["category"].tolist()) == {"Headline CPI"}


def test_cpi_monthly_edge_cases_duplicates_and_missing(tmp_path: Path) -> None:
    """Test CPI monthly handling of duplicates and missing/null values."""
    raw_bytes = _read_fixture("worldbank_cpi_edge_cases.json")
    silver_base = str(tmp_path / "silver")
    rejects_base = str(tmp_path / "rejects")

    result = transform_cpi_monthly(
        raw_bytes=raw_bytes,
        silver_base_uri=silver_base,
        rejects_base_uri=rejects_base,
    )

    assert result.read_count == 4
    assert result.rejected_count == 2
    assert result.written_count == 1  # 2 duplicate 2023 rows deduplicated to 1

    rejects_dt = DeltaTable(f"{rejects_base}/cpi_monthly")
    rejects_df = rejects_dt.to_pandas()
    assert len(rejects_df) == 2
    reasons = set(rejects_df["reason"].tolist())
    assert "missing_index_value" in reasons
    assert "missing_month" in reasons


def test_dagster_silver_assets_definitions() -> None:
    """Verify Dagster definitions include all silver assets and correct dependencies."""
    asset_graph = defs.resolve_asset_graph()
    all_keys = {str(k.path[0]) for k in asset_graph.get_all_asset_keys()}

    silver_keys = {"silver_brent", "silver_fx", "silver_food_prices", "silver_cpi_monthly"}
    for key_name in silver_keys:
        assert key_name in all_keys

    # Verify dependency relationships
    assert AssetKey(["bronze_brent"]) in asset_graph.get(AssetKey(["silver_brent"])).parent_keys
    assert AssetKey(["bronze_fx"]) in asset_graph.get(AssetKey(["silver_fx"])).parent_keys
    assert AssetKey(["bronze_hdx_food"]) in asset_graph.get(
        AssetKey(["silver_food_prices"])
    ).parent_keys
    assert AssetKey(["bronze_worldbank"]) in asset_graph.get(
        AssetKey(["silver_cpi_monthly"])
    ).parent_keys


def test_dagster_silver_assets_execution() -> None:
    """Test execution of silver assets with mocked transformation results."""
    mock_result = TransformationResult(
        table_name="test_table",
        read_count=10,
        rejected_count=2,
        written_count=8,
        table_uri="s3://silver/test_table",
        rejects_uri="s3://silver/_rejects/test_table",
    )

    with patch("dagster_project.assets.silver.transform_brent", return_value=mock_result):
        context = build_asset_context()
        out = silver_brent(context)
        assert out.value["read_count"] == 10
        assert out.value["written_count"] == 8

    with patch("dagster_project.assets.silver.transform_fx", return_value=mock_result):
        context = build_asset_context()
        out = silver_fx(context)
        assert out.value["written_count"] == 8

    with patch("dagster_project.assets.silver.transform_food_prices", return_value=mock_result):
        context = build_asset_context()
        out = silver_food_prices(context)
        assert out.value["rejected_count"] == 2

    with patch("dagster_project.assets.silver.transform_cpi_monthly", return_value=mock_result):
        context = build_asset_context()
        out = silver_cpi_monthly(context)
        assert out.value["table_uri"] == "s3://silver/test_table"
