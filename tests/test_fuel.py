"""Unit tests for fuel price pipeline (PSO, News, Seed backfill, Reconciliation, Gap detection)."""

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa

from dagster_project.assets.bronze import (
    fuel_daily_schedule,
    fuel_revision_window_schedule,
)
from dagster_project.definitions import defs
from ingestion.core.delta_io import read_delta, read_delta_pandas, write_delta_merge
from ingestion.core.schemas import FUEL_PRICE_CONFLICTS_SCHEMA, FUEL_PRICES_SCHEMA
from ingestion.sources.fuel import (
    clean_price_val,
    clean_product_name,
    detect_fuel_price_gaps,
    fetch_news_fuel_prices,
    fetch_pso_fuel_prices,
    load_fuel_seed_history,
    parse_effective_date,
    parse_news_fuel_html,
    parse_pso_fuel_html,
    reconcile_fuel_prices,
    transform_fuel_news,
    transform_fuel_pso,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _read_fixture(filename: str) -> str:
    """Read saved HTML fixture as string."""
    return (FIXTURES_DIR / filename).read_text(encoding="utf-8")


# =====================================================================
# 1. Parsing & Cleaning Helpers Tests
# =====================================================================


def test_clean_price_val() -> None:
    """Verify price string parsing handles currencies, commas, units, and invalid values."""
    assert clean_price_val("Rs. 267.34") == 267.34
    assert clean_price_val("Rs. 276,21") == 276.21
    assert clean_price_val("PKR 259.34 / litre") == 259.34
    assert clean_price_val("285.56") == 285.56
    assert clean_price_val("0.00") is None
    assert clean_price_val("N/A") is None
    assert clean_price_val(None) is None


def test_clean_product_name() -> None:
    """Verify product name normalization to canonical 'Petrol' and 'Diesel'."""
    assert clean_product_name("Motor Gasoline (Petrol)") == "Petrol"
    assert clean_product_name("MOGAS 92") == "Petrol"
    assert clean_product_name("Super Petrol") == "Petrol"
    assert clean_product_name("High Speed Diesel (HSD)") == "Diesel"
    assert clean_product_name("Diesel") == "Diesel"
    assert clean_product_name("Kerosene") is None
    assert clean_product_name(None) is None


def test_parse_effective_date() -> None:
    """Verify parsing multiple Pakistani date notification formats and ordinals."""
    fallback = date(2024, 1, 1)
    assert parse_effective_date("16-Jan-2024") == date(2024, 1, 16)
    assert parse_effective_date("January 16, 2024") == date(2024, 1, 16)
    assert parse_effective_date("2024-01-16") == date(2024, 1, 16)
    assert parse_effective_date("16/01/2024") == date(2024, 1, 16)
    assert parse_effective_date("Effective Date: 1st January 2024") == date(2024, 1, 1)
    assert parse_effective_date("w.e.f. 16th Feb 2024") == date(2024, 2, 16)
    assert parse_effective_date("invalid date", default_date=fallback) == fallback


# =====================================================================
# 2. HTML Parsing Tests (PSO, News, Malformed)
# =====================================================================


def test_parse_pso_fuel_html() -> None:
    """Verify parsing official PSO HTML fixture extracting date and table prices."""
    html_content = _read_fixture("pso_fuel_page.html")
    records = parse_pso_fuel_html(html_content)

    assert len(records) == 2
    petrol = next(r for r in records if r["product"] == "Petrol")
    diesel = next(r for r in records if r["product"] == "Diesel")

    assert petrol["effective_date"] == date(2024, 1, 16)
    assert petrol["price_pkr_per_litre"] == 267.34
    assert petrol["source"] == "pso"
    assert petrol["confidence"] == "high"

    assert diesel["effective_date"] == date(2024, 1, 16)
    assert diesel["price_pkr_per_litre"] == 276.21
    assert diesel["source"] == "pso"
    assert diesel["confidence"] == "high"


def test_parse_news_fuel_html() -> None:
    """Verify parsing secondary news announcement HTML fixture."""
    html_content = _read_fixture("news_fuel_page.html")
    records = parse_news_fuel_html(html_content)

    assert len(records) == 2
    petrol = next(r for r in records if r["product"] == "Petrol")
    diesel = next(r for r in records if r["product"] == "Diesel")

    assert petrol["effective_date"] == date(2024, 1, 16)
    assert petrol["price_pkr_per_litre"] == 267.34
    assert petrol["source"] == "news"
    assert petrol["confidence"] == "medium"

    assert diesel["effective_date"] == date(2024, 1, 16)
    assert diesel["price_pkr_per_litre"] == 276.21
    assert diesel["source"] == "news"
    assert diesel["confidence"] == "medium"


def test_parse_pso_fuel_malformed_html() -> None:
    """Verify malformed HTML (e.g. 503 error) safely returns empty list without error."""
    html_content = _read_fixture("pso_fuel_malformed.html")
    records = parse_pso_fuel_html(html_content)
    assert records == []


# =====================================================================
# 3. Transformations & Delta Writing
# =====================================================================


def test_transform_fuel_pso_and_news(tmp_path: Path) -> None:
    """Verify transforming raw HTML bytes into typed Silver Delta tables."""
    silver_base = str(tmp_path / "silver")

    pso_bytes = _read_fixture("pso_fuel_page.html").encode("utf-8")
    res_pso = transform_fuel_pso(raw_bytes=pso_bytes, silver_base_uri=silver_base)

    assert res_pso.table_name == "fuel_prices"
    assert res_pso.written_count == 2

    news_bytes = _read_fixture("news_fuel_page.html").encode("utf-8")
    res_news = transform_fuel_news(raw_bytes=news_bytes, silver_base_uri=silver_base)

    assert res_news.table_name == "fuel_prices"
    assert res_news.written_count == 2

    dt = read_delta(f"{silver_base}/fuel_prices")
    assert dt.schema == FUEL_PRICES_SCHEMA

    df = read_delta_pandas(f"{silver_base}/fuel_prices")
    assert len(df) == 4
    assert set(df["source"].unique()) == {"pso", "news"}


# =====================================================================
# 4. Seed Backfill Loading
# =====================================================================


def test_load_fuel_seed_history(tmp_path: Path) -> None:
    """Verify loading historical prices from CSV seed file into Silver Delta table."""
    silver_base = str(tmp_path / "silver")
    seed_file = Path("data/seed/fuel_history.csv")

    assert seed_file.exists(), "Seed CSV must exist in data/seed/fuel_history.csv"

    res = load_fuel_seed_history(csv_path=str(seed_file), silver_base_uri=silver_base)

    assert res.table_name == "fuel_prices"
    assert res.written_count == 12

    # Verify rows in Delta table
    df = read_delta_pandas(f"{silver_base}/fuel_prices")
    assert len(df) == 12
    assert set(df["source"].unique()) == {"seed"}
    assert set(df["confidence"].unique()) == {"low"}

    # Verify idempotency by running again
    res2 = load_fuel_seed_history(csv_path=str(seed_file), silver_base_uri=silver_base)
    assert res2.written_count == 12
    df2 = read_delta_pandas(f"{silver_base}/fuel_prices")
    assert len(df2) == 12


# =====================================================================
# 5. Reconciliation & Conflict Detection
# =====================================================================


def test_reconcile_fuel_prices_agreement(tmp_path: Path) -> None:
    """Verify reconciliation sets confidence='high' when sources agree within 0.5%."""
    silver_base = str(tmp_path / "silver")
    conflicts_base = str(tmp_path / "silver")
    table_uri = f"{silver_base}/fuel_prices"

    # Insert two records for same date & product with slight difference (0.1%)
    initial_data = [
        {
            "effective_date": date(2024, 1, 16),
            "product": "Petrol",
            "price_pkr_per_litre": 267.34,
            "source": "pso",
            "confidence": "medium",
        },
        {
            "effective_date": date(2024, 1, 16),
            "product": "Petrol",
            "price_pkr_per_litre": 267.50,  # diff: 0.059% <= 0.5%
            "source": "news",
            "confidence": "medium",
        },
    ]
    pa_table = pa.Table.from_pandas(
        pd.DataFrame(initial_data), schema=FUEL_PRICES_SCHEMA, preserve_index=False
    )
    write_delta_merge(pa_table, table_uri, ["effective_date", "product", "source"])

    agreements, conflicts = reconcile_fuel_prices(
        silver_base_uri=silver_base,
        conflicts_base_uri=conflicts_base,
    )

    assert agreements == 1
    assert conflicts == 0

    df_reconciled = read_delta_pandas(table_uri)
    assert len(df_reconciled) == 2
    assert (df_reconciled["confidence"] == "high").all()


def test_reconcile_fuel_prices_conflict(tmp_path: Path) -> None:
    """Verify reconciliation detects disagreement (>0.5%) and writes conflict table."""
    silver_base = str(tmp_path / "silver")
    conflicts_base = str(tmp_path / "silver")
    table_uri = f"{silver_base}/fuel_prices"
    conflicts_uri = f"{conflicts_base}/fuel_price_conflicts"

    # Insert two records with large difference (diff > 0.5%)
    initial_data = [
        {
            "effective_date": date(2024, 1, 16),
            "product": "Petrol",
            "price_pkr_per_litre": 267.34,
            "source": "pso",
            "confidence": "medium",
        },
        {
            "effective_date": date(2024, 1, 16),
            "product": "Petrol",
            "price_pkr_per_litre": 275.00,  # diff: 2.86% > 0.5%
            "source": "news",
            "confidence": "medium",
        },
    ]
    pa_table = pa.Table.from_pandas(
        pd.DataFrame(initial_data), schema=FUEL_PRICES_SCHEMA, preserve_index=False
    )
    write_delta_merge(pa_table, table_uri, ["effective_date", "product", "source"])

    agreements, conflicts = reconcile_fuel_prices(
        silver_base_uri=silver_base,
        conflicts_base_uri=conflicts_base,
    )

    assert agreements == 0
    assert conflicts == 1

    # Verify both rows kept with confidence='conflict'
    df_reconciled = read_delta_pandas(table_uri)
    assert len(df_reconciled) == 2
    assert (df_reconciled["confidence"] == "conflict").all()

    # Verify conflict record written to fuel_price_conflicts table
    dt_conflicts = read_delta(conflicts_uri)
    assert dt_conflicts.schema == FUEL_PRICE_CONFLICTS_SCHEMA
    df_conf = read_delta_pandas(conflicts_uri)
    assert len(df_conf) == 1
    row = df_conf.iloc[0]
    assert row["product"] == "Petrol"
    assert row["source_a"] == "pso"
    assert row["price_a"] == 267.34
    assert row["source_b"] == "news"
    assert row["price_b"] == 275.00
    assert row["pct_difference"] > 0.5


# =====================================================================
# 6. Fortnight Gap Detection Tests
# =====================================================================


def test_detect_fuel_price_gaps() -> None:
    """Verify gap detector identifies missing fortnights."""
    # Complete dataframe with 2 months and both fortnights (FN1: 1st, FN2: 16th)
    df_complete = pd.DataFrame(
        [
            {"effective_date": date(2024, 1, 1), "product": "Petrol"},
            {"effective_date": date(2024, 1, 16), "product": "Petrol"},
            {"effective_date": date(2024, 2, 1), "product": "Petrol"},
            {"effective_date": date(2024, 2, 16), "product": "Petrol"},
        ]
    )
    gaps = detect_fuel_price_gaps(df_complete)
    assert len(gaps) == 0

    # Dataframe missing 2024-01-16 (FN2)
    df_missing = pd.DataFrame(
        [
            {"effective_date": date(2024, 1, 1), "product": "Petrol"},
            {"effective_date": date(2024, 2, 1), "product": "Petrol"},
            {"effective_date": date(2024, 2, 16), "product": "Petrol"},
        ]
    )
    gaps_found = detect_fuel_price_gaps(df_missing)
    assert len(gaps_found) == 1
    assert gaps_found[0]["fortnight"] == "FN2"
    assert gaps_found[0]["month"] == 1
    assert gaps_found[0]["year"] == 2024
    assert gaps_found[0]["product"] == "Petrol"


# =====================================================================
# 7. Dagster Assets & Schedule Definitions
# =====================================================================


def test_dagster_fuel_assets_and_schedules() -> None:
    """Verify Dagster assets, jobs, and schedules for fuel price pipeline."""
    asset_keys = {str(k.path[0]) for k in defs.resolve_asset_graph().get_all_asset_keys()}

    assert "bronze_fuel_pso" in asset_keys
    assert "bronze_fuel_news" in asset_keys
    assert "silver_fuel_prices" in asset_keys
    assert "silver_fuel_seed_backfill" in asset_keys

    # Verify fuel scrape job
    job = defs.get_job_def("fuel_scrape_job")
    assert job is not None
    job_names = {j.name for j in defs.resolve_all_job_defs()}
    assert "fuel_scrape_job" in job_names

    # Verify schedules
    schedule_names = [s.name for s in (defs.schedules or [])]
    assert "fuel_revision_window_schedule" in schedule_names
    assert "fuel_daily_schedule" in schedule_names

    assert fuel_revision_window_schedule.cron_schedule == "0 */6 14,15,16,28,29,30,31 * *"
    assert fuel_daily_schedule.cron_schedule == "0 6 1-13,17-27 * *"


# =====================================================================
# 8. Browser Fetch Helpers Mocked
# =====================================================================


def test_fetch_pso_fuel_prices_mocked() -> None:
    """Verify fetch_pso_fuel_prices navigates politely and saves page to bronze."""
    mock_page = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.new_page.return_value = mock_page
    mock_storage = MagicMock()

    with (
        patch("ingestion.sources.fuel.polite_goto") as mock_goto,
        patch("ingestion.sources.fuel.save_page") as mock_save,
    ):
        mock_save.return_value = (
            "s3://bronze/fuel_pso/2024-01-16/page.html",
            "s3://bronze/fuel_pso/2024-01-16/page.png",
        )

        html_uri, png_uri = fetch_pso_fuel_prices(
            browser_context=mock_ctx, storage_client=mock_storage
        )

        mock_goto.assert_called_once()
        mock_save.assert_called_once_with(
            source="fuel_pso",
            page=mock_page,
            storage_client=mock_storage,
            fetch_date=None,
        )
        assert html_uri == "s3://bronze/fuel_pso/2024-01-16/page.html"
        assert png_uri == "s3://bronze/fuel_pso/2024-01-16/page.png"
        mock_page.close.assert_called_once()


def test_fetch_news_fuel_prices_mocked() -> None:
    """Verify fetch_news_fuel_prices navigates politely and saves page to bronze."""
    mock_page = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.new_page.return_value = mock_page
    mock_storage = MagicMock()

    with (
        patch("ingestion.sources.fuel.polite_goto") as mock_goto,
        patch("ingestion.sources.fuel.save_page") as mock_save,
    ):
        mock_save.return_value = (
            "s3://bronze/fuel_news/2024-01-16/page.html",
            "s3://bronze/fuel_news/2024-01-16/page.png",
        )

        html_uri, png_uri = fetch_news_fuel_prices(
            browser_context=mock_ctx, storage_client=mock_storage
        )

        mock_goto.assert_called_once()
        mock_save.assert_called_once_with(
            source="fuel_news",
            page=mock_page,
            storage_client=mock_storage,
            fetch_date=None,
        )
        assert html_uri == "s3://bronze/fuel_news/2024-01-16/page.html"
        assert png_uri == "s3://bronze/fuel_news/2024-01-16/page.png"
        mock_page.close.assert_called_once()

