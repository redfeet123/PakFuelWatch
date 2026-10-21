"""Fuel price pipeline module for official (PSO) and secondary (news) sources.

Includes:
1. Browser scraping of Source A (PSO) and Source B (News) with raw HTML & screenshot storage.
2. Robust parsing into fuel_prices Silver table (handling Rs., commas, units, dates).
3. Backfill loading from data/seed/fuel_history.csv with source='seed' and confidence='low'.
4. Reconciliation comparing sources: <=0.5% diff -> 'high', >0.5% diff -> 'conflict'
   with disagreements written to fuel_price_conflicts.
5. Detection and logging of missing fortnights.
"""

import logging
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa

from ingestion.core.browser import (
    get_selector,
    launch_browser_context,
    polite_goto,
    save_page,
)
from ingestion.core.delta_io import read_delta_pandas, write_delta_merge
from ingestion.core.schemas import FUEL_PRICE_CONFLICTS_SCHEMA, FUEL_PRICES_SCHEMA
from ingestion.core.storage import StorageClient, get_storage_client
from ingestion.silver.utils import (
    TransformationResult,
    get_latest_bronze_file,
    get_silver_table_uri,
)

logger = logging.getLogger("pakfuelwatch.sources.fuel")
TABLE_NAME = "fuel_prices"
CONFLICTS_TABLE_NAME = "fuel_price_conflicts"


# =====================================================================
# Ingestion: Source A (PSO) & Source B (News)
# =====================================================================


def fetch_pso_fuel_prices(
    browser_context: Optional[Any] = None,
    storage_client: Optional[StorageClient] = None,
    fetch_date: Optional[str] = None,
) -> Tuple[str, str]:
    """Scrape official fuel price page (PSO) saving HTML snapshot and screenshot to Bronze.

    Args:
        browser_context: Optional BrowserContext instance.
        storage_client: StorageClient instance.
        fetch_date: Optional date string 'YYYY-MM-DD'.

    Returns:
        Tuple of (html_s3_uri, screenshot_s3_uri).
    """
    storage = storage_client or get_storage_client()
    target_url = get_selector(
        "fuel", "pso", "page_url", default="https://psopk.com/en/fuels/fuel-prices"
    )

    def _execute(ctx: Any) -> Tuple[str, str]:
        page = ctx.new_page()
        try:
            polite_goto(page, target_url)
            return save_page(
                source="fuel_pso",
                page=page,
                storage_client=storage,
                fetch_date=fetch_date,
            )
        finally:
            page.close()

    if browser_context:
        return _execute(browser_context)

    with launch_browser_context() as ctx:
        return _execute(ctx)


def fetch_news_fuel_prices(
    browser_context: Optional[Any] = None,
    storage_client: Optional[StorageClient] = None,
    fetch_date: Optional[str] = None,
) -> Tuple[str, str]:
    """Scrape fuel price announcement news coverage saving HTML and screenshot to Bronze.

    Args:
        browser_context: Optional BrowserContext instance.
        storage_client: StorageClient instance.
        fetch_date: Optional date string 'YYYY-MM-DD'.

    Returns:
        Tuple of (html_s3_uri, screenshot_s3_uri).
    """
    storage = storage_client or get_storage_client()
    target_url = get_selector(
        "fuel", "news", "page_url", default="https://www.dawn.com/news/fuel-prices"
    )

    def _execute(ctx: Any) -> Tuple[str, str]:
        page = ctx.new_page()
        try:
            polite_goto(page, target_url)
            return save_page(
                source="fuel_news",
                page=page,
                storage_client=storage,
                fetch_date=fetch_date,
            )
        finally:
            page.close()

    if browser_context:
        return _execute(browser_context)

    with launch_browser_context() as ctx:
        return _execute(ctx)


# =====================================================================
# Parsing Utilities
# =====================================================================


def clean_price_val(raw_price: Any) -> Optional[float]:
    """Extract and parse numeric per-litre price, handling 'Rs.', commas, and symbols."""
    if raw_price is None or pd.isna(raw_price):
        return None

    val_str = str(raw_price).strip()
    if not val_str:
        return None

    # Replace commas with decimal point if comma-separated or strip formatting
    # Example: "Rs. 267.34", "267,34", "PKR 276.21/litre"
    match = re.search(r"(\d+(?:[\.,]\d+)?)", val_str)
    if not match:
        return None

    num_part = match.group(1).replace(",", ".")
    try:
        val = float(num_part)
        return val if val > 0 else None
    except ValueError:
        return None


def clean_product_name(raw_name: Any) -> Optional[str]:
    """Standardize fuel product names to canonical 'Petrol' or 'Diesel'."""
    if not raw_name or pd.isna(raw_name):
        return None

    n = str(raw_name).strip().lower()
    if any(k in n for k in ("petrol", "gasoline", "super", "mogas", "ms 92", "ms-92")):
        return "Petrol"
    if any(k in n for k in ("diesel", "hsd", "high speed diesel")):
        return "Diesel"
    return None


def parse_effective_date(raw_date: Any, default_date: Optional[date] = None) -> date:
    """Parse string or timestamp into datetime.date across multiple Pakistani formats."""
    fallback = default_date or datetime.now(timezone.utc).date()
    if not raw_date or pd.isna(raw_date):
        return fallback

    d_str = str(raw_date).strip()
    d_str = re.sub(r"^(?:effective\s*(?:date)?|from|w\.?e\.?f\.?)\s*:?\s*", "", d_str, flags=re.I)
    d_str = d_str.strip()

    # Search for date patterns within d_str
    # 1. 16-Jan-2024 or 16-January-2024
    m1 = re.search(r"\b(\d{1,2})[-\s]([A-Za-z]{3,9})[-\s](\d{4})\b", d_str)
    if m1:
        try:
            date_str = f"{m1.group(1)} {m1.group(2)[:3]} {m1.group(3)}"
            return datetime.strptime(date_str, "%d %b %Y").date()
        except ValueError:
            pass

    # 2. January 16, 2024 or Jan 16 2024
    m2 = re.search(r"\b([A-Za-z]{3,9})\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})\b", d_str)
    if m2:
        try:
            date_str = f"{m2.group(1)[:3]} {m2.group(2)} {m2.group(3)}"
            return datetime.strptime(date_str, "%b %d %Y").date()
        except ValueError:
            pass

    # 3. 1st January 2024 or 16th January 2024
    m3 = re.search(r"\b(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9}),?\s+(\d{4})\b", d_str)
    if m3:
        try:
            date_str = f"{m3.group(1)} {m3.group(2)[:3]} {m3.group(3)}"
            return datetime.strptime(date_str, "%d %b %Y").date()
        except ValueError:
            pass

    # 4. ISO YYYY-MM-DD
    m4 = re.search(r"\b(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})\b", d_str)
    if m4:
        try:
            date_str = f"{m4.group(1)}-{int(m4.group(2)):02d}-{int(m4.group(3)):02d}"
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            pass

    # 5. DD/MM/YYYY
    m5 = re.search(r"\b(\d{1,2})[-\/](\d{1,2})[-\/](\d{4})\b", d_str)
    if m5:
        try:
            date_str = f"{int(m5.group(1)):02d}-{int(m5.group(2)):02d}-{m5.group(3)}"
            return datetime.strptime(date_str, "%d-%m-%Y").date()
        except ValueError:
            pass

    return fallback


def parse_pso_fuel_html(
    html_content: str,
    default_date: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """Parse official PSO fuel price HTML page into structured records.

    Handles date extraction and table rows. Returns empty list if malformed.
    """
    if not html_content or "503 Error" in html_content or "Service Unavailable" in html_content:
        logger.warning("PSO fuel page content appears malformed or unavailable.")
        return []

    # 1. Extract effective date from text content (stripping tags to avoid class names)
    raw_text = re.sub(r"<[^>]+>", " ", html_content)
    raw_text = re.sub(r"\s+", " ", raw_text)
    date_match = re.search(
        r"(?:effective(?:\s*date)?|w\.?e\.?f\.?)\s*:?\s*([0-9a-zA-Z\s,\-\/]+)",
        raw_text,
        re.IGNORECASE,
    )
    raw_date_str = date_match.group(1).strip() if date_match else None
    eff_date = parse_effective_date(raw_date_str, default_date=default_date)

    # 2. Extract table rows matching product and price
    records: List[Dict[str, Any]] = []
    # Match table rows: <tr>...<td...>Product</td>...<td...>Price</td>...</tr>
    row_pattern = re.compile(r"<tr[^>]*>(.*?)</tr>", re.DOTALL | re.IGNORECASE)
    cell_pattern = re.compile(r"<td[^>]*>(.*?)</td>", re.DOTALL | re.IGNORECASE)

    for row_html in row_pattern.findall(html_content):
        cells = [re.sub(r"<[^>]+>", "", c).strip() for c in cell_pattern.findall(row_html)]
        if len(cells) < 2:
            continue

        prod_candidate = clean_product_name(cells[0])
        price_candidate = clean_price_val(cells[1])

        if prod_candidate and price_candidate:
            records.append(
                {
                    "effective_date": eff_date,
                    "product": prod_candidate,
                    "price_pkr_per_litre": price_candidate,
                    "source": "pso",
                    "confidence": "high",
                }
            )

    return records


def parse_news_fuel_html(
    html_content: str,
    default_date: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """Parse news announcement article HTML into structured fuel price records."""
    if not html_content or len(html_content) < 50:
        return []

    # Strip script/style and head tags to focus on body text
    clean_html = re.sub(
        r"<(script|style|head)[^>]*>.*?</\1>",
        " ",
        html_content,
        flags=re.DOTALL | re.IGNORECASE,
    )
    body_text = re.sub(r"<[^>]+>", " ", clean_html)
    body_text = re.sub(r"\s+", " ", body_text)

    # Extract announcement date
    eff_date = parse_effective_date(body_text, default_date=default_date)

    records: List[Dict[str, Any]] = []

    # Extract petrol price (matching word boundaries, avoiding 'petroleum')
    petrol_match = re.search(
        r"\b(?:petrol|motor gasoline|super|mogas)\b[^\n\.\;]{0,60}?(?:rs\.?|pkr)?\s*([\d,]+\.?\d*)",
        body_text,
        re.IGNORECASE,
    )
    if not petrol_match:
        petrol_match = re.search(
            r"(?:rs\.?|pkr)?\s*([\d,]+\.?\d*)[^\n\.\;]{0,40}?"
            r"\b(?:petrol|motor gasoline|super|mogas)\b",
            body_text,
            re.IGNORECASE,
        )

    if petrol_match:
        p_price = clean_price_val(petrol_match.group(1))
        if p_price and p_price > 50.0:
            records.append(
                {
                    "effective_date": eff_date,
                    "product": "Petrol",
                    "price_pkr_per_litre": p_price,
                    "source": "news",
                    "confidence": "medium",
                }
            )

    # Extract diesel price
    diesel_match = re.search(
        r"\b(?:diesel|hsd|high speed diesel)\b[^\n\.\;]{0,60}?(?:rs\.?|pkr)?\s*([\d,]+\.?\d*)",
        body_text,
        re.IGNORECASE,
    )
    if not diesel_match:
        diesel_match = re.search(
            r"(?:rs\.?|pkr)?\s*([\d,]+\.?\d*)[^\n\.\;]{0,40}?"
            r"\b(?:diesel|hsd|high speed diesel)\b",
            body_text,
            re.IGNORECASE,
        )

    if diesel_match:
        d_price = clean_price_val(diesel_match.group(1))
        if d_price and d_price > 50.0:
            records.append(
                {
                    "effective_date": eff_date,
                    "product": "Diesel",
                    "price_pkr_per_litre": d_price,
                    "source": "news",
                    "confidence": "medium",
                }
            )

    return records


# =====================================================================
# Backfill from Seed Dataset
# =====================================================================


def load_fuel_seed_history(
    csv_path: Optional[str] = None,
    silver_base_uri: Optional[str] = None,
    storage_client: Optional[StorageClient] = None,
) -> TransformationResult:
    """Load historical fuel prices from seed CSV and merge into Silver fuel_prices table.

    Marks records with source='seed' and confidence='low'.

    Args:
        csv_path: Path to seed CSV file. Defaults to 'data/seed/fuel_history.csv'.
        silver_base_uri: Target Silver base URI.
        storage_client: StorageClient instance.

    Returns:
        TransformationResult summary.
    """
    path = Path(csv_path or "data/seed/fuel_history.csv")
    if not path.exists():
        logger.warning("Seed fuel history file not found at: %s", path)
        return TransformationResult(
            TABLE_NAME, 0, 0, 0, get_silver_table_uri(TABLE_NAME, silver_base_uri)
        )

    df_raw = pd.read_csv(path)
    read_count = len(df_raw)
    valid_rows: List[Dict[str, Any]] = []

    for _, row in df_raw.iterrows():
        raw_d = row.get("date") or row.get("effective_date")
        raw_prod = row.get("product")
        raw_p = row.get("price") or row.get("price_pkr_per_litre")

        prod_clean = clean_product_name(raw_prod)
        price_clean = clean_price_val(raw_p)
        if not prod_clean or price_clean is None:
            continue

        eff_date = parse_effective_date(raw_d)
        valid_rows.append(
            {
                "effective_date": eff_date,
                "product": prod_clean,
                "price_pkr_per_litre": price_clean,
                "source": "seed",
                "confidence": "low",
            }
        )

    clean_df = pd.DataFrame(valid_rows)
    if not clean_df.empty:
        clean_df = clean_df.drop_duplicates(
            subset=["effective_date", "product", "source"],
            keep="last",
        )

    written_count = len(clean_df)
    rejected_count = read_count - written_count
    table_uri = get_silver_table_uri(TABLE_NAME, silver_base_uri)

    if written_count > 0:
        pa_table = pa.Table.from_pandas(clean_df, schema=FUEL_PRICES_SCHEMA, preserve_index=False)
        write_delta_merge(
            df=pa_table,
            table_path=table_uri,
            merge_keys=["effective_date", "product", "source"],
        )

    logger.info(
        "[%s] Backfill complete from %s: %d read, %d merged",
        TABLE_NAME,
        path,
        read_count,
        written_count,
    )
    return TransformationResult(
        table_name=TABLE_NAME,
        read_count=read_count,
        rejected_count=rejected_count,
        written_count=written_count,
        table_uri=table_uri,
    )


# =====================================================================
# Reconciliation & Conflict Detection
# =====================================================================


def reconcile_fuel_prices(
    silver_base_uri: Optional[str] = None,
    conflicts_base_uri: Optional[str] = None,
    storage_client: Optional[StorageClient] = None,
) -> Tuple[int, int]:
    """Compare prices across sources for the same effective_date and product.

    Rules:
    - If sources agree within 0.5%: confidence='high'.
    - If sources disagree (>0.5%): keep both rows, set confidence='conflict',
      and write disagreement record to fuel_price_conflicts Delta table.

    Returns:
        Tuple of (agreements_count, conflicts_count).
    """
    table_uri = get_silver_table_uri(TABLE_NAME, silver_base_uri)
    try:
        df = read_delta_pandas(table_uri)
    except Exception as exc:
        logger.warning("Could not read Delta table %s for reconciliation: %s", table_uri, exc)
        return 0, 0

    if df.empty:
        return 0, 0

    agreements_count = 0
    conflicts_count = 0
    conflicts_records: List[Dict[str, Any]] = []
    updated_rows: List[Dict[str, Any]] = []

    # Group by effective_date and product
    grouped = df.groupby(["effective_date", "product"])

    for (eff_date, prod), group in grouped:
        sources = group["source"].unique()
        if len(sources) < 2:
            # Single source observation, preserve existing confidence
            for _, r in group.iterrows():
                updated_rows.append(r.to_dict())
            continue

        # Multiple sources: compare pairwise
        rows_list = group.to_dict(orient="records")
        row_a = rows_list[0]
        row_b = rows_list[1]

        p_a = float(row_a["price_pkr_per_litre"])
        p_b = float(row_b["price_pkr_per_litre"])
        min_p = min(p_a, p_b)

        pct_diff = (abs(p_a - p_b) / min_p) * 100.0 if min_p > 0 else 0.0

        if pct_diff <= 0.5:
            # Agreement within 0.5%
            agreements_count += 1
            row_a["confidence"] = "high"
            row_b["confidence"] = "high"
            updated_rows.extend([row_a, row_b])
        else:
            # Disagreement > 0.5%
            conflicts_count += 1
            row_a["confidence"] = "conflict"
            row_b["confidence"] = "conflict"
            updated_rows.extend([row_a, row_b])

            conflicts_records.append(
                {
                    "effective_date": eff_date,
                    "product": prod,
                    "source_a": str(row_a["source"]),
                    "price_a": p_a,
                    "source_b": str(row_b["source"]),
                    "price_b": p_b,
                    "pct_difference": round(pct_diff, 4),
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                }
            )

    # 1. Write conflicts table if any
    conflicts_uri = get_silver_table_uri(CONFLICTS_TABLE_NAME, conflicts_base_uri)
    if conflicts_records:
        logger.warning(
            "Writing %d fuel price conflicts to %s", len(conflicts_records), conflicts_uri
        )
        conf_df = pd.DataFrame(conflicts_records)
        pa_conf = pa.Table.from_pandas(
            conf_df, schema=FUEL_PRICE_CONFLICTS_SCHEMA, preserve_index=False
        )
        write_delta_merge(
            df=pa_conf,
            table_path=conflicts_uri,
            merge_keys=["effective_date", "product", "source_a", "source_b"],
        )

    # 2. Update fuel_prices with reconciled confidence
    if updated_rows:
        up_df = pd.DataFrame(updated_rows)
        pa_up = pa.Table.from_pandas(up_df, schema=FUEL_PRICES_SCHEMA, preserve_index=False)
        write_delta_merge(
            df=pa_up,
            table_path=table_uri,
            merge_keys=["effective_date", "product", "source"],
        )

    logger.info(
        "Reconciliation complete for %s: %d agreements, %d conflicts",
        TABLE_NAME,
        agreements_count,
        conflicts_count,
    )
    return agreements_count, conflicts_count


# =====================================================================
# Fortnight Gap Detection
# =====================================================================


def detect_fuel_price_gaps(
    df: Optional[pd.DataFrame] = None,
    silver_base_uri: Optional[str] = None,
    storage_client: Optional[StorageClient] = None,
) -> List[Dict[str, Any]]:
    """Detect and log missing fortnights (revisions on ~1st and ~16th) in the price record.

    Args:
        df: Optional in-memory DataFrame of fuel prices.
        silver_base_uri: Silver table base URI to read from if df is None.
        storage_client: StorageClient instance.

    Returns:
        List of missing fortnight dicts with 'product', 'year', 'month', 'fortnight'.
    """
    if df is None:
        table_uri = get_silver_table_uri(TABLE_NAME, silver_base_uri)
        try:
            df = read_delta_pandas(table_uri)
        except Exception:
            return []

    if df.empty or "effective_date" not in df.columns:
        return []

    # Ensure effective_date is date objects
    df["eff_date"] = pd.to_datetime(df["effective_date"]).dt.date
    min_date = df["eff_date"].min()
    max_date = df["eff_date"].max()

    if min_date == max_date:
        return []

    missing_gaps: List[Dict[str, Any]] = []
    products = df["product"].unique()

    # Generate expected fortnights between min_date and max_date
    current_year = min_date.year
    current_month = min_date.month
    end_year = max_date.year
    end_month = max_date.month

    while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
        # Fortnight 1 (1st of month) and Fortnight 2 (16th of month)
        fn1_expected = date(current_year, current_month, 1)
        fn2_expected = date(current_year, current_month, 16)

        for prod in products:
            prod_df = df[df["product"] == prod]

            # Check if any record exists within days 1-10 for FN1
            fn1_exists = any(
                r.year == current_year and r.month == current_month and 1 <= r.day <= 10
                for r in prod_df["eff_date"]
            )
            if not fn1_exists and fn1_expected <= max_date:
                logger.warning(
                    "Detected missing fuel price fortnight: %s (%04d-%02d-01) for %s",
                    f"{current_year}-{current_month:02d}-FN1",
                    current_year,
                    current_month,
                    prod,
                )
                missing_gaps.append(
                    {
                        "product": prod,
                        "year": current_year,
                        "month": current_month,
                        "fortnight": "FN1",
                        "expected_date": fn1_expected.isoformat(),
                    }
                )

            # Check if any record exists within days 14-25 for FN2
            fn2_exists = any(
                r.year == current_year and r.month == current_month and 14 <= r.day <= 25
                for r in prod_df["eff_date"]
            )
            if not fn2_exists and fn2_expected <= max_date:
                logger.warning(
                    "Detected missing fuel price fortnight: %s (%04d-%02d-16) for %s",
                    f"{current_year}-{current_month:02d}-FN2",
                    current_year,
                    current_month,
                    prod,
                )
                missing_gaps.append(
                    {
                        "product": prod,
                        "year": current_year,
                        "month": current_month,
                        "fortnight": "FN2",
                        "expected_date": fn2_expected.isoformat(),
                    }
                )

        # Increment month
        if current_month == 12:
            current_month = 1
            current_year += 1
        else:
            current_month += 1

    return missing_gaps


# =====================================================================
# Transformations
# =====================================================================


def transform_fuel_pso(
    raw_bytes: Optional[bytes] = None,
    silver_base_uri: Optional[str] = None,
    rejects_base_uri: Optional[str] = None,
    storage_client: Optional[StorageClient] = None,
) -> TransformationResult:
    """Transform raw Bronze PSO HTML and merge into fuel_prices Silver table."""
    storage = storage_client or get_storage_client()
    if raw_bytes is None:
        _, raw_bytes, _ = get_latest_bronze_file("fuel_pso", storage_client=storage)
        if raw_bytes is None:
            logger.warning("[%s] No bronze PSO fuel data found.", TABLE_NAME)
            return TransformationResult(
                TABLE_NAME, 0, 0, 0, get_silver_table_uri(TABLE_NAME, silver_base_uri)
            )

    html_str = raw_bytes.decode("utf-8", errors="replace")
    parsed_records = parse_pso_fuel_html(html_str)
    read_count = len(parsed_records)

    clean_df = pd.DataFrame(parsed_records)
    if not clean_df.empty:
        clean_df = clean_df.drop_duplicates(
            subset=["effective_date", "product", "source"],
            keep="last",
        )

    written_count = len(clean_df)
    rejected_count = read_count - written_count
    table_uri = get_silver_table_uri(TABLE_NAME, silver_base_uri)

    if written_count > 0:
        pa_table = pa.Table.from_pandas(clean_df, schema=FUEL_PRICES_SCHEMA, preserve_index=False)
        write_delta_merge(
            df=pa_table,
            table_path=table_uri,
            merge_keys=["effective_date", "product", "source"],
        )

    return TransformationResult(
        table_name=TABLE_NAME,
        read_count=read_count,
        rejected_count=rejected_count,
        written_count=written_count,
        table_uri=table_uri,
    )


def transform_fuel_news(
    raw_bytes: Optional[bytes] = None,
    silver_base_uri: Optional[str] = None,
    rejects_base_uri: Optional[str] = None,
    storage_client: Optional[StorageClient] = None,
) -> TransformationResult:
    """Transform raw Bronze News HTML and merge into fuel_prices Silver table."""
    storage = storage_client or get_storage_client()
    if raw_bytes is None:
        _, raw_bytes, _ = get_latest_bronze_file("fuel_news", storage_client=storage)
        if raw_bytes is None:
            logger.warning("[%s] No bronze news fuel data found.", TABLE_NAME)
            return TransformationResult(
                TABLE_NAME, 0, 0, 0, get_silver_table_uri(TABLE_NAME, silver_base_uri)
            )

    html_str = raw_bytes.decode("utf-8", errors="replace")
    parsed_records = parse_news_fuel_html(html_str)
    read_count = len(parsed_records)

    clean_df = pd.DataFrame(parsed_records)
    if not clean_df.empty:
        clean_df = clean_df.drop_duplicates(
            subset=["effective_date", "product", "source"],
            keep="last",
        )

    written_count = len(clean_df)
    rejected_count = read_count - written_count
    table_uri = get_silver_table_uri(TABLE_NAME, silver_base_uri)

    if written_count > 0:
        pa_table = pa.Table.from_pandas(clean_df, schema=FUEL_PRICES_SCHEMA, preserve_index=False)
        write_delta_merge(
            df=pa_table,
            table_path=table_uri,
            merge_keys=["effective_date", "product", "source"],
        )

    return TransformationResult(
        table_name=TABLE_NAME,
        read_count=read_count,
        rejected_count=rejected_count,
        written_count=written_count,
        table_uri=table_uri,
    )
