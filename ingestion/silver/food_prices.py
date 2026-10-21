"""Silver transformation for food_prices Delta table."""

import io
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import pyarrow as pa

from ingestion.core.delta_io import write_delta_merge
from ingestion.core.schemas import FOOD_PRICES_SCHEMA
from ingestion.core.storage import StorageClient, get_storage_client
from ingestion.silver.utils import (
    TransformationResult,
    get_latest_bronze_file,
    get_silver_table_uri,
    write_rejects,
)

logger = logging.getLogger("pakfuelwatch.silver.food_prices")
TABLE_NAME = "food_prices"

# Canonical units allowed in Silver food_prices layer
VALID_UNITS_MAP: Dict[str, str] = {
    "kg": "KG",
    "kgs": "KG",
    "kilogram": "KG",
    "kilograms": "KG",
    "l": "L",
    "ltr": "L",
    "litre": "L",
    "liter": "L",
    "litres": "L",
    "liters": "L",
    "dozen": "Dozen",
    "dz": "Dozen",
    "12 pcs": "Dozen",
    "g": "G",
    "gm": "G",
    "gram": "G",
    "grams": "G",
    "piece": "Piece",
    "pcs": "Piece",
    "item": "Piece",
    "unit": "Piece",
}


def standardize_commodity(raw_commodity: str) -> str:
    """Standardize food commodity names to consistent clean title case."""
    if not raw_commodity:
        return ""
    clean = str(raw_commodity).strip()
    # Strip common market suffixes
    clean = re.sub(r"\s*-\s*Retail\b", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\s*-\s*Wholesale\b", "", clean, flags=re.IGNORECASE)
    # Collapse multiple whitespaces
    clean = " ".join(clean.split())
    return clean.title()


def standardize_market(raw_market: str) -> str:
    """Standardize market / city names to consistent clean title case."""
    if not raw_market:
        return ""
    clean = " ".join(str(raw_market).strip().split())
    return clean.title()


def normalize_unit(raw_unit: Optional[str]) -> Optional[str]:
    """Normalize raw measurement units to canonical forms or return None if mismatched."""
    if not raw_unit or pd.isna(raw_unit):
        return None
    cleaned = str(raw_unit).strip().lower()
    if cleaned in VALID_UNITS_MAP:
        return VALID_UNITS_MAP[cleaned]
    # Check if unit has a quantity prefix (e.g. '20 kg', '1 kg', '5 l', '2.5 kg')
    unit_token = re.sub(r"^[\d\.\s]+", "", cleaned).strip()
    return VALID_UNITS_MAP.get(unit_token)


def parse_food_prices_raw(raw_bytes: bytes) -> pd.DataFrame:
    """Parse raw HDX WFP Food Prices CSV bytes into an uncleaned DataFrame.

    Filters out HXL comment rows (lines or tokens starting with '#').

    Args:
        raw_bytes: Raw CSV bytes from Bronze storage.

    Returns:
        DataFrame containing parsed raw rows.
    """
    try:
        # Read with dtype=str to preserve exact values before cleaning
        df = pd.read_csv(
            io.BytesIO(raw_bytes),
            dtype=str,
            skipinitialspace=True,
            encoding="utf-8-sig",
        )
    except Exception as exc:
        logger.error("Failed to parse raw CSV bytes: %s", exc)
        return pd.DataFrame()

    if df.empty:
        return df

    # Drop HXL tag rows (common in HDX datasets where the row right below headers starts with '#')
    first_col = df.columns[0]
    df = df[~df[first_col].astype(str).str.startswith("#")].copy()

    return df


def transform_food_prices(
    raw_bytes: Optional[bytes] = None,
    metadata: Optional[Dict[str, Any]] = None,
    silver_base_uri: Optional[str] = None,
    rejects_base_uri: Optional[str] = None,
    storage_client: Optional[StorageClient] = None,
) -> TransformationResult:
    """Transform raw Bronze HDX Food Prices data, clean, reject invalid rows, and merge to Silver.

    Args:
        raw_bytes: Optional raw CSV bytes. If None, reads the latest file from Bronze.
        metadata: Optional sidecar metadata dictionary.
        silver_base_uri: Override base URI for Silver table.
        rejects_base_uri: Override base URI for rejects table.
        storage_client: Optional storage client instance.

    Returns:
        TransformationResult containing row counts and destination URIs.
    """
    storage = storage_client or get_storage_client()

    # 1. Acquire raw data
    if raw_bytes is None:
        s3_uri, raw_bytes, metadata = get_latest_bronze_file(
            source="hdx_food", storage_client=storage
        )
        if raw_bytes is None:
            logger.warning("[%s] No bronze data found to transform.", TABLE_NAME)
            return TransformationResult(
                table_name=TABLE_NAME,
                read_count=0,
                rejected_count=0,
                written_count=0,
                table_uri=get_silver_table_uri(TABLE_NAME, silver_base_uri),
            )

    raw_df = parse_food_prices_raw(raw_bytes)
    read_count = len(raw_df)

    if read_count == 0:
        logger.info("[%s] 0 rows read from bronze payload.", TABLE_NAME)
        return TransformationResult(
            table_name=TABLE_NAME,
            read_count=0,
            rejected_count=0,
            written_count=0,
            table_uri=get_silver_table_uri(TABLE_NAME, silver_base_uri),
        )

    valid_rows: List[Dict[str, Any]] = []
    reject_rows: List[Dict[str, Any]] = []

    # 2. Clean, validate, and identify rejects
    for _, row in raw_df.iterrows():
        raw_d = row.get("date")
        raw_m = row.get("market") or row.get("admin2") or row.get("admin1")
        raw_c = row.get("commodity")
        raw_p = row.get("price")
        raw_u = row.get("unit")

        # Validate date
        if pd.isna(raw_d) or not str(raw_d).strip():
            reject_rows.append({**row.to_dict(), "reason": "missing_date"})
            continue

        date_str = str(raw_d).strip()
        parsed_date = None
        for fmt in ("%Y-%m-%d", "%Y-%m", "%d/%m/%Y"):
            try:
                parsed_date = datetime.strptime(date_str, fmt).date()
                break
            except ValueError:
                continue

        if parsed_date is None:
            reject_rows.append({**row.to_dict(), "reason": "invalid_date_format"})
            continue

        # Validate market
        clean_market = standardize_market(raw_m)
        if not clean_market:
            reject_rows.append({**row.to_dict(), "reason": "missing_market"})
            continue

        # Validate commodity
        clean_commodity = standardize_commodity(raw_c)
        if not clean_commodity:
            reject_rows.append({**row.to_dict(), "reason": "missing_commodity"})
            continue

        # Validate unit (detect unit mismatches)
        clean_unit = normalize_unit(raw_u)
        if not clean_unit:
            reject_rows.append({**row.to_dict(), "reason": "unit_mismatch"})
            continue

        # Validate price
        if pd.isna(raw_p) or str(raw_p).strip() in (".", "", "None", "null", "nan"):
            reject_rows.append({**row.to_dict(), "reason": "missing_price"})
            continue

        try:
            price_val = float(raw_p)
        except (ValueError, TypeError):
            reject_rows.append({**row.to_dict(), "reason": "non_numeric_price"})
            continue

        if price_val <= 0:
            reject_rows.append({**row.to_dict(), "reason": "non_positive_price"})
            continue

        if price_val > 1_000_000.0:
            reject_rows.append({**row.to_dict(), "reason": "price_out_of_bounds"})
            continue

        valid_rows.append(
            {
                "date": parsed_date,
                "market": clean_market,
                "commodity": clean_commodity,
                "price": price_val,
                "unit": clean_unit,
            }
        )

    # 3. Deduplicate valid rows on natural key (date, market, commodity, unit) keeping latest
    clean_df = pd.DataFrame(valid_rows)
    if not clean_df.empty:
        clean_df = clean_df.drop_duplicates(
            subset=["date", "market", "commodity", "unit"],
            keep="last",
        )

    rejects_df = pd.DataFrame(reject_rows)
    rejected_count = len(rejects_df)
    written_count = len(clean_df)

    # 4. Write rejects if any
    rejects_uri = write_rejects(
        rejects_df=rejects_df,
        table_name=TABLE_NAME,
        base_rejects_uri=rejects_base_uri,
    )

    # 5. Write clean rows to Silver Delta table with schema casting
    table_uri = get_silver_table_uri(TABLE_NAME, silver_base_uri)
    if written_count > 0:
        pa_table = pa.Table.from_pandas(clean_df, schema=FOOD_PRICES_SCHEMA, preserve_index=False)
        write_delta_merge(
            df=pa_table,
            table_path=table_uri,
            merge_keys=["date", "market", "commodity", "unit"],
        )

    logger.info(
        "[%s] Transformation complete: %d read, %d rejected, %d written to %s",
        TABLE_NAME,
        read_count,
        rejected_count,
        written_count,
        table_uri,
    )

    return TransformationResult(
        table_name=TABLE_NAME,
        read_count=read_count,
        rejected_count=rejected_count,
        written_count=written_count,
        table_uri=table_uri,
        rejects_uri=rejects_uri,
    )
