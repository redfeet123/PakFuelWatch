"""Silver transformation for cpi_monthly Delta table."""

import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa

from ingestion.core.delta_io import write_delta_merge
from ingestion.core.schemas import CPI_MONTHLY_SCHEMA
from ingestion.core.storage import StorageClient, get_storage_client
from ingestion.silver.utils import (
    TransformationResult,
    get_silver_table_uri,
    write_rejects,
)

logger = logging.getLogger("pakfuelwatch.silver.cpi_monthly")
TABLE_NAME = "cpi_monthly"


def normalize_cpi_month(raw_date: Optional[str]) -> Optional[str]:
    """Normalize annual, monthly, or quarterly date strings into ISO 'YYYY-MM' format."""
    if not raw_date or pd.isna(raw_date):
        return None
    d_str = str(raw_date).strip()

    # Pattern 1: YYYY-MM
    if re.match(r"^\d{4}-\d{2}$", d_str):
        return d_str

    # Pattern 2: YYYY-MM-DD
    if re.match(r"^\d{4}-\d{2}-\d{2}$", d_str):
        try:
            dt = datetime.strptime(d_str, "%Y-%m-%d")
            return dt.strftime("%Y-%m")
        except ValueError:
            return None

    # Pattern 3: YYYYMmm (e.g. 2023M05)
    m_match = re.match(r"^(\d{4})M(\d{2})$", d_str, re.IGNORECASE)
    if m_match:
        year, month = m_match.groups()
        return f"{year}-{month}"

    # Pattern 4: Annual YYYY (normalize to January of that year)
    if re.match(r"^\d{4}$", d_str):
        return f"{d_str}-01"

    return None


def parse_worldbank_cpi_raw(raw_bytes: bytes) -> pd.DataFrame:
    """Parse World Bank JSON payload (CPI index or inflation) into an uncleaned DataFrame.

    Args:
        raw_bytes: Raw JSON bytes from Bronze storage.

    Returns:
        DataFrame containing parsed raw observation rows.
    """
    data = json.loads(raw_bytes.decode("utf-8"))
    records: List[Dict[str, Any]] = []

    # Format 1: Standard World Bank API response [page_meta, [records...]]
    if isinstance(data, list) and len(data) >= 2 and isinstance(data[1], list):
        for item in data[1]:
            indicator_info = item.get("indicator", {})
            indicator_id = indicator_info.get("id", "")
            raw_val = item.get("value")
            raw_d = item.get("date")

            # Determine whether this payload represents index or YoY %
            if indicator_id == "FP.CPI.TOTL":
                records.append(
                    {
                        "raw_month": raw_d,
                        "raw_index_value": raw_val,
                        "raw_yoy_pct": item.get("yoy_pct"),
                        "category": "Headline CPI",
                    }
                )
            elif indicator_id == "FP.CPI.TOTL.ZG":
                records.append(
                    {
                        "raw_month": raw_d,
                        "raw_index_value": item.get("index_value"),
                        "raw_yoy_pct": raw_val,
                        "category": "Headline CPI",
                    }
                )
            else:
                # Generic fallback
                records.append(
                    {
                        "raw_month": raw_d,
                        "raw_index_value": raw_val,
                        "raw_yoy_pct": item.get("yoy_pct"),
                        "category": indicator_info.get("value", "Headline CPI"),
                    }
                )
    # Format 2: Direct list of record dictionaries
    elif isinstance(data, list):
        for item in data:
            records.append(
                {
                    "raw_month": item.get("month") or item.get("date"),
                    "raw_index_value": item.get("index_value") or item.get("value"),
                    "raw_yoy_pct": item.get("yoy_pct"),
                    "category": item.get("category", "Headline CPI"),
                }
            )
    # Format 3: Direct dictionary
    elif isinstance(data, dict):
        records.append(
            {
                "raw_month": data.get("month") or data.get("date"),
                "raw_index_value": data.get("index_value") or data.get("value"),
                "raw_yoy_pct": data.get("yoy_pct"),
                "category": data.get("category", "Headline CPI"),
            }
        )

    return pd.DataFrame(records)


def load_latest_worldbank_bronze(
    storage: StorageClient,
) -> Tuple[Optional[pd.DataFrame], Optional[Dict[str, Any]]]:
    """Load and join the latest CPI index and CPI inflation files from Bronze storage."""
    files = storage.list_raw(source="worldbank")
    if not files:
        return None, None

    # Identify latest cpi_index and cpi_inflation files
    index_files = sorted(
        [f for f in files if "cpi_index" in f or "FP.CPI.TOTL" in f],
        reverse=True,
    )
    inflation_files = sorted(
        [f for f in files if "cpi_inflation" in f or "FP.CPI.TOTL.ZG" in f],
        reverse=True,
    )

    index_df = pd.DataFrame()
    if index_files:
        path = index_files[0].replace(f"s3://{storage.bucket}/", "")
        parts = path.split("/", 2)
        if len(parts) == 3:
            raw_b, _ = storage.get_raw("worldbank", parts[1], parts[2])
            if raw_b:
                index_df = parse_worldbank_cpi_raw(raw_b)

    inflation_df = pd.DataFrame()
    if inflation_files:
        path = inflation_files[0].replace(f"s3://{storage.bucket}/", "")
        parts = path.split("/", 2)
        if len(parts) == 3:
            raw_b, _ = storage.get_raw("worldbank", parts[1], parts[2])
            if raw_b:
                inflation_df = parse_worldbank_cpi_raw(raw_b)

    # If both are present, merge on normalized month and category
    if not index_df.empty and not inflation_df.empty:
        # Normalize month before joining
        index_df["norm_month"] = index_df["raw_month"].apply(normalize_cpi_month)
        inflation_df["norm_month"] = inflation_df["raw_month"].apply(normalize_cpi_month)

        merged = pd.merge(
            index_df[["norm_month", "raw_index_value", "category"]],
            inflation_df[["norm_month", "raw_yoy_pct"]],
            on="norm_month",
            how="outer",
        )
        merged["raw_month"] = merged["norm_month"]
        return merged, {"source": "worldbank_merged"}

    if not index_df.empty:
        return index_df, {"source": "worldbank_index"}
    if not inflation_df.empty:
        return inflation_df, {"source": "worldbank_inflation"}

    # Fallback to absolute latest file in worldbank prefix
    sorted_all = sorted(files, reverse=True)
    path = sorted_all[0].replace(f"s3://{storage.bucket}/", "")
    parts = path.split("/", 2)
    raw_b, meta = storage.get_raw("worldbank", parts[1], parts[2])
    return (parse_worldbank_cpi_raw(raw_b) if raw_b else None), meta


def transform_cpi_monthly(
    raw_bytes: Optional[bytes] = None,
    metadata: Optional[Dict[str, Any]] = None,
    silver_base_uri: Optional[str] = None,
    rejects_base_uri: Optional[str] = None,
    storage_client: Optional[StorageClient] = None,
) -> TransformationResult:
    """Transform raw Bronze World Bank CPI data, clean, reject invalid rows, and merge to Silver.

    Args:
        raw_bytes: Optional raw byte payload. If None, reads latest CPI files from Bronze.
        metadata: Optional sidecar metadata dictionary.
        silver_base_uri: Override base URI for Silver table.
        rejects_base_uri: Override base URI for rejects table.
        storage_client: Optional storage client instance.

    Returns:
        TransformationResult containing row counts and destination URIs.
    """
    storage = storage_client or get_storage_client()

    # 1. Acquire raw data
    if raw_bytes is not None:
        raw_df = parse_worldbank_cpi_raw(raw_bytes)
    else:
        loaded_df, _ = load_latest_worldbank_bronze(storage=storage)
        if loaded_df is None or loaded_df.empty:
            logger.warning("[%s] No bronze data found to transform.", TABLE_NAME)
            return TransformationResult(
                table_name=TABLE_NAME,
                read_count=0,
                rejected_count=0,
                written_count=0,
                table_uri=get_silver_table_uri(TABLE_NAME, silver_base_uri),
            )
        raw_df = loaded_df

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
        raw_m = row.get("raw_month")
        raw_idx = row.get("raw_index_value")
        raw_yoy = row.get("raw_yoy_pct")
        cat = row.get("category", "Headline CPI")

        # Validate month
        if pd.isna(raw_m) or not str(raw_m).strip():
            reject_rows.append({**row.to_dict(), "reason": "missing_month"})
            continue

        clean_month = normalize_cpi_month(str(raw_m))
        if not clean_month:
            reject_rows.append({**row.to_dict(), "reason": "invalid_month_format"})
            continue

        # Validate category
        if pd.isna(cat) or not str(cat).strip():
            reject_rows.append({**row.to_dict(), "reason": "missing_category"})
            continue
        clean_category = " ".join(str(cat).strip().split())

        # Validate index_value (schema requires non-null index_value)
        if pd.isna(raw_idx) or str(raw_idx).strip() in (".", "", "None", "null", "nan"):
            # Check if this row is completely missing value or missing index
            reject_rows.append({**row.to_dict(), "reason": "missing_index_value"})
            continue

        try:
            index_val = float(raw_idx)
        except (ValueError, TypeError):
            reject_rows.append({**row.to_dict(), "reason": "non_numeric_index_value"})
            continue

        if index_val <= 0:
            reject_rows.append({**row.to_dict(), "reason": "non_positive_index_value"})
            continue

        # Validate YoY % if provided (nullable=True in schema)
        yoy_val: Optional[float] = None
        if not pd.isna(raw_yoy) and str(raw_yoy).strip() not in (".", "", "None", "null", "nan"):
            try:
                yoy_val = float(raw_yoy)
            except (ValueError, TypeError):
                reject_rows.append({**row.to_dict(), "reason": "non_numeric_yoy_pct"})
                continue

        valid_rows.append(
            {
                "month": clean_month,
                "category": clean_category,
                "index_value": index_val,
                "yoy_pct": yoy_val,
            }
        )

    # 3. Deduplicate valid rows on natural key (month, category) keeping latest
    clean_df = pd.DataFrame(valid_rows)
    if not clean_df.empty:
        clean_df = clean_df.drop_duplicates(subset=["month", "category"], keep="last")

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
        pa_table = pa.Table.from_pandas(clean_df, schema=CPI_MONTHLY_SCHEMA, preserve_index=False)
        write_delta_merge(
            df=pa_table,
            table_path=table_uri,
            merge_keys=["month", "category"],
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
