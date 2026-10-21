"""Silver transformation for brent_daily Delta table."""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import pyarrow as pa

from ingestion.core.delta_io import write_delta_merge
from ingestion.core.schemas import BRENT_DAILY_SCHEMA
from ingestion.core.storage import StorageClient, get_storage_client
from ingestion.silver.utils import (
    TransformationResult,
    get_latest_bronze_file,
    get_silver_table_uri,
    write_rejects,
)

logger = logging.getLogger("pakfuelwatch.silver.brent")
TABLE_NAME = "brent_daily"


def parse_brent_raw(raw_bytes: bytes, default_source: str = "fred") -> pd.DataFrame:
    """Parse raw FRED or EIA JSON bytes into an uncleaned DataFrame.

    Args:
        raw_bytes: Raw JSON bytes from Bronze storage.
        default_source: Fallback source name if not detectable from JSON.

    Returns:
        DataFrame containing parsed records.
    """
    data = json.loads(raw_bytes.decode("utf-8"))
    records: List[Dict[str, Any]] = []

    # Format 1: FRED API response
    if "observations" in data:
        for obs in data["observations"]:
            records.append(
                {
                    "raw_date": obs.get("date"),
                    "raw_price": obs.get("value"),
                    "source": "fred",
                }
            )
    # Format 2: EIA API v2 response
    elif "response" in data and "data" in data["response"]:
        for item in data["response"]["data"]:
            records.append(
                {
                    "raw_date": item.get("period"),
                    "raw_price": item.get("value"),
                    "source": "eia",
                }
            )
    else:
        logger.warning("Unrecognized raw JSON structure for Brent crude")

    return pd.DataFrame(records)


def transform_brent(
    raw_bytes: Optional[bytes] = None,
    metadata: Optional[Dict[str, Any]] = None,
    silver_base_uri: Optional[str] = None,
    rejects_base_uri: Optional[str] = None,
    storage_client: Optional[StorageClient] = None,
) -> TransformationResult:
    """Transform raw Bronze Brent data, clean, reject invalid rows, and merge to Silver Delta.

    Args:
        raw_bytes: Optional raw byte payload. If None, reads the latest file from Bronze.
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
            source="brent", storage_client=storage
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

    provider = (metadata or {}).get("provider", "fred")
    raw_df = parse_brent_raw(raw_bytes, default_source=provider)
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
    for idx, row in raw_df.iterrows():
        raw_d = row.get("raw_date")
        raw_p = row.get("raw_price")
        src = row.get("source", provider)

        # Validate date
        if pd.isna(raw_d) or not str(raw_d).strip():
            reject_rows.append({**row.to_dict(), "reason": "missing_date"})
            continue

        try:
            parsed_date = datetime.strptime(str(raw_d).strip(), "%Y-%m-%d").date()
        except ValueError:
            reject_rows.append({**row.to_dict(), "reason": "invalid_date_format"})
            continue

        # Validate price: handle FRED '.' missing markers and negative prices
        if pd.isna(raw_p) or str(raw_p).strip() in (".", "", "None", "null"):
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

        if price_val > 500:
            reject_rows.append({**row.to_dict(), "reason": "price_out_of_bounds"})
            continue

        valid_rows.append(
            {
                "date": parsed_date,
                "price_usd": price_val,
                "source": str(src).lower(),
            }
        )

    # 3. Deduplicate valid rows on natural key (date, source) keeping the latest
    clean_df = pd.DataFrame(valid_rows)
    if not clean_df.empty:
        clean_df = clean_df.drop_duplicates(subset=["date", "source"], keep="last")

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
        # Cast to defined PyArrow schema
        pa_table = pa.Table.from_pandas(clean_df, schema=BRENT_DAILY_SCHEMA, preserve_index=False)
        write_delta_merge(
            df=pa_table,
            table_path=table_uri,
            merge_keys=["date", "source"],
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
