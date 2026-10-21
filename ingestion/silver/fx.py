"""Silver transformation for fx_daily Delta table."""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import pyarrow as pa

from ingestion.core.delta_io import write_delta_merge
from ingestion.core.schemas import FX_DAILY_SCHEMA
from ingestion.core.storage import StorageClient, get_storage_client
from ingestion.silver.utils import (
    TransformationResult,
    get_latest_bronze_file,
    get_silver_table_uri,
    write_rejects,
)

logger = logging.getLogger("pakfuelwatch.silver.fx")
TABLE_NAME = "fx_daily"


def parse_fx_raw(raw_bytes: bytes, default_source: str = "sbp") -> pd.DataFrame:
    """Parse raw SBP or Frankfurter JSON bytes into an uncleaned DataFrame.

    Args:
        raw_bytes: Raw JSON bytes from Bronze storage.
        default_source: Fallback source name if not detectable from JSON.

    Returns:
        DataFrame containing parsed raw records.
    """
    data = json.loads(raw_bytes.decode("utf-8"))
    records: List[Dict[str, Any]] = []

    # Format 1: SBP EasyData API response (has observations array)
    if isinstance(data, dict) and "observations" in data:
        pair_str = data.get("pair", "USD/PKR")
        for obs in data["observations"]:
            records.append(
                {
                    "raw_date": obs.get("date"),
                    "raw_rate": obs.get("rate") or obs.get("value"),
                    "raw_pair": obs.get("pair", pair_str),
                    "source": "sbp",
                }
            )
    # Format 2: Frankfurter API response (has rates object)
    elif isinstance(data, dict) and "rates" in data:
        base = data.get("base", "USD")
        rates_data = data["rates"]
        # Case 2a: Range of dates {"2024-01-02": {"PKR": 281.25}, ...}
        if isinstance(rates_data, dict):
            for key, val in rates_data.items():
                if isinstance(val, dict):
                    date_str = key
                    for target_currency, rate_val in val.items():
                        records.append(
                            {
                                "raw_date": date_str,
                                "raw_rate": rate_val,
                                "raw_pair": f"{base}/{target_currency}",
                                "source": "frankfurter",
                            }
                        )
                else:
                    # Case 2b: Single date response {"date": "...", "rates": {"PKR": 281.25}}
                    date_str = data.get("date", key)
                    records.append(
                        {
                            "raw_date": date_str,
                            "raw_rate": val,
                            "raw_pair": f"{base}/{key}",
                            "source": "frankfurter",
                        }
                    )
    # Format 3: Generic list of records
    elif isinstance(data, list):
        for item in data:
            records.append(
                {
                    "raw_date": item.get("date"),
                    "raw_rate": item.get("rate") or item.get("value"),
                    "raw_pair": item.get("pair", "USD/PKR"),
                    "source": default_source,
                }
            )
    else:
        logger.warning("Unrecognized raw FX JSON structure")

    return pd.DataFrame(records)


def transform_fx(
    raw_bytes: Optional[bytes] = None,
    metadata: Optional[Dict[str, Any]] = None,
    silver_base_uri: Optional[str] = None,
    rejects_base_uri: Optional[str] = None,
    storage_client: Optional[StorageClient] = None,
) -> TransformationResult:
    """Transform raw Bronze FX data, clean, reject invalid rows, and merge to Silver Delta.

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
        s3_uri, raw_bytes, metadata = get_latest_bronze_file(source="fx", storage_client=storage)
        if raw_bytes is None:
            logger.warning("[%s] No bronze data found to transform.", TABLE_NAME)
            return TransformationResult(
                table_name=TABLE_NAME,
                read_count=0,
                rejected_count=0,
                written_count=0,
                table_uri=get_silver_table_uri(TABLE_NAME, silver_base_uri),
            )

    provider = (metadata or {}).get("provider", "sbp")
    raw_df = parse_fx_raw(raw_bytes, default_source=provider)
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
        raw_d = row.get("raw_date")
        raw_r = row.get("raw_rate")
        raw_p = row.get("raw_pair")
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

        # Validate currency pair
        if pd.isna(raw_p) or not str(raw_p).strip():
            reject_rows.append({**row.to_dict(), "reason": "missing_pair"})
            continue
        clean_pair = str(raw_p).strip().upper()

        # Validate rate
        if pd.isna(raw_r) or str(raw_r).strip() in (".", "", "None", "null"):
            reject_rows.append({**row.to_dict(), "reason": "missing_rate"})
            continue

        try:
            rate_val = float(raw_r)
        except (ValueError, TypeError):
            reject_rows.append({**row.to_dict(), "reason": "non_numeric_rate"})
            continue

        if rate_val <= 0:
            reject_rows.append({**row.to_dict(), "reason": "non_positive_rate"})
            continue

        if rate_val < 1.0 or rate_val > 10000.0:
            reject_rows.append({**row.to_dict(), "reason": "rate_out_of_bounds"})
            continue

        valid_rows.append(
            {
                "date": parsed_date,
                "pair": clean_pair,
                "rate": rate_val,
                "source": str(src).lower(),
            }
        )

    # 3. Deduplicate valid rows on natural key (date, pair, source) keeping the latest
    clean_df = pd.DataFrame(valid_rows)
    if not clean_df.empty:
        clean_df = clean_df.drop_duplicates(subset=["date", "pair", "source"], keep="last")

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
        pa_table = pa.Table.from_pandas(clean_df, schema=FX_DAILY_SCHEMA, preserve_index=False)
        write_delta_merge(
            df=pa_table,
            table_path=table_uri,
            merge_keys=["date", "pair", "source"],
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
