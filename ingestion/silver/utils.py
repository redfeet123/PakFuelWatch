"""Shared utilities for Silver layer transformations and reject handling."""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from deltalake import write_deltalake

from ingestion.core.delta_io import get_default_storage_options
from ingestion.core.storage import StorageClient, get_storage_client

logger = logging.getLogger("pakfuelwatch.silver.utils")


@dataclass
class TransformationResult:
    """Summary of a silver transformation run."""

    table_name: str
    read_count: int
    rejected_count: int
    written_count: int
    table_uri: str
    rejects_uri: Optional[str] = None


def get_silver_table_uri(table_name: str, base_uri: Optional[str] = None) -> str:
    """Return the target Silver Delta table URI."""
    base = base_uri or "s3://silver"
    return f"{base.rstrip('/')}/{table_name}"


def get_rejects_table_uri(table_name: str, base_uri: Optional[str] = None) -> str:
    """Return the Silver rejects Delta table URI."""
    base = base_uri or "s3://silver/_rejects"
    return f"{base.rstrip('/')}/{table_name}"


def get_quarantine_uri(source: str, filename: str, base_uri: Optional[str] = None) -> str:
    """Return the Silver quarantine URI for a failed file."""
    base = base_uri or "s3://silver/_quarantine"
    return f"{base.rstrip('/')}/{source}/{filename}"


def write_quarantine(
    content: bytes,
    filename: str,
    source: str,
    error_message: str,
    page_or_sheet: Optional[str] = None,
    base_quarantine_uri: Optional[str] = None,
    storage_client: Optional[StorageClient] = None,
) -> str:
    """Write an unparseable or corrupted file to the quarantine location with diagnostic metadata.

    Args:
        content: Raw binary bytes of the failing file.
        filename: Name of the file that failed parsing.
        source: Source identifier (e.g. 'pbs_spi', 'pbs_cpi').
        error_message: Summary description of the parse failure.
        page_or_sheet: Optional identifier for the page or worksheet that failed.
        base_quarantine_uri: Override base URI for quarantine destination.
        storage_client: Optional StorageClient instance.

    Returns:
        The URI where the quarantined file was written.
    """
    target_uri = get_quarantine_uri(source, filename, base_quarantine_uri)
    logger.error(
        "[%s] Quarantining failing file %s (location: %s, error: %s) to %s",
        source,
        filename,
        page_or_sheet or "unknown",
        error_message,
        target_uri,
    )

    if target_uri.startswith("s3://"):
        storage = storage_client or get_storage_client()
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        storage.put_raw(
            source=f"_quarantine/{source}",
            filename=filename,
            content=content,
            metadata={
                "error_message": error_message,
                "failing_page_or_sheet": str(page_or_sheet or "none"),
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "original_filename": filename,
            },
            fetch_date=today_str,
        )
    else:
        # Local filesystem path (e.g. in test fixtures / tmp_path)
        out_path = Path(target_uri)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(content)
        # Write metadata sidecar
        meta_path = out_path.with_name(f"{filename}.metadata.json")
        meta_dict = {
            "error_message": error_message,
            "failing_page_or_sheet": str(page_or_sheet or "none"),
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "original_filename": filename,
        }
        meta_path.write_text(json.dumps(meta_dict, indent=2), encoding="utf-8")

    return target_uri


def write_rejects(
    rejects_df: pd.DataFrame,
    table_name: str,
    base_rejects_uri: Optional[str] = None,
    storage_options: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """Write rejected rows to a Silver rejects Delta table with a reason column.

    Args:
        rejects_df: DataFrame containing the rejected records. Must include a 'reason' column.
        table_name: Target Silver table name (e.g., 'brent_daily', 'food_prices').
        base_rejects_uri: Base URI for rejects. Defaults to s3://silver/_rejects.
        storage_options: Optional custom S3/MinIO options.

    Returns:
        The rejects table URI if rows were written, or None if rejects_df is empty.
    """
    if rejects_df.empty:
        return None

    df = rejects_df.copy()
    if "reason" not in df.columns:
        df["reason"] = "unspecified_validation_failure"

    if "rejected_at" not in df.columns:
        df["rejected_at"] = datetime.now(timezone.utc).isoformat()

    # Convert complex or object columns to strings to ensure clean schema in rejects table
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str)

    rejects_uri = get_rejects_table_uri(table_name, base_rejects_uri)
    opts = storage_options
    if opts is None and rejects_uri.startswith("s3://"):
        opts = get_default_storage_options()

    logger.warning(
        "[%s] Writing %d rejected records to %s",
        table_name,
        len(df),
        rejects_uri,
    )

    write_deltalake(
        rejects_uri,
        df,
        mode="append",
        schema_mode="merge",
        storage_options=opts,
    )
    return rejects_uri


def get_latest_bronze_file(
    source: str,
    storage_client: Optional[StorageClient] = None,
) -> Tuple[Optional[str], Optional[bytes], Optional[Dict[str, Any]]]:
    """Find and retrieve the most recent raw Bronze file and its sidecar metadata.

    Args:
        source: Name of the data source (e.g., 'brent', 'fx', 'hdx_food', 'worldbank').
        storage_client: StorageClient instance. Defaults to singleton.

    Returns:
        Tuple of (s3_uri, raw_bytes, metadata_dict), or (None, None, None) if not found.
    """
    storage = storage_client or get_storage_client()
    files = storage.list_raw(source=source)
    if not files:
        logger.warning("No raw bronze files found for source: %s", source)
        return None, None, None

    # Sort lexicographically by key path which starts with {source}/{YYYY-MM-DD}/
    sorted_files = sorted(files, reverse=True)
    latest_uri = sorted_files[0]

    # Parse key: s3://{bucket}/{source}/{date_str}/{filename}
    clean_path = latest_uri.replace(f"s3://{storage.bucket}/", "")
    parts = clean_path.split("/", 2)
    if len(parts) < 3:
        logger.error("Unexpected S3 key format for %s: %s", source, latest_uri)
        return None, None, None

    _, date_str, filename = parts
    raw_bytes, sidecar = storage.get_raw(source=source, date_str=date_str, filename=filename)
    return latest_uri, raw_bytes, sidecar
