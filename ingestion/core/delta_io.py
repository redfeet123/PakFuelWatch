"""Delta Lake I/O helpers supporting idempotent upserts/merges and reads."""

import logging
from typing import Dict, List, Optional, Union

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from ingestion.config import get_settings

logger = logging.getLogger("pakfuelwatch.delta")


def get_default_storage_options() -> Dict[str, str]:
    """Retrieve S3 / MinIO storage options for delta-rs from application settings.

    Returns:
        Dictionary of AWS / MinIO storage configuration options.
    """
    settings = get_settings()
    return {
        "AWS_ENDPOINT_URL": settings.minio_endpoint,
        "AWS_ACCESS_KEY_ID": settings.minio_access_key,
        "AWS_SECRET_ACCESS_KEY": settings.minio_secret_key,
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def write_delta_merge(
    df: Union[pd.DataFrame, pa.Table],
    table_path: str,
    merge_keys: List[str],
    storage_options: Optional[Dict[str, str]] = None,
) -> None:
    """Write data to a Delta Lake table with idempotent merge/upsert semantics.

    If the table does not exist at table_path, it will be created.
    If the table exists, it will perform an atomic upsert matching on merge_keys.

    Args:
        df: Pandas DataFrame or PyArrow Table containing incoming records.
        table_path: Target Delta table path (local filesystem path or s3:// URI).
        merge_keys: List of natural primary key column names used to match records.
        storage_options: Optional custom S3/MinIO options. Automatically populated for s3:// URIs.

    Raises:
        ValueError: If merge_keys is empty or missing from incoming data.
    """
    if not merge_keys:
        raise ValueError("merge_keys must contain at least one column name for merge operation.")

    row_count = len(df)
    opts = storage_options
    if opts is None and table_path.startswith("s3://"):
        opts = get_default_storage_options()

    table_exists = False
    try:
        table_exists = DeltaTable.is_deltatable(table_path, storage_options=opts)
    except Exception as exc:
        logger.debug("DeltaTable.is_deltatable checked %s with result: %s", table_path, exc)
        table_exists = False

    if not table_exists:
        logger.info("Initializing new Delta table at %s with %d rows", table_path, row_count)
        write_deltalake(
            table_path,
            df,
            mode="error",
            storage_options=opts,
        )
        return

    # Table exists; execute idempotent merge
    dt = DeltaTable(table_path, storage_options=opts)
    predicate = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])

    logger.info(
        "Merging %d records into Delta table %s on predicate: %s",
        row_count,
        table_path,
        predicate,
    )

    (
        dt.merge(
            source=df,
            predicate=predicate,
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )


def read_delta(
    table_path: str,
    storage_options: Optional[Dict[str, str]] = None,
    columns: Optional[List[str]] = None,
) -> pa.Table:
    """Read a Delta table and return as a PyArrow Table.

    Args:
        table_path: Target Delta table path (local filesystem path or s3:// URI).
        storage_options: Optional custom S3/MinIO options. Automatically populated for s3:// URIs.
        columns: Optional list of column names to project.

    Returns:
        PyArrow Table containing the Delta table data.
    """
    opts = storage_options
    if opts is None and table_path.startswith("s3://"):
        opts = get_default_storage_options()

    dt = DeltaTable(table_path, storage_options=opts)
    return dt.to_pyarrow_table(columns=columns)


def read_delta_pandas(
    table_path: str,
    storage_options: Optional[Dict[str, str]] = None,
    columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Read a Delta table and return as a Pandas DataFrame.

    Args:
        table_path: Target Delta table path.
        storage_options: Optional storage options.
        columns: Optional column projection.

    Returns:
        Pandas DataFrame containing table rows.
    """
    opts = storage_options
    if opts is None and table_path.startswith("s3://"):
        opts = get_default_storage_options()

    dt = DeltaTable(table_path, storage_options=opts)
    return dt.to_pandas(columns=columns)
