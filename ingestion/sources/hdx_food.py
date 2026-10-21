"""WFP Food Prices for Pakistan ingestion module from HDX (Humanitarian Data Exchange).

Discovers the current CSV resource URL dynamically via the HDX CKAN API
instead of using a hardcoded URL.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from ingestion.core.http_client import HttpClient, get_http_client
from ingestion.core.storage import StorageClient, get_storage_client

logger = logging.getLogger("pakfuelwatch.sources.hdx_food")

HDX_PACKAGE_SHOW_URL = "https://data.humdata.org/api/3/action/package_show"
HDX_DATASET_ID = "wfp-food-prices-for-pakistan"


def discover_hdx_csv_resource(
    dataset_id: str = HDX_DATASET_ID,
    http_client: Optional[HttpClient] = None,
) -> Dict[str, Any]:
    """Query the HDX API to find the active CSV resource metadata and download URL.

    Args:
        dataset_id: The HDX dataset identifier.
        http_client: Optional HTTP client instance.

    Returns:
        Dictionary containing resource metadata (download_url, name, id, format, etc.).

    Raises:
        RuntimeError: If package metadata fails to fetch or no CSV resource is discovered.
    """
    client = http_client or get_http_client()
    params = {"id": dataset_id}

    logger.info("Discovering resources for HDX dataset: %s", dataset_id)
    response = client.get(HDX_PACKAGE_SHOW_URL, params=params)

    data = response.json()
    if not data.get("success"):
        raise RuntimeError(f"HDX package_show returned error for dataset {dataset_id}: {data}")

    resources = data.get("result", {}).get("resources", [])
    csv_resource: Optional[Dict[str, Any]] = None

    for res in resources:
        res_format = res.get("format", "").upper()
        res_url = (res.get("download_url") or res.get("url") or "").lower()
        res_name = res.get("name", "").lower()

        # Skip QuickCharts and metadata summary resources
        if "quickchart" in res_name or "quickchart" in res_url:
            continue

        # Match primary CSV data file
        if res_format == "CSV" or res_url.endswith(".csv"):
            csv_resource = res
            if "pakistan" in res_name or "wfp" in res_name:
                break

    if not csv_resource:
        raise RuntimeError(f"No CSV resource found in HDX dataset {dataset_id}")

    download_url = csv_resource.get("download_url") or csv_resource.get("url")
    logger.info("Discovered HDX CSV resource URL: %s", download_url)

    return {
        "download_url": download_url,
        "resource_id": csv_resource.get("id"),
        "resource_name": csv_resource.get("name"),
        "last_modified": csv_resource.get("last_modified"),
        "package_id": dataset_id,
    }


def fetch_hdx_food_prices(
    dataset_id: str = HDX_DATASET_ID,
    is_backfill: bool = False,
    http_client: Optional[HttpClient] = None,
    storage_client: Optional[StorageClient] = None,
) -> Tuple[str, bool]:
    """Fetch raw WFP Food Prices CSV from HDX and store directly in Bronze layer.

    Args:
        dataset_id: HDX dataset identifier.
        is_backfill: Whether this execution is flagged as a backfill.
        http_client: Optional HTTP client instance.
        storage_client: Optional storage client instance.

    Returns:
        Tuple of (s3_uri, was_written).
    """
    client = http_client or get_http_client()
    storage = storage_client or get_storage_client()
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Step 1: Dynamically discover the current CSV URL from HDX API
    resource_info = discover_hdx_csv_resource(dataset_id=dataset_id, http_client=client)
    download_url = resource_info["download_url"]

    # Step 2: Download raw CSV payload
    logger.info("Downloading raw WFP food prices CSV from: %s", download_url)
    csv_response = client.get(download_url)

    # Step 3: Write raw CSV bytes directly to Bronze MinIO storage (no parsing)
    mode = "backfill" if is_backfill else "weekly"
    metadata: Dict[str, Any] = {
        "source_url": download_url,
        "dataset_id": dataset_id,
        "resource_id": resource_info.get("resource_id"),
        "resource_name": resource_info.get("resource_name"),
        "http_status": csv_response.status_code,
        "content_type": csv_response.headers.get("content-type", "text/csv"),
        "mode": mode,
        "hdx_last_modified": resource_info.get("last_modified"),
    }

    filename = (
        f"wfp_food_prices_pakistan_backfill_{today_str}.csv"
        if is_backfill
        else f"wfp_food_prices_pakistan_{today_str}.csv"
    )

    return storage.put_raw(
        source="hdx_food",
        filename=filename,
        content=csv_response.content,
        metadata=metadata,
        fetch_date=today_str,
    )
