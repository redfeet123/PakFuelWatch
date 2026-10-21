"""World Bank API indicators ingestion module for Pakistan.

Fetches key economic indicators:
1. FP.CPI.TOTL.ZG: Inflation, consumer prices (annual %)
2. TM.VAL.FUEL.ZS.UN: Fuel imports (% of merchandise imports)
3. FP.CPI.TOTL: Consumer price index (2010 = 100)
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ingestion.core.http_client import HttpClient, get_http_client
from ingestion.core.storage import StorageClient, get_storage_client

logger = logging.getLogger("pakfuelwatch.sources.worldbank")

WORLDBANK_BASE_URL = "https://api.worldbank.org/v2/country/PAK/indicator"

DEFAULT_INDICATORS = {
    "cpi_inflation": "FP.CPI.TOTL.ZG",
    "fuel_imports_pct": "TM.VAL.FUEL.ZS.UN",
    "cpi_index": "FP.CPI.TOTL",
}


def fetch_worldbank_indicator(
    indicator_id: str,
    indicator_alias: str,
    date_range: Optional[str] = None,
    is_backfill: bool = False,
    http_client: Optional[HttpClient] = None,
    storage_client: Optional[StorageClient] = None,
) -> Tuple[str, bool]:
    """Fetch a single indicator series for Pakistan from World Bank API and write to Bronze.

    Args:
        indicator_id: Official World Bank indicator code (e.g. 'FP.CPI.TOTL.ZG').
        indicator_alias: Short descriptive name used in filename (e.g. 'cpi_inflation').
        date_range: Date filter formatted as 'YYYY:YYYY' (e.g. '2000:2025'). Defaults to all years.
        is_backfill: Whether this execution represents a full historical backfill.
        http_client: Optional HTTP client instance.
        storage_client: Optional storage client instance.

    Returns:
        Tuple of (s3_uri, was_written).
    """
    client = http_client or get_http_client()
    storage = storage_client or get_storage_client()
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    url = f"{WORLDBANK_BASE_URL}/{indicator_id}"
    params: Dict[str, Any] = {
        "format": "json",
        "per_page": 1000,
    }
    if date_range:
        params["date"] = date_range

    logger.info("Requesting World Bank indicator %s (%s)", indicator_id, indicator_alias)
    response = client.get(url, params=params)

    mode = "backfill" if is_backfill else "monthly"
    metadata = {
        "source_url": response.url,
        "http_status": response.status_code,
        "content_type": response.headers.get("content-type", "application/json"),
        "indicator_id": indicator_id,
        "indicator_alias": indicator_alias,
        "date_range": date_range or "all",
        "mode": mode,
    }

    clean_alias = indicator_alias.lower().replace(".", "_")
    filename = (
        f"worldbank_{clean_alias}_backfill.json"
        if is_backfill
        else f"worldbank_{clean_alias}_{today_str}.json"
    )

    return storage.put_raw(
        source="worldbank",
        filename=filename,
        content=response.content,
        metadata=metadata,
        fetch_date=today_str,
    )


def fetch_worldbank_all(
    date_range: Optional[str] = None,
    is_backfill: bool = False,
    indicators: Optional[Dict[str, str]] = None,
    http_client: Optional[HttpClient] = None,
    storage_client: Optional[StorageClient] = None,
) -> List[Tuple[str, bool]]:
    """Fetch all configured World Bank indicators for Pakistan and store to Bronze.

    Args:
        date_range: Optional date range filter ('YYYY:YYYY').
        is_backfill: Whether this is a backfill or scheduled ingestion.
        indicators: Optional dictionary mapping alias -> indicator_id. Defaults to standard set.
        http_client: Optional HTTP client instance.
        storage_client: Optional storage client instance.

    Returns:
        List of (s3_uri, was_written) tuples for each indicator ingested.
    """
    target_indicators = indicators or DEFAULT_INDICATORS
    results: List[Tuple[str, bool]] = []

    for alias, indicator_id in target_indicators.items():
        try:
            res = fetch_worldbank_indicator(
                indicator_id=indicator_id,
                indicator_alias=alias,
                date_range=date_range,
                is_backfill=is_backfill,
                http_client=http_client,
                storage_client=storage_client,
            )
            results.append(res)
        except Exception as exc:
            logger.error(
                "Failed to ingest World Bank indicator %s (%s): %s",
                indicator_id,
                alias,
                exc,
            )

    return results
