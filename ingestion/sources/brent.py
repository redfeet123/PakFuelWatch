"""Brent crude daily price ingestion module.

Primary source: FRED API (Federal Reserve Economic Data, series DCOILBRENTEU).
Documented fallback: EIA API v2 (US Energy Information Administration, series RBRTE).
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from ingestion.config import get_settings
from ingestion.core.http_client import HttpClient, get_http_client
from ingestion.core.storage import StorageClient, get_storage_client

logger = logging.getLogger("pakfuelwatch.sources.brent")

FRED_SERIES_ID = "DCOILBRENTEU"
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
EIA_BASE_URL = "https://api.eia.gov/v2/petroleum/pri/spt/data/"
EIA_SERIES_ID = "RBRTE"


def _fetch_from_fred(
    start_date: Optional[str],
    end_date: Optional[str],
    api_key: str,
    client: HttpClient,
) -> Tuple[bytes, str, Dict[str, Any]]:
    """Fetch Brent observations from FRED API."""
    params: Dict[str, Any] = {
        "series_id": FRED_SERIES_ID,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "asc",
    }
    if start_date:
        params["observation_start"] = start_date
    if end_date:
        params["observation_end"] = end_date

    logger.info("Requesting Brent crude prices from FRED API (series=%s)", FRED_SERIES_ID)
    response = client.get(FRED_BASE_URL, params=params)
    metadata = {
        "source_url": response.url,
        "http_status": response.status_code,
        "content_type": response.headers.get("content-type", "application/json"),
        "provider": "fred",
        "series_id": FRED_SERIES_ID,
    }
    return response.content, response.url, metadata


def _fetch_from_eia(
    start_date: Optional[str],
    end_date: Optional[str],
    api_key: str,
    client: HttpClient,
) -> Tuple[bytes, str, Dict[str, Any]]:
    """Fetch Brent observations from EIA API v2 as fallback."""
    params: Dict[str, Any] = {
        "api_key": api_key,
        "frequency": "daily",
        "data[0]": "value",
        "facets[series][]": EIA_SERIES_ID,
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
    }
    if start_date:
        params["start"] = start_date
    if end_date:
        params["end"] = end_date

    logger.info("Requesting Brent crude prices from EIA API v2 fallback (series=%s)", EIA_SERIES_ID)
    response = client.get(EIA_BASE_URL, params=params)
    metadata = {
        "source_url": response.url,
        "http_status": response.status_code,
        "content_type": response.headers.get("content-type", "application/json"),
        "provider": "eia",
        "series_id": EIA_SERIES_ID,
    }
    return response.content, response.url, metadata


def fetch_brent(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    is_backfill: bool = False,
    http_client: Optional[HttpClient] = None,
    storage_client: Optional[StorageClient] = None,
) -> Tuple[str, bool]:
    """Fetch raw Brent crude prices and store to Bronze layer.

    Tries FRED API first; falls back to EIA API if FRED key is missing or request fails.

    Args:
        start_date: Starting observation date (YYYY-MM-DD). None implies beginning of series.
        end_date: Ending observation date (YYYY-MM-DD). Defaults to current UTC date.
        is_backfill: True if executing full history backfill; False for incremental.
        http_client: Optional HTTP client instance.
        storage_client: Optional storage client instance.

    Returns:
        Tuple of (s3_uri, was_written).
    """
    settings = get_settings()
    client = http_client or get_http_client()
    storage = storage_client or get_storage_client()
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    effective_end = end_date or today_str

    raw_bytes: Optional[bytes] = None
    metadata: Dict[str, Any] = {}

    # Attempt primary provider: FRED
    if settings.fred_api_key:
        try:
            raw_bytes, _, metadata = _fetch_from_fred(
                start_date=start_date,
                end_date=effective_end,
                api_key=settings.fred_api_key,
                client=client,
            )
        except Exception as exc:
            logger.warning("FRED API fetch failed: %s. Attempting EIA fallback...", exc)

    # Attempt fallback: EIA
    if raw_bytes is None:
        if settings.eia_api_key:
            logger.info("Using documented fallback EIA API for Brent prices...")
            raw_bytes, _, metadata = _fetch_from_eia(
                start_date=start_date,
                end_date=effective_end,
                api_key=settings.eia_api_key,
                client=client,
            )
        else:
            raise RuntimeError(
                "Neither FRED_API_KEY nor EIA_API_KEY is available or succeeded for Brent fetch."
            )

    mode = "backfill" if is_backfill else "incremental"
    metadata["mode"] = mode
    metadata["start_date"] = start_date
    metadata["end_date"] = effective_end

    filename = (
        f"brent_backfill_{start_date or 'inception'}_{effective_end}.json"
        if is_backfill
        else f"brent_incremental_{start_date or effective_end}_{effective_end}.json"
    )

    return storage.put_raw(
        source="brent",
        filename=filename,
        content=raw_bytes,
        metadata=metadata,
        fetch_date=today_str,
    )


def fetch_brent_incremental(
    days: int = 30,
    http_client: Optional[HttpClient] = None,
    storage_client: Optional[StorageClient] = None,
) -> Tuple[str, bool]:
    """Fetch recent days of Brent crude prices incrementally."""
    start_dt = datetime.now(timezone.utc) - timedelta(days=days)
    start_date = start_dt.strftime("%Y-%m-%d")
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return fetch_brent(
        start_date=start_date,
        end_date=today_str,
        is_backfill=False,
        http_client=http_client,
        storage_client=storage_client,
    )


def fetch_brent_backfill(
    start_date: str = "1987-05-20",
    end_date: Optional[str] = None,
    http_client: Optional[HttpClient] = None,
    storage_client: Optional[StorageClient] = None,
) -> Tuple[str, bool]:
    """Fetch complete historical record of Brent crude prices."""
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return fetch_brent(
        start_date=start_date,
        end_date=end_date or today_str,
        is_backfill=True,
        http_client=http_client,
        storage_client=storage_client,
    )
