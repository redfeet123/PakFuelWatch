"""USD/PKR foreign exchange rate daily ingestion module.

Primary source: State Bank of Pakistan (SBP) EasyData API.
Fallback / Alternative: Frankfurter Open FX API (free, open-access ECB rates).
Provider is configurable via application settings (FX_PROVIDER) or function parameters.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from ingestion.config import get_settings
from ingestion.core.http_client import HttpClient, get_http_client
from ingestion.core.storage import StorageClient, get_storage_client

logger = logging.getLogger("pakfuelwatch.sources.fx")

SBP_EASYDATA_URL = "https://easydata.sbp.org.pk/api/v1/forex/usd-pkr"
FRANKFURTER_BASE_URL = "https://api.frankfurter.dev/v1"


def _fetch_from_sbp(
    start_date: Optional[str],
    end_date: Optional[str],
    api_key: Optional[str],
    client: HttpClient,
) -> Tuple[bytes, str, Dict[str, Any]]:
    """Fetch USD/PKR exchange rates from SBP EasyData API."""
    params: Dict[str, Any] = {"pair": "USD/PKR"}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    headers: Dict[str, str] = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    logger.info("Requesting USD/PKR rates from SBP EasyData API")
    response = client.get(SBP_EASYDATA_URL, params=params, headers=headers)
    metadata = {
        "source_url": response.url,
        "http_status": response.status_code,
        "content_type": response.headers.get("content-type", "application/json"),
        "provider": "sbp",
        "pair": "USD/PKR",
    }
    return response.content, response.url, metadata


def _fetch_from_frankfurter(
    start_date: Optional[str],
    end_date: Optional[str],
    client: HttpClient,
) -> Tuple[bytes, str, Dict[str, Any]]:
    """Fetch USD/PKR exchange rates from Frankfurter free FX API."""
    if start_date and end_date:
        url = f"{FRANKFURTER_BASE_URL}/{start_date}..{end_date}"
    elif start_date:
        url = f"{FRANKFURTER_BASE_URL}/{start_date}.."
    else:
        url = f"{FRANKFURTER_BASE_URL}/latest"

    params = {"from": "USD", "to": "PKR"}

    logger.info("Requesting USD/PKR rates from Frankfurter FX fallback (%s)", url)
    response = client.get(url, params=params)
    metadata = {
        "source_url": response.url,
        "http_status": response.status_code,
        "content_type": response.headers.get("content-type", "application/json"),
        "provider": "frankfurter",
        "pair": "USD/PKR",
    }
    return response.content, response.url, metadata


def fetch_fx(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    provider: Optional[str] = None,
    is_backfill: bool = False,
    http_client: Optional[HttpClient] = None,
    storage_client: Optional[StorageClient] = None,
) -> Tuple[str, bool]:
    """Fetch raw USD/PKR daily exchange rates and store in Bronze layer.

    Args:
        start_date: Starting observation date (YYYY-MM-DD).
        end_date: Ending observation date (YYYY-MM-DD). Defaults to current UTC date.
        provider: Configurable provider ('sbp' or 'frankfurter'). Defaults to env setting.
        is_backfill: Whether this execution is historical backfill or daily incremental.
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
    selected_provider = (provider or settings.fx_provider).lower()

    raw_bytes: Optional[bytes] = None
    metadata: Dict[str, Any] = {}

    if selected_provider == "sbp":
        try:
            raw_bytes, _, metadata = _fetch_from_sbp(
                start_date=start_date,
                end_date=effective_end,
                api_key=settings.sbp_api_key,
                client=client,
            )
        except Exception as exc:
            logger.warning(
                "SBP EasyData API request failed: %s. Falling back to Frankfurter free FX API...",
                exc,
            )

    # Fallback to Frankfurter if SBP was not selected or failed
    if raw_bytes is None:
        logger.info("Using Frankfurter free open FX API for USD/PKR rates...")
        raw_bytes, _, metadata = _fetch_from_frankfurter(
            start_date=start_date,
            end_date=effective_end,
            client=client,
        )

    mode = "backfill" if is_backfill else "incremental"
    metadata["mode"] = mode
    metadata["start_date"] = start_date
    metadata["end_date"] = effective_end

    filename = (
        f"fx_usd_pkr_backfill_{start_date or 'inception'}_{effective_end}.json"
        if is_backfill
        else f"fx_usd_pkr_incremental_{start_date or effective_end}_{effective_end}.json"
    )

    return storage.put_raw(
        source="fx",
        filename=filename,
        content=raw_bytes,
        metadata=metadata,
        fetch_date=today_str,
    )


def fetch_fx_incremental(
    days: int = 30,
    provider: Optional[str] = None,
    http_client: Optional[HttpClient] = None,
    storage_client: Optional[StorageClient] = None,
) -> Tuple[str, bool]:
    """Fetch recent days of USD/PKR daily exchange rates incrementally."""
    start_dt = datetime.now(timezone.utc) - timedelta(days=days)
    start_date = start_dt.strftime("%Y-%m-%d")
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return fetch_fx(
        start_date=start_date,
        end_date=today_str,
        provider=provider,
        is_backfill=False,
        http_client=http_client,
        storage_client=storage_client,
    )


def fetch_fx_backfill(
    start_date: str = "2000-01-01",
    end_date: Optional[str] = None,
    provider: Optional[str] = None,
    http_client: Optional[HttpClient] = None,
    storage_client: Optional[StorageClient] = None,
) -> Tuple[str, bool]:
    """Fetch complete historical USD/PKR exchange rates."""
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return fetch_fx(
        start_date=start_date,
        end_date=end_date or today_str,
        provider=provider,
        is_backfill=True,
        http_client=http_client,
        storage_client=storage_client,
    )
