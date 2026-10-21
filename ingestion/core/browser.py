"""Browser automation and scraping foundation using Patchright."""

import logging
import time
import urllib.robotparser
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Generator, Optional, Tuple
from urllib.parse import urlparse

import requests
import yaml

from ingestion.config import get_settings
from ingestion.core.storage import StorageClient, get_storage_client

logger = logging.getLogger("pakfuelwatch.core.browser")

# Global in-memory cache for robots.txt parsers by scheme+netloc
_ROBOTS_CACHE: Dict[str, urllib.robotparser.RobotFileParser] = {}
# Global in-memory tracker of last request timestamp by hostname
_LAST_REQUEST_TIME: Dict[str, float] = {}

DEFAULT_SELECTORS_PATH = Path(__file__).parent.parent / "selectors.yaml"


@lru_cache(maxsize=1)
def load_selectors(yaml_path: Optional[str] = None) -> Dict[str, Any]:
    """Load and cache external site selectors from the YAML configuration file.

    Args:
        yaml_path: Optional path to selectors.yaml. Defaults to ingestion/selectors.yaml.

    Returns:
        Dictionary representing the parsed selectors configuration.
    """
    path = Path(yaml_path) if yaml_path else DEFAULT_SELECTORS_PATH
    if not path.exists():
        logger.warning("Selectors YAML file not found at: %s", path)
        return {}

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    logger.debug("Loaded selectors from: %s", path)
    return data


def get_selector(
    source: str,
    section: str,
    key: str,
    default: Optional[str] = None,
    yaml_path: Optional[str] = None,
) -> str:
    """Retrieve a selector string from the centralized selectors YAML file.

    Args:
        source: Top-level source name (e.g. 'pbs').
        section: Section name (e.g. 'spi' or 'cpi').
        key: Selector field name (e.g. 'release_links').
        default: Fallback selector string if not found.
        yaml_path: Optional path to override selectors YAML.

    Returns:
        CSS or XPath selector string.
    """
    selectors = load_selectors(yaml_path)
    val = selectors.get(source, {}).get(section, {}).get(key)
    if val is not None:
        return str(val)
    if default is not None:
        return default
    raise KeyError(f"Selector not found for {source}.{section}.{key} in selectors.yaml")


def is_url_allowed_by_robots(url: str, user_agent: str = "*", timeout: float = 10.0) -> bool:
    """Verify whether a target URL is permitted according to domain's robots.txt.

    Args:
        url: Destination URL to test.
        user_agent: User-Agent name to check directives for. Defaults to '*'.
        timeout: HTTP request timeout when fetching robots.txt.

    Returns:
        True if URL is allowed or robots.txt does not exist/fails to load, False if disallowed.
    """
    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}"

    if origin not in _ROBOTS_CACHE:
        robots_url = f"{origin}/robots.txt"
        rp = urllib.robotparser.RobotFileParser()
        try:
            resp = requests.get(robots_url, timeout=timeout)
            if resp.status_code == 200:
                rp.parse(resp.text.splitlines())
            elif resp.status_code in (404, 410):
                # Standard convention: missing robots.txt implies no restrictions
                rp.allow_all = True
            else:
                rp.allow_all = True
        except Exception as exc:
            logger.debug(
                "Could not retrieve robots.txt from %s: %s (defaulting to allow)",
                origin,
                exc,
            )
            rp.allow_all = True

        _ROBOTS_CACHE[origin] = rp

    rp = _ROBOTS_CACHE[origin]
    allowed = rp.can_fetch(user_agent, url)
    if not allowed:
        logger.warning("URL disallowed by robots.txt: %s", url)
    return allowed


@contextmanager
def launch_browser_context(
    user_data_dir: Optional[str] = None,
    headless: Optional[bool] = None,
    channel: Optional[str] = None,
) -> Generator[Any, None, None]:
    """Launch a Patchright persistent browser context.

    Uses Chromium with launch_persistent_context, no_viewport=True,
    and NO custom user agent or extra HTTP headers to preserve undetected fingerprinting.

    Args:
        user_data_dir: Persistent browser profile directory. Defaults to setting.
        headless: Headless browser mode boolean. Defaults to setting.
        channel: Browser channel (e.g. 'chrome' for local Chrome, None for bundled Chromium).

    Yields:
        Patchright BrowserContext instance.
    """
    # Lazy import of patchright so non-browser workflows don't fail if not installed
    from patchright.sync_api import sync_playwright

    settings = get_settings()
    data_dir = user_data_dir or settings.browser_user_data_dir
    is_headless = headless if headless is not None else settings.browser_headless
    ch = channel if channel is not None else settings.browser_channel

    Path(data_dir).mkdir(parents=True, exist_ok=True)

    launch_args: Dict[str, Any] = {
        "user_data_dir": data_dir,
        "headless": is_headless,
        "no_viewport": True,
    }
    if ch:
        launch_args["channel"] = ch

    logger.info(
        "Launching Patchright persistent context (headless=%s, channel=%s, dir=%s)",
        is_headless,
        ch or "bundled-chromium",
        data_dir,
    )

    with sync_playwright() as playwright:
        context = playwright.chromium.launch_persistent_context(**launch_args)
        try:
            yield context
        finally:
            logger.info("Closing Patchright persistent context.")
            context.close()


def polite_goto(
    page: Any,
    url: str,
    min_delay: Optional[float] = None,
    timeout_seconds: Optional[float] = None,
    max_retries: Optional[int] = None,
    check_robots: bool = True,
) -> None:
    """Navigate to a URL politely, enforcing rate limits, robots.txt, and retries.

    Args:
        page: Patchright Page instance.
        url: Target destination URL.
        min_delay: Minimum delay in seconds since last request to the same host.
        timeout_seconds: Navigation timeout in seconds.
        max_retries: Maximum navigation retries on failure.
        check_robots: Whether to check robots.txt permission before navigation.

    Raises:
        PermissionError: If URL is disallowed by robots.txt.
        RuntimeError: If navigation fails after all retries.
    """
    settings = get_settings()
    effective_delay = (
        min_delay if min_delay is not None else settings.browser_min_delay_seconds
    )
    effective_timeout = (
        timeout_seconds if timeout_seconds is not None else settings.browser_timeout_seconds
    )
    effective_retries = (
        max_retries if max_retries is not None else settings.browser_max_retries
    )

    parsed = urlparse(url)
    host = parsed.netloc

    # 1. Robots.txt check
    if check_robots and not is_url_allowed_by_robots(url):
        raise PermissionError(f"Scraping disallowed by robots.txt for: {url}")

    # 2. Host rate limiting (minimum polite delay)
    now = time.monotonic()
    last_time = _LAST_REQUEST_TIME.get(host)
    if last_time is not None:
        elapsed = now - last_time
        if elapsed < effective_delay:
            sleep_needed = effective_delay - elapsed
            logger.debug("Rate-limiting %s: sleeping %.2fs", host, sleep_needed)
            time.sleep(sleep_needed)

    _LAST_REQUEST_TIME[host] = time.monotonic()

    # 3. Navigation with retries and exponential backoff
    timeout_ms = int(effective_timeout * 1000)
    attempt = 0
    backoff = 2.0

    while attempt < effective_retries:
        attempt += 1
        try:
            logger.info("Navigating to %s (attempt %d/%d)", url, attempt, effective_retries)
            page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")
            return
        except Exception as exc:
            logger.warning("Navigation failed on attempt %d for %s: %s", attempt, url, exc)
            if attempt >= effective_retries:
                raise RuntimeError(
                    f"polite_goto failed after {effective_retries} attempts for {url}: {exc}"
                ) from exc
            time.sleep(backoff)
            backoff *= 2.0


def save_page(
    source: str,
    page: Any,
    storage_client: Optional[StorageClient] = None,
    filename_prefix: Optional[str] = None,
    fetch_date: Optional[str] = None,
) -> Tuple[str, str]:
    """Capture full rendered HTML and screenshot of the current page and store to Bronze.

    Args:
        source: Name of the data source (e.g. 'pbs_spi', 'pbs_cpi').
        page: Patchright Page instance.
        storage_client: StorageClient instance.
        filename_prefix: Optional base filename prefix. Defaults to slug of page URL.
        fetch_date: Optional date string 'YYYY-MM-DD'. Defaults to current UTC date.

    Returns:
        Tuple of (html_s3_uri, screenshot_s3_uri).
    """
    storage = storage_client or get_storage_client()
    today_str = fetch_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    current_url = page.url

    # Construct file slug
    if filename_prefix:
        prefix = filename_prefix
    else:
        path_part = urlparse(current_url).path.strip("/").replace("/", "_") or "index"
        prefix = f"{source}_{path_part}_{today_str}"

    html_content = page.content().encode("utf-8")
    screenshot_bytes = page.screenshot(full_page=True)

    # 1. Save HTML
    html_meta = {
        "source_url": current_url,
        "content_type": "text/html; charset=utf-8",
        "kind": "rendered_html",
        "page_title": page.title(),
    }
    html_uri, _ = storage.put_raw(
        source=source,
        filename=f"{prefix}.html",
        content=html_content,
        metadata=html_meta,
        fetch_date=today_str,
    )

    # 2. Save Screenshot
    png_meta = {
        "source_url": current_url,
        "content_type": "image/png",
        "kind": "full_page_screenshot",
        "page_title": page.title(),
    }
    png_uri, _ = storage.put_raw(
        source=source,
        filename=f"{prefix}.png",
        content=screenshot_bytes,
        metadata=png_meta,
        fetch_date=today_str,
    )

    logger.info("Saved page HTML to %s and screenshot to %s", html_uri, png_uri)
    return html_uri, png_uri


def download_file(
    page: Any,
    selector_or_url: str,
    source: str,
    storage_client: Optional[StorageClient] = None,
    filename: Optional[str] = None,
    fetch_date: Optional[str] = None,
) -> Tuple[str, bool]:
    """Download a file (PDF, Excel, etc.) and save raw bytes directly to Bronze.

    Supports either a direct download URL or a clickable selector triggering a browser download.

    Args:
        page: Patchright Page instance.
        selector_or_url: Absolute URL or CSS selector of download trigger.
        source: Name of the data source (e.g. 'pbs_spi', 'pbs_cpi').
        storage_client: StorageClient instance.
        filename: Optional filename override.
        fetch_date: Optional partition date string.

    Returns:
        Tuple of (s3_uri, was_written).
    """
    storage = storage_client or get_storage_client()
    today_str = fetch_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Path A: Direct URL
    if selector_or_url.startswith("http://") or selector_or_url.startswith("https://"):
        url = selector_or_url
        logger.info("Downloading file from URL: %s", url)
        resp = requests.get(url, timeout=30.0)
        resp.raise_for_status()
        file_bytes = resp.content
        resolved_name = (
            filename or urlparse(url).path.split("/")[-1] or f"download_{today_str}.bin"
        )
        content_type = resp.headers.get("content-type", "application/octet-stream")
        meta = {
            "source_url": url,
            "content_type": content_type,
            "http_status": resp.status_code,
        }
    # Path B: Clickable selector trigger
    else:
        logger.info("Triggering browser download via selector: %s", selector_or_url)
        with page.expect_download() as download_info:
            page.locator(selector_or_url).first.click()
        download = download_info.value
        download_path = download.path()
        with open(download_path, "rb") as f:
            file_bytes = f.read()
        resolved_name = filename or download.suggested_filename
        meta = {
            "source_url": page.url,
            "trigger_selector": selector_or_url,
            "suggested_filename": download.suggested_filename,
        }

    return storage.put_raw(
        source=source,
        filename=resolved_name,
        content=file_bytes,
        metadata=meta,
        fetch_date=today_str,
    )
