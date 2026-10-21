"""Polite and resilient HTTP client with timeouts, exponential backoff, and logging."""

import logging
import threading
import time
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import requests
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger("pakfuelwatch.http")

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class RetryableHTTPError(requests.exceptions.HTTPError):
    """Exception raised for HTTP responses that qualify for tenacity retry (5xx, 429)."""

    def __init__(self, message: str, response: requests.Response) -> None:
        super().__init__(message, response=response)
        self.status_code = response.status_code


def _log_retry_attempt(retry_state: RetryCallState) -> None:
    """Log retry attempts triggered by tenacity."""
    exception = retry_state.outcome.exception() if retry_state.outcome else None
    attempt = retry_state.attempt_number
    next_action = retry_state.next_action
    sleep_seconds = next_action.sleep if next_action else 0.0
    logger.warning(
        "Request failed on attempt %d (%s). Retrying in %.2fs...",
        attempt,
        exception,
        sleep_seconds,
    )


class HttpClient:
    """HTTP client with per-host polite delays, logging, and exponential backoff retries."""

    def __init__(
        self,
        default_timeout: float = 30.0,
        min_delay_seconds: float = 2.0,
        max_retries: int = 3,
        user_agent: str = "PakFuelWatch/1.0 (+https://github.com/redfeet123/PakFuelWatch)",
    ) -> None:
        """Initialize HTTP client.

        Args:
            default_timeout: Default timeout in seconds for connect and read.
            min_delay_seconds: Minimum seconds between requests to the same hostname (default 2s).
            max_retries: Maximum retry attempts for retryable errors (5xx, 429, timeout, net).
            user_agent: Custom User-Agent header string.
        """
        self.default_timeout = default_timeout
        self.min_delay_seconds = min_delay_seconds
        self.max_retries = max_retries

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})

        self._host_last_request: Dict[str, float] = {}
        self._lock = threading.Lock()

    def _respect_polite_delay(self, url: str) -> None:
        """Enforce polite rate limit by sleeping if min_delay_seconds has not elapsed for host."""
        host = urlparse(url).netloc.lower()
        if not host:
            return

        with self._lock:
            last_time = self._host_last_request.get(host)
            now = time.monotonic()
            if last_time is not None:
                elapsed = now - last_time
                if elapsed < self.min_delay_seconds:
                    wait_time = self.min_delay_seconds - elapsed
                    logger.debug(
                        "Polite delay: sleeping %.2fs before contacting %s", wait_time, host
                    )
                    time.sleep(wait_time)
            self._host_last_request[host] = time.monotonic()

    def request(
        self,
        method: str,
        url: str,
        timeout: Optional[float] = None,
        **kwargs: Any,
    ) -> requests.Response:
        """Execute HTTP request with tenacity retry and polite delay.

        Args:
            method: HTTP verb ('GET', 'POST', etc.).
            url: Target URL.
            timeout: Optional override for request timeout in seconds.
            **kwargs: Extra parameters passed to requests.Session.request.

        Returns:
            requests.Response object.

        Raises:
            requests.exceptions.HTTPError: On non-retryable 4xx client errors or exhausted retries.
            requests.exceptions.RequestException: On non-retryable network errors.
        """
        req_timeout = timeout if timeout is not None else self.default_timeout

        @retry(
            retry=retry_if_exception_type(
                (
                    requests.exceptions.Timeout,
                    requests.exceptions.ConnectionError,
                    RetryableHTTPError,
                )
            ),
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=1, min=2, max=30),
            before_sleep=_log_retry_attempt,
            reraise=True,
        )
        def _execute_with_retry() -> requests.Response:
            self._respect_polite_delay(url)
            start_time = time.perf_counter()

            try:
                response = self.session.request(
                    method=method.upper(),
                    url=url,
                    timeout=req_timeout,
                    **kwargs,
                )
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as net_err:
                elapsed_ms = (time.perf_counter() - start_time) * 1000
                logger.warning(
                    "HTTP %s %s failed network call after %.1fms: %s",
                    method.upper(),
                    url,
                    elapsed_ms,
                    net_err,
                )
                raise

            elapsed_ms = (time.perf_counter() - start_time) * 1000
            content_length = len(response.content) if response.content else 0

            logger.info(
                "HTTP %s %s -> %s (%d bytes in %.1fms)",
                method.upper(),
                url,
                response.status_code,
                content_length,
                elapsed_ms,
            )

            if response.status_code in RETRYABLE_STATUS_CODES:
                raise RetryableHTTPError(
                    f"Retryable HTTP {response.status_code} for {url}",
                    response=response,
                )

            # Raise for client errors (e.g., 400, 401, 403, 404) without retrying
            response.raise_for_status()
            return response

        return _execute_with_retry()

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        """Execute HTTP GET request."""
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> requests.Response:
        """Execute HTTP POST request."""
        return self.request("POST", url, **kwargs)


# Module-level singleton
_default_http_client: Optional[HttpClient] = None


def get_http_client() -> HttpClient:
    """Return singleton HttpClient instance."""
    global _default_http_client
    if _default_http_client is None:
        _default_http_client = HttpClient()
    return _default_http_client
