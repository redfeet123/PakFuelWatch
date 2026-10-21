"""Tests for polite, resilient HttpClient with retries."""

import time

import pytest
import requests
import responses

from ingestion.core.http_client import HttpClient


@responses.activate
def test_http_client_successful_get() -> None:
    """Test successful GET request through HttpClient."""
    url = "https://api.example.com/data"
    responses.add(responses.GET, url, json={"status": "ok"}, status=200)

    client = HttpClient(min_delay_seconds=0.01)
    response = client.get(url)

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@responses.activate
def test_http_client_retries_on_500_and_succeeds() -> None:
    """Test that 500 triggers retry and succeeds on subsequent attempt."""
    url = "https://api.example.com/unstable"
    responses.add(responses.GET, url, body="Server Error", status=500)
    responses.add(responses.GET, url, json={"recovered": True}, status=200)

    # Use small delay and retries for fast test execution
    client = HttpClient(min_delay_seconds=0.01, max_retries=3)
    response = client.get(url)

    assert response.status_code == 200
    assert response.json() == {"recovered": True}
    assert len(responses.calls) == 2


@responses.activate
def test_http_client_retries_on_429() -> None:
    """Test that 429 Too Many Requests triggers retry."""
    url = "https://api.example.com/rate_limited"
    responses.add(responses.GET, url, body="Too Many Requests", status=429)
    responses.add(responses.GET, url, json={"data": 123}, status=200)

    client = HttpClient(min_delay_seconds=0.01, max_retries=3)
    response = client.get(url)

    assert response.status_code == 200
    assert len(responses.calls) == 2


@responses.activate
def test_http_client_no_retry_on_404() -> None:
    """Test that non-retryable 4xx client errors fail immediately without retry."""
    url = "https://api.example.com/not_found"
    responses.add(responses.GET, url, body="Not Found", status=404)

    client = HttpClient(min_delay_seconds=0.01, max_retries=3)
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        client.get(url)

    assert exc_info.value.response.status_code == 404
    # Ensure it only called the endpoint once (no retries)
    assert len(responses.calls) == 1


@responses.activate
def test_http_client_polite_delay() -> None:
    """Test that min_delay_seconds enforces rate limit between requests to the same host."""
    url = "https://api.rate-limit.com/resource"
    responses.add(responses.GET, url, body="ok", status=200)
    responses.add(responses.GET, url, body="ok", status=200)

    min_delay = 0.25
    client = HttpClient(min_delay_seconds=min_delay)

    start = time.monotonic()
    client.get(url)
    client.get(url)
    elapsed = time.monotonic() - start

    assert elapsed >= min_delay
