"""Tests for S3 / MinIO storage client using moto."""

from datetime import date

from moto import mock_aws

from ingestion.core.storage import StorageClient


@mock_aws
def test_storage_put_get_and_list() -> None:
    """Test raw storage put, get, and list operations."""
    storage = StorageClient(
        endpoint_url="https://s3.amazonaws.com",
        access_key="test-key",
        secret_key="test-secret",
        bucket="bronze",
    )

    test_content = b"sample raw fuel price content"
    source = "psopk"
    filename = "prices.html"
    today = date(2024, 1, 15)

    metadata = {
        "source_url": "https://psopk.com/prices",
        "http_status": 200,
        "content_type": "text/html",
        "extra_info": "official",
    }

    # 1. Put raw file
    uri, written = storage.put_raw(
        source=source,
        filename=filename,
        content=test_content,
        metadata=metadata,
        fetch_date=today,
    )

    assert written is True
    assert uri == "s3://bronze/psopk/2024-01-15/prices.html"

    # 2. Get raw file and verify sidecar metadata
    retrieved_content, sidecar = storage.get_raw(
        source=source, date_str="2024-01-15", filename=filename
    )
    assert retrieved_content == test_content
    assert sidecar["source"] == "psopk"
    assert sidecar["filename"] == "prices.html"
    assert sidecar["source_url"] == "https://psopk.com/prices"
    assert sidecar["http_status"] == 200
    assert sidecar["content_type"] == "text/html"
    assert sidecar["extra_info"] == "official"
    assert "sha256" in sidecar
    assert "fetched_at" in sidecar

    # 3. List raw files
    files = storage.list_raw(source=source, date_str="2024-01-15")
    assert len(files) == 1
    assert files[0] == "s3://bronze/psopk/2024-01-15/prices.html"


@mock_aws
def test_storage_sha256_deduplication() -> None:
    """Test that writing identical content for the same source and date skips write."""
    storage = StorageClient(
        endpoint_url="https://s3.amazonaws.com",
        access_key="test-key",
        secret_key="test-secret",
        bucket="bronze",
    )

    content = b"identical content payload"
    source = "eia"
    filename = "brent_feed.json"
    today = "2024-01-16"

    # First write
    uri1, written1 = storage.put_raw(
        source=source,
        filename=filename,
        content=content,
        fetch_date=today,
    )
    assert written1 is True

    # Second write with identical content: should be skipped
    uri2, written2 = storage.put_raw(
        source=source,
        filename=filename,
        content=content,
        fetch_date=today,
    )
    assert written2 is False
    assert uri1 == uri2

    # Third write with modified content: should write
    uri3, written3 = storage.put_raw(
        source=source,
        filename="brent_feed_v2.json",
        content=b"modified content payload",
        fetch_date=today,
    )
    assert written3 is True
