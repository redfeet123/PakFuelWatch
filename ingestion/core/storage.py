"""S3 / MinIO storage client for Bronze raw data ingestion."""

import hashlib
import json
import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from ingestion.config import get_settings

logger = logging.getLogger("pakfuelwatch.storage")


class StorageClient:
    """MinIO / S3 storage manager for raw bronze data and metadata sidecars."""

    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        bucket: str = "bronze",
    ) -> None:
        """Initialize the S3 / MinIO storage client.

        Args:
            endpoint_url: Custom S3/MinIO endpoint URL. Defaults to env config.
            access_key: S3 access key ID. Defaults to env config.
            secret_key: S3 secret access key. Defaults to env config.
            bucket: Default S3 bucket name for bronze raw data. Defaults to 'bronze'.
        """
        settings = get_settings()
        self.endpoint_url = endpoint_url or settings.minio_endpoint
        self.access_key = access_key or settings.minio_access_key
        self.secret_key = secret_key or settings.minio_secret_key
        self.bucket = bucket

        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name="us-east-1",
        )

    def _ensure_bucket_exists(self) -> None:
        """Ensure the target bucket exists, creating it if necessary."""
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError:
            try:
                self.client.create_bucket(Bucket=self.bucket)
                logger.info("Created bucket: %s", self.bucket)
            except ClientError as exc:
                logger.warning("Could not auto-create bucket %s: %s", self.bucket, exc)

    def put_raw(
        self,
        source: str,
        filename: str,
        content: bytes,
        metadata: Optional[Dict[str, Any]] = None,
        fetch_date: Optional[str | date] = None,
    ) -> Tuple[str, bool]:
        """Write raw bytes and an associated JSON metadata sidecar to Bronze S3 storage.

        Skips write if an identical payload (same sha256) already exists for source and date.

        Args:
            source: Name of the data source (e.g., 'eia', 'sbp', 'pbs').
            filename: Name of the raw file (e.g., 'brent_prices.json', 'cpi_report.pdf').
            content: Raw byte content to store.
            metadata: Metadata dictionary (source_url, http_status, content_type, etc.).
            fetch_date: Date associated with partition. Defaults to current UTC date.

        Returns:
            Tuple of (s3_uri, was_written), where was_written is False if duplicate sha256.
        """
        self._ensure_bucket_exists()

        if isinstance(fetch_date, date):
            date_str = fetch_date.isoformat()
        elif fetch_date:
            date_str = fetch_date
        else:
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        content_sha256 = hashlib.sha256(content).hexdigest()
        file_key = f"{source}/{date_str}/{filename}"
        sidecar_key = f"{source}/{date_str}/{filename}.meta.json"
        s3_uri = f"s3://{self.bucket}/{file_key}"

        # Check if an object with the same sha256 already exists for this source and date
        prefix = f"{source}/{date_str}/"
        try:
            response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            for item in response.get("Contents", []):
                key = item["Key"]
                if key.endswith(".meta.json"):
                    try:
                        sidecar_obj = self.client.get_object(Bucket=self.bucket, Key=key)
                        existing_sidecar = json.loads(sidecar_obj["Body"].read().decode("utf-8"))
                        if existing_sidecar.get("sha256") == content_sha256:
                            logger.info(
                                "Duplicate sha256 %s exists at %s for %s. Skipping write.",
                                content_sha256,
                                key,
                                date_str,
                            )
                            return s3_uri, False
                    except (ClientError, json.JSONDecodeError) as exc:
                        logger.warning(
                            "Error reading sidecar %s for deduplication: %s", key, exc
                        )
        except ClientError as exc:
            logger.warning("Error listing objects under prefix %s: %s", prefix, exc)

        # Prepare sidecar payload
        meta_dict = metadata.copy() if metadata else {}
        sidecar_data: Dict[str, Any] = {
            "source": source,
            "filename": filename,
            "source_url": meta_dict.pop("source_url", ""),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "sha256": content_sha256,
            "http_status": meta_dict.pop("http_status", 200),
            "content_type": meta_dict.pop("content_type", "application/octet-stream"),
        }
        # Merge remaining custom metadata fields
        sidecar_data.update(meta_dict)

        content_type = sidecar_data["content_type"]

        # Write raw data file
        self.client.put_object(
            Bucket=self.bucket,
            Key=file_key,
            Body=content,
            ContentType=content_type,
        )

        # Write JSON sidecar
        self.client.put_object(
            Bucket=self.bucket,
            Key=sidecar_key,
            Body=json.dumps(sidecar_data, indent=2, default=str).encode("utf-8"),
            ContentType="application/json",
        )

        logger.info(
            "Wrote raw file to %s and sidecar to s3://%s/%s (sha256=%s)",
            s3_uri,
            self.bucket,
            sidecar_key,
            content_sha256,
        )
        return s3_uri, True

    def get_raw(self, source: str, date_str: str, filename: str) -> Tuple[bytes, Dict[str, Any]]:
        """Retrieve raw content bytes and metadata sidecar dictionary.

        Args:
            source: Name of the data source.
            date_str: Partition date string (YYYY-MM-DD).
            filename: Name of the file.

        Returns:
            Tuple of (raw_bytes, metadata_dict).

        Raises:
            FileNotFoundError: If the raw file or sidecar does not exist in storage.
        """
        file_key = f"{source}/{date_str}/{filename}"
        sidecar_key = f"{source}/{date_str}/{filename}.meta.json"

        try:
            raw_response = self.client.get_object(Bucket=self.bucket, Key=file_key)
            raw_bytes = raw_response["Body"].read()
        except ClientError as exc:
            raise FileNotFoundError(
                f"Raw file s3://{self.bucket}/{file_key} not found: {exc}"
            ) from exc

        try:
            sidecar_response = self.client.get_object(Bucket=self.bucket, Key=sidecar_key)
            sidecar_dict = json.loads(sidecar_response["Body"].read().decode("utf-8"))
        except ClientError as exc:
            logger.warning("Sidecar s3://%s/%s missing: %s", self.bucket, sidecar_key, exc)
            sidecar_dict = {}

        return raw_bytes, sidecar_dict

    def list_raw(self, source: str, date_str: Optional[str] = None) -> List[str]:
        """List raw data files for a source, optionally filtered by partition date.

        Args:
            source: Name of the data source.
            date_str: Optional partition date (YYYY-MM-DD).

        Returns:
            List of S3 URIs for the raw data files (excluding metadata sidecars).
        """
        prefix = f"{source}/{date_str}/" if date_str else f"{source}/"
        results: List[str] = []

        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                key = item["Key"]
                if not key.endswith(".meta.json") and not key.endswith(".json"):
                    results.append(f"s3://{self.bucket}/{key}")

        return results


# Default global instance
_default_storage: Optional[StorageClient] = None


def get_storage_client() -> StorageClient:
    """Return singleton StorageClient instance."""
    global _default_storage
    if _default_storage is None:
        _default_storage = StorageClient()
    return _default_storage


def put_raw(
    source: str,
    filename: str,
    content: bytes,
    metadata: Optional[Dict[str, Any]] = None,
    fetch_date: Optional[str | date] = None,
) -> Tuple[str, bool]:
    """Module-level convenience wrapper for put_raw."""
    return get_storage_client().put_raw(
        source=source,
        filename=filename,
        content=content,
        metadata=metadata,
        fetch_date=fetch_date,
    )


def get_raw(source: str, date_str: str, filename: str) -> Tuple[bytes, Dict[str, Any]]:
    """Module-level convenience wrapper for get_raw."""
    return get_storage_client().get_raw(source=source, date_str=date_str, filename=filename)


def list_raw(source: str, date_str: Optional[str] = None) -> List[str]:
    """Module-level convenience wrapper for list_raw."""
    return get_storage_client().list_raw(source=source, date_str=date_str)
