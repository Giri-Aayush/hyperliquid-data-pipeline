"""S3-compatible object store for caching raw archive pulls and processed output.

Works with any S3-compatible backend — Cloudflare R2, AWS S3, Backblaze B2, MinIO —
by pointing ``OBJECT_STORE_ENDPOINT_URL`` at the provider. R2 is the cheap default:
zero egress, so you pay AWS once to pull Hyperliquid's requester-pays archive, store
it here, and every later read (backtests, re-runs, sharing) is free.

Note: this does NOT change where Hyperliquid publishes its archive (AWS S3,
requester-pays). It's your own bucket, used as a read-through cache and an output sink.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional
import shutil

from loguru import logger

from ..config import settings


class ObjectStore(ABC):
    """Minimal whole-object store: put/get files, check existence, list keys."""

    @abstractmethod
    def put_file(self, local_path: Path, key: str) -> bool:
        """Upload a local file to ``key``. Returns True on success."""

    @abstractmethod
    def get_file(self, key: str, local_path: Path) -> bool:
        """Download ``key`` to ``local_path``. Returns True on success."""

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Return True if ``key`` exists in the store."""

    @abstractmethod
    def put_bytes(self, data: bytes, key: str) -> bool:
        """Write raw bytes to ``key``. Returns True on success."""

    @abstractmethod
    def list(self, prefix: str = "") -> List[str]:
        """List keys under ``prefix`` (relative to the configured prefix)."""


class S3ObjectStore(ObjectStore):
    """S3-compatible store via boto3. Endpoint-configurable for R2 and friends."""

    def __init__(
        self,
        bucket: str,
        access_key_id: str,
        secret_access_key: str,
        endpoint_url: Optional[str] = None,
        region: str = "auto",
        prefix: str = "",
    ):
        import boto3
        from botocore.config import Config as BotoConfig

        self.bucket = bucket
        self.prefix = prefix
        self.logger = logger.bind(component="object_store")
        # R2 (and most S3-compatible stores) require SigV4.
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url or None,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region or "auto",
            config=BotoConfig(signature_version="s3v4"),
        )
        where = endpoint_url or "AWS S3"
        self.logger.info(f"Object store ready: s3-compatible bucket '{bucket}' at {where}")

    def _full_key(self, key: str) -> str:
        return f"{self.prefix}{key}" if self.prefix else key

    def put_file(self, local_path: Path, key: str) -> bool:
        try:
            self.client.upload_file(str(local_path), self.bucket, self._full_key(key))
            return True
        except Exception as e:
            self.logger.error(f"Failed to upload {local_path} to {key}: {e}")
            return False

    def get_file(self, key: str, local_path: Path) -> bool:
        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            self.client.download_file(self.bucket, self._full_key(key), str(local_path))
            return True
        except Exception as e:
            self.logger.debug(f"Object store miss for {key}: {e}")
            return False

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.bucket, Key=self._full_key(key))
            return True
        except ClientError:
            return False
        except Exception as e:
            self.logger.debug(f"exists() check failed for {key}: {e}")
            return False

    def put_bytes(self, data: bytes, key: str) -> bool:
        try:
            self.client.put_object(Bucket=self.bucket, Key=self._full_key(key), Body=data)
            return True
        except Exception as e:
            self.logger.error(f"Failed to put bytes to {key}: {e}")
            return False

    def list(self, prefix: str = "") -> List[str]:
        try:
            full = self._full_key(prefix)
            keys: List[str] = []
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=full):
                for obj in page.get("Contents", []):
                    k = obj["Key"]
                    keys.append(k[len(self.prefix):] if self.prefix and k.startswith(self.prefix) else k)
            return keys
        except Exception as e:
            self.logger.error(f"Failed to list {prefix}: {e}")
            return []


class LocalObjectStore(ObjectStore):
    """Filesystem-backed store. Used as a dev/test fallback and for offline mirrors."""

    def __init__(self, root: Path, prefix: str = ""):
        self.root = Path(root)
        self.prefix = prefix
        self.logger = logger.bind(component="object_store")
        self.root.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Object store ready: local dir '{self.root}'")

    def _path(self, key: str) -> Path:
        rel = f"{self.prefix}{key}" if self.prefix else key
        return self.root / rel

    def put_file(self, local_path: Path, key: str) -> bool:
        try:
            dest = self._path(key)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(local_path, dest)
            return True
        except Exception as e:
            self.logger.error(f"Failed to copy {local_path} to {key}: {e}")
            return False

    def get_file(self, key: str, local_path: Path) -> bool:
        src = self._path(key)
        if not src.exists():
            return False
        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, local_path)
            return True
        except Exception as e:
            self.logger.error(f"Failed to copy {key} to {local_path}: {e}")
            return False

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def put_bytes(self, data: bytes, key: str) -> bool:
        try:
            dest = self._path(key)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(data)
            return True
        except Exception as e:
            self.logger.error(f"Failed to write bytes to {key}: {e}")
            return False

    def list(self, prefix: str = "") -> List[str]:
        base = self._path(prefix)
        search_root = base if base.is_dir() else base.parent
        if not search_root.exists():
            return []
        keys = []
        store_root = self.root / self.prefix if self.prefix else self.root
        for p in search_root.rglob("*"):
            if p.is_file():
                keys.append(str(p.relative_to(store_root)))
        return keys


def get_object_store() -> Optional[ObjectStore]:
    """Build the configured object store, or None when disabled.

    Resolution follows ``settings.object_store_kind``:
      - "s3"   -> S3ObjectStore (R2/AWS/etc.), requires bucket + credentials
      - "local"-> LocalObjectStore under OBJECT_STORE_LOCAL_PATH
      - "none" -> None (feature disabled; callers no-op)
    """
    kind = settings.object_store_kind

    if kind == "s3":
        if not (
            settings.object_store_bucket
            and settings.object_store_access_key_id
            and settings.object_store_secret_access_key
        ):
            logger.warning(
                "Object store backend is 's3' but bucket/credentials are missing — disabling it."
            )
            return None
        return S3ObjectStore(
            bucket=settings.object_store_bucket,
            access_key_id=settings.object_store_access_key_id,
            secret_access_key=settings.object_store_secret_access_key,
            endpoint_url=settings.object_store_endpoint_url,
            region=settings.object_store_region,
            prefix=settings.object_store_prefix,
        )

    if kind == "local":
        return LocalObjectStore(
            root=settings.object_store_local_path,
            prefix=settings.object_store_prefix,
        )

    if kind != "none":
        logger.warning(
            f"Unknown OBJECT_STORE_BACKEND '{settings.object_store_backend}' "
            "(expected: auto, s3, r2, local, none) — object store disabled."
        )
    return None
