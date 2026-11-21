"""The object store of the replay on the host: the landed object versions in the project's bucket, read with boto3.

It implements the ``ObjectStore`` of the ingestion checks, for reads only. The replay reads a version by its ID, so a version that was substituted after the
gate cannot be read by mistake. The credentials come from the standard AWS credential chain of the container: the replay's own short-lived workload access.
Nothing in this module writes.
"""

from __future__ import annotations

import io
from collections.abc import Iterator
from datetime import UTC
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from scripts.ingestion.store import BLOCK_BYTES, ObjectVersion, StoreError

RANGE_BYTES = 64 * 1024 * 1024


class S3ReadStore:
    def __init__(self, bucket: str, region: str, client: Any | None = None) -> None:
        self.bucket = bucket
        self.client = client or boto3.client("s3", region_name=region)

    def list_versions(self, prefix: str) -> list[ObjectVersion]:
        found: list[ObjectVersion] = []
        try:
            for page in self.client.get_paginator("list_object_versions").paginate(Bucket=self.bucket, Prefix=prefix):
                for entry in page.get("Versions", []):
                    found.append(self.version(entry, marker=False))
                for entry in page.get("DeleteMarkers", []):
                    found.append(self.version(entry, marker=True))
        except (BotoCoreError, ClientError) as error:
            raise StoreError(f"listing the versions under {prefix} failed: {type(error).__name__}") from error
        return sorted(found, key=lambda item: (item.key, item.last_modified, not item.is_latest))

    @staticmethod
    def version(entry: dict[str, Any], *, marker: bool) -> ObjectVersion:
        stamp = entry["LastModified"].astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        return ObjectVersion(entry["Key"], entry["VersionId"], 0 if marker else int(entry["Size"]), stamp, bool(entry["IsLatest"]), marker)

    def fetch(self, version: ObjectVersion, start: int, end: int) -> bytes:
        try:
            reply = self.client.get_object(Bucket=self.bucket, Key=version.key, VersionId=version.version_id, Range=f"bytes={start}-{end}")
        except (BotoCoreError, ClientError) as error:
            raise StoreError(f"{version.key}: reading the version failed: {type(error).__name__}") from error
        if reply.get("VersionId") != version.version_id:
            raise StoreError(f"{version.key}: the reply was for another version than the one asked for")
        data: bytes = reply["Body"].read()
        if len(data) != end - start + 1:
            raise StoreError(f"{version.key}: the reply held {len(data)} bytes of the {end - start + 1} asked for")
        return data

    def iter_version(self, version: ObjectVersion) -> Iterator[bytes]:
        for start in range(0, version.size, RANGE_BYTES):
            data = self.fetch(version, start, min(start + RANGE_BYTES, version.size) - 1)
            stream = io.BytesIO(data)
            while block := stream.read(BLOCK_BYTES):
                yield block

    def get_version_bytes(self, version: ObjectVersion, limit: int) -> bytes:
        if version.size > limit:
            raise StoreError(f"{version.key}: the object is larger than {limit} bytes")
        return self.fetch(version, 0, version.size - 1) if version.size else b""

    def put_object(self, key: str, data: bytes) -> ObjectVersion:
        raise StoreError("the replay's store is read only")
