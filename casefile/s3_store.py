"""The object store of the replay on the host: the landed object versions in the project's bucket, read with boto3.

It implements the ``ObjectStore`` of the ingestion checks, for reads only: its ``create_object`` refuses. The replay reads a version by its ID, so a version
that was substituted after the gate cannot be read by mistake. The credentials come from the standard AWS credential chain of the container: the replay's
own short-lived workload access. Nothing in this module writes.
"""

from __future__ import annotations

import io
from collections.abc import Iterator, Mapping
from datetime import UTC
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from scripts.ingestion.notice import Code, Notice
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
            raise StoreError(Notice(Code.AWS_FAILED_NOT_KEPT)) from error
        return sorted(found, key=lambda item: (item.key, item.last_modified, not item.is_latest))

    @staticmethod
    def version(entry: Mapping[str, Any], *, marker: bool) -> ObjectVersion:
        stamp = entry["LastModified"].astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        return ObjectVersion(entry["Key"], entry["VersionId"], 0 if marker else int(entry["Size"]), stamp, bool(entry["IsLatest"]), marker)

    def fetch(self, version: ObjectVersion, start: int, end: int) -> bytes:
        try:
            reply = self.client.get_object(Bucket=self.bucket, Key=version.key, VersionId=version.version_id, Range=f"bytes={start}-{end}")
        except (BotoCoreError, ClientError) as error:
            raise StoreError(Notice(Code.AWS_FAILED_NOT_KEPT)) from error
        if reply.get("VersionId") != version.version_id:
            raise StoreError(Notice(Code.OTHER_VERSION))
        data: bytes = reply["Body"].read()
        if len(data) != end - start + 1:
            raise StoreError(Notice(Code.SHORT_REPLY))
        return data

    def iter_version(self, version: ObjectVersion) -> Iterator[bytes]:
        for start in range(0, version.size, RANGE_BYTES):
            data = self.fetch(version, start, min(start + RANGE_BYTES, version.size) - 1)
            stream = io.BytesIO(data)
            while block := stream.read(BLOCK_BYTES):
                yield block

    def get_version_bytes(self, version: ObjectVersion, limit: int) -> bytes:
        if version.size > limit:
            raise StoreError(Notice(Code.TOO_LARGE, count=limit))
        return self.fetch(version, 0, version.size - 1) if version.size else b""

    def create_object(self, key: str, data: bytes) -> ObjectVersion:
        raise StoreError(Notice(Code.READ_ONLY_STORE))
