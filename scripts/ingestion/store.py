"""The object store the ingestion checks read and write, with object versions as the unit.

``ObjectStore`` is what the checks need. ``S3Store`` implements it with the project's AWS helper, so the caller's own credentials and profile apply. A write
checks that versioning is enabled and creates its key with ``If-None-Match: *``, so the command line's retry of a lost reply cannot add a second version.
A payload is read in ranges of ``RANGE_BYTES`` into a temporary file and handed on in blocks of ``BLOCK_BYTES``, so a file of any size is never held whole.
"""

from __future__ import annotations

import tempfile
from collections.abc import Generator, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

RANGE_BYTES = 64 * 1024 * 1024
BLOCK_BYTES = 1024 * 1024
LIST_TIMEOUT_SECONDS = 300
READ_TIMEOUT_SECONDS = 600
WRITE_TIMEOUT_SECONDS = 120
NO_SUCH_VERSION = ("NoSuchVersion", "NoSuchKey", "(404)")


class StoreError(RuntimeError):
    """A read or write of the object store failed or returned something other than what was asked for."""


@dataclass(frozen=True)
class ObjectVersion:
    key: str
    version_id: str
    size: int
    last_modified: str
    is_latest: bool
    is_delete_marker: bool


class ObjectStore(Protocol):
    bucket: str

    def list_versions(self, prefix: str) -> list[ObjectVersion]:
        """Every object version and delete marker whose key starts with ``prefix``."""
        ...

    def iter_version(self, version: ObjectVersion) -> Iterator[bytes]:
        """The bytes of exactly this version, in blocks. Raise ``StoreError`` when the version does not exist or the reply is for another."""
        ...

    def get_version_bytes(self, version: ObjectVersion, limit: int) -> bytes:
        """The whole of a small version. Raise ``StoreError`` when it is larger than ``limit`` bytes."""
        ...

    def create_object(self, key: str, data: bytes) -> ObjectVersion:
        """Create the first version of ``key`` and return it, or return its current version when that already holds exactly ``data``.

        Raise ``StoreError`` when versioning is not enabled, when the key holds other bytes or when the store gives the write no version ID. A second
        call with the same arguments, also after a lost reply, never adds a version.
        """
        ...


class AwsCaller(Protocol):
    def call(self, *arguments: str, missing: tuple[str, ...] = (), timeout: float = ...) -> Any: ...


def utc_text(stamp: str) -> str:
    return datetime.fromisoformat(stamp).astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def listed_version(entry: dict[str, Any], *, marker: bool) -> ObjectVersion:
    return ObjectVersion(
        entry["Key"], entry["VersionId"], 0 if marker else int(entry["Size"]), utc_text(entry["LastModified"]), bool(entry["IsLatest"]), marker
    )


class S3Store:
    def __init__(self, aws: AwsCaller, bucket: str, *, range_bytes: int = RANGE_BYTES, block_bytes: int = BLOCK_BYTES) -> None:
        self.aws = aws
        self.bucket = bucket
        self.range_bytes = range_bytes
        self.block_bytes = block_bytes

    def list_versions(self, prefix: str) -> list[ObjectVersion]:
        listing = self.aws.call("s3api", "list-object-versions", "--bucket", self.bucket, "--prefix", prefix, timeout=LIST_TIMEOUT_SECONDS)
        found = [listed_version(entry, marker=False) for entry in listing.get("Versions") or []]
        found += [listed_version(entry, marker=True) for entry in listing.get("DeleteMarkers") or []]
        return sorted(found, key=lambda item: (item.key, item.last_modified, not item.is_latest))

    def fetch(self, version: ObjectVersion, target: Path, byte_range: tuple[int, int] | None) -> int:
        """Read the range of exactly this version into ``target`` and return the byte count. The reply must name the requested version."""
        arguments = ["s3api", "get-object", "--bucket", self.bucket, "--key", version.key, "--version-id", version.version_id]
        if byte_range is not None:
            arguments += ["--range", f"bytes={byte_range[0]}-{byte_range[1]}"]
        reply = self.aws.call(*arguments, str(target), missing=NO_SUCH_VERSION, timeout=READ_TIMEOUT_SECONDS)
        if reply is None:
            raise StoreError(f"{version.key}: the version does not exist")
        if reply.get("VersionId") != version.version_id:
            raise StoreError(f"{version.key}: the reply was for another version than the one asked for")
        length = reply.get("ContentLength")
        if type(length) is not int:
            raise StoreError(f"{version.key}: the reply gave no byte count")
        return length

    def iter_version(self, version: ObjectVersion) -> Generator[bytes]:
        with tempfile.TemporaryDirectory(prefix="ingestion_range_") as scratch:
            target = Path(scratch) / "range"
            for start in range(0, version.size, self.range_bytes):
                end = min(start + self.range_bytes, version.size) - 1
                length = self.fetch(version, target, (start, end))
                if not target.is_file() or length != end - start + 1 or target.stat().st_size != length:
                    raise StoreError(f"{version.key}: the reply held {length} bytes of the {end - start + 1} asked for")
                with target.open("rb") as handle:
                    while block := handle.read(self.block_bytes):
                        yield block
                target.unlink()

    def get_version_bytes(self, version: ObjectVersion, limit: int) -> bytes:
        if version.size > limit:
            raise StoreError(f"{version.key}: the object is larger than {limit} bytes")
        with tempfile.TemporaryDirectory(prefix="ingestion_object_") as scratch:
            target = Path(scratch) / "object"
            self.fetch(version, target, None)
            if not target.is_file():
                raise StoreError(f"{version.key}: the reply wrote no data")
            return target.read_bytes()

    def create_object(self, key: str, data: bytes) -> ObjectVersion:
        status = (self.aws.call("s3api", "get-bucket-versioning", "--bucket", self.bucket, timeout=LIST_TIMEOUT_SECONDS) or {}).get("Status")
        if status != "Enabled":
            raise StoreError(f"{key}: versioning is not enabled on the bucket, so nothing was written")
        with tempfile.TemporaryDirectory(prefix="ingestion_put_") as scratch:
            source = Path(scratch) / "object"
            source.write_bytes(data)
            reply = self.aws.call(
                "s3api",
                "put-object",
                "--bucket",
                self.bucket,
                "--key",
                key,
                "--body",
                str(source),
                "--content-type",
                "application/json",
                "--if-none-match",
                "*",
                missing=("PreconditionFailed",),
                timeout=WRITE_TIMEOUT_SECONDS,
            )
        if reply is None:
            return self.existing_version(key, data)
        version_id = reply.get("VersionId")
        if not version_id or version_id == "null":
            raise StoreError(f"{key}: the write returned no version ID; versioning is not enabled on the bucket")
        for version in self.list_versions(key):
            if version.key == key and version.version_id == version_id:
                return version
        raise StoreError(f"{key}: the written version is not in the listing")

    def existing_version(self, key: str, data: bytes) -> ObjectVersion:
        """The current version of a key that refused the conditional write, once its bytes are confirmed to be ``data``: the write of an earlier try."""
        current = [version for version in self.list_versions(key) if version.key == key and version.is_latest and not version.is_delete_marker]
        if not current:
            raise StoreError(f"{key}: the write was refused because the key exists, but no current version is listed")
        if current[0].size != len(data) or self.get_version_bytes(current[0], len(data)) != data:
            raise StoreError(f"{key}: the key already holds other content")
        return current[0]
