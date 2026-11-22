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

from scripts.ingestion.notice import ClosedError, Code, Notice, Subject

RANGE_BYTES = 64 * 1024 * 1024
BLOCK_BYTES = 1024 * 1024
LIST_TIMEOUT_SECONDS = 300
READ_TIMEOUT_SECONDS = 600
WRITE_TIMEOUT_SECONDS = 120
NO_SUCH_VERSION = ("NoSuchVersion", "NoSuchKey", "(404)")
NULL_VERSION_IDS = ("", "null")


class StoreError(ClosedError, RuntimeError):
    """A read or write of the object store failed or returned something other than what was asked for."""


@dataclass(frozen=True, slots=True)
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


def reply_object(reply: Any, call: Subject) -> dict[str, Any]:
    """The reply of an AWS call as the JSON object it documents. Raise ``StoreError``, which names the call only, for any other reply."""
    if type(reply) is not dict:
        raise StoreError(Notice(Code.REPLY_FORM, subject=call))
    return reply


def listed_version(entry: Any, *, marker: bool) -> ObjectVersion:
    """One entry of a version listing. Raise ``StoreError`` unless it has its key, version ID, latest flag and time, and a size unless it is a marker."""
    try:
        key, version_id, latest, stamp = entry["Key"], entry["VersionId"], entry["IsLatest"], utc_text(entry["LastModified"])
        size = 0 if marker else entry["Size"]
    except KeyError, TypeError, ValueError:
        raise StoreError(Notice(Code.LISTING_ENTRY_FORM)) from None
    if type(key) is not str or type(version_id) is not str or type(latest) is not bool or type(size) is not int:
        raise StoreError(Notice(Code.LISTING_ENTRY_FORM))
    return ObjectVersion(key, version_id, size, stamp, latest, marker)


def listed_entries(listing: dict[str, Any], name: str) -> list[Any]:
    """The entries of one part of a listing. An absent part has none; a part that is present and not a list is refused."""
    entries = listing.get(name, [])
    if type(entries) is not list:
        raise StoreError(Notice(Code.LISTING_FORM))
    return entries


class S3Store:
    def __init__(self, aws: AwsCaller, bucket: str, *, range_bytes: int = RANGE_BYTES, block_bytes: int = BLOCK_BYTES) -> None:
        self.aws = aws
        self.bucket = bucket
        self.range_bytes = range_bytes
        self.block_bytes = block_bytes

    def list_versions(self, prefix: str) -> list[ObjectVersion]:
        reply = self.aws.call("s3api", "list-object-versions", "--bucket", self.bucket, "--prefix", prefix, timeout=LIST_TIMEOUT_SECONDS)
        listing = reply_object(reply, Subject.LIST_CALL)
        found = [listed_version(entry, marker=False) for entry in listed_entries(listing, "Versions")]
        found += [listed_version(entry, marker=True) for entry in listed_entries(listing, "DeleteMarkers")]
        return sorted(found, key=lambda item: (item.key, item.last_modified, not item.is_latest))

    def fetch(self, version: ObjectVersion, target: Path, byte_range: tuple[int, int] | None) -> int:
        """Read the range of exactly this version into ``target`` and return the byte count. The reply must name the requested version."""
        arguments = ["s3api", "get-object", "--bucket", self.bucket, "--key", version.key, "--version-id", version.version_id]
        if byte_range is not None:
            arguments += ["--range", f"bytes={byte_range[0]}-{byte_range[1]}"]
        reply = self.aws.call(*arguments, str(target), missing=NO_SUCH_VERSION, timeout=READ_TIMEOUT_SECONDS)
        if reply is None:
            raise StoreError(Notice(Code.VERSION_MISSING))
        if reply_object(reply, Subject.GET_CALL).get("VersionId") != version.version_id:
            raise StoreError(Notice(Code.OTHER_VERSION))
        length = reply.get("ContentLength")
        if type(length) is not int:
            raise StoreError(Notice(Code.NO_BYTE_COUNT))
        return length

    def iter_version(self, version: ObjectVersion) -> Generator[bytes]:
        with tempfile.TemporaryDirectory(prefix="ingestion_range_") as scratch:
            target = Path(scratch) / "range"
            for start in range(0, version.size, self.range_bytes):
                end = min(start + self.range_bytes, version.size) - 1
                length = self.fetch(version, target, (start, end))
                if not target.is_file() or length != end - start + 1 or target.stat().st_size != length:
                    raise StoreError(Notice(Code.SHORT_REPLY))
                with target.open("rb") as handle:
                    while block := handle.read(self.block_bytes):
                        yield block
                target.unlink()

    def get_version_bytes(self, version: ObjectVersion, limit: int) -> bytes:
        if version.size > limit:
            raise StoreError(Notice(Code.TOO_LARGE, count=limit))
        with tempfile.TemporaryDirectory(prefix="ingestion_object_") as scratch:
            target = Path(scratch) / "object"
            self.fetch(version, target, None)
            if not target.is_file():
                raise StoreError(Notice(Code.NO_DATA))
            return target.read_bytes()

    def create_object(self, key: str, data: bytes) -> ObjectVersion:
        status = reply_object(
            self.aws.call("s3api", "get-bucket-versioning", "--bucket", self.bucket, timeout=LIST_TIMEOUT_SECONDS), Subject.VERSIONING_CALL
        ).get("Status")
        if status != "Enabled":
            raise StoreError(Notice(Code.VERSIONING_OFF))
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
        version_id = reply_object(reply, Subject.PUT_CALL).get("VersionId")
        if type(version_id) is not str or version_id in NULL_VERSION_IDS:
            raise StoreError(Notice(Code.NO_VERSION_ID))
        for version in self.list_versions(key):
            if version.key == key and version.version_id == version_id:
                return version
        raise StoreError(Notice(Code.NOT_LISTED))

    def existing_version(self, key: str, data: bytes) -> ObjectVersion:
        """The current version of a key that refused the conditional write, once its bytes are confirmed to be ``data``: the write of an earlier try."""
        current = [version for version in self.list_versions(key) if version.key == key and version.is_latest and not version.is_delete_marker]
        if not current:
            raise StoreError(Notice(Code.EXISTS_UNLISTED))
        if current[0].version_id in NULL_VERSION_IDS:
            raise StoreError(Notice(Code.NULL_HELD))
        if current[0].size != len(data) or self.get_version_bytes(current[0], len(data)) != data:
            raise StoreError(Notice(Code.OTHER_CONTENT))
        return current[0]
