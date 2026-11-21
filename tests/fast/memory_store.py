"""A stand-in for the object store, for fast tests of the ingestion checks.

It keeps object versions in memory. ``put_object`` plays the copy, which writes any version of any key, and ``create_object`` is the one write of the
ingestion code. Its limits: no multipart upload, no ETag, no listing pages, no delay between a write and a listing, no
permissions and no lifecycle rule. It reads a payload only in blocks of ``BLOCK_SIZE`` bytes and records every read, so a test can prove which
versions were read and that no payload was read whole. The real store is proven by the acceptance test against S3.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, replace

from scripts.ingestion.store import NULL_VERSION_IDS, ObjectVersion, StoreError

BLOCK_SIZE = 8


@dataclass(frozen=True)
class Read:
    method: str
    key: str
    version_id: str


@dataclass
class Stored:
    version: ObjectVersion
    data: bytes


class MemoryStore:
    def __init__(self, bucket: str = "example-bucket", *, versioned: bool = True) -> None:
        self.bucket = bucket
        self.versioned = versioned
        self.history: dict[str, list[Stored]] = {}
        self.reads: list[Read] = []
        self.writes: list[tuple[str, str]] = []
        self.largest_block = 0
        self.counter = 0

    def next_version_id(self) -> str:
        self.counter += 1
        return f"version{self.counter:04d}" if self.versioned else "null"

    def stamp(self) -> str:
        return f"2026-01-01T00:{self.counter // 60:02d}:{self.counter % 60:02d}Z"

    def append(self, key: str, data: bytes, *, marker: bool) -> ObjectVersion:
        stored = self.history.setdefault(key, [])
        if not self.versioned:
            stored.clear()
        version_id = self.next_version_id()
        version = ObjectVersion(key, version_id, 0 if marker else len(data), self.stamp(), True, marker)
        stored[:] = [Stored(replace(item.version, is_latest=False), item.data) for item in stored]
        stored.append(Stored(version, b"" if marker else data))
        return version

    def put_object(self, key: str, data: bytes) -> ObjectVersion:
        self.writes.append((key, f"{len(data)} bytes"))
        return self.append(key, data, marker=False)

    def create_object(self, key: str, data: bytes) -> ObjectVersion:
        """The store's rule for a write of the ingestion code: versioning must be on, the first version is created and a key that holds ``data`` is returned."""
        if not self.versioned:
            raise StoreError(f"{key}: versioning is not enabled on the bucket, so nothing was written")
        current = [item for item in self.history.get(key, []) if item.version.is_latest and not item.version.is_delete_marker]
        if current:
            if current[0].version.version_id in NULL_VERSION_IDS:
                raise StoreError(f"{key}: the key holds a null version, which versioning does not keep apart from the next write")
            if current[0].data != data:
                raise StoreError(f"{key}: the key already holds other content")
            return current[0].version
        return self.put_object(key, data)

    def put_delete_marker(self, key: str) -> ObjectVersion:
        return self.append(key, b"", marker=True)

    def remove_version(self, key: str, version_id: str) -> None:
        stored = self.history[key]
        stored[:] = [item for item in stored if item.version.version_id != version_id]
        if stored:
            stored[-1] = Stored(replace(stored[-1].version, is_latest=True), stored[-1].data)

    def list_versions(self, prefix: str) -> list[ObjectVersion]:
        return [item.version for key in sorted(self.history) if key.startswith(prefix) for item in self.history[key]]

    def find(self, version: ObjectVersion) -> Stored:
        for item in self.history.get(version.key, []):
            if item.version.version_id == version.version_id and not item.version.is_delete_marker:
                return item
        raise StoreError(f"{version.key}: the version does not exist")

    def iter_version(self, version: ObjectVersion) -> Iterator[bytes]:
        self.reads.append(Read("iter_version", version.key, version.version_id))
        data = self.find(version).data
        for start in range(0, len(data), BLOCK_SIZE):
            block = data[start : start + BLOCK_SIZE]
            self.largest_block = max(self.largest_block, len(block))
            yield block

    def get_version_bytes(self, version: ObjectVersion, limit: int) -> bytes:
        self.reads.append(Read("get_version_bytes", version.key, version.version_id))
        data = self.find(version).data
        if len(data) > limit:
            raise StoreError(f"{version.key}: the object is larger than {limit} bytes")
        return data

    def payload_reads(self, prefix: str) -> list[Read]:
        return [read for read in self.reads if read.key.startswith(prefix)]
