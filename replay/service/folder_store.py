"""A folder that plays the role of the landed copy in the project's bucket: object versions as files, for a test that has no bucket.

Layout: ``<root>/<key>/<version ID>``, where the version IDs are ``v0001``, ``v0002`` and so on and a file ``<version ID>.deleted`` is a delete marker. It
implements the ``ObjectStore`` of the ingestion checks, so the project's own check, manifest and gate code run against it unchanged.

What it does not prove compared with the bucket: S3's own listing of versions and delete markers, its version IDs, the range reads of a large object, the
credentials and the role that reach the bucket, and the latency of a real network. The acceptance test of the replay names these limits in its record.
"""

from __future__ import annotations

import re
import shutil
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

from scripts.ingestion.folder import list_folder
from scripts.ingestion.notice import Code, Notice
from scripts.ingestion.store import BLOCK_BYTES, ObjectVersion, StoreError

VERSION_FILE = re.compile(r"v(\d{4})(\.deleted)?")


class FolderStore:
    def __init__(self, root: Path, bucket: str = "local-landed-copy", read_log: Path | None = None) -> None:
        self.root = root
        self.bucket = bucket
        self.read_log = read_log

    def note_read(self, version: ObjectVersion) -> None:
        """Append the key and version ID of a read to the read log, so a test can show which versions were read."""
        if self.read_log is not None:
            with self.read_log.open("a") as handle:
                handle.write(f"{version.key} {version.version_id}\n")

    def entries(self, key: str) -> list[tuple[int, bool, Path]]:
        folder = self.root / key
        found = []
        for path in list_folder(folder):
            match = VERSION_FILE.fullmatch(path.name)
            if match and path.is_file():
                found.append((int(match.group(1)), match.group(2) is not None, path))
        return sorted(found)

    def version_of(self, key: str, number: int, marker: bool, path: Path, *, latest: bool) -> ObjectVersion:
        stamp = datetime.fromtimestamp(path.stat().st_mtime, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        return ObjectVersion(key, f"v{number:04d}", 0 if marker else path.stat().st_size, stamp, latest, marker)

    def list_versions(self, prefix: str) -> list[ObjectVersion]:
        found: list[ObjectVersion] = []
        keys = {
            str(path.parent.relative_to(self.root))
            for pattern in ("v????", "v????.deleted")
            for path in list_folder(self.root, pattern, recursive=True)
            if path.is_file()
        }
        for key in sorted(key for key in keys if key.startswith(prefix)):
            entries = self.entries(key)
            found += [self.version_of(key, number, marker, path, latest=index == len(entries) - 1) for index, (number, marker, path) in enumerate(entries)]
        return found

    def path_of(self, version: ObjectVersion) -> Path:
        path = self.root / version.key / version.version_id
        if not path.is_file():
            raise StoreError(Notice(Code.VERSION_MISSING))
        return path

    def iter_version(self, version: ObjectVersion) -> Iterator[bytes]:
        self.note_read(version)
        with self.path_of(version).open("rb") as handle:
            while block := handle.read(BLOCK_BYTES):
                yield block

    def get_version_bytes(self, version: ObjectVersion, limit: int) -> bytes:
        self.note_read(version)
        path = self.path_of(version)
        if path.stat().st_size > limit:
            raise StoreError(Notice(Code.TOO_LARGE, count=limit))
        return path.read_bytes()

    def next_number(self, key: str) -> int:
        entries = self.entries(key)
        return entries[-1][0] + 1 if entries else 1

    def create_object(self, key: str, data: bytes) -> ObjectVersion:
        """Create the first version of ``key``, or return its current version when that already holds exactly ``data``; other bytes are refused."""
        entries = self.entries(key)
        if entries and not entries[-1][1]:
            number, _, path = entries[-1]
            if path.read_bytes() != data:
                raise StoreError(Notice(Code.OTHER_CONTENT))
            return self.version_of(key, number, False, path, latest=True)
        number = self.next_number(key)
        folder = self.root / key
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"v{number:04d}"
        path.write_bytes(data)
        return self.version_of(key, number, False, path, latest=True)

    def land_file(self, key: str, source: Path) -> ObjectVersion:
        """Land a copy of ``source`` as the next version of ``key``."""
        number = self.next_number(key)
        folder = self.root / key
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"v{number:04d}"
        shutil.copyfile(source, path)
        return self.version_of(key, number, False, path, latest=True)
