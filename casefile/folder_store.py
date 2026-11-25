"""A folder that plays the role of the landed copy in the project's bucket: object versions as files, for a test that has no bucket.

Layout: ``<root>/<key>/<version ID>``, where the version IDs are ``v0001``, ``v0002`` and so on and a file ``<version ID>.deleted`` is a delete marker. It
implements the ``ObjectStore`` of the ingestion checks, so the project's own check, manifest and gate code run against it unchanged.

What it does not prove compared with the bucket: S3's own listing of versions and delete markers, its version IDs, the range reads of a large object, the
credentials and the role that reach the bucket, and the latency of a real network. The acceptance test of the replay names these limits in its record.
"""

from __future__ import annotations

import glob
import os
import re
import stat
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

from scripts.ingestion.folder import list_files, list_folder
from scripts.ingestion.notice import Code, Notice, Subject
from scripts.ingestion.output_file import write_new_file
from scripts.ingestion.store import BLOCK_BYTES, ObjectVersion, StoreError

VERSION_FILE = re.compile(r"v(\d{4})(\.deleted)?")


# The files that the replay writes in a run's state folder. A read log (the folder store's record of what it read, written by the replay and the Bronze
# decoder) is never given one of these names, so it cannot take the place of a saved position, a schedule or the health file.
SCHEDULE_NAME = "schedule.json"
STATE_NAME = "state.json"
HEALTH_NAME = "health.json"
STATE_FOLDER_NAMES = frozenset(name.casefold() for name in (SCHEDULE_NAME, STATE_NAME, HEALTH_NAME))


def is_state_folder_name(name: str) -> bool:
    """Whether ``name`` cannot be a read log's last part: it is empty, it is ``..`` or it is a file that the replay writes in its state folder, compared
    without letter case."""
    return name in {"", ".."} or name.casefold() in STATE_FOLDER_NAMES


def refused() -> StoreError:
    return StoreError(Notice(Code.CANNOT_WRITE, subject=Subject.OUTPUT))


class ReadLog:
    """The read log of a test run: a file that gets the key and version ID of every read, so a test can show which versions were read.

    The log is always a new file. ``create`` opens it once, when the run opens its folder store, with ``O_CREAT | O_EXCL | O_NOFOLLOW``: a path that exists in
    any form (a file, an empty file, a link, a dangling link, a folder, a named pipe, a hard link) is refused with ``CANNOT_WRITE`` before any read and what it
    names stays as it was. Every read then appends one whole line to that open file; a write that stores fewer bytes than the line is ``CANNOT_WRITE`` too.
    ``attach`` is for a job worker (a Spark partition) that gets the path of the log that its driver created: it opens that file again for appending, without
    following a link or waiting for a reader, and refuses everything that is not a regular file.
    """

    def __init__(self, descriptor: int) -> None:
        self.descriptor: int | None = descriptor

    @classmethod
    def create(cls, path: Path) -> ReadLog:
        try:
            return cls(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW, 0o644))
        except OSError:
            raise refused() from None

    @classmethod
    def attach(cls, path: Path) -> ReadLog:
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW | os.O_NONBLOCK)
        except OSError:
            raise refused() from None
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            os.close(descriptor)
            raise refused()
        return cls(descriptor)

    def append(self, line: str) -> None:
        data = line.encode()
        if self.descriptor is None:
            raise refused()
        try:
            written = os.write(self.descriptor, data)
        except OSError:
            raise refused() from None
        if written != len(data):
            raise refused()

    def close(self) -> None:
        if self.descriptor is not None:
            os.close(self.descriptor)
            self.descriptor = None

    def __del__(self) -> None:
        self.close()


class FolderStore:
    def __init__(self, root: Path, bucket: str = "local-landed-copy", read_log: ReadLog | None = None) -> None:
        self.root = root
        self.bucket = bucket
        self.read_log = read_log

    def note_read(self, version: ObjectVersion) -> None:
        """Append the key and version ID of a read to the read log, when the store has one."""
        if self.read_log is not None:
            self.read_log.append(f"{version.key} {version.version_id}\n")

    def entries(self, key: str) -> list[tuple[int, bool, Path]]:
        folder = self.root
        for part in key.split("/"):
            if not list_folder(folder, glob.escape(part)):
                return []
            folder = folder / part
        found = []
        for path in list_files(folder):
            match = VERSION_FILE.fullmatch(path.name)
            if match:
                found.append((int(match.group(1)), match.group(2) is not None, path))
        return sorted(found)

    def version_of(self, key: str, number: int, marker: bool, path: Path, *, latest: bool) -> ObjectVersion:
        stamp = datetime.fromtimestamp(path.stat().st_mtime, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        return ObjectVersion(key, f"v{number:04d}", 0 if marker else path.stat().st_size, stamp, latest, marker)

    def list_versions(self, prefix: str) -> list[ObjectVersion]:
        found: list[ObjectVersion] = []
        keys = {str(path.parent.relative_to(self.root)) for pattern in ("v????", "v????.deleted") for path in list_files(self.root, pattern, recursive=True)}
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
        write_new_file(path, data, mode=0o644)
        return self.version_of(key, number, False, path, latest=True)

    def land_file(self, key: str, source: Path) -> ObjectVersion:
        """Land a copy of ``source`` as the next version of ``key``."""
        number = self.next_number(key)
        folder = self.root / key
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"v{number:04d}"
        write_new_file(path, source.read_bytes(), mode=0o644)
        return self.version_of(key, number, False, path, latest=True)
