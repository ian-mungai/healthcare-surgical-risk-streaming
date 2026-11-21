"""Shared set-up for the fast tests of the ingestion checks: a small release in the stand-in store, with the publisher's checksum list for it."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass

from scripts.ingestion.check import CheckResult, run_check
from scripts.ingestion.landing import PairingRecord
from scripts.ingestion.selection import Selection
from scripts.ingestion.store import ObjectVersion
from tests.fast.memory_store import MemoryStore

SOURCE_PREFIX = "vitaldb/1.0.0/"
SOURCE = {
    "vital_files/0001.vital": b"recording one, with enough bytes to need several blocks",
    "vital_files/0002.vital": b"recording two, also longer than one block of the stand-in",
    "clinical_data.csv": b"caseid,age\n1,60\n2,71\n",
}


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def list_text_for(source: Mapping[str, bytes]) -> str:
    """The publisher's list: the SHA-256 of each file's source bytes, one space, the path relative to the release."""
    return "".join(f"{digest(data)} {path}\n" for path, data in sorted(source.items()))


def selection_for(paths: list[str], list_bytes: bytes, name: str = "probe_set") -> Selection:
    return Selection(
        name=name,
        release="1.0.0",
        source_bucket="example-publisher",
        source_prefix=SOURCE_PREFIX,
        measurement_period="2016-08 to 2017-06",
        checksum_list_key=f"{SOURCE_PREFIX}SHA256SUMS.txt",
        checksum_list_sha256=digest(list_bytes),
        files=tuple(sorted(paths)),
    )


@dataclass
class Landed:
    store: MemoryStore
    selection: Selection
    list_bytes: bytes
    versions: dict[str, ObjectVersion]
    keys: dict[str, str]
    sizes: dict[str, int]

    def land(self, path: str, data: bytes) -> ObjectVersion:
        """Land a version of a file. The rule of the tests: each file lands in a folder of its own, as if each came from its own sync."""
        key = self.keys.setdefault(path, f"landing/sync_{len(self.keys):02d}/{SOURCE_PREFIX}{path}")
        version = self.store.put_object(key, data)
        self.versions[path] = version
        self.sizes[path] = len(data)
        return version

    def key_for(self, source_key: str) -> str:
        return self.keys[source_key.removeprefix(SOURCE_PREFIX)]

    def pairings(self) -> list[PairingRecord]:
        return [PairingRecord(SOURCE_PREFIX + path, key, self.sizes[path]) for path, key in self.keys.items()]

    def check(self) -> CheckResult:
        return run_check(self.store, self.selection, self.list_bytes, self.pairings())


def landed_set(source: Mapping[str, bytes] = SOURCE, *, landed: Mapping[str, bytes] | None = None, skip: tuple[str, ...] = ()) -> Landed:
    """Land every file of ``source`` (or the bytes in ``landed`` where a file is planted changed), except those in ``skip``."""
    list_bytes = list_text_for(source).encode()
    store = MemoryStore()
    result = Landed(store, selection_for(list(source), list_bytes), list_bytes, {}, {}, {})
    for path, data in source.items():
        if path not in skip:
            result.land(path, (landed or {}).get(path, data))
    return result
