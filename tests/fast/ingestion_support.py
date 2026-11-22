"""Shared set-up for the fast tests of the ingestion checks: a small release in the stand-in store, with the publisher's checksum list for it."""

from __future__ import annotations

import hashlib
import unittest
from collections.abc import Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any
from unittest import mock

from scripts.ingestion import selection
from scripts.ingestion.check import CheckResult, Finding, run_check
from scripts.ingestion.landing import PairingRecord
from scripts.ingestion.selection import Selection
from scripts.ingestion.store import ObjectVersion
from tests.fast.memory_store import MemoryStore

SOURCE_PREFIX = "vitaldb/1.0.0/"
PUBLISHER_BUCKET = "physionet-open"
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


TRACKED_PIN_VALUES = selection.pinned_values


def tracked_pin() -> AbstractContextManager[Any]:
    """The tracked pin itself, inside a test module that ``pin_the_synthetic_list`` holds to a synthetic release."""
    return mock.patch.object(selection, "pinned_values", TRACKED_PIN_VALUES)


def synthetic_pin(list_bytes: bytes) -> AbstractContextManager[Any]:
    """The tracked pin with the SHA-256 of a synthetic release's checksum list in the place of the publisher's list.

    The repository does not hold the publisher's list, so a test of a synthetic release pins the list it built, and every other field stays the
    tracked pin's. Nothing outside the fast tests replaces the pin.
    """
    release, bucket, prefix, key, _ = TRACKED_PIN_VALUES()
    return mock.patch.object(selection, "pinned_values", return_value=(release, bucket, prefix, key, digest(list_bytes)))


def pin_the_synthetic_list(list_bytes: bytes | None = None) -> None:
    """Hold a test module to the checksum list of its synthetic release, ``SOURCE``'s by default. Call it from ``setUpModule``; the module's cleanup ends it."""
    unittest.enterModuleContext(synthetic_pin(list_text_for(SOURCE).encode() if list_bytes is None else list_bytes))


def selection_for(paths: list[str], list_bytes: bytes, name: str = "probe_set") -> Selection:
    return Selection(
        name=name,
        release="1.0.0",
        source_bucket=PUBLISHER_BUCKET,
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


def finding_label(finding: Finding) -> str:
    """How a message names the file of a finding: its source key when the pinned list names it, its place in the selection otherwise."""
    return finding.source_key or f"selected file {finding.position}"
