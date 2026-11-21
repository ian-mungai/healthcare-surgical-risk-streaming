"""The checksum check: every selected file is present as a landed object version and equal to the publisher's SHA-256.

The check is the only reader of an unverified payload. For each selected file it takes the current version of the landed key, streams exactly
that version and compares its SHA-256 with the pinned checksum list, which it first checks against its own pinned SHA-256. It reports every
fault of the set in one run, each as a key and a cause, and never prints a credential or an account value. A new landed version of a key that
was already verified is a new check of that version.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field, replace
from typing import Any

from scripts.ingestion.closed import ClosedInputError, closed_object, parse_json
from scripts.ingestion.landing import LANDING_PREFIX, PairingRecord
from scripts.ingestion.selection import Selection
from scripts.ingestion.store import ObjectStore, ObjectVersion

LIST_LINE = re.compile(r"([0-9a-f]{64}) ([A-Za-z0-9._/-]+)")
MISSING = "missing"
DUPLICATE_PAIRING = "duplicate_pairing"
LANDED_OBJECT_MISSING = "landed_object_missing"
PAIRING_SIZE_DIFFERS = "pairing_size_differs"
CHECKSUM_DIFFERS = "checksum_differs"
NOT_IN_CHECKSUM_LIST = "not_in_checksum_list"
NULL_VERSION_ID = "null_version_id"


class PinnedListError(ValueError):
    """The checksum list differs from its pinned SHA-256."""


class ResultFormatError(ValueError):
    """A check result file is not valid JSON or has an unknown, missing or mistyped field."""


class ChecksumListError(ValueError):
    """The checksum list is not a list of one SHA-256, one space and one relative path on each line."""


def canonical(document: Any) -> bytes:
    return (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()


def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class Finding:
    source_key: str
    cause: str
    detail: str = ""

    @property
    def message(self) -> str:
        return f"{self.source_key}: {self.cause}" + (f" ({self.detail})" if self.detail else "")


@dataclass(frozen=True)
class CheckedFile:
    """One landed object version that passed, with the fields the manifest binds."""

    bucket: str
    key: str
    version_id: str
    sha256: str
    source_key: str
    size: int
    release: str
    retrieved_utc: str


CHECKED_FILE_FIELDS: dict[str, type] = {
    "bucket": str,
    "key": str,
    "version_id": str,
    "sha256": str,
    "source_key": str,
    "size": int,
    "release": str,
    "retrieved_utc": str,
}


def checked_file_from(entry: object, what: str) -> CheckedFile:
    """A ``CheckedFile`` from a JSON object with exactly its fields and types. Raise ``ClosedInputError`` otherwise."""
    return CheckedFile(**closed_object(entry, CHECKED_FILE_FIELDS, what))


def result_digest(selection_sha256: str, list_sha256: str, files: Iterable[Mapping[str, Any]]) -> str:
    """The SHA-256 that identifies a passed check: its selection, its pinned list and the versions it verified, in source key order."""
    ordered = sorted((dict(file) for file in files), key=lambda file: str(file["source_key"]))
    return sha256_of(canonical({"selection_sha256": selection_sha256, "list_sha256": list_sha256, "files": ordered}))


@dataclass(frozen=True)
class CheckResult:
    selection_name: str
    selection_sha256: str
    list_sha256: str
    file_count: int
    files: tuple[CheckedFile, ...]
    findings: tuple[Finding, ...]
    claimed_sha256: str = field(default="", compare=False)

    @property
    def passed(self) -> bool:
        return not self.findings and self.file_count > 0 and len(self.files) == self.file_count

    @property
    def result_sha256(self) -> str:
        return result_digest(self.selection_sha256, self.list_sha256, (asdict(file) for file in self.files))

    def to_json(self) -> str:
        document = {
            "selection_name": self.selection_name,
            "selection_sha256": self.selection_sha256,
            "list_sha256": self.list_sha256,
            "file_count": self.file_count,
            "files": [asdict(file) for file in self.files],
            "findings": [asdict(finding) for finding in self.findings],
            "result_sha256": self.claimed_sha256,
        }
        return json.dumps(document, indent=1, sort_keys=True) + "\n"

    @classmethod
    def from_json(cls, text: str) -> CheckResult:
        try:
            document = closed_object(
                parse_json(text, "the check result"),
                {"selection_name": str, "selection_sha256": str, "list_sha256": str, "file_count": int, "files": list, "findings": list, "result_sha256": str},
                "the check result",
            )
            findings = [
                closed_object(finding, {"source_key": str, "cause": str, "detail": str}, "a finding of the check result") for finding in document["findings"]
            ]
            return cls(
                document["selection_name"],
                document["selection_sha256"],
                document["list_sha256"],
                document["file_count"],
                tuple(checked_file_from(file, "a file of the check result") for file in document["files"]),
                tuple(Finding(**finding) for finding in findings),
                document["result_sha256"],
            )
        except ClosedInputError as error:
            raise ResultFormatError(str(error)) from None


def parse_checksum_list(data: bytes) -> dict[str, str]:
    """The publisher's list as a mapping of relative path to SHA-256. A line that is not one hash, one space and one path stops the check."""
    try:
        text = data.decode()
    except UnicodeDecodeError as error:
        raise ChecksumListError("the checksum list is not UTF-8 text") from error
    lines = text.split("\n")
    if lines[-1] == "":
        lines.pop()
    listed: dict[str, str] = {}
    for number, line in enumerate(lines, 1):
        match = LIST_LINE.fullmatch(line)
        if match is None:
            raise ChecksumListError(f"line {number} of the checksum list is not a SHA-256, one space and a path")
        if match.group(2) in listed:
            raise ChecksumListError(f"line {number} of the checksum list repeats a path")
        listed[match.group(2)] = match.group(1)
    if not listed:
        raise ChecksumListError("the checksum list is empty")
    return listed


def current_versions(versions: Iterable[ObjectVersion]) -> dict[str, ObjectVersion]:
    return {version.key: version for version in versions if version.is_latest}


def stream_sha256(store: ObjectStore, version: ObjectVersion) -> str:
    digest = hashlib.sha256()
    for block in store.iter_version(version):
        digest.update(block)
    return digest.hexdigest()


def run_check(
    store: ObjectStore, selection: Selection, list_bytes: bytes, pairings: Sequence[PairingRecord], progress: Callable[[int, int], None] | None = None
) -> CheckResult:
    """Check every selected file of the set and return the result. Raise ``PinnedListError`` before any read when the list is not the pinned one.

    ``pairings`` pairs each source key with the landed key and size that its sync wrote. A file with no record is missing; two records for one
    file, a landed key with no current object and a record whose size differs from the landed object each fail that file without reading it.
    """
    list_sha256 = sha256_of(list_bytes)
    if list_sha256 != selection.checksum_list_sha256:
        raise PinnedListError("the checksum list's SHA-256 differs from the pinned SHA-256")
    listed = parse_checksum_list(list_bytes)
    current = current_versions(store.list_versions(LANDING_PREFIX))
    recorded: dict[str, list[PairingRecord]] = defaultdict(list)
    for record in pairings:
        recorded[record.source_key].append(record)
    files: list[CheckedFile] = []
    findings: list[Finding] = []
    for done, path in enumerate(selection.files, 1):
        source_key = selection.source_prefix + path
        records = recorded.get(source_key, [])
        version = current.get(records[0].landed_key) if len(records) == 1 else None
        if path not in listed:
            findings.append(Finding(source_key, NOT_IN_CHECKSUM_LIST))
        if not records:
            findings.append(Finding(source_key, MISSING, "no pairing record"))
        elif len(records) > 1:
            findings.append(Finding(source_key, DUPLICATE_PAIRING, f"{len(records)} pairing records, for example from a second sync"))
        elif version is None or version.is_delete_marker:
            findings.append(Finding(source_key, LANDED_OBJECT_MISSING, "the landed key has no current object"))
        elif version.version_id in ("", "null"):
            findings.append(Finding(source_key, NULL_VERSION_ID))
        elif version.size != records[0].size:
            findings.append(Finding(source_key, PAIRING_SIZE_DIFFERS, f"record {records[0].size} bytes, landed object {version.size} bytes"))
        elif path in listed:
            found = stream_sha256(store, version)
            if found == listed[path]:
                files.append(
                    CheckedFile(store.bucket, version.key, version.version_id, found, source_key, version.size, selection.release, version.last_modified)
                )
            else:
                findings.append(Finding(source_key, CHECKSUM_DIFFERS, f"landed {found[:12]}, listed {listed[path][:12]}"))
        if progress:
            progress(done, len(selection.files))
    result = CheckResult(selection.name, selection.selection_sha256, list_sha256, len(selection.files), tuple(files), tuple(findings))
    return replace(result, claimed_sha256=result.result_sha256 if result.passed else "")
