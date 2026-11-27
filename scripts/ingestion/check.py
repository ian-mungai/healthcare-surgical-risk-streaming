"""The checksum check: every selected file is present as a landed object version and equal to the publisher's SHA-256.

The check is the only reader of an unverified payload. For each selected file it takes the current version of the landed key, streams exactly
that version and compares its SHA-256 with the pinned checksum list, which it first checks against its own pinned SHA-256. It reports every
fault of the set in one run, each as a ``Finding``: the file's place in the sorted selection, its ``Cause`` and, only when the pinned list names the
file's path, its source key, which is then the tracked pin's release path and a path of the pinned list. A new landed version of a key that was
already verified is a new check of that version.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field, replace
from types import MappingProxyType
from typing import Any

from scripts.ingestion.closed import ClosedInputError, closed_object, parse_json
from scripts.ingestion.landing import LANDING_PREFIX, PairingRecord
from scripts.ingestion.notice import Cause, ClosedError, Code, Notice, Subject
from scripts.ingestion.selection import Selection
from scripts.ingestion.store import NULL_VERSION_IDS, ObjectStore, ObjectVersion

LIST_LINE = re.compile(r"([0-9a-f]{64}) ([A-Za-z0-9._/-]+)")
RESULT_FORMAT = "check_result_3"
RESULT_FIELDS: Mapping[str, type] = MappingProxyType(
    {
        "format": str,
        "selection_name": str,
        "selection_sha256": str,
        "list_sha256": str,
        "file_count": int,
        "files": list,
        "findings": list,
        "result_sha256": str,
    }
)
FINDING_FIELDS: Mapping[str, type] = MappingProxyType({"position": int, "cause": str, "source_key": str})


class PinnedListError(ClosedError, ValueError):
    """The checksum list differs from its pinned SHA-256."""


class ResultFormatError(ClosedError, ValueError):
    """A check result file is not valid JSON or has an unknown, missing or mistyped field."""


class ChecksumListError(ClosedError, ValueError):
    """The checksum list is not a list of one SHA-256, one space and one relative path on each line."""


def canonical(document: Any) -> bytes:
    return (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()


def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True, slots=True)
class Finding:
    """One fault of one selected file: its place in the sorted selection, its cause and its source key when the pinned list names it, or ""."""

    position: int
    cause: Cause
    source_key: str = ""

    @property
    def notice(self) -> Notice:
        if self.source_key:
            return Notice(Code.FINDING_KEY, key=self.source_key, cause=self.cause)
        return Notice(Code.FINDING_POSITION, position=self.position, cause=self.cause)


@dataclass(frozen=True, slots=True)
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


CHECKED_FILE_FIELDS: Mapping[str, type] = MappingProxyType(
    {"bucket": str, "key": str, "version_id": str, "sha256": str, "source_key": str, "size": int, "release": str, "retrieved_utc": str}
)


def checked_file_from(entry: object, what: Subject) -> CheckedFile:
    """A ``CheckedFile`` from a JSON object with exactly its fields and types. Raise ``ClosedInputError`` otherwise."""
    return CheckedFile(**closed_object(entry, CHECKED_FILE_FIELDS, what))


def result_digest(selection_sha256: str, list_sha256: str, files: Iterable[Mapping[str, Any]]) -> str:
    """The SHA-256 that identifies a passed check: its selection, its pinned list and the versions it verified, in source key order."""
    ordered = sorted((dict(file) for file in files), key=lambda file: str(file["source_key"]))
    return sha256_of(canonical({"selection_sha256": selection_sha256, "list_sha256": list_sha256, "files": ordered}))


@dataclass(frozen=True, slots=True)
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
            "format": RESULT_FORMAT,
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
            parsed = parse_json(text, Subject.RESULT)
            if type(parsed) is dict and parsed.get("format") != RESULT_FORMAT:
                raise ResultFormatError(Notice(Code.RESULT_EARLIER))
            document = closed_object(parsed, RESULT_FIELDS, Subject.RESULT)
            findings = [closed_object(finding, FINDING_FIELDS, Subject.RESULT_FINDING) for finding in document["findings"]]
            if any(finding["cause"] not in set(Cause) for finding in findings):
                raise ResultFormatError(Notice(Code.FINDING_CAUSE))
            return cls(
                document["selection_name"],
                document["selection_sha256"],
                document["list_sha256"],
                document["file_count"],
                tuple(checked_file_from(file, Subject.RESULT_FILE) for file in document["files"]),
                tuple(Finding(finding["position"], Cause(finding["cause"]), finding["source_key"]) for finding in findings),
                document["result_sha256"],
            )
        except ClosedInputError as error:
            raise ResultFormatError(*error.notices) from None


def parse_checksum_list(data: bytes) -> dict[str, str]:
    """The publisher's list as a mapping of relative path to SHA-256. A line that is not one hash, one space and one path stops the check."""
    try:
        text = data.decode()
    except UnicodeDecodeError:
        raise ChecksumListError(Notice(Code.LIST_NOT_TEXT)) from None
    lines = text.split("\n")
    if lines[-1] == "":
        lines.pop()
    listed: dict[str, str] = {}
    for number, line in enumerate(lines, 1):
        match = LIST_LINE.fullmatch(line)
        if match is None:
            raise ChecksumListError(Notice(Code.LIST_LINE_FORM, position=number))
        if match.group(2) in listed:
            raise ChecksumListError(Notice(Code.LIST_LINE_REPEAT, position=number))
        listed[match.group(2)] = match.group(1)
    if not listed:
        raise ChecksumListError(Notice(Code.LIST_EMPTY))
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
    file, a landed key that another source is also paired with (a landed version stands for one source), a landed key with no current object and a record
    whose size differs from the landed object each fail that file without reading it.
    A finding holds a file's source key only when the pinned list names its path: the selection's source prefix is the tracked pin's, so that key is
    verified. Any other file is named by its place in the sorted selection, so a value that nothing verified never reaches a message or the result.
    """
    list_sha256 = sha256_of(list_bytes)
    if list_sha256 != selection.checksum_list_sha256:
        raise PinnedListError(Notice(Code.LIST_NOT_PINNED))
    listed = parse_checksum_list(list_bytes)
    current = current_versions(store.list_versions(LANDING_PREFIX))
    recorded: dict[str, list[PairingRecord]] = defaultdict(list)
    for record in pairings:
        recorded[record.source_key].append(record)
    sources_of: dict[str, set[str]] = defaultdict(set)
    for record in pairings:
        sources_of[record.landed_key].add(record.source_key)
    files: list[CheckedFile] = []
    findings: list[Finding] = []
    for done, path in enumerate(selection.files, 1):
        source_key = selection.source_prefix + path
        verified = source_key if path in listed else ""
        records = recorded.get(source_key, [])
        version = current.get(records[0].landed_key) if len(records) == 1 else None
        if path not in listed:
            findings.append(Finding(done, Cause.NOT_IN_CHECKSUM_LIST))
        if not records:
            findings.append(Finding(done, Cause.MISSING, verified))
        elif len(records) > 1:
            findings.append(Finding(done, Cause.DUPLICATE_PAIRING, verified))
        elif len(sources_of[records[0].landed_key]) > 1:
            findings.append(Finding(done, Cause.LANDED_SHARED, verified))
        elif version is None or version.is_delete_marker:
            findings.append(Finding(done, Cause.LANDED_OBJECT_MISSING, verified))
        elif version.version_id in NULL_VERSION_IDS:
            findings.append(Finding(done, Cause.NULL_VERSION_ID, verified))
        elif version.size != records[0].size:
            findings.append(Finding(done, Cause.PAIRING_SIZE_DIFFERS, verified))
        elif path in listed:
            found = stream_sha256(store, version)
            if found == listed[path]:
                files.append(
                    CheckedFile(store.bucket, version.key, version.version_id, found, source_key, version.size, selection.release, version.last_modified)
                )
            else:
                findings.append(Finding(done, Cause.CHECKSUM_DIFFERS, verified))
        if progress:
            progress(done, len(selection.files))
    result = CheckResult(selection.name, selection.selection_sha256, list_sha256, len(selection.files), tuple(files), tuple(findings))
    return replace(result, claimed_sha256=result.result_sha256 if result.passed else "")
