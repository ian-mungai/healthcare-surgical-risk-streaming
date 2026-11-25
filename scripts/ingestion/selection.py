"""The selection of files that a check covers, which is an input and never a constant.

A selection file is JSON::

    {"name": "full_release", "release": "1.0.0", "source_bucket": "<publisher bucket>", "source_prefix": "vitaldb/1.0.0/",
     "measurement_period": "<text>", "checksum_list": {"key": "vitaldb/1.0.0/SHA256SUMS.txt", "sha256": "<64 hex>"},
     "files": ["clinical_data.csv", "lab_data.csv"],
     "case_files": {"pattern": "vital_files/{:04d}.vital", "first": 1, "last": 6388}}

``files`` and ``case_files`` are paths relative to ``source_prefix``; either may be absent, and ``case_files`` expands to one path for each number from
``first`` to ``last``. The pattern is one known form: a path with one ``{:0Nd}`` place for the number. The checksum list is pinned by its key and its SHA-256.

Every selection is of the one release that the tracked pin (``tests/fixtures/vitaldb/release.json``) names: its release, source bucket, source prefix,
checksum list key and checksum list SHA-256 are the pin's, so the list a check verifies against is always the publisher's own, never a substitute, and a
source key that a check reports is the pin's release path and a path of that list. The case numbers of ``case_files`` are within the release's 1 to
6,388. A selection named ``full_release`` is the whole release and is not an input to shorten: its files are exactly the two tables and the 6,388 case
files.
"""

from __future__ import annotations

import functools
import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Any

from scripts.ingestion.closed import ClosedInputError, closed_object, parse_json
from scripts.ingestion.notice import ClosedError, Code, Notice, Subject
from scripts.sample.candidates import RELEASE_FILE, release_problems

NAME = re.compile(r"[a-z0-9][a-z0-9_]{0,63}")
RELATIVE_PATH = re.compile(r"[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*")
CASE_FORMAT = re.compile(r"([A-Za-z0-9._/-]*)\{:0([1-9])d\}([A-Za-z0-9._/-]*)")
FULL_RELEASE = "full_release"
FULL_RELEASE_TABLES = ("clinical_data.csv", "lab_data.csv")
FULL_RELEASE_CASES = 6388
FULL_RELEASE_CASE_FORMAT = "vital_files/{:04d}.vital"
SOURCE_FIELDS = ("release", "source_bucket", "source_prefix", "checksum_list_key", "checksum_list_sha256")
SELECTION_FIELDS: Mapping[str, type] = MappingProxyType(
    {"name": str, "release": str, "source_bucket": str, "source_prefix": str, "measurement_period": str, "checksum_list": dict}
)
SELECTION_OPTIONAL: Mapping[str, type] = MappingProxyType({"files": list, "case_files": dict})
LIST_PIN_FIELDS: Mapping[str, type] = MappingProxyType({"key": str, "sha256": str})
CASE_FILES_FIELDS: Mapping[str, type] = MappingProxyType({"pattern": str, "first": int, "last": int})


class SelectionError(ClosedError, ValueError):
    """The selection cannot name one set of files."""


def relative_path(text: str) -> bool:
    """True for a relative path of letters, digits, dots, dashes and underscores, with no empty, ``.`` or ``..`` part."""
    return RELATIVE_PATH.fullmatch(text) is not None and not {".", ".."} & set(text.split("/"))


def case_paths(pattern: str, first: int, last: int) -> list[str]:
    """One path for each number from ``first`` to ``last``. Raise ``SelectionError`` unless the pattern is the known form and the range is in the release."""
    form = CASE_FORMAT.fullmatch(pattern)
    if form is None:
        raise SelectionError(Notice(Code.CASE_PATTERN))
    if not 1 <= first <= last <= FULL_RELEASE_CASES:
        raise SelectionError(Notice(Code.CASE_RANGE, total=FULL_RELEASE_CASES))
    head, width, tail = form.groups()
    return [f"{head}{number:0{width}d}{tail}" for number in range(first, last + 1)]


def keys_digest(source_keys: Iterable[str]) -> str:
    """The SHA-256 of a set of source keys, in sorted order, one on each line."""
    return hashlib.sha256(("\n".join(sorted(source_keys)) + "\n").encode()).hexdigest()


def selection_digest(
    name: str, release: str, source_bucket: str, source_prefix: str, measurement_period: str, list_key: str, list_sha256: str, keys_sha256: str
) -> str:
    """The SHA-256 of every field of a selection. The selection computes it to bind a check, and a manifest's reader recomputes it from the manifest."""
    document = {
        "name": name,
        "release": release,
        "source_bucket": source_bucket,
        "source_prefix": source_prefix,
        "measurement_period": measurement_period,
        "checksum_list": {"key": list_key, "sha256": list_sha256},
        "keys_sha256": keys_sha256,
    }
    return hashlib.sha256((json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class PinnedRelease:
    """The fields of the release that the tracked pin fixes, under the names of the selection's fields.

    Frozen with slots, so neither an assignment nor ``vars()`` can change the cached pin.
    """

    release: str
    source_bucket: str
    source_prefix: str
    checksum_list_key: str
    checksum_list_sha256: str


def pinned_release() -> PinnedRelease:
    """The tracked release pin, checked for its form, as a new frozen record for each caller. Raise ``SelectionError`` when it is not valid.

    The cache holds a tuple of text, which nothing can change, so not even ``object.__setattr__`` on a returned record reaches the next caller.
    """
    return PinnedRelease(*pinned_values())


@functools.cache
def pinned_values() -> tuple[str, str, str, str, str]:
    try:
        pin = parse_json(RELEASE_FILE.read_text(encoding="utf-8"), Subject.TRACKED_PIN)
    except ClosedInputError, OSError, UnicodeDecodeError:
        raise SelectionError(Notice(Code.PIN_UNREADABLE)) from None
    problems = release_problems(pin)
    if problems:
        raise SelectionError(Notice(Code.PIN_INVALID), *problems)
    source, listed = pin["source"], pin["checksum_list"]
    return PurePosixPath(source["release_path"]).name, source["bucket"], source["release_path"], listed["key"], listed["sha256"]


@functools.cache
def full_release_files() -> tuple[str, ...]:
    return tuple(sorted([*FULL_RELEASE_TABLES, *case_paths(FULL_RELEASE_CASE_FORMAT, 1, FULL_RELEASE_CASES)]))


@dataclass(frozen=True, slots=True)
class Selection:
    name: str
    release: str
    source_bucket: str
    source_prefix: str
    measurement_period: str
    checksum_list_key: str
    checksum_list_sha256: str
    files: tuple[str, ...]

    def __post_init__(self) -> None:
        problems = []
        if not NAME.fullmatch(self.name):
            problems.append(Notice(Code.NAME_FORM))
        if not self.measurement_period:
            problems.append(Notice(Code.PERIOD_EMPTY))
        pin = pinned_release()
        problems += [Notice(Code.SOURCE_NOT_PIN, fields=(name,)) for name in SOURCE_FIELDS if getattr(self, name) != getattr(pin, name)]
        if not self.files:
            problems.append(Notice(Code.NO_FILES))
        if len(set(self.files)) != len(self.files):
            problems.append(Notice(Code.FILE_TWICE))
        problems += [Notice(Code.FILE_NOT_RELATIVE, position=position) for position, path in enumerate(self.files, 1) if not relative_path(path)]
        object.__setattr__(self, "files", tuple(sorted(self.files)))
        if self.name == FULL_RELEASE and not problems:
            problems += self.full_release_problems()
        if problems:
            raise SelectionError(*problems)

    def full_release_problems(self) -> list[Notice]:
        """What differs from the whole release as the 6,390 keys define it; its source, list included, is already held to the tracked pin."""
        return [] if self.files == full_release_files() else [Notice(Code.FULL_RELEASE_FILES, total=FULL_RELEASE_CASES)]

    @property
    def source_keys(self) -> tuple[str, ...]:
        return tuple(self.source_prefix + path for path in self.files)

    @property
    def keys_sha256(self) -> str:
        return keys_digest(self.source_keys)

    @property
    def selection_sha256(self) -> str:
        """The SHA-256 of every field of the selection: the identity that a check result is bound to, so that a publication cannot change any of them."""
        return selection_digest(
            self.name,
            self.release,
            self.source_bucket,
            self.source_prefix,
            self.measurement_period,
            self.checksum_list_key,
            self.checksum_list_sha256,
            self.keys_sha256,
        )

    def record(self) -> dict[str, Any]:
        """What the manifest records once for the set."""
        return {
            "name": self.name,
            "source_bucket": self.source_bucket,
            "source_prefix": self.source_prefix,
            "file_count": len(self.files),
            "keys_sha256": self.keys_sha256,
            "selection_sha256": self.selection_sha256,
        }

    @classmethod
    def from_json(cls, text: str) -> Selection:
        try:
            values = closed_object(parse_json(text, Subject.SELECTION), SELECTION_FIELDS, Subject.SELECTION, SELECTION_OPTIONAL)
            listed = closed_object(values["checksum_list"], LIST_PIN_FIELDS, Subject.SELECTION_LIST)
            files = list(values.get("files", []))
            if any(type(path) is not str for path in files):
                raise SelectionError(Notice(Code.FILES_NOT_TEXT))
            if "case_files" in values:
                cases = closed_object(values["case_files"], CASE_FILES_FIELDS, Subject.SELECTION_CASES)
                files += case_paths(cases["pattern"], cases["first"], cases["last"])
            return cls(
                name=values["name"],
                release=values["release"],
                source_bucket=values["source_bucket"],
                source_prefix=values["source_prefix"],
                measurement_period=values["measurement_period"],
                checksum_list_key=listed["key"],
                checksum_list_sha256=listed["sha256"],
                files=tuple(files),
            )
        except ClosedInputError as error:
            raise SelectionError(*error.notices) from None
