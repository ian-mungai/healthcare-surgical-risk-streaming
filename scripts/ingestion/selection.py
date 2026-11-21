"""The selection of files that a check covers, which is an input and never a constant.

A selection file is JSON::

    {"name": "full_release", "release": "1.0.0", "source_bucket": "<publisher bucket>", "source_prefix": "vitaldb/1.0.0/",
     "measurement_period": "<text>", "checksum_list": {"key": "vitaldb/1.0.0/SHA256SUMS.txt", "sha256": "<64 hex>"},
     "files": ["clinical_data.csv", "lab_data.csv"],
     "case_files": {"pattern": "vital_files/{:04d}.vital", "first": 1, "last": 6388}}

``files`` and ``case_files`` are paths relative to ``source_prefix``; either may be absent, and ``case_files`` expands to one path for each number from
``first`` to ``last``. The pattern is one known form: a path with one ``{:0Nd}`` place for the number. The checksum list is pinned by its key and its SHA-256.

A selection named ``full_release`` is the whole release and is not an input to shorten: its files are exactly the two tables and the 6,388 case files, and its
source, release and checksum list are the tracked pin's (``tests/fixtures/vitaldb/release.json``), so its list digest never comes from the caller.
"""

from __future__ import annotations

import functools
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from pathlib import PurePosixPath
from typing import Any

from scripts.ingestion.closed import ClosedInputError, closed_object, parse_json
from scripts.sample.candidates import RELEASE_FILE, release_problems

NAME = re.compile(r"[a-z0-9][a-z0-9_]{0,63}")
SHA256 = re.compile(r"[0-9a-f]{64}")
RELATIVE_PATH = re.compile(r"[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*")
CASE_FORMAT = re.compile(r"([A-Za-z0-9._/-]*)\{:0([1-9])d\}([A-Za-z0-9._/-]*)")
FULL_RELEASE = "full_release"
FULL_RELEASE_TABLES = ("clinical_data.csv", "lab_data.csv")
FULL_RELEASE_CASES = 6388
FULL_RELEASE_CASE_FORMAT = "vital_files/{:04d}.vital"


class SelectionError(ValueError):
    """The selection cannot name one set of files."""


def case_paths(pattern: str, first: int, last: int) -> list[str]:
    """One path for each number from ``first`` to ``last``. Raise ``SelectionError`` unless the pattern is the known form and ``0 <= first <= last``."""
    form = CASE_FORMAT.fullmatch(pattern)
    if form is None:
        raise SelectionError("the selection's case_files pattern is not a path with one {:0Nd} place for the number")
    if not 0 <= first <= last:
        raise SelectionError("the selection's case_files must have 0 <= first <= last")
    head, width, tail = form.groups()
    return [f"{head}{number:0{width}d}{tail}" for number in range(first, last + 1)]


@dataclass(frozen=True)
class PinnedRelease:
    """The fields of a full release that the tracked pin fixes, under the names of the selection's fields. Frozen, so no caller can change the cached pin."""

    release: str
    source_bucket: str
    source_prefix: str
    checksum_list_key: str
    checksum_list_sha256: str


@functools.cache
def pinned_release() -> PinnedRelease:
    """The tracked release pin, checked for its form. Raise ``SelectionError`` when it is not valid."""
    try:
        pin = parse_json(RELEASE_FILE.read_text(), "the tracked release pin")
    except (ClosedInputError, OSError, UnicodeDecodeError) as error:
        raise SelectionError(f"the tracked release pin cannot be read: {type(error).__name__}") from None
    problems = release_problems(pin)
    if problems:
        raise SelectionError("the tracked release pin is not valid: " + "; ".join(problems))
    source, listed = pin["source"], pin["checksum_list"]
    return PinnedRelease(PurePosixPath(source["release_path"]).name, source["bucket"], source["release_path"], listed["key"], listed["sha256"])


@functools.cache
def full_release_files() -> tuple[str, ...]:
    return tuple(sorted([*FULL_RELEASE_TABLES, *case_paths(FULL_RELEASE_CASE_FORMAT, 1, FULL_RELEASE_CASES)]))


@dataclass(frozen=True)
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
            problems.append("name must be lower-case letters, digits and underscores")
        if not (self.release and self.source_bucket and self.measurement_period and self.checksum_list_key):
            problems.append("release, source_bucket, measurement_period and the checksum list key must not be empty")
        if not self.source_prefix.endswith("/") or self.source_prefix.startswith("/"):
            problems.append("source_prefix must end with a slash and not start with one")
        if not SHA256.fullmatch(self.checksum_list_sha256):
            problems.append("the checksum list's sha256 must be 64 lower-case hex digits")
        if not self.files:
            problems.append("the selection names no file")
        if len(set(self.files)) != len(self.files):
            problems.append("a file is named twice")
        problems += [
            f"files[{position}] is not a relative path"
            for position, path in enumerate(self.files, 1)
            if not RELATIVE_PATH.fullmatch(path) or ".." in path.split("/")
        ]
        object.__setattr__(self, "files", tuple(sorted(self.files)))
        if self.name == FULL_RELEASE and not problems:
            problems += self.full_release_problems()
        if problems:
            raise SelectionError("; ".join(problems))

    def full_release_problems(self) -> list[str]:
        """What differs from the whole release as the tracked pin and the 6,390 keys define it. Field names only."""
        problems = [f"the full release's {name} is not the tracked pin's" for name, value in asdict(pinned_release()).items() if getattr(self, name) != value]
        if self.files != full_release_files():
            problems.append(f"the full release's files are not exactly {', '.join(FULL_RELEASE_TABLES)} and {FULL_RELEASE_CASES} case files")
        return problems

    @property
    def source_keys(self) -> tuple[str, ...]:
        return tuple(self.source_prefix + path for path in self.files)

    @property
    def keys_sha256(self) -> str:
        return hashlib.sha256(("\n".join(sorted(self.source_keys)) + "\n").encode()).hexdigest()

    @property
    def selection_sha256(self) -> str:
        """The SHA-256 of every field of the selection: the identity that a check result is bound to, so that a publication cannot change any of them."""
        document = {
            "name": self.name,
            "release": self.release,
            "source_bucket": self.source_bucket,
            "source_prefix": self.source_prefix,
            "measurement_period": self.measurement_period,
            "checksum_list": {"key": self.checksum_list_key, "sha256": self.checksum_list_sha256},
            "keys_sha256": self.keys_sha256,
        }
        return hashlib.sha256((json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()).hexdigest()

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
            values = closed_object(
                parse_json(text, "the selection"),
                {"name": str, "release": str, "source_bucket": str, "source_prefix": str, "measurement_period": str, "checksum_list": dict},
                "the selection",
                {"files": list, "case_files": dict},
            )
            listed = closed_object(values["checksum_list"], {"key": str, "sha256": str}, "the selection's checksum_list")
            files = list(values.get("files", []))
            if any(type(path) is not str for path in files):
                raise SelectionError("the selection's files must all be text")
            if "case_files" in values:
                cases = closed_object(values["case_files"], {"pattern": str, "first": int, "last": int}, "the selection's case_files")
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
            raise SelectionError(str(error)) from None
