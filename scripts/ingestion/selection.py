"""The selection of files that a check covers, which is an input and never a constant.

A selection file is JSON::

    {"name": "full_release", "release": "1.0.0", "source_bucket": "<publisher bucket>", "source_prefix": "vitaldb/1.0.0/",
     "measurement_period": "<text>", "checksum_list": {"key": "vitaldb/1.0.0/SHA256SUMS.txt", "sha256": "<64 hex>"},
     "files": ["clinical_data.csv", "lab_data.csv"],
     "case_files": {"pattern": "vital_files/{:04d}.vital", "first": 1, "last": 6388}}

``files`` and ``case_files`` are paths relative to ``source_prefix``; either may be absent, and ``case_files`` expands to one path for each number.
The checksum list is pinned by its release path and its SHA-256.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

NAME = re.compile(r"[a-z0-9][a-z0-9_]{0,63}")
SHA256 = re.compile(r"[0-9a-f]{64}")
RELATIVE_PATH = re.compile(r"[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*")


class SelectionError(ValueError):
    """The selection cannot name one set of files."""


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
        problems += [f"not a relative path: {path}" for path in self.files if not RELATIVE_PATH.fullmatch(path) or ".." in path.split("/")]
        if problems:
            raise SelectionError("; ".join(problems))

    @property
    def source_keys(self) -> tuple[str, ...]:
        return tuple(self.source_prefix + path for path in self.files)

    @property
    def keys_sha256(self) -> str:
        return hashlib.sha256(("\n".join(sorted(self.source_keys)) + "\n").encode()).hexdigest()

    def record(self) -> dict[str, Any]:
        """What the manifest records once for the set."""
        return {
            "name": self.name,
            "source_bucket": self.source_bucket,
            "source_prefix": self.source_prefix,
            "file_count": len(self.files),
            "keys_sha256": self.keys_sha256,
        }

    @classmethod
    def from_json(cls, text: str) -> Selection:
        try:
            values = json.loads(text)
            files = [*values.get("files", [])]
            cases = values.get("case_files")
            if cases:
                files += [cases["pattern"].format(number) for number in range(int(cases["first"]), int(cases["last"]) + 1)]
            return cls(
                name=values["name"],
                release=values["release"],
                source_bucket=values["source_bucket"],
                source_prefix=values["source_prefix"],
                measurement_period=values["measurement_period"],
                checksum_list_key=values["checksum_list"]["key"],
                checksum_list_sha256=values["checksum_list"]["sha256"],
                files=tuple(sorted(files)),
            )
        except (ValueError, KeyError, TypeError, AttributeError, IndexError) as error:
            if isinstance(error, SelectionError):
                raise
            raise SelectionError(f"the selection is not valid: {type(error).__name__}: {error}") from error
