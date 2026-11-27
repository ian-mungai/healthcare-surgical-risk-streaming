"""The pinned track list: the row count and the digest of its rows, and the check of a file against them.

The track list is the publisher's table of every track of every case (``caseid``, ``tname``, ``tid``). Airbyte's File source loads its rows as the table
``track_list``. The pin in ``tests/fixtures/vitaldb/release.json`` holds the row count and the digest that the loaded rows must equal. The digest is the
SHA-256 over the rows sorted by their full text, each row as its fields joined by a tab, the lines joined by a newline with none after the last line. The
header is not a row. A field with a tab or a line break is refused, because the joiners would make the text ambiguous.

``.venv/bin/python -m scripts.ingestion.track_list check --file track_list.csv`` checks a downloaded file against the pin and exits 1 on any difference.
"""

from __future__ import annotations

import csv
import hashlib
import json
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

from scripts.ingestion.closed import QuietParser
from scripts.ingestion.notice import describe_error
from scripts.sample.candidates import RELEASE_FILE, release_problems
from scripts.sample.publisher import TRACK_LIST_COLUMNS

PROGRAM = "scripts.ingestion.track_list"


@dataclass(frozen=True, slots=True)
class PinnedTrackList:
    url: str
    rows: int
    rows_sha256: str


class TrackListPinError(ValueError):
    """The tracked release pin is not valid, so it holds no usable row count and digest."""


class TrackListRowError(ValueError):
    """A row holds a field with a tab or a line break, which the digest's joiners would make ambiguous. The message names the row and never a value."""


def row_text(fields: Sequence[str]) -> str:
    return "\t".join(fields)


def rows_digest(rows: Iterable[Sequence[str]]) -> tuple[int, str]:
    """The number of rows and the digest of the rows. Rows are compared as a multiset: the order they come in does not matter.

    Raise ``TrackListRowError`` for a field with a tab or a line break (a newline or a carriage return): the tab joins a row's fields and the newline
    joins the lines, so such a field would make the text ambiguous.
    """
    checked = list(rows)
    for number, fields in enumerate(checked, 1):
        if any(character in field for field in fields for character in ("\t", "\n", "\r")):
            raise TrackListRowError(f"row {number} holds a field with a tab or a line break")
    lines = sorted(row_text(fields) for fields in checked)
    return len(lines), hashlib.sha256("\n".join(lines).encode()).hexdigest()


def pinned_track_list() -> PinnedTrackList:
    """The pinned address, row count and digest of the tracked release pin. Raise ``TrackListPinError`` when the pin is not valid."""
    try:
        release = json.loads(RELEASE_FILE.read_text(encoding="utf-8"))
    except OSError, ValueError:
        raise TrackListPinError("the tracked release pin cannot be read") from None
    if release_problems(release):
        raise TrackListPinError("the tracked release pin is not valid")
    pin = release["track_list"]
    return PinnedTrackList(pin["url"], pin["rows"], pin["rows_sha256"])


def file_problems(path: Path, pin: PinnedTrackList) -> list[str]:
    """What differs between the file and the pin, or nothing. The file has the header ``caseid,tname,tid`` and a row of three fields on every line."""
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.reader(handle)
            header = next(reader, [])
            if tuple(header) != TRACK_LIST_COLUMNS:
                return ["the header is not caseid, tname, tid"]
            rows: list[list[str]] = []
            for row in reader:
                if len(row) != len(TRACK_LIST_COLUMNS):
                    return [f"line {reader.line_num}: the row has {len(row)} fields, not {len(TRACK_LIST_COLUMNS)}"]
                rows.append(row)
    except OSError, UnicodeDecodeError, csv.Error:
        return ["the file cannot be read as CSV text"]
    try:
        count, digest = rows_digest(rows)
    except TrackListRowError as error:
        return [str(error)]
    problems = []
    if count != pin.rows:
        problems.append(f"the file has {count} rows, the pin {pin.rows}")
    if digest != pin.rows_sha256:
        problems.append("the digest of the rows differs from the pin")
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = QuietParser(prog=PROGRAM, description="Check a track list file against the pinned row count and digest.")
    commands = parser.add_subparsers(dest="command", required=True)
    checker = commands.add_parser("check")
    checker.add_argument("--file", type=Path, required=True)
    arguments = parser.parse_args(argv)
    try:
        problems = file_problems(arguments.file, pinned_track_list())
    except TrackListPinError as error:
        sys.stderr.write(f"{describe_error(error)}\n")
        return 1
    for problem in problems:
        sys.stderr.write(f"{problem}\n")
    if not problems:
        sys.stdout.write("the file's rows equal the pinned row count and digest\n")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
