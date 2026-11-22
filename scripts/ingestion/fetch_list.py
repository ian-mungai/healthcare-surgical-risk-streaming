"""Fetch the publisher's checksum list as verification metadata: pinned by its release path, size and SHA-256, and kept apart from the payloads.

The list is read without credentials from the pinned key of the publisher's bucket into a temporary file beside the target. It takes the target's place only
after its size and SHA-256 equal the tracked pin's, so an unverified byte is never at the target. A target that already holds the pinned list is left alone and
nothing is read. The check reads the list from this file and verifies it again against the same pin. The file is a local verification record and never lands in
the project's bucket among the payloads: the manifest of a passed check records the list's key and SHA-256.

``.venv/bin/python -m scripts.ingestion.fetch_list --output runs/ingestion/SHA256SUMS.txt``
"""

from __future__ import annotations

import contextlib
import os
import sys
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from scripts.ingestion.closed import ClosedInputError, QuietParser, parse_json
from scripts.ingestion.notice import ClosedError, Code, Notice, Subject, render
from scripts.sample.candidates import RELEASE_FILE, release_problems
from scripts.sample.publisher import PublisherError, download_key, file_sha256, read_pinned

PROGRAM = "scripts.ingestion.fetch_list"


@dataclass(frozen=True, slots=True)
class ListPin:
    key: str
    size_bytes: int
    sha256: str


def tracked_list_pin() -> ListPin:
    """The key, size and SHA-256 of the checksum list in the tracked release pin. Raise ``PublisherError`` when the pin is not valid."""
    try:
        pin = parse_json(RELEASE_FILE.read_text(), Subject.TRACKED_PIN)
    except ClosedInputError, OSError, UnicodeDecodeError:
        raise PublisherError(Notice(Code.PIN_UNREADABLE)) from None
    if release_problems(pin):
        raise PublisherError(Notice(Code.PIN_INVALID))
    listed = pin["checksum_list"]
    return ListPin(listed["key"], listed["size_bytes"], listed["sha256"])


def holds_pinned_list(target: Path, pin: ListPin) -> bool:
    return target.is_file() and file_sha256(target) == pin.sha256


def fetch_checksum_list(target: Path, pin: ListPin, download: Callable[[str, Path], None] = download_key) -> bool:
    """Put the pinned list at ``target`` and return True, or return False when it is already there. Raise ``PublisherError`` and keep nothing otherwise."""
    if holds_pinned_list(target, pin):
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=".SHA256SUMS_", dir=target.parent)
    os.close(descriptor)
    partial = Path(name)
    try:
        download(pin.key, partial)
        read_pinned(Subject.READING_LIST, partial, pin.size_bytes, pin.sha256)
        partial.replace(target)
    finally:
        with contextlib.suppress(FileNotFoundError):
            partial.unlink()
    return True


def main(argv: list[str] | None = None) -> int:
    parser = QuietParser(prog=PROGRAM, description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--output", type=Path, required=True, help="where to keep the verified list")
    arguments = parser.parse_args(argv)
    try:
        fetched = fetch_checksum_list(arguments.output, tracked_list_pin(), download_key)
    except ClosedError as error:
        for notice in error.notices:
            sys.stderr.write(f"{render(notice)}\n")
        return 1
    except OSError:
        sys.stderr.write(f"{render(Notice(Code.CANNOT_WRITE, subject=Subject.CHECKSUM_LIST))}\n")
        return 1
    sys.stdout.write(f"{render(Notice(Code.LIST_FETCHED if fetched else Code.LIST_PRESENT))}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
