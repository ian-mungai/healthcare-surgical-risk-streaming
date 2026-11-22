"""Read the publisher's public files without credentials, into a folder the caller owns and removes.

Every read goes through the project's one launcher with a deadline. The AWS command line runs with no profile, no credential file and no instance
metadata, and signs nothing, so it cannot use the owner's AWS access. Each read reads one of the files the code knows, and every message is a ``Notice``
that names the file by its role, never by a key, an address or a value.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType

from scripts.ingestion.notice import ClosedError, Code, Notice, Subject
from scripts.process import CompletedProcess, TimeoutExpired, run_command

BUCKET = "physionet-open"
REGION = "us-east-1"
RELEASE_PREFIX = "vitaldb/1.0.0/"
CASE_PREFIX = f"{RELEASE_PREFIX}vital_files/"
CHECKSUM_LIST_KEY = f"{RELEASE_PREFIX}SHA256SUMS.txt"
CLINICAL_KEY = f"{RELEASE_PREFIX}clinical_data.csv"
LAB_KEY = f"{RELEASE_PREFIX}lab_data.csv"
TRACK_LIST_URL = "https://api.vitaldb.net/trks"
TRACK_LIST_COLUMNS = ("caseid", "tname", "tid")
CLINICAL_COLUMNS = ("caseid", "subjectid", "casestart", "caseend")
LIST_TIMEOUT_SECONDS = 300
READ_TIMEOUT_SECONDS = 300
SHA256 = re.compile(r"[0-9a-f]{64}")
CHECKSUM_LINE = re.compile(r"([0-9a-f]{64}) {1,2}(\S+)")
CASE_NUMBER = re.compile(r"\d+")
TIME_SECONDS = re.compile(r"-?\d+")
KEY_READS: Mapping[str, Subject] = MappingProxyType({CHECKSUM_LIST_KEY: Subject.READING_LIST, CLINICAL_KEY: Subject.READING_CLINICAL})
URL_READS: Mapping[str, Subject] = MappingProxyType({TRACK_LIST_URL: Subject.READING_TRACKS})
MISSING_PROGRAM: Mapping[str, Code] = MappingProxyType({"aws": Code.NEEDS_AWS, "curl": Code.NEEDS_CURL})


class PublisherError(ClosedError, RuntimeError):
    """A read from the publisher failed, or a file is not the pinned one."""


def public_environment() -> dict[str, str]:
    """The environment of an unsigned read: the caller's settings without any AWS variable, and empty AWS configuration files."""
    kept = {name: value for name, value in os.environ.items() if not name.startswith("AWS_")}
    return kept | {
        "AWS_CONFIG_FILE": os.devnull,
        "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
        "AWS_DEFAULT_REGION": REGION,
        "AWS_EC2_METADATA_DISABLED": "true",
    }


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def run_public(label: Subject, program: str, arguments: list[str], deadline: int, *, unsigned_aws: bool = True) -> CompletedProcess[str]:
    """Run one read through the project's launcher. A deadline, a missing program or a non-zero exit becomes a ``PublisherError`` that names the read."""
    try:
        result = run_command(program, arguments, env=public_environment() if unsigned_aws else None, timeout=deadline)
    except TimeoutExpired:
        raise PublisherError(Notice(Code.TIMED_OUT, subject=label, count=deadline)) from None
    except FileNotFoundError:
        raise PublisherError(Notice(MISSING_PROGRAM[program], subject=label)) from None
    if result.returncode != 0:
        raise PublisherError(Notice(Code.EXIT_STATUS, subject=label, count=result.returncode))
    return result


def list_release() -> dict[str, int]:
    """The size in bytes of every key under the release path."""
    arguments = ["s3api", "list-objects-v2", "--bucket", BUCKET, "--prefix", RELEASE_PREFIX, "--no-sign-request", "--output", "json", "--no-cli-pager"]
    result = run_public(Subject.LISTING_RELEASE, "aws", arguments, LIST_TIMEOUT_SECONDS)
    try:
        entries = json.loads(result.stdout)["Contents"]
        return {entry["Key"]: int(entry["Size"]) for entry in entries}
    except ValueError, KeyError, TypeError:
        raise PublisherError(Notice(Code.RELEASE_LISTING_FORM)) from None


def download_key(key: str, target: Path) -> None:
    """Read one of the keys in ``KEY_READS``, whose role names the read in every message."""
    arguments = ["s3", "cp", f"s3://{BUCKET}/{key}", str(target), "--no-sign-request", "--only-show-errors"]
    run_public(KEY_READS[key], "aws", arguments, READ_TIMEOUT_SECONDS)


def download_url(url: str, target: Path) -> None:
    """Read one of the addresses in ``URL_READS``, whose role names the read in every message."""
    arguments = ["--fail", "--silent", "--show-error", "--compressed", "--max-time", str(READ_TIMEOUT_SECONDS), "--output", str(target), "--", url]
    run_public(URL_READS[url], "curl", arguments, READ_TIMEOUT_SECONDS + 30, unsigned_aws=False)


def read_pinned(label: Subject, target: Path, size_bytes: int, sha256: str) -> None:
    """Refuse a file whose size or SHA-256 is not the pinned one."""
    if target.stat().st_size != size_bytes:
        raise PublisherError(Notice(Code.PINNED_SIZE, subject=label))
    if file_sha256(target) != sha256:
        raise PublisherError(Notice(Code.PINNED_DIGEST, subject=label))


def parse_checksum_list(text: str) -> dict[str, str]:
    """The publisher's checksum list as a map from key to SHA-256. A malformed or repeated line is an error."""
    sums: dict[str, str] = {}
    for number, line in enumerate(text.splitlines(), 1):
        match = CHECKSUM_LINE.fullmatch(line)
        if match is None:
            raise PublisherError(Notice(Code.SUMS_LINE_FORM, position=number))
        key = RELEASE_PREFIX + match.group(2)
        if key in sums:
            raise PublisherError(Notice(Code.SUMS_LINE_REPEAT, position=number))
        sums[key] = match.group(1)
    return sums


def csv_rows(path: Path) -> tuple[list[str], list[tuple[int, list[str]]]]:
    """The header and the numbered rows of a CSV file of the publisher. A byte order mark is dropped."""
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        header = next(reader, [])
        return header, [(reader.line_num, row) for row in reader]


def parse_track_list(path: Path, track: str) -> set[int]:
    """The cases that have the named track. The file must have exactly the columns ``caseid``, ``tname`` and ``tid`` and every line three fields."""
    header, rows = csv_rows(path)
    if tuple(header) != TRACK_LIST_COLUMNS:
        raise PublisherError(Notice(Code.TRACK_COLUMNS))
    cases: set[int] = set()
    for number, row in rows:
        if len(row) != len(TRACK_LIST_COLUMNS):
            raise PublisherError(Notice(Code.TRACK_FIELDS, position=number, count=len(TRACK_LIST_COLUMNS)))
        if CASE_NUMBER.fullmatch(row[0]) is None:
            raise PublisherError(Notice(Code.TRACK_CASE, position=number))
        if row[1] == track:
            cases.add(int(row[0]))
    return cases


def parse_clinical_table(path: Path) -> list[tuple[int, int, int]]:
    """The case number, the patient number and the length in seconds of every case of the clinical table, in file order.

    The table must have each of the columns ``caseid``, ``subjectid``, ``casestart`` and ``caseend`` once, every line must have as many fields as the header,
    the four values must be integers, a case must not end before it starts and no case may be listed twice.
    """
    header, rows = csv_rows(path)
    if any(header.count(name) != 1 for name in CLINICAL_COLUMNS):
        raise PublisherError(Notice(Code.CLINICAL_COLUMNS))
    position = {name: header.index(name) for name in CLINICAL_COLUMNS}
    cases: list[tuple[int, int, int]] = []
    seen: set[int] = set()
    for number, row in rows:
        if len(row) != len(header):
            raise PublisherError(Notice(Code.CLINICAL_FIELDS, position=number, count=len(header)))
        values = {name: row[position[name]] for name in CLINICAL_COLUMNS}
        if any(
            pattern.fullmatch(values[name]) is None
            for name, pattern in (("caseid", CASE_NUMBER), ("subjectid", CASE_NUMBER), ("casestart", TIME_SECONDS), ("caseend", TIME_SECONDS))
        ):
            raise PublisherError(Notice(Code.CLINICAL_INTEGERS, position=number))
        case, patient, start, end = (int(values[name]) for name in CLINICAL_COLUMNS)
        if end < start:
            raise PublisherError(Notice(Code.CLINICAL_ORDER, position=number))
        if case in seen:
            raise PublisherError(Notice(Code.CLINICAL_TWICE, position=number))
        seen.add(case)
        cases.append((case, patient, end - start))
    return cases
