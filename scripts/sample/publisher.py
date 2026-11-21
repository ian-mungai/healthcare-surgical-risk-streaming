"""Read the publisher's public files without credentials, into a folder the caller owns and removes.

Every read goes through the project's one launcher with a deadline. The AWS command line runs with no profile, no credential file and no instance
metadata, and signs nothing, so it cannot use the owner's AWS access.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
from pathlib import Path

from scripts.process import CompletedProcess, TimeoutExpired, run_command, timeout_text

BUCKET = "physionet-open"
REGION = "us-east-1"
RELEASE_PREFIX = "vitaldb/1.0.0/"
CASE_PREFIX = f"{RELEASE_PREFIX}vital_files/"
CHECKSUM_LIST_KEY = f"{RELEASE_PREFIX}SHA256SUMS.txt"
TRACK_LIST_URL = "https://api.vitaldb.net/trks"
TRACK_LIST_COLUMNS = ("caseid", "tname", "tid")
CLINICAL_COLUMNS = ("caseid", "subjectid", "casestart", "caseend")
LIST_TIMEOUT_SECONDS = 300
READ_TIMEOUT_SECONDS = 300
SHA256 = re.compile(r"[0-9a-f]{64}")
CHECKSUM_LINE = re.compile(r"([0-9a-f]{64}) {1,2}(\S+)")
CASE_NUMBER = re.compile(r"\d+")
TIME_SECONDS = re.compile(r"-?\d+")


class PublisherError(RuntimeError):
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


def run_public(label: str, program: str, arguments: list[str], deadline: float, *, unsigned_aws: bool = True) -> CompletedProcess[str]:
    """Run one read through the project's launcher. A deadline, a missing program or a non-zero exit becomes a ``PublisherError`` that names the read."""
    try:
        result = run_command(program, arguments, env=public_environment() if unsigned_aws else None, timeout=deadline)
    except TimeoutExpired:
        raise PublisherError(timeout_text(label, deadline)) from None
    except FileNotFoundError:
        raise PublisherError(f"{label} needs {program}, which is not installed or not on PATH") from None
    if result.returncode != 0:
        raise PublisherError(f"{label} failed with exit status {result.returncode}")
    return result


def list_release() -> dict[str, int]:
    """The size in bytes of every key under the release path."""
    arguments = ["s3api", "list-objects-v2", "--bucket", BUCKET, "--prefix", RELEASE_PREFIX, "--no-sign-request", "--output", "json", "--no-cli-pager"]
    result = run_public("listing the release", "aws", arguments, LIST_TIMEOUT_SECONDS)
    try:
        entries = json.loads(result.stdout)["Contents"]
        return {entry["Key"]: int(entry["Size"]) for entry in entries}
    except ValueError, KeyError, TypeError:
        raise PublisherError("the release listing is not JSON with a Contents list of entries that each hold a Key and a Size") from None


def download_key(key: str, target: Path) -> None:
    run_public(f"reading {key}", "aws", ["s3", "cp", f"s3://{BUCKET}/{key}", str(target), "--no-sign-request", "--only-show-errors"], READ_TIMEOUT_SECONDS)


def download_url(url: str, target: Path) -> None:
    arguments = ["--fail", "--silent", "--show-error", "--compressed", "--max-time", str(READ_TIMEOUT_SECONDS), "--output", str(target), "--", url]
    run_public(f"reading {url}", "curl", arguments, READ_TIMEOUT_SECONDS + 30, unsigned_aws=False)


def read_pinned(label: str, target: Path, size_bytes: int, sha256: str) -> None:
    """Refuse a file whose size or SHA-256 is not the pinned one."""
    if target.stat().st_size != size_bytes:
        raise PublisherError(f"{label} has {target.stat().st_size} bytes, not the pinned {size_bytes}")
    if file_sha256(target) != sha256:
        raise PublisherError(f"{label} does not match the pinned SHA-256")


def parse_checksum_list(text: str) -> dict[str, str]:
    """The publisher's checksum list as a map from key to SHA-256. A malformed or repeated line is an error."""
    sums: dict[str, str] = {}
    for number, line in enumerate(text.splitlines(), 1):
        match = CHECKSUM_LINE.fullmatch(line)
        if match is None:
            raise PublisherError(f"checksum list line {number} is malformed")
        key = RELEASE_PREFIX + match.group(2)
        if key in sums:
            raise PublisherError(f"checksum list names {key} twice")
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
        raise PublisherError(f"the track list does not have the columns {', '.join(TRACK_LIST_COLUMNS)}")
    cases: set[int] = set()
    for number, row in rows:
        if len(row) != len(TRACK_LIST_COLUMNS):
            raise PublisherError(f"track list line {number} does not have {len(TRACK_LIST_COLUMNS)} fields")
        if CASE_NUMBER.fullmatch(row[0]) is None:
            raise PublisherError(f"track list line {number}: the case number is not an integer")
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
        raise PublisherError(f"the clinical table does not have each of the columns {', '.join(CLINICAL_COLUMNS)} once")
    position = {name: header.index(name) for name in CLINICAL_COLUMNS}
    cases: list[tuple[int, int, int]] = []
    seen: set[int] = set()
    for number, row in rows:
        if len(row) != len(header):
            raise PublisherError(f"clinical table line {number} does not have {len(header)} fields")
        values = {name: row[position[name]] for name in CLINICAL_COLUMNS}
        if any(
            pattern.fullmatch(values[name]) is None
            for name, pattern in (("caseid", CASE_NUMBER), ("subjectid", CASE_NUMBER), ("casestart", TIME_SECONDS), ("caseend", TIME_SECONDS))
        ):
            raise PublisherError(f"clinical table line {number}: a case, patient or time value is not an integer")
        case, patient, start, end = (int(values[name]) for name in CLINICAL_COLUMNS)
        if end < start:
            raise PublisherError(f"clinical table line {number}: the case ends before it starts")
        if case in seen:
            raise PublisherError(f"clinical table line {number}: case {case} is listed twice")
        seen.add(case)
        cases.append((case, patient, end - start))
    return cases
