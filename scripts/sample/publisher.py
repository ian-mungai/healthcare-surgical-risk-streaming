"""Read the publisher's public files without credentials, into a folder the caller owns and removes.

Every read goes through the project's one launcher with a deadline. The AWS command line runs with no profile, no credential file and no instance
metadata, and signs nothing, so it cannot use the owner's AWS access.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any

from scripts.process import run_command

BUCKET = "physionet-open"
REGION = "us-east-1"
RELEASE_PREFIX = "vitaldb/1.0.0/"
CASE_PREFIX = f"{RELEASE_PREFIX}vital_files/"
LIST_TIMEOUT_SECONDS = 300
READ_TIMEOUT_SECONDS = 300
SHA256 = re.compile(r"[0-9a-f]{64}")
CHECKSUM_LINE = re.compile(r"([0-9a-f]{64}) {1,2}(\S+)")


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


def list_release() -> dict[str, int]:
    """The size in bytes of every key under the release path."""
    arguments = ["s3api", "list-objects-v2", "--bucket", BUCKET, "--prefix", RELEASE_PREFIX, "--no-sign-request", "--output", "json", "--no-cli-pager"]
    result = run_command("aws", arguments, env=public_environment(), timeout=LIST_TIMEOUT_SECONDS)
    if result.returncode != 0:
        raise PublisherError(f"listing the release failed with exit status {result.returncode}")
    listing: dict[str, Any] = json.loads(result.stdout)
    return {item["Key"]: int(item["Size"]) for item in listing["Contents"]}


def download_key(key: str, target: Path) -> None:
    arguments = ["s3", "cp", f"s3://{BUCKET}/{key}", str(target), "--no-sign-request", "--only-show-errors"]
    result = run_command("aws", arguments, env=public_environment(), timeout=READ_TIMEOUT_SECONDS)
    if result.returncode != 0:
        raise PublisherError(f"reading {key} failed with exit status {result.returncode}")


def download_url(url: str, target: Path) -> None:
    arguments = ["--fail", "--silent", "--show-error", "--compressed", "--max-time", str(READ_TIMEOUT_SECONDS), "--output", str(target), "--", url]
    result = run_command("curl", arguments, timeout=READ_TIMEOUT_SECONDS + 30)
    if result.returncode != 0:
        raise PublisherError(f"reading {url} failed with exit status {result.returncode}")


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
