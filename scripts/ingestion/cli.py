"""Run the ingestion checks from the shell under the project profile in ``.env``.

``check``: stream every selected landed object version and compare it with the pinned checksum list; write the result file and exit 1 on any fault.

``.venv/bin/python -m scripts.ingestion.cli check --selection S.json --checksum-list SHA256SUMS.txt --pairings pairings.json --result result.json``

``publish``: publish the manifest of a complete passed result and write its reference file.

``.venv/bin/python -m scripts.ingestion.cli publish --selection S.json --result result.json --reference reference.json``

``verify``: the gate. Confirm that the bucket's current versions are the versions of the manifest, for every key or for each ``--source-key``.

``.venv/bin/python -m scripts.ingestion.cli verify --reference reference.json [--source-key KEY ...]``

``--bucket`` names a bucket other than ``S3_BUCKET`` of ``.env``, for a test's own short-lived bucket. Each command is safe to run again.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from scripts.aws_cli import Aws, AwsError
from scripts.ingestion.check import CheckResult, ChecksumListError, PinnedListError, ResultFormatError, run_check
from scripts.ingestion.closed import QuietParser
from scripts.ingestion.gate import GateRefused, verify_before_read
from scripts.ingestion.landing import PairingError, load_pairing_records
from scripts.ingestion.manifest import ManifestError, ManifestRef, PublishRefused, publish_manifest
from scripts.ingestion.selection import Selection, SelectionError
from scripts.ingestion.store import S3Store, StoreError
from scripts.project_config import ConfigurationError, load_configuration

PROGRESS_EVERY = 250


class InputError(RuntimeError):
    """A file the command reads or writes cannot be used. The message names the file by its role (``the selection``), never by its path."""


CLI_ERRORS = (
    ConfigurationError,
    AwsError,
    StoreError,
    SelectionError,
    PairingError,
    PinnedListError,
    ChecksumListError,
    ResultFormatError,
    ManifestError,
    PublishRefused,
    GateRefused,
    InputError,
)


def read_input(path: Path, role: str) -> str:
    try:
        return path.read_text()
    except OSError, UnicodeDecodeError:
        raise InputError(f"cannot read {role}") from None


def read_input_bytes(path: Path, role: str) -> bytes:
    try:
        return path.read_bytes()
    except OSError:
        raise InputError(f"cannot read {role}") from None


def write_private(path: Path, text: str, role: str) -> None:
    """Write a result or reference file for its owner only: it holds the bucket's name and object version IDs."""
    try:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w") as handle:
            handle.write(text)
        path.chmod(0o600)
    except OSError:
        raise InputError(f"cannot write {role}") from None


def show_progress(done: int, total: int) -> None:
    if done % PROGRESS_EVERY == 0 or done == total:
        sys.stderr.write(f"checked {done} of {total} files\n")


def command_check(store: S3Store, arguments: argparse.Namespace) -> int:
    selection = Selection.from_json(read_input(arguments.selection, "the selection"))
    pairings = load_pairing_records(read_input(arguments.pairings, "the pairing records"))
    result = run_check(store, selection, read_input_bytes(arguments.checksum_list, "the checksum list"), pairings, show_progress)
    write_private(arguments.result, result.to_json(), "the check result")
    for finding in result.findings:
        sys.stderr.write(f"{finding.message}\n")
    if not result.passed:
        sys.stderr.write(f"check failed: {len(result.findings)} faults in {result.file_count} selected files; no manifest may be published\n")
        return 1
    sys.stdout.write(f"check passed: {len(result.files)} of {result.file_count} selected files verified; result {result.result_sha256}\n")
    return 0


def command_publish(store: S3Store, arguments: argparse.Namespace) -> int:
    selection = Selection.from_json(read_input(arguments.selection, "the selection"))
    reference = publish_manifest(store, selection, CheckResult.from_json(read_input(arguments.result, "the check result")))
    write_private(arguments.reference, reference.to_json(), "the manifest reference")
    sys.stdout.write(f"manifest published: {reference.key} version {reference.version_id}\n")
    return 0


def command_verify(store: S3Store, arguments: argparse.Namespace) -> int:
    entries = verify_before_read(store, ManifestRef.from_json(read_input(arguments.reference, "the manifest reference")), arguments.source_key or None)
    sys.stdout.write(f"verified: {len(entries)} object versions are the versions of the passed manifest\n")
    return 0


def main(argv: list[str] | None = None) -> int:
    """Run one command and return 0 when it holds, 1 for a fault, a refusal or a failed call."""
    parser = QuietParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--bucket", help="a bucket other than S3_BUCKET of .env, for a test's own short-lived bucket")
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("check")
    check.add_argument("--selection", type=Path, required=True)
    check.add_argument("--checksum-list", type=Path, required=True, help="the publisher's checksum list, pinned in the selection by its SHA-256")
    check.add_argument("--pairings", type=Path, required=True, help="the pairing records: each source key with its landed key and size, as a JSON list")
    check.add_argument("--result", type=Path, required=True, help="where to write the result, also when the check fails")
    publish = commands.add_parser("publish")
    publish.add_argument("--selection", type=Path, required=True)
    publish.add_argument("--result", type=Path, required=True)
    publish.add_argument("--reference", type=Path, required=True, help="where to write the manifest reference")
    verify = commands.add_parser("verify")
    verify.add_argument("--reference", type=Path, required=True)
    verify.add_argument("--source-key", action="append", help="a source key the reader needs; repeat for each; every entry when none")
    arguments = parser.parse_args(argv)
    try:
        configuration = load_configuration()
        store = S3Store(Aws(configuration["AWS_PROFILE"], configuration["AWS_REGION"]), arguments.bucket or configuration["S3_BUCKET"])
        return {"check": command_check, "publish": command_publish, "verify": command_verify}[arguments.command](store, arguments)
    except CLI_ERRORS as error:
        sys.stderr.write(f"{error}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
