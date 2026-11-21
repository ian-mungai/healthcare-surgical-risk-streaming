"""Run the ingestion checks from the shell under the project profile in ``.env``.

``check``: stream every selected landed object version and compare it with the pinned checksum list; write the result file and exit 1 on any fault.

``.venv/bin/python -m scripts.ingestion.cli check --selection S.json --checksum-list SHA256SUMS.txt --result result.json``

``publish``: publish the manifest of a complete passed result and write its reference file.

``.venv/bin/python -m scripts.ingestion.cli publish --selection S.json --result result.json --reference reference.json``

``verify``: the gate. Confirm that the bucket's current versions are the versions of the manifest, for every key or for each ``--source-key``.

``.venv/bin/python -m scripts.ingestion.cli verify --reference reference.json [--source-key KEY ...]``

``--bucket`` names a bucket other than ``S3_BUCKET`` of ``.env``, for a test's own short-lived bucket. Each command is safe to run again.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from scripts.aws_cli import Aws, AwsError
from scripts.ingestion.check import CheckResult, ChecksumListError, PinnedListError, run_check
from scripts.ingestion.gate import GateRefused, verify_before_read
from scripts.ingestion.manifest import ManifestRef, PublishRefused, publish_manifest
from scripts.ingestion.selection import Selection, SelectionError
from scripts.ingestion.store import S3Store, StoreError
from scripts.project_config import ConfigurationError, load_configuration

PROGRESS_EVERY = 250


def write_file(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def show_progress(done: int, total: int) -> None:
    if done % PROGRESS_EVERY == 0 or done == total:
        sys.stderr.write(f"checked {done} of {total} files\n")


def command_check(store: S3Store, arguments: argparse.Namespace) -> int:
    selection = Selection.from_json(arguments.selection.read_text())
    result = run_check(store, selection, arguments.checksum_list.read_bytes(), show_progress)
    write_file(arguments.result, result.to_json())
    for finding in result.findings:
        sys.stderr.write(f"{finding.message}\n")
    if not result.passed:
        sys.stderr.write(f"check failed: {len(result.findings)} faults in {result.file_count} selected files; no manifest may be published\n")
        return 1
    sys.stdout.write(f"check passed: {len(result.files)} of {result.file_count} selected files verified; result {result.result_sha256}\n")
    return 0


def command_publish(store: S3Store, arguments: argparse.Namespace) -> int:
    selection = Selection.from_json(arguments.selection.read_text())
    reference = publish_manifest(store, selection, CheckResult.from_json(arguments.result.read_text()))
    write_file(arguments.reference, reference.to_json())
    sys.stdout.write(f"manifest published: {reference.key} version {reference.version_id}\n")
    return 0


def command_verify(store: S3Store, arguments: argparse.Namespace) -> int:
    entries = verify_before_read(store, ManifestRef.from_json(arguments.reference.read_text()), arguments.source_key or None)
    sys.stdout.write(f"verified: {len(entries)} object versions are the versions of the passed manifest\n")
    return 0


def main(argv: list[str] | None = None) -> int:
    """Run one command and return 0 when it holds, 1 for a fault, a refusal or a failed call."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--bucket", help="a bucket other than S3_BUCKET of .env, for a test's own short-lived bucket")
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("check")
    check.add_argument("--selection", type=Path, required=True)
    check.add_argument("--checksum-list", type=Path, required=True, help="the publisher's checksum list, pinned in the selection by its SHA-256")
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
    except (
        ConfigurationError,
        AwsError,
        StoreError,
        SelectionError,
        PinnedListError,
        ChecksumListError,
        PublishRefused,
        GateRefused,
        OSError,
        json.JSONDecodeError,
    ) as error:
        sys.stderr.write(f"{error}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
