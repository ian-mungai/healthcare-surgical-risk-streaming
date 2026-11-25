"""Run the ingestion checks from the shell under the project profile in ``.env``.

``check``: stream every selected landed object version and compare it with the pinned checksum list; write the result file and exit 1 on any fault.

``.venv/bin/python -m scripts.ingestion.cli check --selection S.json --checksum-list SHA256SUMS.txt --pairings pairings.json --result result.json``

``pairings``: read the metadata records that Airbyte wrote for the copied files and write the pairing records the check takes.

``.venv/bin/python -m scripts.ingestion.cli pairings --pairings pairings.json``

``publish``: publish the manifest of a complete passed result and write its reference file.

``.venv/bin/python -m scripts.ingestion.cli publish --selection S.json --result result.json --reference reference.json``

``verify``: the gate. Confirm that the bucket's current versions are the versions of the manifest, for every key or for each ``--source-key``.

``.venv/bin/python -m scripts.ingestion.cli verify --reference reference.json [--source-key KEY ...]``

``--bucket`` names a test's own short-lived bucket, ``<S3_BUCKET>-test-<8 digits><6 hex digits>``, in the place of ``S3_BUCKET`` of ``.env``. Each
command is safe to run again.

Every line a command writes is a ``Notice`` rendered by ``say`` or ``refuse``: an error of another module (the configuration, an AWS call) becomes the
notice of its kind, and its own text is never written, so no value of an input or of a reply reaches standard error or standard output. A failed AWS
call's notice says its error text was kept only when the AWS helper's text names the kept file.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import posixpath
import re
import sys
from pathlib import Path

from scripts.aws_cli import RAW_ERRORS, Aws, AwsError, AwsTimeout
from scripts.ingestion.check import CheckResult, run_check
from scripts.ingestion.closed import QuietParser
from scripts.ingestion.gate import verify_before_read
from scripts.ingestion.landing import LANDING_PREFIX, METADATA_SUFFIX, load_pairing_records, pairings_from_metadata
from scripts.ingestion.manifest import ManifestRef, publish_manifest
from scripts.ingestion.notice import ClosedError, Code, Notice, Subject, render
from scripts.ingestion.output_file import check_new_output, write_new_file
from scripts.ingestion.selection import Selection, pinned_release
from scripts.ingestion.store import S3Store
from scripts.project_config import ROOT, ConfigurationError, load_configuration

PROGRAM = "scripts.ingestion.cli"
PROGRESS_EVERY = 250
METADATA_LIMIT_BYTES = 256 * 1024 * 1024
TEST_BUCKET_SUFFIX = re.compile(r"-test-[0-9]{8}[0-9a-f]{6}")
KEPT_ERROR_TEXT = f"; its full error text is in {RAW_ERRORS.relative_to(ROOT)}/"


class InputError(ClosedError, RuntimeError):
    """A file the command reads or writes cannot be used, or the command line names a bucket that is not the project's. Named by role, never by value."""


def say(notice: Notice) -> None:
    sys.stdout.write(f"{render(notice)}\n")


def refuse(*notices: Notice) -> int:
    for notice in notices:
        sys.stderr.write(f"{render(notice)}\n")
    return 1


def read_input(path: Path, role: Subject) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError, UnicodeDecodeError:
        raise InputError(Notice(Code.CANNOT_READ, subject=role)) from None


def read_input_bytes(path: Path, role: Subject) -> bytes:
    try:
        return path.read_bytes()
    except OSError:
        raise InputError(Notice(Code.CANNOT_READ, subject=role)) from None


def write_private(path: Path, text: str) -> None:
    """Write a result or reference file for its owner only: it holds the bucket's name and object version IDs. A path that is held, in any form, is refused."""
    write_new_file(path, text, mode=0o600)


def show_progress(done: int, total: int) -> None:
    if done % PROGRESS_EVERY == 0 or done == total:
        sys.stderr.write(f"{render(Notice(Code.PROGRESS, count=done, total=total))}\n")


def bucket_in_use(configured: str, requested: str | None) -> str:
    """The project's bucket, or the test bucket of the command line when it has the form that a test bucket of the project's bucket has."""
    if requested is None or requested == configured:
        return configured
    if requested.startswith(configured) and TEST_BUCKET_SUFFIX.fullmatch(requested.removeprefix(configured)):
        return requested
    raise InputError(Notice(Code.BUCKET_FORM))


def command_check(store: S3Store, arguments: argparse.Namespace) -> int:
    selection = Selection.from_json(read_input(arguments.selection, Subject.SELECTION))
    pairings = load_pairing_records(read_input(arguments.pairings, Subject.PAIRINGS))
    result = run_check(store, selection, read_input_bytes(arguments.checksum_list, Subject.CHECKSUM_LIST), pairings, show_progress)
    write_private(arguments.result, result.to_json())
    if not result.passed:
        return refuse(*(finding.notice for finding in result.findings), Notice(Code.CHECK_FAILED, count=len(result.findings), total=result.file_count))
    say(Notice(Code.CHECK_PASSED, count=len(result.files), total=result.file_count, digest=result.result_sha256))
    return 0


def inside_landing(key: str) -> bool:
    """Whether a listed key is a plain key below the landing prefix: it starts with the prefix and is its own normal form (no ``..``, ``.`` or empty part)."""
    return key.startswith(LANDING_PREFIX) and posixpath.normpath(key) == key


def command_pairings(store: S3Store, arguments: argparse.Namespace) -> int:
    """Read every current metadata file under the landing prefix, and only those, and write the pairing records sorted by source key."""
    listed = store.list_versions(LANDING_PREFIX)
    if any(not inside_landing(item.key) for item in listed):
        raise InputError(Notice(Code.LISTED_OUTSIDE))
    versions = [item for item in listed if item.is_latest and not item.is_delete_marker and item.key.endswith(METADATA_SUFFIX)]
    if not versions:
        raise InputError(Notice(Code.NO_METADATA))
    try:
        texts = [store.get_version_bytes(item, METADATA_LIMIT_BYTES).decode() for item in versions]
    except UnicodeDecodeError:
        raise InputError(Notice(Code.METADATA_LINE, position=1)) from None
    records = sorted(pairings_from_metadata(texts, pinned_release().source_prefix), key=lambda item: (item.source_key, item.landed_key))
    write_private(arguments.pairings, json.dumps([dataclasses.asdict(item) for item in records], indent=1) + "\n")
    say(Notice(Code.PAIRINGS_WRITTEN, count=len(records)))
    return 0


def command_publish(store: S3Store, arguments: argparse.Namespace) -> int:
    selection = Selection.from_json(read_input(arguments.selection, Subject.SELECTION))
    reference = publish_manifest(store, selection, CheckResult.from_json(read_input(arguments.result, Subject.RESULT)))
    write_private(arguments.reference, reference.to_json())
    say(Notice(Code.PUBLISHED, digest=reference.sha256))
    return 0


def command_verify(store: S3Store, arguments: argparse.Namespace) -> int:
    entries = verify_before_read(store, ManifestRef.from_json(read_input(arguments.reference, Subject.REFERENCE)), arguments.source_key or None)
    say(Notice(Code.VERIFIED, count=len(entries)))
    return 0


def command_outputs(arguments: argparse.Namespace) -> list[Path]:
    """The output paths of the command, which must be free before anything is read."""
    if arguments.command == "check":
        return [arguments.result]
    if arguments.command == "pairings":
        return [arguments.pairings]
    if arguments.command == "publish":
        return [arguments.reference]
    return []


def main(argv: list[str] | None = None) -> int:
    """Run one command and return 0 when it holds, 1 for a fault, a refusal or a failed call."""
    parser = QuietParser(prog=PROGRAM, description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--bucket", help="a test's own short-lived bucket, <S3_BUCKET>-test-<8 digits><6 hex digits>, in the place of S3_BUCKET of .env")
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("check")
    check.add_argument("--selection", type=Path, required=True)
    check.add_argument("--checksum-list", type=Path, required=True, help="the publisher's checksum list, pinned in the selection by its SHA-256")
    check.add_argument("--pairings", type=Path, required=True, help="the pairing records: each source key with its landed key and size, as a JSON list")
    check.add_argument("--result", type=Path, required=True, help="where to write the result, also when the check fails")
    pairings = commands.add_parser("pairings")
    pairings.add_argument("--pairings", type=Path, required=True, help="where to write the pairing records, each source key with its landed key and size")
    publish = commands.add_parser("publish")
    publish.add_argument("--selection", type=Path, required=True)
    publish.add_argument("--result", type=Path, required=True)
    publish.add_argument("--reference", type=Path, required=True, help="where to write the manifest reference")
    verify = commands.add_parser("verify")
    verify.add_argument("--reference", type=Path, required=True)
    verify.add_argument("--source-key", action="append", help="a source key the reader needs; repeat for each; every entry when none")
    arguments = parser.parse_args(argv)
    try:
        for output in command_outputs(arguments):
            check_new_output(output)
        configuration = load_configuration()
        store = S3Store(Aws(configuration["AWS_PROFILE"], configuration["AWS_REGION"]), bucket_in_use(configuration["S3_BUCKET"], arguments.bucket))
        return {"check": command_check, "pairings": command_pairings, "publish": command_publish, "verify": command_verify}[arguments.command](store, arguments)
    except ClosedError as error:
        return refuse(*error.notices)
    except ConfigurationError:
        return refuse(Notice(Code.CONFIGURATION_INVALID))
    except AwsTimeout:
        return refuse(Notice(Code.AWS_TIMED_OUT))
    except AwsError as error:
        return refuse(Notice(Code.AWS_FAILED if KEPT_ERROR_TEXT in str(error) else Code.AWS_FAILED_NOT_KEPT))


if __name__ == "__main__":
    sys.exit(main())
