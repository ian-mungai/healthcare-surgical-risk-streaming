"""The raw connection as data: the source, the destination and the connection that Airbyte copies the release with.

``desired_state`` builds one document from a selection and the project's bucket. The source reads the publisher's bucket and selects exactly the keys
of the selection, so the selection is the tracked release pin's two tables and 6,388 case files and nothing a wildcard could add. It copies raw files,
with their folders, through the delivery method "Copy raw files". The destination writes the ``landing`` prefix of the project's bucket and the
metadata record of each file as JSON lines. The connection has no schedule, so only the orchestrator starts it. It syncs incrementally, so a rerun
copies only a file that is new. Each connector is held as an image and an exact tag: the apply module takes Airbyte's one built-in definition of the image and
refuses it when its tag is another. The document holds no key, no secret and no name of a field that takes one: the apply module holds one closed table of
the two field paths of each pinned connector and fills them from two root-only files on the host. The document is built on the owner's computer, where
no key is.

``render`` prints the document for the Ansible role, which hands it to the module that applies it to a running Airbyte.

``selection`` prints the selection file that the check and the manifest read, for the whole release or the fixed sample.

``.venv/bin/python -m scripts.ingestion.connection render --bucket B --region R --source-tag T --destination-tag T [--selection S.json | --fixed-sample]``

``.venv/bin/python -m scripts.ingestion.connection selection [--selection S.json | --fixed-sample]``
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

from scripts.ingestion.closed import QuietParser
from scripts.ingestion.landing import LANDING_PREFIX
from scripts.ingestion.notice import ClosedError, Code, Notice, render
from scripts.ingestion.selection import FULL_RELEASE, FULL_RELEASE_TABLES, Selection, full_release_files, pinned_release
from scripts.sample.candidates import CANDIDATES_FILE, development_candidates, read_candidates
from scripts.sample.publisher import REGION as PUBLISHER_REGION

SOURCE_REPOSITORY = "airbyte/source-s3"
DESTINATION_REPOSITORY = "airbyte/destination-s3"
SOURCE_NAME = "vitaldb_release"
DESTINATION_NAME = "project_landing"
CONNECTION_NAME = "vitaldb_raw_copy"
STREAM_NAME = "vitaldb_files"
MEASUREMENT_PERIOD = "2016-08 to 2017-06"
FIXED_SAMPLE = "fixed_sample"
BUCKET_NAME = re.compile(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]")
REGION_NAME = re.compile(r"[a-z]{2}(?:-[a-z]+)+-[0-9]")
FULL_VERSION = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")
PROGRAM = "scripts.ingestion.connection"


class ConnectionDefinitionError(ClosedError, ValueError):
    """An argument of the document is not valid."""


def full_release_selection() -> Selection:
    """The whole release, taken from the tracked pin: the two tables and the 6,388 case files."""
    pin = pinned_release()
    return Selection(
        FULL_RELEASE,
        pin.release,
        pin.source_bucket,
        pin.source_prefix,
        MEASUREMENT_PERIOD,
        pin.checksum_list_key,
        pin.checksum_list_sha256,
        full_release_files(),
    )


def fixed_sample_selection() -> Selection:
    """The fixed sample: the development candidate cases and the two tables, all of the pinned release, so a later full copy adds only the rest.

    The candidates of test patients are never selected: no fixed-sample job reads them.
    """
    pin = pinned_release()
    cases = [candidate.key.removeprefix(pin.source_prefix) for candidate in development_candidates(read_candidates(CANDIDATES_FILE))]
    return Selection(
        FIXED_SAMPLE,
        pin.release,
        pin.source_bucket,
        pin.source_prefix,
        MEASUREMENT_PERIOD,
        pin.checksum_list_key,
        pin.checksum_list_sha256,
        (*FULL_RELEASE_TABLES, *cases),
    )


def selection_document(selection: Selection) -> dict[str, Any]:
    """The selection in the form of a selection file, which the check and the manifest read."""
    return {
        "name": selection.name,
        "release": selection.release,
        "source_bucket": selection.source_bucket,
        "source_prefix": selection.source_prefix,
        "measurement_period": selection.measurement_period,
        "checksum_list": {"key": selection.checksum_list_key, "sha256": selection.checksum_list_sha256},
        "files": list(selection.files),
    }


def source_configuration(selection: Selection) -> dict[str, Any]:
    return {
        "bucket": selection.source_bucket,
        "region_name": PUBLISHER_REGION,
        "credentials": {"auth_type": "iam_user"},
        "delivery_method": {"delivery_type": "use_file_transfer", "preserve_directory_structure": True},
        "streams": [{"name": STREAM_NAME, "globs": list(selection.source_keys), "format": {"filetype": "csv"}}],
    }


def destination_configuration(bucket: str, region: str) -> dict[str, Any]:
    return {
        "s3_bucket_name": bucket,
        "s3_bucket_path": LANDING_PREFIX.rstrip("/"),
        "s3_bucket_region": region,
        "format": {"format_type": "JSONL", "compression": {"compression_type": "No Compression"}, "flattening": "No flattening"},
    }


def pinned_definition(repository: str, tag: str, field: str) -> dict[str, str]:
    """The image and the exact tag that the connection must run. Airbyte already holds one built-in definition for the image; the module takes it by the image
    and refuses it when its tag is not this one. No name is held, because a name selects nothing."""
    if FULL_VERSION.fullmatch(tag) is None:
        raise ConnectionDefinitionError(Notice(Code.CONNECTION_TAG, fields=(field,)))
    return {"dockerRepository": repository, "dockerImageTag": tag}


def source_definition(tag: str) -> dict[str, str]:
    """The pinned S3 source connector. Every connection that reads S3 takes Airbyte's one definition of the image, at this tag, so all run the same version."""
    return pinned_definition(SOURCE_REPOSITORY, tag, "source_tag")


def destination_definition(tag: str) -> dict[str, str]:
    return pinned_definition(DESTINATION_REPOSITORY, tag, "destination_tag")


def require_pinned_payloads(selection: Selection) -> Selection:
    """The selection itself when every key of it is a payload of the pinned release: one of the two tables or one of the 6,388 case files. Refuse anything else.

    A selection is never empty, so it is a non-empty subset of the pinned payloads. The checksum list and every other file of the release are not payloads.
    """
    if not set(selection.source_keys) <= set(full_release_selection().source_keys):
        raise ConnectionDefinitionError(Notice(Code.CONNECTION_SELECTION))
    return selection


def desired_state(selection: Selection, *, bucket: str, region: str, source_tag: str, destination_tag: str) -> dict[str, Any]:
    """The document that the module applies. Raise ``ConnectionDefinitionError`` for a selection, bucket, region or tag that is not valid."""
    require_pinned_payloads(selection)
    if BUCKET_NAME.fullmatch(bucket) is None or bucket == selection.source_bucket:
        raise ConnectionDefinitionError(Notice(Code.CONNECTION_BUCKET))
    if REGION_NAME.fullmatch(region) is None:
        raise ConnectionDefinitionError(Notice(Code.CONNECTION_REGION))
    tags = {"source_tag": source_tag, "destination_tag": destination_tag}
    wrong = tuple(name for name, tag in tags.items() if FULL_VERSION.fullmatch(tag) is None)
    if wrong:
        raise ConnectionDefinitionError(Notice(Code.CONNECTION_TAG, fields=wrong))
    pin = pinned_release()
    return {
        "source": {
            "name": SOURCE_NAME,
            "definition": source_definition(source_tag),
            "configuration": source_configuration(selection),
        },
        "destination": {
            "name": DESTINATION_NAME,
            "definition": destination_definition(destination_tag),
            "configuration": destination_configuration(bucket, region),
        },
        "connection": {
            "name": CONNECTION_NAME,
            "status": "active",
            "schedule": {"scheduleType": "manual"},
            "namespaceDefinition": "destination",
            "configurations": {"streams": [{"name": STREAM_NAME, "syncMode": "incremental_append", "includeFiles": True}]},
        },
        "release": pin.release,
        "keys_sha256": selection.keys_sha256,
    }


def chosen_selection(arguments: argparse.Namespace) -> Selection:
    if arguments.selection:
        return require_pinned_payloads(Selection.from_json(arguments.selection.read_text()))
    return fixed_sample_selection() if arguments.fixed_sample else full_release_selection()


def command_selection(arguments: argparse.Namespace) -> int:
    sys.stdout.write(json.dumps(selection_document(chosen_selection(arguments)), indent=1) + "\n")
    return 0


def command_render(arguments: argparse.Namespace) -> int:
    selection = chosen_selection(arguments)
    document = desired_state(
        selection, bucket=arguments.bucket, region=arguments.region, source_tag=arguments.source_tag, destination_tag=arguments.destination_tag
    )
    sys.stdout.write(json.dumps(document, sort_keys=True) + "\n")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = QuietParser(prog=PROGRAM, description=(__doc__ or "").splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    renderer = commands.add_parser("render")
    renderer.add_argument("--bucket", required=True, help="the project's bucket")
    renderer.add_argument("--region", required=True, help="the region of the project's bucket")
    renderer.add_argument("--source-tag", required=True, help="the pinned version of the S3 source connector")
    renderer.add_argument("--destination-tag", required=True, help="the pinned version of the S3 destination connector")
    chooser = renderer.add_mutually_exclusive_group()
    chooser.add_argument("--selection", type=Path, help="a selection file; the whole release when absent")
    chooser.add_argument("--fixed-sample", action="store_true", help="the fixed sample and the two tables")
    printer = commands.add_parser("selection")
    printer_chooser = printer.add_mutually_exclusive_group()
    printer_chooser.add_argument("--selection", type=Path, help="a selection file; the whole release when absent")
    printer_chooser.add_argument("--fixed-sample", action="store_true", help="the fixed sample and the two tables")
    arguments = parser.parse_args(argv)
    try:
        return {"render": command_render, "selection": command_selection}[arguments.command](arguments)
    except ClosedError as error:
        for notice in error.notices:
            sys.stderr.write(f"{render(notice)}\n")
        return 1
    except OSError:
        sys.stderr.write("cannot read the selection file\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
