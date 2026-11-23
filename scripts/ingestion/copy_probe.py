"""The copy probe: Airbyte copies the pinned candidate cases and both tables into the run's landing prefix, and these commands judge the result.

The probe is no second copy path. The raw connection copies, and ``cli`` pairs, checks and publishes the manifest. This module adds what the probe alone
asks for. The copy's selection is the two tables and ``PROBE_CASES``, development candidates of ``tests/fixtures/vitaldb/candidates.tsv``, so a test
patient's case can never be copied. After the manifest is published, ``landed`` confirms that the landed payloads are exactly the selected keys, each at the
size and SHA-256 that the tracked pins give and each with an object version ID that is not null. ``job`` confirms Airbyte's own record of the sync and its
log. ``sample`` derives the Platform sample from the landed cases and writes its pin, and only from a manifest that ``landed`` passed.

Every command reads the manifest and the list of object versions. None reads a payload.

``.venv/bin/python -m scripts.ingestion.copy_probe selection > selection.json``

``.venv/bin/python -m scripts.ingestion.copy_probe landed --reference reference.json``

``.venv/bin/python -m scripts.ingestion.copy_probe job --job job.json --log job.log``

``.venv/bin/python -m scripts.ingestion.copy_probe sample --reference reference.json --output tests/fixtures/vitaldb/platform_sample.json``

``selection`` prints the selection file for ``connection render --selection`` and for ``cli check`` and ``cli publish``. ``job`` reads the job's record as
Airbyte's public API gives it for ``GET /jobs/{jobId}`` and the job's log as text. ``--bucket`` is the one of ``cli``. Every line a command writes is a
``Notice``, so no value of an input reaches the output.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from scripts.aws_cli import Aws, AwsError, AwsTimeout
from scripts.ingestion.check import CheckedFile
from scripts.ingestion.cli import KEPT_ERROR_TEXT, bucket_in_use, read_input, refuse, say
from scripts.ingestion.closed import ClosedInputError, QuietParser, parse_json
from scripts.ingestion.connection import MEASUREMENT_PERIOD, selection_document
from scripts.ingestion.gate import verify_before_read
from scripts.ingestion.landing import LANDING_PREFIX, METADATA_SUFFIX
from scripts.ingestion.manifest import ManifestRef, read_manifest
from scripts.ingestion.notice import ClosedError, Code, Notice, Subject
from scripts.ingestion.output_path import refuse_output_over_input
from scripts.ingestion.selection import FULL_RELEASE_TABLES, Selection, SelectionError, pinned_release
from scripts.ingestion.store import NULL_VERSION_IDS, ObjectStore, ObjectVersion, S3Store
from scripts.project_config import ConfigurationError, load_configuration
from scripts.sample.candidates import FIXTURES, RELEASE_FILE, Candidate, is_development_candidate, read_candidates, release_problems

PROGRAM = "scripts.ingestion.copy_probe"
PROBE_NAME = "copy_probe"
PROBE_CASES = (908, 1147, 2466, 5145, 5754)
SAMPLE_FILE = FIXTURES / "platform_sample.json"
SAMPLE_FORMAT = "platform_sample_1"
SHORT_SLOT = "short_development"
CASE_KEY = re.compile(r".*/vital_files/[0-9]{4}\.vital")
ERROR_LINE = re.compile(r"\b(?:ERROR|FATAL)\b|^Traceback \(most recent call last\)")
CAPTURE_PREFIX = re.compile(
    r"(?:\[pod/[A-Za-z0-9.-]+/[A-Za-z0-9._-]+\] )?"
    r"(?:[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2}) )?"
)
SUCCEEDED = "succeeded"


class ProbeRefused(ClosedError, RuntimeError):
    """The copy probe's landed payloads, job or sample are refused, or an input of the probe cannot be read."""


@dataclass(frozen=True, slots=True)
class Pins:
    """What the repository pins about the files of the probe: the candidate cases and the two tables, each with its size and SHA-256."""

    candidates: tuple[Candidate, ...]
    tables: Mapping[str, tuple[int, str]]

    def candidate_of(self, key: str) -> Candidate | None:
        return next((item for item in self.candidates if item.key == key), None)

    def pinned(self, key: str) -> tuple[int, str] | None:
        """The size and SHA-256 that the pins give for a key, or None when no pin names it."""
        if key in self.tables:
            return self.tables[key]
        found = self.candidate_of(key)
        return None if found is None else (found.size_bytes, found.sha256)


def tracked_pins() -> Pins:
    """The pins of the tracked files: the candidate file and the release pin's two tables. Raise ``ProbeRefused`` when the release pin is not valid."""
    try:
        release = parse_json(RELEASE_FILE.read_text(), Subject.TRACKED_PIN)
    except ClosedInputError, OSError, UnicodeDecodeError:
        raise ProbeRefused(Notice(Code.PIN_UNREADABLE)) from None
    problems = release_problems(release)
    if problems:
        raise ProbeRefused(Notice(Code.PIN_INVALID), *problems)
    tables = MappingProxyType({table["key"]: (table["size_bytes"], table["sha256"]) for table in release["tables"]})
    return Pins(tuple(read_candidates(FIXTURES / "candidates.tsv")), tables)


def probe_selection(cases: Sequence[int] = PROBE_CASES, pins: Pins | None = None) -> Selection:
    """The selection of the probe: both tables and the given cases of the release. Raise ``SelectionError`` for a case that is no development candidate."""
    known = pins or tracked_pins()
    release = pinned_release()
    by_case = {item.case: item for item in known.candidates}
    problems = []
    for position, case in enumerate(cases, 1):
        if case not in by_case:
            problems.append(Notice(Code.PROBE_CASE_NOT_CANDIDATE, position=position))
        elif not is_development_candidate(by_case[case]):
            problems.append(Notice(Code.PROBE_CASE_NOT_DEVELOPMENT, position=position))
    if problems:
        raise SelectionError(*problems)
    paths = [by_case[case].key.removeprefix(release.source_prefix) for case in cases]
    return Selection(
        PROBE_NAME,
        release.release,
        release.source_bucket,
        release.source_prefix,
        MEASUREMENT_PERIOD,
        release.checksum_list_key,
        release.checksum_list_sha256,
        (*FULL_RELEASE_TABLES, *paths),
    )


def landed_problems(entries: Sequence[CheckedFile], selection: Selection, pins: Pins) -> list[Notice]:
    """Why the landed payloads are not the probe's: a key set other than the selection's, or a payload with a null version ID or another size or SHA-256.

    A payload is named by its place among the entries in source key order. A key set that is wrong is the only problem named, because the entries then
    are not the ones to compare with the pins.
    """
    keys = [entry.source_key for entry in entries]
    selected = set(selection.source_keys)
    wrong = len(selected - set(keys)) + len(set(keys) - selected) + len(keys) - len(set(keys))
    if wrong:
        return [Notice(Code.PROBE_KEYS, count=wrong, total=len(selected))]
    problems = []
    for position, entry in enumerate(sorted(entries, key=lambda item: item.source_key), 1):
        if entry.version_id in NULL_VERSION_IDS:
            problems.append(Notice(Code.PROBE_NULL_VERSION, position=position))
        if pins.pinned(entry.source_key) != (entry.size, entry.sha256):
            problems.append(Notice(Code.PROBE_PIN_DIFFERS, position=position))
    return problems


def landing_problems(listing: Sequence[ObjectVersion], entries: Sequence[CheckedFile]) -> list[Notice]:
    """Why the current payloads of the landing prefix are not the manifest's: an extra current payload, or a bound key that is no current object.

    The listing is the full listing of the landing prefix. A key counts when its current version is not a delete marker. The metadata files that the copy
    writes beside the payloads are no payloads.
    """
    current = {item.key for item in listing if item.is_latest and not item.is_delete_marker and not item.key.endswith(METADATA_SUFFIX)}
    bound = {entry.key for entry in entries}
    problems = []
    if current - bound:
        problems.append(Notice(Code.PROBE_LANDING_EXTRA, count=len(current - bound)))
    if bound - current:
        problems.append(Notice(Code.PROBE_LANDING_MISSING, count=len(bound - current)))
    return problems


def verify_landed(store: ObjectStore, reference: ManifestRef, selection: Selection, pins: Pins) -> tuple[CheckedFile, ...]:
    """The manifest entries of the probe once the passed manifest is confirmed and its payloads are the probe's. Raise before any payload is read.

    The manifest must be the published one of this selection: its selection digest, which covers the name, is the selection's. The gate then
    confirms that the bucket's current versions are the manifest's. Only then are the entries compared with the pins.
    """
    manifest = read_manifest(store, reference)
    if manifest.selection["selection_sha256"] != selection.selection_sha256:
        raise ProbeRefused(Notice(Code.PROBE_NOT_THIS))
    problems = landing_problems(store.list_versions(LANDING_PREFIX), manifest.entries)
    if problems:
        raise ProbeRefused(Notice(Code.PROBE_REFUSED), *problems)
    entries = verify_before_read(store, reference)
    problems = landed_problems(entries, selection, pins)
    if problems:
        raise ProbeRefused(Notice(Code.PROBE_REFUSED), *problems)
    return entries


@dataclass(frozen=True, slots=True)
class JobFacts:
    status: str
    rows_synced: int
    bytes_synced: int


def read_job(text: str) -> JobFacts:
    """The facts of Airbyte's job record that the probe judges. Airbyte adds fields of its own, so only these three are read, each of exactly its type."""
    try:
        record = parse_json(text, Subject.JOB_RECORD)
    except ClosedInputError as error:
        raise ProbeRefused(*error.notices) from None
    if (
        type(record) is not dict
        or type(record.get("status")) is not str
        or type(record.get("rowsSynced")) is not int
        or type(record.get("bytesSynced")) is not int
    ):
        raise ProbeRefused(Notice(Code.JOB_NOT_RECORD, subject=Subject.JOB_RECORD))
    return JobFacts(record["status"], record["rowsSynced"], record["bytesSynced"])


def is_error_line(line: str) -> bool:
    """Whether a log line is an error or a trace, also after the prefix that ``kubectl logs --prefix --timestamps`` puts before it.

    Only that exact form is stripped, in this order: ``[pod/<pod>/<container>]`` and an RFC 3339 timestamp that ends in ``Z`` or a numeric offset, each
    optional. Any other bracketed token, such as a level in ``[ERROR]``, stays in the line.
    """
    return ERROR_LINE.search(CAPTURE_PREFIX.sub("", line, count=1)) is not None


def job_problems(job: JobFacts, log: str, file_count: int) -> list[Notice]:
    """Each way that Airbyte's record of the sync and its log do not show a clean copy of ``file_count`` files, in one pass."""
    problems = []
    if job.status != SUCCEEDED:
        problems.append(Notice(Code.JOB_STATUS))
    if job.rows_synced != file_count:
        problems.append(Notice(Code.JOB_ROWS, count=job.rows_synced, total=file_count))
    if job.bytes_synced <= 0:
        problems.append(Notice(Code.JOB_BYTES))
    if not log.strip():
        problems.append(Notice(Code.LOG_EMPTY))
    problems += [Notice(Code.LOG_ERROR_LINE, position=number) for number, line in enumerate(log.splitlines(), 1) if is_error_line(line)]
    return problems


def sample_problems(entries: Sequence[CheckedFile], pins: Pins) -> list[Notice]:
    """Why the landed cases cannot be the Platform sample.

    Each landed case file must be a candidate of the tracked file at its pinned size and SHA-256, in the development data. Then the cases must hold a patient
    with two of them, a case with and a case without the arterial waveform and the short development case. A case is named by its place among the landed
    case files in source key order. The tables are no cases.
    """
    cases = sorted((entry for entry in entries if CASE_KEY.fullmatch(entry.source_key)), key=lambda entry: entry.source_key)
    problems: list[Notice] = []
    found: list[Candidate] = []
    for position, entry in enumerate(cases, 1):
        item = pins.candidate_of(entry.source_key)
        if item is None:
            problems.append(Notice(Code.SAMPLE_NOT_CANDIDATE, position=position))
        elif (item.size_bytes, item.sha256) != (entry.size, entry.sha256):
            problems.append(Notice(Code.PROBE_PIN_DIFFERS, position=position))
        elif not is_development_candidate(item):
            problems.append(Notice(Code.SAMPLE_NOT_DEVELOPMENT, position=position))
        else:
            found.append(item)
    if problems:
        return problems
    patients = [item.patient for item in found]
    if len(set(patients)) == len(patients):
        problems.append(Notice(Code.SAMPLE_NO_REPEAT))
    if not any(item.arterial_waveform for item in found):
        problems.append(Notice(Code.SAMPLE_NO_ARTERIAL))
    if all(item.arterial_waveform for item in found):
        problems.append(Notice(Code.SAMPLE_NO_NON_ARTERIAL))
    if not any(item.slot == SHORT_SLOT for item in found):
        problems.append(Notice(Code.SAMPLE_NO_SHORT))
    return problems


def sample_cases(entries: Sequence[CheckedFile], pins: Pins) -> list[Candidate]:
    """The candidates of the landed case files, in case order."""
    keys = {entry.source_key for entry in entries}
    return sorted((item for item in pins.candidates if item.key in keys), key=lambda item: item.case)


def format_sample(items: Sequence[Candidate]) -> str:
    """The pin file of the Platform sample: each case with its key, size, SHA-256 and slot, in case order. It holds no object version ID of a run."""
    cases = [
        {
            "case": item.case,
            "patient": item.patient,
            "key": item.key,
            "size_bytes": item.size_bytes,
            "sha256": item.sha256,
            "arterial_waveform": item.arterial_waveform,
            "slot": item.slot,
        }
        for item in sorted(items, key=lambda item: item.case)
    ]
    return json.dumps({"format": SAMPLE_FORMAT, "cases": cases}, indent=1) + "\n"


def store_of(arguments: argparse.Namespace) -> S3Store:
    configuration = load_configuration()
    return S3Store(Aws(configuration["AWS_PROFILE"], configuration["AWS_REGION"]), bucket_in_use(configuration["S3_BUCKET"], arguments.bucket))


def command_selection(arguments: argparse.Namespace) -> int:
    sys.stdout.write(json.dumps(selection_document(probe_selection()), indent=1) + "\n")
    return 0


def command_landed(arguments: argparse.Namespace) -> int:
    reference = ManifestRef.from_json(read_input(arguments.reference, Subject.REFERENCE))
    entries = verify_landed(store_of(arguments), reference, probe_selection(), tracked_pins())
    say(Notice(Code.PROBE_PASSED, total=len(entries)))
    return 0


def command_job(arguments: argparse.Namespace) -> int:
    job = read_job(read_input(arguments.job, Subject.JOB_RECORD))
    files = len(probe_selection().files)
    problems = job_problems(job, read_input(arguments.log, Subject.JOB_LOG), files)
    if problems:
        return refuse(*problems)
    say(Notice(Code.JOB_PASSED, total=files))
    return 0


def command_sample(arguments: argparse.Namespace) -> int:
    pins = tracked_pins()
    reference = ManifestRef.from_json(read_input(arguments.reference, Subject.REFERENCE))
    entries = verify_landed(store_of(arguments), reference, probe_selection(), pins)
    problems = sample_problems(entries, pins)
    if problems:
        return refuse(*problems)
    items = sample_cases(entries, pins)
    try:
        arguments.output.write_text(format_sample(items))
    except OSError:
        return refuse(Notice(Code.CANNOT_WRITE, subject=Subject.SAMPLE_PIN))
    say(Notice(Code.SAMPLE_WROTE, count=len(items)))
    return 0


def main(argv: list[str] | None = None) -> int:
    """Run one command and return 0 when it holds, 1 for a refusal or a failed call."""
    parser = QuietParser(prog=PROGRAM, description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--bucket", help="a test's own short-lived bucket, <S3_BUCKET>-test-<8 digits><6 hex digits>, in the place of S3_BUCKET of .env")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("selection")
    landed = commands.add_parser("landed")
    landed.add_argument("--reference", type=Path, required=True)
    job = commands.add_parser("job")
    job.add_argument("--job", type=Path, required=True, help="Airbyte's record of the sync job, as JSON")
    job.add_argument("--log", type=Path, required=True, help="the log of the sync job, as text")
    sample = commands.add_parser("sample")
    sample.add_argument("--reference", type=Path, required=True)
    sample.add_argument("--output", type=Path, default=SAMPLE_FILE, help="where to write the Platform sample pin")
    arguments = parser.parse_args(argv)
    try:
        if arguments.command == "sample":
            refuse_output_over_input([arguments.output], [arguments.reference])
        return {"selection": command_selection, "landed": command_landed, "job": command_job, "sample": command_sample}[arguments.command](arguments)
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
