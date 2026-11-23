"""Build and check the candidate cases for the fixed sample, from the publisher's files and the frozen patient assignment rule.

The candidates are plain data in ``tests/fixtures/vitaldb/candidates.tsv`` (one line for each case) and the publisher's release is pinned in
``release.json`` next to it. The repository holds keys, case numbers, sizes and checksums only, never a recording or a copy of a table. A candidate is
a case that a later step may use, once it has verified the copy, as part of the fixed sample. The choice follows the rules in ``SLOTS`` and uses neither
an outcome nor a hand-picked number. Training and validation are development data. Run from the repository root:

    .venv/bin/python -m scripts.sample.candidates build --output tests/fixtures/vitaldb/candidates.tsv
    .venv/bin/python -m scripts.sample.candidates check

``build`` writes the candidate file only after the pinned checksum list, the clinical table and the track list match their pins. ``check`` also compares
the tracked file with a rebuild and with the checksum list. Each run reads into a new temporary folder and removes it. Every line either command writes
is a ``Notice``: a candidate is named by its line in the candidate file or its place in the rebuild, never by its case number, key or size.

VitalDB credit: Lee, H., & Jung, C. (2022). VitalDB, a high-fidelity multi-parameter vital signs database in surgical patients (version 1.0.0). PhysioNet.
https://doi.org/10.13026/czw8-9p62. Source: https://physionet.org/content/vitaldb/1.0.0/. Licence: https://creativecommons.org/licenses/by/4.0/.
Changes: the publisher's files are not changed. This module reads them and writes keys, case numbers, sizes and checksums only.
"""

from __future__ import annotations

import re
import shutil
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any

from scripts.ingestion.closed import QuietParser, parse_json
from scripts.ingestion.notice import ClosedError, Code, Notice, Subject, render
from scripts.ingestion.output_file import check_new_output, write_new_file
from scripts.project_config import ROOT
from scripts.sample.publisher import (
    BUCKET,
    CASE_PREFIX,
    CHECKSUM_LIST_KEY,
    CLINICAL_KEY,
    LAB_KEY,
    REGION,
    RELEASE_PREFIX,
    SHA256,
    TRACK_LIST_URL,
    PublisherError,
    download_key,
    download_url,
    list_release,
    parse_checksum_list,
    parse_clinical_table,
    parse_track_list,
    read_pinned,
)
from scripts.sample.split import SEED, is_development

PROGRAM = "scripts.sample.candidates"
FIXTURES = ROOT / "tests" / "fixtures" / "vitaldb"
CANDIDATES_FILE = FIXTURES / "candidates.tsv"
RELEASE_FILE = FIXTURES / "release.json"
TOTAL_SIZE_LIMIT_BYTES = 1 << 30
ARTERIAL_TRACK = "SNUADC/ART"
HEADER = ("case", "patient", "key", "size_bytes", "sha256", "arterial_waveform", "slot")
INTEGER = re.compile(r"[0-9]+")
RELEASE_FIELDS: Mapping[str, type] = MappingProxyType({"source": dict, "checksum_list": dict, "tables": list, "track_list": dict, "credit": dict})
SOURCE_FIELDS: Mapping[str, type] = MappingProxyType({"bucket": str, "region": str, "release_path": str, "page": str})
FILE_PIN_FIELDS: Mapping[str, type] = MappingProxyType({"key": str, "size_bytes": int, "sha256": str})
TRACK_LIST_FIELDS: Mapping[str, type] = MappingProxyType({"url": str, "size_bytes": int, "sha256": str, "rows": int, "rows_sha256": str})
CREDIT_FIELDS: Mapping[str, type] = MappingProxyType({"citation": str, "source": str, "licence": str, "changes": str})
SOURCE_VALUES: Mapping[str, str] = MappingProxyType({"bucket": BUCKET, "region": REGION, "release_path": RELEASE_PREFIX})
TABLE_SUBJECTS: Mapping[str, Subject] = MappingProxyType({CLINICAL_KEY: Subject.CLINICAL_TABLE, LAB_KEY: Subject.LAB_TABLE})


class CandidateError(ClosedError, ValueError):
    """The tracked candidate file is not a header and candidate lines."""


@dataclass(frozen=True, slots=True)
class Slot:
    name: str
    rule: str


SLOTS = (
    Slot(
        "repeat_development",
        "2 development patients with exactly two cases, both with the arterial waveform, the smallest combined file size first; both cases of each",
    ),
    Slot("repeat_test", "1 test patient with exactly two cases, both with the arterial waveform, the smallest combined file size first; both cases"),
    Slot("no_arterial_development", "2 development patients with one case and no arterial waveform, the smallest file first"),
    Slot("no_arterial_test", "1 test patient with one case and no arterial waveform, the smallest file first"),
    Slot("arterial_test", "1 test patient with one case and the arterial waveform, the smallest file first"),
    Slot("short_development", "the development patient with one case and the arterial waveform whose case is the shortest, by the publisher's case end time"),
    Slot("median_development", "the development patient with one case and the arterial waveform whose file is in the middle of the size order"),
    Slot("largest_development", "the development patient with one case and the arterial waveform whose file is the largest"),
)


@dataclass(frozen=True, slots=True)
class Candidate:
    case: int
    patient: int
    key: str
    size_bytes: int
    sha256: str
    arterial_waveform: bool
    slot: str


@dataclass(frozen=True, slots=True)
class CaseFacts:
    case: int
    patient: int
    duration_seconds: int
    size_bytes: int
    sha256: str
    arterial_waveform: bool


def is_development_candidate(item: Candidate) -> bool:
    """True for a candidate of a development patient. Every selection of the fixed sample's cases uses this one definition."""
    return is_development(item.patient)


def development_candidates(items: Sequence[Candidate]) -> list[Candidate]:
    """The candidates of development patients, in the order given."""
    return [item for item in items if is_development_candidate(item)]


def case_key(case: int) -> str:
    return f"{CASE_PREFIX}{case:04d}.vital"


def read_text(path: Path, role: Subject) -> str:
    """The text of a file the command reads. Raise ``PublisherError`` that names the file by its role, never by its path."""
    try:
        return path.read_text()
    except OSError, UnicodeDecodeError:
        raise PublisherError(Notice(Code.CANNOT_READ, subject=role)) from None


def shape_problems(label: Subject, value: Any, fields: Mapping[str, type], index: int | None = None) -> list[Notice]:
    """The problem of an object that must hold exactly ``fields``, each of exactly its type. Names the object only, never a field it holds or a value."""
    if value is None:
        return [Notice(Code.SHAPE_MISSING, subject=label, index=index)]
    if type(value) is not dict or set(value) != set(fields) or any(type(value[name]) is not kind for name, kind in fields.items()):
        return [Notice(Code.SHAPE, subject=label, index=index, fields=tuple(sorted(fields)))]
    return []


def format_candidates(candidates: Sequence[Candidate]) -> str:
    lines = ["\t".join(HEADER)]
    for item in candidates:
        fields = (item.case, item.patient, item.key, item.size_bytes, item.sha256, "true" if item.arterial_waveform else "false", item.slot)
        lines.append("\t".join(str(field) for field in fields))
    return "\n".join(lines) + "\n"


def read_candidates(path: Path) -> list[Candidate]:
    """Read the tracked candidate file. A malformed line is an error that names the line by its number, never the file by its path."""
    lines = read_text(path, Subject.CANDIDATE_FILE).splitlines()
    if not lines or tuple(lines[0].split("\t")) != HEADER:
        raise CandidateError(Notice(Code.CANDIDATE_HEADER))
    candidates = []
    for number, line in enumerate(lines[1:], 2):
        fields = line.split("\t")
        if len(fields) != len(HEADER) or fields[5] not in ("true", "false") or not all(INTEGER.fullmatch(fields[position]) for position in (0, 1, 3)):
            raise CandidateError(Notice(Code.CANDIDATE_LINE, position=number))
        try:
            case, patient, size = int(fields[0]), int(fields[1]), int(fields[3])
        except ValueError:
            raise CandidateError(Notice(Code.CANDIDATE_LINE, position=number)) from None
        candidates.append(Candidate(case, patient, fields[2], size, fields[4], fields[5] == "true", fields[6]))
    return candidates


def about(code: Code, label: Subject, position: int) -> Notice:
    """A notice about the candidate at ``position`` in its list: named by its line in the candidate file, below the header, or its place in the rebuild."""
    return Notice(code, subject=label, position=position + 1 if label is Subject.CANDIDATE_LINE else position)


def candidate_problems(candidates: Sequence[Candidate], label: Subject = Subject.CANDIDATE_LINE) -> list[Notice]:
    """The problems in the form of a candidate list: a key outside the release or naming another case, a bad checksum, a repeated case, a bad size."""
    problems = []
    seen: set[int] = set()
    for position, item in enumerate(candidates, 1):
        if not item.key.startswith(RELEASE_PREFIX):
            problems.append(about(Code.CANDIDATE_KEY_OUTSIDE, label, position))
        elif item.key != case_key(item.case):
            problems.append(about(Code.CANDIDATE_KEY_OTHER_CASE, label, position))
        if SHA256.fullmatch(item.sha256) is None:
            problems.append(about(Code.CANDIDATE_CHECKSUM_FORM, label, position))
        if item.size_bytes <= 0:
            problems.append(about(Code.CANDIDATE_SIZE, label, position))
        if item.case in seen:
            problems.append(about(Code.CANDIDATE_TWICE, label, position))
        seen.add(item.case)
    return problems


def pin_problems(label: Subject, pin: Any, fields: Mapping[str, type], index: int | None = None) -> list[Notice]:
    """The problems of a pinned file: its shape first, then a key outside the release, a checksum that is not 64 hex digits and a size that is not positive."""
    problems = shape_problems(label, pin, fields, index)
    if problems:
        return problems
    if "key" in fields and not pin["key"].startswith(RELEASE_PREFIX):
        problems.append(Notice(Code.PIN_KEY_OUTSIDE, subject=label, index=index))
    if SHA256.fullmatch(pin["sha256"]) is None:
        problems.append(Notice(Code.PIN_CHECKSUM_FORM, subject=label, index=index))
    if pin["size_bytes"] <= 0:
        problems.append(Notice(Code.PIN_SIZE, subject=label, index=index))
    return problems


def source_problems(source: Any) -> list[Notice]:
    """The pin names the bucket, region and release path that every read of this package uses, and no others."""
    shape = shape_problems(Subject.PIN_SOURCE, source, SOURCE_FIELDS)
    if shape:
        return shape
    return [Notice(Code.SOURCE_VALUE, fields=(name,)) for name, used in SOURCE_VALUES.items() if source.get(name) != used]


def release_problems(release: Any) -> list[Notice]:
    """The problems in the form of the release pin: the checksum list, the two tables and the track list. The pin names exactly the files the code reads.

    The pin is closed: each object holds exactly its known fields, each of its type. A problem names the object, never a field it holds or a value.
    """
    if type(release) is not dict:
        return [Notice(Code.PIN_NOT_OBJECT)]
    problems = [] if set(release) == set(RELEASE_FIELDS) else [Notice(Code.PIN_TOP_FIELDS, fields=tuple(sorted(RELEASE_FIELDS)))]
    problems += source_problems(release.get("source"))
    problems += pin_problems(Subject.PIN_LIST, release.get("checksum_list"), FILE_PIN_FIELDS)
    if not shape_problems(Subject.PIN_LIST, release.get("checksum_list"), FILE_PIN_FIELDS) and release["checksum_list"]["key"] != CHECKSUM_LIST_KEY:
        problems.append(Notice(Code.LIST_KEY_PIN))
    tables = release.get("tables")
    if type(tables) is not list:
        problems.append(Notice(Code.TABLES_NOT_LIST))
        tables = []
    for position, table in enumerate(tables):
        problems += pin_problems(Subject.PIN_TABLE, table, FILE_PIN_FIELDS, position)
    keys = sorted(table["key"] for table in tables if not shape_problems(Subject.PIN_TABLE_ENTRY, table, FILE_PIN_FIELDS))
    if len(tables) != 2 or keys != sorted(TABLE_SUBJECTS):
        problems.append(Notice(Code.TABLES_NAMES))
    problems += pin_problems(Subject.PIN_TRACKS, release.get("track_list"), TRACK_LIST_FIELDS)
    if not shape_problems(Subject.PIN_TRACKS, release.get("track_list"), TRACK_LIST_FIELDS):
        track_list = release["track_list"]
        if track_list["url"] != TRACK_LIST_URL:
            problems.append(Notice(Code.TRACK_URL))
        if track_list["rows"] <= 0 or SHA256.fullmatch(track_list["rows_sha256"]) is None:
            problems.append(Notice(Code.PIN_ROWS, subject=Subject.PIN_TRACKS))
    return problems + shape_problems(Subject.PIN_CREDIT, release.get("credit"), CREDIT_FIELDS)


def select_candidates(facts: Sequence[CaseFacts]) -> list[Candidate]:
    """Apply the rules in ``SLOTS``, in that order, to every case of the release. The order inside a rule is the size or length, then the case number."""
    cases: dict[int, list[CaseFacts]] = {}
    for item in facts:
        cases.setdefault(item.patient, []).append(item)
    chosen: dict[int, Candidate] = {}

    def take(item: CaseFacts, slot: str) -> None:
        chosen.setdefault(item.case, Candidate(item.case, item.patient, case_key(item.case), item.size_bytes, item.sha256, item.arterial_waveform, slot))

    def repeat(slot: str, development: bool, count: int) -> None:
        pairs = [
            group
            for group in cases.values()
            if len(group) == 2 and all(item.arterial_waveform for item in group) and is_development(group[0].patient) == development
        ]
        for group in sorted(pairs, key=lambda group: (sum(item.size_bytes for item in group), group[0].patient))[:count]:
            for item in group:
                take(item, slot)

    def single(development: bool, arterial: bool) -> list[CaseFacts]:
        pool = [
            group[0]
            for group in cases.values()
            if len(group) == 1 and group[0].arterial_waveform == arterial and is_development(group[0].patient) == development
        ]
        return sorted(pool, key=lambda item: (item.size_bytes, item.case))

    repeat("repeat_development", True, 2)
    repeat("repeat_test", False, 1)
    for item in single(True, False)[:2]:
        take(item, "no_arterial_development")
    for item in single(False, False)[:1]:
        take(item, "no_arterial_test")
    for item in single(False, True)[:1]:
        take(item, "arterial_test")
    arterial = single(True, True)
    if not arterial:
        raise PublisherError(Notice(Code.NO_ARTERIAL_DEVELOPMENT))
    take(min(arterial, key=lambda item: (item.duration_seconds, item.case)), "short_development")
    take(arterial[len(arterial) // 2], "median_development")
    take(min(arterial, key=lambda item: (-item.size_bytes, item.case)), "largest_development")
    return sorted(chosen.values(), key=lambda item: item.case)


def case_facts(rows: Sequence[tuple[int, int, int]], sums: dict[str, str], sizes: dict[str, int], arterial: set[int]) -> list[CaseFacts]:
    """Bind each case of the table to the publisher's checksum and size. A case with no checksum or file, or a waveform case the table lacks, is an error."""
    known = {case for case, _, _ in rows}
    if arterial - known:
        raise PublisherError(Notice(Code.STRAY_TRACK))
    facts = []
    for position, (case, patient, duration) in enumerate(rows, 1):
        key = case_key(case)
        if key not in sums or key not in sizes:
            raise PublisherError(Notice(Code.CASE_UNLISTED, position=position))
        facts.append(CaseFacts(case, patient, duration, sizes[key], sums[key], case in arterial))
    return facts


def read_inputs(release: dict[str, Any], workdir: Path) -> tuple[list[CaseFacts], dict[str, str], dict[str, int]]:
    """Read the publisher's files into ``workdir`` and return the facts of every case, the checksum list and the sizes of the keys.

    The checksum list, the clinical table and the track list must match their pins, and the pinned tables must be the ones in the checksum list.
    """
    checksum_list = release["checksum_list"]
    sums_path = workdir / "SHA256SUMS.txt"
    download_key(checksum_list["key"], sums_path)
    read_pinned(Subject.CHECKSUM_LIST, sums_path, checksum_list["size_bytes"], checksum_list["sha256"])
    sums = parse_checksum_list(sums_path.read_text())
    for table in release["tables"]:
        if sums.get(table["key"]) != table["sha256"]:
            raise PublisherError(Notice(Code.TABLE_NOT_IN_LIST, subject=TABLE_SUBJECTS[table["key"]]))
    sizes = list_release()
    clinical_path = workdir / "clinical_data.csv"
    clinical_pin = next(table for table in release["tables"] if table["key"] == CLINICAL_KEY)
    download_key(CLINICAL_KEY, clinical_path)
    read_pinned(Subject.CLINICAL_TABLE, clinical_path, clinical_pin["size_bytes"], clinical_pin["sha256"])
    tracks_path = workdir / "track_list.csv"
    download_url(release["track_list"]["url"], tracks_path)
    read_pinned(Subject.TRACK_LIST, tracks_path, release["track_list"]["size_bytes"], release["track_list"]["sha256"])
    facts = case_facts(parse_clinical_table(clinical_path), sums, sizes, parse_track_list(tracks_path, ARTERIAL_TRACK))
    return facts, sums, sizes


def listing_problems(candidates: Sequence[Candidate], sums: dict[str, str], sizes: dict[str, int]) -> list[Notice]:
    """The problems of the tracked candidates against the publisher's checksum list and key listing, each named by its line."""
    problems = []
    for position, item in enumerate(candidates, 1):
        if sums.get(item.key) != item.sha256:
            problems.append(about(Code.LISTING_CHECKSUM, Subject.CANDIDATE_LINE, position))
        if sizes.get(item.key) != item.size_bytes:
            problems.append(about(Code.LISTING_SIZE, Subject.CANDIDATE_LINE, position))
    return problems


def difference_problems(tracked: Sequence[Candidate], rebuilt: Sequence[Candidate]) -> list[Notice]:
    """How the tracked candidates differ from the rebuild: a tracked line by its number, a rebuilt candidate by its place in the rebuild."""
    tracked_cases = {item.case: item for item in tracked}
    rebuilt_cases = {item.case: item for item in rebuilt}
    problems = []
    for position, item in enumerate(tracked, 1):
        if item.case not in rebuilt_cases:
            problems.append(about(Code.TRACKED_NOT_CHOSEN, Subject.CANDIDATE_LINE, position))
        elif rebuilt_cases[item.case] != item:
            problems.append(about(Code.LINE_DIFFERS, Subject.CANDIDATE_LINE, position))
    problems += [
        Notice(Code.CHOSEN_NOT_TRACKED, subject=Subject.REBUILT_CANDIDATE, position=position)
        for position, item in enumerate(rebuilt, 1)
        if item.case not in tracked_cases
    ]
    return problems


def rebuild(release_path: Path) -> tuple[list[Candidate], dict[str, str], dict[str, int]]:
    release = parse_json(read_text(release_path, Subject.RELEASE_PIN), Subject.RELEASE_PIN)
    problems = release_problems(release)
    if problems:
        raise PublisherError(*problems)
    workdir = Path(tempfile.mkdtemp(prefix="fixed_sample_"))
    try:
        facts, sums, sizes = read_inputs(release, workdir)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
    candidates = select_candidates(facts)
    problems = candidate_problems(candidates, Subject.REBUILT_CANDIDATE)
    if sum(item.size_bytes for item in candidates) >= TOTAL_SIZE_LIMIT_BYTES:
        problems.append(Notice(Code.OVER_LIMIT, count=TOTAL_SIZE_LIMIT_BYTES))
    if problems:
        raise PublisherError(*problems)
    return candidates, sums, sizes


def report(notices: Sequence[Notice]) -> int:
    for notice in notices:
        sys.stderr.write(f"fixed sample: {render(notice)}\n")
    return 1 if notices else 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run ``build`` or ``check`` and return 1 on any problem, with its cause on standard error."""
    parser = QuietParser(prog=PROGRAM, description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["build", "check"])
    parser.add_argument("--release", type=Path, default=RELEASE_FILE, help="the release pin")
    parser.add_argument("--candidates", type=Path, default=CANDIDATES_FILE, help="check: the tracked candidate file")
    parser.add_argument("--output", type=Path, help="build: where to write the candidate file")
    args = parser.parse_args(argv)
    if args.command == "build" and args.output is None:
        raise SystemExit(render(Notice(Code.BUILD_NEEDS_OUTPUT)))
    try:
        if args.command == "build":
            check_new_output(args.output)
        tracked = read_candidates(args.candidates) if args.command == "check" else []
        candidates, sums, sizes = rebuild(args.release)
        if args.command == "build":
            write_new_file(args.output, format_candidates(candidates))
            sys.stdout.write(f"{render(Notice(Code.WROTE, count=len(candidates)))}\n")
            return 0
        problems = candidate_problems(tracked) + listing_problems(tracked, sums, sizes) + difference_problems(tracked, candidates)
    except ClosedError as error:
        return report(error.notices)
    if not problems:
        sys.stdout.write(f"{render(Notice(Code.CHECKED, count=len(tracked), total=SEED))}\n")
    return report(problems)


if __name__ == "__main__":
    sys.exit(main())
