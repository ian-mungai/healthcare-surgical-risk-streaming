"""Build and check the candidate cases for the fixed sample, from the publisher's files and the frozen patient assignment rule.

The candidates are plain data in ``tests/fixtures/vitaldb/candidates.tsv`` (one line for each case) and the publisher's release is pinned in
``release.json`` next to it. The repository holds keys, case numbers, sizes and checksums only, never a recording or a copy of a table. A candidate is
a case that a later step may use, once it has verified the copy, as part of the fixed sample. The choice follows the rules in ``SLOTS`` and uses neither
an outcome nor a hand-picked number. Training and validation are development data. Run from the repository root:

    .venv/bin/python -m scripts.sample.candidates build --output tests/fixtures/vitaldb/candidates.tsv
    .venv/bin/python -m scripts.sample.candidates check

``build`` writes the candidate file only after the pinned checksum list, the clinical table and the track list match their pins. ``check`` also compares
the tracked file with a rebuild and with the checksum list. Each run reads into a new temporary folder and removes it.

VitalDB credit: Lee, H., & Jung, C. (2022). VitalDB, a high-fidelity multi-parameter vital signs database in surgical patients (version 1.0.0). PhysioNet.
https://doi.org/10.13026/czw8-9p62. Source: https://physionet.org/content/vitaldb/1.0.0/. Licence: https://creativecommons.org/licenses/by/4.0/.
Changes: the publisher's files are not changed. This module reads them and writes keys, case numbers, sizes and checksums only.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.project_config import ROOT
from scripts.sample.publisher import (
    BUCKET,
    CASE_PREFIX,
    CHECKSUM_LIST_KEY,
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

FIXTURES = ROOT / "tests" / "fixtures" / "vitaldb"
CANDIDATES_FILE = FIXTURES / "candidates.tsv"
RELEASE_FILE = FIXTURES / "release.json"
TOTAL_SIZE_LIMIT_BYTES = 1 << 30
ARTERIAL_TRACK = "SNUADC/ART"
CLINICAL_KEY = f"{RELEASE_PREFIX}clinical_data.csv"
HEADER = ("case", "patient", "key", "size_bytes", "sha256", "arterial_waveform", "slot")
INTEGER = re.compile(r"[0-9]+")


@dataclass(frozen=True)
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


@dataclass(frozen=True)
class Candidate:
    case: int
    patient: int
    key: str
    size_bytes: int
    sha256: str
    arterial_waveform: bool
    slot: str


@dataclass(frozen=True)
class CaseFacts:
    case: int
    patient: int
    duration_seconds: int
    size_bytes: int
    sha256: str
    arterial_waveform: bool


def case_key(case: int) -> str:
    return f"{CASE_PREFIX}{case:04d}.vital"


def format_candidates(candidates: Sequence[Candidate]) -> str:
    lines = ["\t".join(HEADER)]
    for item in candidates:
        fields = (item.case, item.patient, item.key, item.size_bytes, item.sha256, "true" if item.arterial_waveform else "false", item.slot)
        lines.append("\t".join(str(field) for field in fields))
    return "\n".join(lines) + "\n"


def read_candidates(path: Path) -> list[Candidate]:
    """Read the tracked candidate file. A malformed line is an error that names the line."""
    lines = path.read_text().splitlines()
    if not lines or tuple(lines[0].split("\t")) != HEADER:
        raise ValueError(f"{path.name}: the first line is not the header {HEADER}")
    candidates = []
    for number, line in enumerate(lines[1:], 2):
        fields = line.split("\t")
        if len(fields) != len(HEADER) or fields[5] not in ("true", "false") or not all(INTEGER.fullmatch(fields[position]) for position in (0, 1, 3)):
            raise ValueError(f"{path.name}:{number}: not a candidate line")
        candidates.append(Candidate(int(fields[0]), int(fields[1]), fields[2], int(fields[3]), fields[4], fields[5] == "true", fields[6]))
    return candidates


def candidate_problems(candidates: Sequence[Candidate]) -> list[str]:
    """The problems in the form of a candidate list: a key outside the release or naming another case, a bad checksum, a repeated case, a bad size."""
    problems = []
    seen: set[int] = set()
    for item in candidates:
        if not item.key.startswith(RELEASE_PREFIX):
            problems.append(f"case {item.case}: the key is outside the release path {RELEASE_PREFIX}")
        elif item.key != case_key(item.case):
            problems.append(f"case {item.case}: the key names another case")
        if SHA256.fullmatch(item.sha256) is None:
            problems.append(f"case {item.case}: checksum is not 64 hex characters in lower case")
        if item.size_bytes <= 0:
            problems.append(f"case {item.case}: size {item.size_bytes} is not positive")
        if item.case in seen:
            problems.append(f"case {item.case}: listed twice")
        seen.add(item.case)
    return problems


def pin_problems(label: str, pin: Any, *, key: bool) -> list[str]:
    if not isinstance(pin, dict):
        return [f"{label}: missing"]
    problems = []
    if key and not str(pin.get("key", "")).startswith(RELEASE_PREFIX):
        problems.append(f"{label}: the key is outside the release path {RELEASE_PREFIX}")
    if SHA256.fullmatch(str(pin.get("sha256", ""))) is None:
        problems.append(f"{label}: checksum is not 64 hex characters in lower case")
    if type(pin.get("size_bytes")) is not int or pin["size_bytes"] <= 0:
        problems.append(f"{label}: size is not a positive number")
    return problems


def source_problems(source: Any) -> list[str]:
    """The pin names the bucket, region and release path that every read of this package uses, and no others."""
    if not isinstance(source, dict):
        return ["source: missing"]
    return [
        f"source: the {name} must be {used}"
        for name, used in (("bucket", BUCKET), ("region", REGION), ("release_path", RELEASE_PREFIX))
        if source.get(name) != used
    ]


def release_problems(release: dict[str, Any]) -> list[str]:
    """The problems in the form of the release pin: the checksum list, the two tables and the track list. The pin names exactly the files the code reads."""
    if not isinstance(release, dict):
        return ["release pin: not a JSON object"]
    problems = source_problems(release.get("source"))
    problems += pin_problems("checksum_list", release.get("checksum_list"), key=True)
    if isinstance(release.get("checksum_list"), dict) and release["checksum_list"].get("key") != CHECKSUM_LIST_KEY:
        problems.append(f"checksum_list: the key must be {CHECKSUM_LIST_KEY}")
    tables = release.get("tables")
    if not isinstance(tables, list):
        problems.append("tables: not a list")
        tables = []
    keys = {table.get("key") for table in tables if isinstance(table, dict)}
    if len(tables) != 2 or keys != {CLINICAL_KEY, f"{RELEASE_PREFIX}lab_data.csv"}:
        problems.append("tables: the pin must name clinical_data.csv and lab_data.csv of the release once each")
    for position, table in enumerate(tables):
        problems += pin_problems(f"tables[{position}]", table, key=True)
    problems += pin_problems("track_list", release.get("track_list"), key=False)
    if isinstance(release.get("track_list"), dict) and release["track_list"].get("url") != TRACK_LIST_URL:
        problems.append(f"track_list: the address must be {TRACK_LIST_URL}")
    return problems


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
    take(min(arterial, key=lambda item: (item.duration_seconds, item.case)), "short_development")
    take(arterial[len(arterial) // 2], "median_development")
    take(min(arterial, key=lambda item: (-item.size_bytes, item.case)), "largest_development")
    return sorted(chosen.values(), key=lambda item: item.case)


def case_facts(rows: Sequence[tuple[int, int, int]], sums: dict[str, str], sizes: dict[str, int], arterial: set[int]) -> list[CaseFacts]:
    """Bind each case of the table to the publisher's checksum and size. A case with no checksum or file, or a waveform case the table lacks, is an error."""
    known = {case for case, _, _ in rows}
    stray = sorted(arterial - known)
    if stray:
        raise PublisherError(f"the track list names case {stray[0]}, which the clinical table does not list")
    facts = []
    for case, patient, duration in rows:
        key = case_key(case)
        if key not in sums or key not in sizes:
            raise PublisherError(f"case {case}: {key} is missing from the publisher's checksum list or key listing")
        facts.append(CaseFacts(case, patient, duration, sizes[key], sums[key], case in arterial))
    return facts


def read_inputs(release: dict[str, Any], workdir: Path) -> tuple[list[CaseFacts], dict[str, str], dict[str, int]]:
    """Read the publisher's files into ``workdir`` and return the facts of every case, the checksum list and the sizes of the keys.

    The checksum list, the clinical table and the track list must match their pins, and the pinned tables must be the ones in the checksum list.
    """
    checksum_list = release["checksum_list"]
    sums_path = workdir / "SHA256SUMS.txt"
    download_key(checksum_list["key"], sums_path)
    read_pinned("the checksum list", sums_path, checksum_list["size_bytes"], checksum_list["sha256"])
    sums = parse_checksum_list(sums_path.read_text())
    for table in release["tables"]:
        if sums.get(table["key"]) != table["sha256"]:
            raise PublisherError(f"the pinned checksum of {table['key']} is not the one in the checksum list")
    sizes = list_release()
    clinical_path = workdir / "clinical_data.csv"
    clinical_pin = next(table for table in release["tables"] if table["key"] == CLINICAL_KEY)
    download_key(CLINICAL_KEY, clinical_path)
    read_pinned("the clinical table", clinical_path, clinical_pin["size_bytes"], clinical_pin["sha256"])
    tracks_path = workdir / "track_list.csv"
    download_url(release["track_list"]["url"], tracks_path)
    read_pinned("the track list", tracks_path, release["track_list"]["size_bytes"], release["track_list"]["sha256"])
    facts = case_facts(parse_clinical_table(clinical_path), sums, sizes, parse_track_list(tracks_path, ARTERIAL_TRACK))
    return facts, sums, sizes


def listing_problems(candidates: Sequence[Candidate], sums: dict[str, str], sizes: dict[str, int]) -> list[str]:
    """The problems of a candidate list against the publisher's checksum list and key listing."""
    problems = []
    for item in candidates:
        if sums.get(item.key) != item.sha256:
            problems.append(f"case {item.case}: the checksum is not the one the publisher's checksum list gives for {item.key}")
        if sizes.get(item.key) != item.size_bytes:
            problems.append(f"case {item.case}: the size is not the one the publisher lists for {item.key}")
    return problems


def difference_problems(tracked: Sequence[Candidate], rebuilt: Sequence[Candidate]) -> list[str]:
    tracked_by_case = {item.case: item for item in tracked}
    rebuilt_by_case = {item.case: item for item in rebuilt}
    problems = [f"case {case}: tracked but not chosen by the rules" for case in sorted(tracked_by_case.keys() - rebuilt_by_case.keys())]
    problems += [f"case {case}: chosen by the rules but not tracked" for case in sorted(rebuilt_by_case.keys() - tracked_by_case.keys())]
    for case in sorted(tracked_by_case.keys() & rebuilt_by_case.keys()):
        if tracked_by_case[case] != rebuilt_by_case[case]:
            problems.append(f"case {case}: the tracked line differs from the rebuilt line")
    return problems


def rebuild(release_path: Path) -> tuple[list[Candidate], dict[str, str], dict[str, int]]:
    release = json.loads(release_path.read_text())
    problems = release_problems(release)
    if problems:
        raise PublisherError("; ".join(problems))
    workdir = Path(tempfile.mkdtemp(prefix="fixed_sample_"))
    try:
        facts, sums, sizes = read_inputs(release, workdir)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
    candidates = select_candidates(facts)
    problems = candidate_problems(candidates)
    total = sum(item.size_bytes for item in candidates)
    if total >= TOTAL_SIZE_LIMIT_BYTES:
        problems.append(f"the candidates total {total} bytes, over the limit of {TOTAL_SIZE_LIMIT_BYTES}")
    if problems:
        raise PublisherError("; ".join(problems))
    return candidates, sums, sizes


def main() -> int:
    """Run ``build`` or ``check`` and return 1 on any problem, with its cause on standard error."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["build", "check"])
    parser.add_argument("--release", type=Path, default=RELEASE_FILE, help="the release pin")
    parser.add_argument("--candidates", type=Path, default=CANDIDATES_FILE, help="check: the tracked candidate file")
    parser.add_argument("--output", type=Path, help="build: where to write the candidate file")
    args = parser.parse_args()
    try:
        candidates, sums, sizes = rebuild(args.release)
        if args.command == "build":
            if args.output is None:
                raise SystemExit("build needs --output")
            args.output.write_text(format_candidates(candidates))
            sys.stdout.write(
                f"wrote {len(candidates)} candidates to {args.output}; the checksum list, the clinical table and the track list match their pins\n"
            )
            return 0
        tracked = read_candidates(args.candidates)
        problems = candidate_problems(tracked) + listing_problems(tracked, sums, sizes) + difference_problems(tracked, candidates)
    except (PublisherError, ValueError) as error:
        sys.stderr.write(f"fixed sample: {error}\n")
        return 1
    for problem in problems:
        sys.stderr.write(f"fixed sample: {problem}\n")
    if not problems:
        sys.stdout.write(f"{len(tracked)} tracked candidates equal the rebuild and match the pinned checksum list (seed {SEED})\n")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
