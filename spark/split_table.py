"""The rows, the checks and the freeze of the patient split table, in plain data with no Spark.

The job ``spark/jobs/patient_split.py`` reads and writes the tables and calls these functions, so that the card's checks run in the fast tests on plain rows.
The assignment rule is only in ``scripts/sample/split.py``: this module calls it and keeps no copy. A check returns its problems as plain sentences and
never raises for a failed check, so that one run names every cause.
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from scripts.sample.split import BUCKETS, HEX_DIGITS, SEED, TRAIN_BUCKETS, VALIDATION_BUCKETS, Part, patient_part

SET_NAMES: Mapping[Part, str] = {"train": "training", "validation": "validation", "test": "test"}
RULE_VERSION = f"patient_split:seed={SEED}:sha256_hex{HEX_DIGITS}_mod{BUCKETS}:training<{TRAIN_BUCKETS}:validation<{TRAIN_BUCKETS + VALIDATION_BUCKETS}:test"
TARGET_PERCENT = {"training": 70, "validation": 10, "test": 20}
PINNED_COUNTS = {"training": 4287, "validation": 594, "test": 1209}
SHARE_TOLERANCE_POINTS = 1
COLUMNS = (("subjectid", "string"), ("set", "string"), ("rule_version", "string"), ("source_snapshot_id", "bigint"))
SOURCE_COLUMNS = (("caseid", "string"), ("subjectid", "string"))
PATIENT_NUMBER = re.compile(r"[1-9][0-9]*")
CASE_NUMBER = re.compile(r"[0-9]+")
SAFE_VERSION = re.compile(r"[A-Za-z0-9_:=<>.-]+")
CANDIDATE_HEADER = ("case", "patient", "key", "size_bytes", "sha256", "arterial_waveform", "slot")
SLOT_KINDS = {"development": ("training", "validation"), "test": ("test",)}
MESSAGE_LIMIT = 10


class PatientSplitError(ValueError):
    """The split cannot be built or a check failed. ``problems`` holds one plain sentence for each cause."""

    def __init__(self, *problems: str) -> None:
        super().__init__("; ".join(problems))
        self.problems = list(problems)


@dataclass(frozen=True, slots=True)
class SplitRow:
    subjectid: str
    set: str
    rule_version: str
    source_snapshot_id: int


@dataclass(frozen=True, slots=True)
class SampleCase:
    case: int
    patient: int
    slot: str


def patient_number(subjectid: str) -> int:
    """The patient number of a ``subjectid`` in the published form (whole number, no sign, no zero before the first digit), or ``PatientSplitError``."""
    if PATIENT_NUMBER.fullmatch(subjectid) is None or not subjectid.isascii():
        raise PatientSplitError(f"the subjectid {subjectid!r} is not a patient number in its published form: digits only, no leading zero")
    return int(subjectid)


def build_assignments(subject_ids: Iterable[str]) -> dict[str, str]:
    """The set of each patient, by the one rule of ``scripts/sample/split.py``. A ``subjectid`` that is not in its published form is an error that names it."""
    assignments: dict[str, str] = {}
    refused: list[str] = []
    for subjectid in subject_ids:
        try:
            assignments[subjectid] = SET_NAMES[patient_part(patient_number(subjectid))]
        except PatientSplitError as error:
            refused.append(str(error))
    if refused:
        raise PatientSplitError(*refused[:MESSAGE_LIMIT])
    return assignments


def share_problems(counts: Mapping[str, int]) -> list[str]:
    """Each set whose share of the patients is more than one percentage point from its target of 70, 10 or 20 percent. Whole-number arithmetic only."""
    total = sum(counts.get(name, 0) for name in TARGET_PERCENT)
    problems = []
    for name, percent in TARGET_PERCENT.items():
        count = counts.get(name, 0)
        if abs(count * 100 - percent * total) > SHARE_TOLERANCE_POINTS * total:
            problems.append(f"the {name} share is {count} of {total} patients, which is more than {SHARE_TOLERANCE_POINTS} percentage point from {percent}%")
    return problems


def table_problems(rows: Sequence[SplitRow], *, pinned: bool) -> list[str]:
    """What is wrong with the rows of the table by themselves: a patient twice, a set or a version that is not the rule's, shares and pinned counts."""
    if not rows:
        return ["the split table has no row"]
    problems: list[str] = []
    for subjectid, seen in Counter(row.subjectid for row in rows).items():
        if seen > 1:
            names = sorted({row.set for row in rows if row.subjectid == subjectid})
            problems.append(f"patient {subjectid} is in two sets: {names}" if len(names) > 1 else f"patient {subjectid} is in the table more than once")
    for version in sorted({row.rule_version for row in rows} - {RULE_VERSION}):
        problems.append(f"the rule version {version!r} is not the current {RULE_VERSION!r}")
    if len({row.source_snapshot_id for row in rows}) > 1:
        problems.append("the rows were built from more than one source snapshot")
    for row in rows:
        if row.set not in TARGET_PERCENT:
            problems.append(f"patient {row.subjectid} has the set {row.set!r}, which is not one of {sorted(TARGET_PERCENT)}")
            continue
        try:
            expected = SET_NAMES[patient_part(patient_number(row.subjectid))]
        except PatientSplitError as error:
            problems.append(str(error))
            continue
        if row.set != expected:
            problems.append(f"patient {row.subjectid} is in {row.set}, which does not match the rule ({expected})")
    counts = dict(Counter(row.set for row in rows))
    problems += share_problems(counts)
    if pinned and {name: counts.get(name, 0) for name in PINNED_COUNTS} != PINNED_COUNTS:
        problems.append(f"the pinned release needs exactly {PINNED_COUNTS}, the table holds {counts}")
    return problems


def frozen_problems(existing: Sequence[SplitRow], rebuilt: Mapping[str, str], *, pinned: bool) -> list[str]:
    """How a rebuild would change the merged table: another rule version, a changed set, a patient that left the source and, for a pinned release, a new one."""
    problems = [
        f"the merged table was built with the rule version {version!r}, not {RULE_VERSION!r}"
        for version in sorted({row.rule_version for row in existing} - {RULE_VERSION})
    ]
    held = {row.subjectid: row.set for row in existing}
    for subjectid, name in sorted(held.items()):
        if subjectid not in rebuilt:
            problems.append(f"patient {subjectid} is in the merged table and no longer in the source")
        elif rebuilt[subjectid] != name:
            problems.append(f"patient {subjectid} would change from {name} to {rebuilt[subjectid]}: the split is frozen")
    if pinned and held:
        problems += [f"new patient {subjectid} cannot appear in a pinned release" for subjectid in sorted(set(rebuilt) - set(held))]
    return problems


def source_problems(rows: Sequence[SplitRow], source_subjects: set[str], source_snapshot_id: int) -> list[str]:
    """The table must be built from the source as it is now: the same snapshot and exactly the same patients."""
    problems = []
    held = {row.subjectid for row in rows}
    for label, names in (
        ("have no row in the table", sorted(source_subjects - held)),
        ("are in the table and not in the source", sorted(held - source_subjects)),
    ):
        if names:
            problems.append(f"{len(names)} patients {label}, first {names[:3]}")
    if {row.source_snapshot_id for row in rows} - {source_snapshot_id}:
        problems.append(f"the table was built from another source snapshot than the current {source_snapshot_id}")
    return problems


def schema_problems(fields: Sequence[tuple[str, str]], expected: Sequence[tuple[str, str]], label: str) -> list[str]:
    return [] if list(fields) == list(expected) else [f"the {label} has the columns {list(fields)}, not {list(expected)}"]


def fixed_sample_problems(assignments: Mapping[str, str], cases: Mapping[int, str], sample: Sequence[SampleCase]) -> list[str]:
    """Each fixed-sample case must be in Bronze under its patient, and its patient must be in the set that its slot expects (development or test)."""
    if not sample:
        return ["the fixed sample has no case, so there is nothing to check"]
    problems = []
    for item in sample:
        kind = item.slot.rsplit("_", 1)[-1]
        if kind not in SLOT_KINDS:
            problems.append(f"case {item.case}: the slot {item.slot!r} is neither a development nor a test slot")
        elif item.case not in cases:
            problems.append(f"case {item.case} (slot {item.slot}) has no row in the source table")
        elif cases[item.case] != str(item.patient):
            problems.append(
                f"case {item.case} (slot {item.slot}) is under patient {cases[item.case]} in the source table, not the fixed sample's patient {item.patient}"
            )
        elif str(item.patient) not in assignments:
            problems.append(f"case {item.case} (slot {item.slot}): patient {item.patient} has no row in the split table")
        elif assignments[str(item.patient)] not in SLOT_KINDS[kind]:
            held = assignments[str(item.patient)]
            problems.append(f"case {item.case} (slot {item.slot}) of patient {item.patient} is in {held}, but a {kind} case must be in {SLOT_KINDS[kind]}")
    return problems


def read_fixed_sample(path: Path) -> list[SampleCase]:
    """The case, patient and slot of each line of a candidate file (``tests/fixtures/vitaldb/candidates.tsv``), or ``PatientSplitError``."""
    try:
        lines = path.read_text().splitlines()
    except OSError, UnicodeDecodeError:
        raise PatientSplitError("the fixed-sample file cannot be read") from None
    if not lines or tuple(lines[0].split("\t")) != CANDIDATE_HEADER:
        raise PatientSplitError(f"the fixed-sample file does not start with the header {CANDIDATE_HEADER}")
    sample = []
    for number, line in enumerate(lines[1:], 2):
        fields = line.split("\t")
        if len(fields) != len(CANDIDATE_HEADER) or not CASE_NUMBER.fullmatch(fields[0]) or not CASE_NUMBER.fullmatch(fields[1]):
            raise PatientSplitError(f"line {number} of the fixed-sample file is not a candidate line")
        sample.append(SampleCase(int(fields[0]), int(fields[1]), fields[6]))
    return sample


def source_cases(rows: Iterable[tuple[str | None, str | None]]) -> tuple[dict[int, str], set[str]]:
    """The patient of each case and the distinct patients of the source rows. A missing value or a case under two patients is an error."""
    cases: dict[int, str] = {}
    patients: set[str] = set()
    problems: list[str] = []
    for caseid, subjectid in rows:
        if caseid is None or subjectid is None or not CASE_NUMBER.fullmatch(caseid):
            problems.append(f"a source row has the caseid {caseid!r} and the subjectid {subjectid!r}: both must be present and the caseid must be digits")
            continue
        if cases.setdefault(int(caseid), subjectid) != subjectid:
            problems.append(f"case {caseid} is listed under two patients in the source table")
        patients.add(subjectid)
    if problems:
        raise PatientSplitError(*problems[:MESSAGE_LIMIT])
    return cases, patients


def checks(rows: Sequence[SplitRow], source: tuple[dict[int, str], set[str], int], sample: Sequence[SampleCase] | None, *, pinned: bool) -> list[str]:
    """Every check of the card on ``rows`` against the source: the table by itself, the source, and the fixed sample when there is one."""
    cases, patients, snapshot = source
    problems = table_problems(rows, pinned=pinned) + source_problems(rows, patients, snapshot)
    if sample is not None:
        problems += fixed_sample_problems({row.subjectid: row.set for row in rows}, cases, sample)
    return problems


def counts_of(rows: Sequence[SplitRow]) -> dict[str, int]:
    counted = Counter(row.set for row in rows)
    return {name: counted.get(name, 0) for name in TARGET_PERCENT}


def values_query(rows: Sequence[SplitRow]) -> str:
    """One ``VALUES`` query that holds the rows as literals in the order of ``COLUMNS``, so that Spark builds the frame in the JVM.

    No Python worker process starts: Spark's Python workers hang from time to time under the amd64 emulation of the test computer. Every value is
    checked before it enters the text: a patient number in its published form, one of the three set names, a rule version of plain characters and a
    whole number. A value outside that is an error.
    """
    if not rows:
        raise PatientSplitError("the split table has no row to write")
    tuples = []
    for row in rows:
        patient_number(row.subjectid)
        if row.set not in TARGET_PERCENT or not SAFE_VERSION.fullmatch(row.rule_version) or type(row.source_snapshot_id) is not int:
            raise PatientSplitError(f"the row of patient {row.subjectid} holds a value that cannot be written")
        tuples.append(f"('{row.subjectid}', '{row.set}', '{row.rule_version}', {row.source_snapshot_id}L)")
    return f"VALUES {', '.join(tuples)}"
