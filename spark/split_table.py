"""The rows, the checks, the freeze and the order of a build of the patient split table, in plain data with no Spark.

The job ``spark/jobs/patient_split.py`` reads and writes the tables and passes its Spark steps to ``run_build`` and ``run_verify``, so that the card's checks
and the order of a build run in the fast tests on plain rows and stand-in steps. The assignment rule is only in ``scripts/sample/split.py``: this module
calls it and keeps no copy.

The test set is sealed until the choice record is frozen, so nothing here may name which set a patient or a case is in. Every failure is a code and a count
(``two_sets: 1 patient``), never an identifier, except a table whose columns are not those of its card, which is ``schema: failed``. A check returns its
problems and never raises for a failed check, so that one run names every cause.

A build resolves the branch to one hash and reads only at that hash. It writes only while the branch still has that hash, reads the written table at the
hash of the write and checks it, and ends only when the branch still has that hash.
"""

from __future__ import annotations

import hashlib
import re
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.sample.split import BUCKETS, HEX_DIGITS, SEED, TRAIN_BUCKETS, VALIDATION_BUCKETS, Part, patient_part
from spark.settings import CATALOG, catalog_options

SET_NAMES: Mapping[Part, str] = {"train": "training", "validation": "validation", "test": "test"}
RULE_VERSION = f"patient_split:seed={SEED}:sha256_hex{HEX_DIGITS}_mod{BUCKETS}:training<{TRAIN_BUCKETS}:validation<{TRAIN_BUCKETS + VALIDATION_BUCKETS}:test"
TARGET_PERCENT = {"training": 70, "validation": 10, "test": 20}
PINNED_COUNTS = {"training": 4287, "validation": 594, "test": 1209}
SHARE_TOLERANCE_POINTS = 1
COLUMNS = (("subjectid", "string"), ("set", "string"), ("rule_version", "string"), ("source_snapshot_id", "bigint"))
SOURCE_COLUMNS = (("caseid", "string"), ("subjectid", "string"))
HASH = re.compile(r"[0-9a-f]{64}")
PATIENT_NUMBER = re.compile(r"[1-9][0-9]*")
CASE_NUMBER = re.compile(r"[0-9]+")
SAFE_VERSION = re.compile(r"[A-Za-z0-9_:=<>.-]+")
CANDIDATE_HEADER = ("case", "patient", "key", "size_bytes", "sha256", "arterial_waveform", "slot")
# The assignment roster: the 13 case-to-patient assignments of tests/fixtures/vitaldb/candidates.tsv (the 9 development candidates of the fixed sample
# and the 4 test-patient refusal cases). A fast test holds both values to the tracked file.
FIXED_SAMPLE_ROWS = 13
FIXED_SAMPLE_SHA256 = "2e512b4530fe1ca8a0bfc6a3fde62eb5c102e7ea9d4b182ed1197ca6f0b4212e"
SLOT_KINDS = {"development": ("training", "validation"), "test": ("test",)}
CODES = frozenset(
    {
        "bad_subjectid",
        "two_sets",
        "repeated_patient",
        "unknown_set",
        "rule_mismatch",
        "rule_version",
        "mixed_snapshots",
        "empty_table",
        "shares",
        "pinned_counts",
        "inherited_empty",
        "inherited_duplicates",
        "inherited_versions",
        "inherited_rule_version",
        "changed_set",
        "left_source",
        "new_patient",
        "source_missing",
        "source_extra",
        "source_snapshot",
        "schema",
        "sample_empty",
        "sample_bad_slot",
        "sample_missing_case",
        "sample_wrong_patient",
        "sample_missing_patient",
        "sample_mismatch",
        "sample_file",
        "case_two_patients",
        "source_row",
        "unwritable_row",
        "readback_differs",
        "table_missing",
        "branch_moved",
        "catalog_hash",
        "source_no_snapshot",
        "source_empty",
        "source_table",
    }
)
CODES_OF_THE_JOB = frozenset({"source_no_snapshot", "source_empty", "source_table"})


class PatientSplitError(ValueError):
    """The split cannot be built or a check failed. ``problems`` holds one message for each cause, each a code and a count."""

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


@dataclass(frozen=True, slots=True)
class Source:
    """What a read of Bronze at one hash gives: the patient of each case, the distinct patients and the snapshot that was read."""

    cases: dict[int, str]
    patients: set[str]
    snapshot_id: int


@dataclass(frozen=True, slots=True)
class TableState:
    """The rows of the split table at one hash and the snapshot that holds them."""

    rows: list[SplitRow]
    snapshot_id: int | None


@dataclass(frozen=True, slots=True)
class Operations:
    """The Spark steps of a build. ``head`` is the hash of the branch now. The reads are at the hash that is given. ``read_table`` gives ``None`` for a hash
    that holds no table. ``write`` writes the whole table to the branch. ``write_hash`` is given the hash that was checked and gives the hash of the write,
    or ``None`` when the branch head is not the one commit of the write directly on that hash. ``prepare`` is given the checked hash, creates the table's
    namespace when the branch has none, as a commit of its own, and gives the hash that the write is made on (the checked hash when nothing was needed)."""

    head: Callable[[], str]
    read_source: Callable[[str], Source]
    read_table: Callable[[str], TableState | None]
    write: Callable[..., None]
    write_hash: Callable[[str], str | None]
    prepare: Callable[[str], str]


@dataclass(frozen=True, slots=True)
class Outcome:
    outcome: str
    head: str
    rows: list[SplitRow]
    source: Source
    table_snapshot_id: int | None
    checks_run: list[str]
    sample_cases: int | None
    checked_hash: str
    write_hash: str | None

    def facts(self) -> dict[str, Any]:
        """What the result line may hold: the hash that was checked, the hash of the write, snapshot IDs, counts, counts per set and the checks that ran.

        Nothing in it is a function of individual rows, so nothing in it can tell one patient or one set of patients from another.
        """
        return {
            "outcome": self.outcome,
            "checked_hash": self.checked_hash,
            "write_hash": self.write_hash,
            "branch_hash": self.head,
            "snapshot_id": self.table_snapshot_id,
            "source_snapshot_id": self.source.snapshot_id,
            "rule_version": RULE_VERSION,
            "patients": len(self.rows),
            "cases": len(self.source.cases),
            "counts": counts_of(self.rows),
            "checks_run": self.checks_run,
            "fixed_sample_cases_checked": self.sample_cases,
        }


def unit(count: int, noun: str) -> str:
    return f"{count} {noun}{'' if count == 1 else 's'}"


def row_order(row: SplitRow) -> tuple[int, str]:
    return len(row.subjectid), row.subjectid


def patient_number(subjectid: str) -> int:
    """The patient number of a ``subjectid`` in the published form (whole number, no sign, no zero before the first digit), or ``PatientSplitError``."""
    if PATIENT_NUMBER.fullmatch(subjectid) is None or not subjectid.isascii():
        raise PatientSplitError(f"bad_subjectid: {unit(1, 'subjectid')}")
    return int(subjectid)


def build_assignments(subject_ids: Iterable[str]) -> dict[str, str]:
    """The set of each patient, by the one rule of ``scripts/sample/split.py``. A ``subjectid`` that is not in its published form is counted, never named."""
    assignments: dict[str, str] = {}
    refused: set[str] = set()
    for subjectid in subject_ids:
        try:
            assignments[subjectid] = SET_NAMES[patient_part(patient_number(subjectid))]
        except PatientSplitError:
            refused.add(subjectid)
    if refused:
        raise PatientSplitError(f"bad_subjectid: {unit(len(refused), 'subjectid')}")
    return assignments


def share_problems(counts: Mapping[str, int]) -> list[str]:
    """The sets whose share of the patients is more than one percentage point from its target of 70, 10 or 20 percent. Whole-number arithmetic only."""
    total = sum(counts.get(name, 0) for name in TARGET_PERCENT)
    off = [name for name, percent in TARGET_PERCENT.items() if abs(counts.get(name, 0) * 100 - percent * total) > SHARE_TOLERANCE_POINTS * total]
    if not off:
        return []
    return [f"shares: {unit(len(off), 'set')} more than {SHARE_TOLERANCE_POINTS} percentage point from 70, 10 and 20 percent, counts {counts_text(counts)}"]


def counts_text(counts: Mapping[str, int]) -> str:
    return ", ".join(f"{name} {counts.get(name, 0)}" for name in TARGET_PERCENT)


def table_problems(rows: Sequence[SplitRow], *, pinned: bool) -> list[str]:
    """What is wrong with the rows by themselves: a patient twice, a set or version that is not the rule's, and for the pinned release the shares and counts."""
    if not rows:
        return [f"empty_table: {unit(0, 'row')}"]
    problems: list[str] = []
    sets_of: dict[str, set[str]] = {}
    seen = Counter(row.subjectid for row in rows)
    for row in rows:
        sets_of.setdefault(row.subjectid, set()).add(row.set)
    two = sum(1 for names in sets_of.values() if len(names) > 1)
    repeated = sum(1 for subjectid, count in seen.items() if count > 1 and len(sets_of[subjectid]) == 1)
    for code, count, noun in (("two_sets", two, "patient"), ("repeated_patient", repeated, "patient")):
        if count:
            problems.append(f"{code}: {unit(count, noun)}")
    other_version = sum(1 for row in rows if row.rule_version != RULE_VERSION)
    if other_version:
        problems.append(f"rule_version: {unit(other_version, 'row')}")
    snapshots = len({row.source_snapshot_id for row in rows})
    if snapshots > 1:
        problems.append(f"mixed_snapshots: {unit(snapshots, 'snapshot')}")
    unknown = 0
    bad: set[str] = set()
    mismatch: set[str] = set()
    for row in rows:
        if row.set not in TARGET_PERCENT:
            unknown += 1
            continue
        try:
            expected = SET_NAMES[patient_part(patient_number(row.subjectid))]
        except PatientSplitError:
            bad.add(row.subjectid)
            continue
        if row.set != expected:
            mismatch.add(row.subjectid)
    for code, count, noun in (("unknown_set", unknown, "row"), ("bad_subjectid", len(bad), "subjectid"), ("rule_mismatch", len(mismatch), "patient")):
        if count:
            problems.append(f"{code}: {unit(count, noun)}")
    if pinned:
        counts = dict(Counter(row.set for row in rows))
        problems += share_problems(counts)
        if {name: counts.get(name, 0) for name in PINNED_COUNTS} != PINNED_COUNTS:
            problems.append(f"pinned_counts: counts {counts_text(counts)} are not exactly {counts_text(PINNED_COUNTS)}")
    return problems


def inherited_problems(rows: Sequence[SplitRow]) -> list[str]:
    """What makes the table that a rebuild inherits unfit as a freeze, before it is compared with anything: no row, a patient twice or two rule versions."""
    if not rows:
        return [f"inherited_empty: {unit(0, 'row')}"]
    problems = []
    twice = sum(1 for count in Counter(row.subjectid for row in rows).values() if count > 1)
    if twice:
        problems.append(f"inherited_duplicates: {unit(twice, 'patient')}")
    versions = len({row.rule_version for row in rows})
    if versions > 1:
        problems.append(f"inherited_versions: {unit(versions, 'rule version')}")
    return problems


def frozen_problems(existing: Sequence[SplitRow] | None, rebuilt: Mapping[str, str], *, pinned: bool) -> list[str]:
    """How a rebuild would change the table that the branch holds. ``None`` is a first build and has nothing to freeze.

    The inherited table must be valid by itself first. Then: another rule version, a changed set, a patient that left the source and, for a pinned release,
    a new patient.
    """
    if existing is None:
        return []
    problems = inherited_problems(existing)
    if problems:
        return problems
    other_version = sum(1 for row in existing if row.rule_version != RULE_VERSION)
    if other_version:
        return [f"inherited_rule_version: {unit(other_version, 'row')}"]
    held = {row.subjectid: row.set for row in existing}
    left = sum(1 for subjectid in held if subjectid not in rebuilt)
    changed = sum(1 for subjectid, name in held.items() if subjectid in rebuilt and rebuilt[subjectid] != name)
    new = len(set(rebuilt) - set(held)) if pinned else 0
    return [f"{code}: {unit(count, 'patient')}" for code, count in (("changed_set", changed), ("left_source", left), ("new_patient", new)) if count]


def source_problems(rows: Sequence[SplitRow], source_subjects: set[str], source_snapshot_id: int) -> list[str]:
    """The table must be built from the source as it is now: the same snapshot and exactly the same patients."""
    held = {row.subjectid for row in rows}
    problems = [
        f"{code}: {unit(count, 'patient')}"
        for code, count in (("source_missing", len(source_subjects - held)), ("source_extra", len(held - source_subjects)))
        if count
    ]
    if {row.source_snapshot_id for row in rows} - {source_snapshot_id}:
        problems.append("source_snapshot: the table was built from another source snapshot")
    return problems


def schema_problems(fields: Sequence[tuple[str, str]], expected: Sequence[tuple[str, str]]) -> list[str]:
    """The refusal of a table whose columns are not those of its card: the code and a flag, with no table and no count of columns."""
    if list(fields) == list(expected):
        return []
    return ["schema: failed"]


def fixed_sample_problems(assignments: Mapping[str, str], cases: Mapping[int, str], sample: Sequence[SampleCase]) -> list[str]:
    """Each fixed-sample case must be in Bronze under its patient, and its patient must be in the set that its slot expects (development or test)."""
    if not sample:
        return [f"sample_empty: {unit(0, 'case')}"]
    found: dict[str, set[int]] = {}
    for item in sample:
        kind = item.slot.rsplit("_", 1)[-1]
        if kind not in SLOT_KINDS:
            code = "sample_bad_slot"
        elif item.case not in cases:
            code = "sample_missing_case"
        elif cases[item.case] != str(item.patient):
            code = "sample_wrong_patient"
        elif str(item.patient) not in assignments:
            code = "sample_missing_patient"
        elif assignments[str(item.patient)] not in SLOT_KINDS[kind]:
            code = "sample_mismatch"
        else:
            continue
        found.setdefault(code, set()).add(item.case)
    return [f"{code}: {unit(len(numbers), 'case')}" for code, numbers in found.items()]


def read_fixed_sample(path: Path) -> list[SampleCase]:
    """The case, patient and slot of each line of the complete tracked candidate file, or ``PatientSplitError``. Any other file is refused."""
    try:
        data = path.read_bytes()
    except OSError:
        raise PatientSplitError("sample_file: the fixed-sample file cannot be read") from None
    if hashlib.sha256(data).hexdigest() != FIXED_SAMPLE_SHA256:
        raise PatientSplitError("sample_file: the file is not the complete pinned fixed sample")
    try:
        lines = data.decode().splitlines()
    except UnicodeDecodeError:
        raise PatientSplitError("sample_file: the fixed-sample file cannot be read") from None
    if len(lines) - 1 != FIXED_SAMPLE_ROWS or tuple(lines[0].split("\t")) != CANDIDATE_HEADER:
        raise PatientSplitError("sample_file: the pinned file does not have the pinned form")
    sample = []
    for line in lines[1:]:
        fields = line.split("\t")
        if len(fields) != len(CANDIDATE_HEADER) or not CASE_NUMBER.fullmatch(fields[0]) or not CASE_NUMBER.fullmatch(fields[1]):
            raise PatientSplitError("sample_file: a line is not a candidate line")
        sample.append(SampleCase(int(fields[0]), int(fields[1]), fields[6]))
    return sample


def source_cases(rows: Iterable[tuple[str | None, str | None]]) -> tuple[dict[int, str], set[str]]:
    """The patient of each case and the distinct patients of the source rows. A missing value or a case under two patients is counted."""
    cases: dict[int, str] = {}
    patients: set[str] = set()
    bad = 0
    twice: set[int] = set()
    for caseid, subjectid in rows:
        if caseid is None or subjectid is None or not CASE_NUMBER.fullmatch(caseid):
            bad += 1
            continue
        if cases.setdefault(int(caseid), subjectid) != subjectid:
            twice.add(int(caseid))
        patients.add(subjectid)
    problems = [f"{code}: {unit(count, noun)}" for code, count, noun in (("source_row", bad, "row"), ("case_two_patients", len(twice), "case")) if count]
    if problems:
        raise PatientSplitError(*problems)
    return cases, patients


def check_names(*, pinned: bool, sample: Sequence[SampleCase] | None) -> list[str]:
    """The checks that ``checks`` runs for these options, for the result line."""
    names = ["two_sets", "rule_match", "rule_version", "source_snapshot", "source_patients"]
    return names + (["shares", "pinned_counts"] if pinned else []) + (["fixed_sample"] if sample is not None else [])


def checks(rows: Sequence[SplitRow], source: Source, sample: Sequence[SampleCase] | None, *, pinned: bool) -> list[str]:
    """Every check of the card on ``rows`` against the source. The shares and the exact counts are the full cohort's and run only for the pinned release."""
    problems = table_problems(rows, pinned=pinned) + source_problems(rows, source.patients, source.snapshot_id)
    if sample is not None:
        problems += fixed_sample_problems({row.subjectid: row.set for row in rows}, source.cases, sample)
    return problems


def counts_of(rows: Sequence[SplitRow]) -> dict[str, int]:
    counted = Counter(row.set for row in rows)
    return {name: counted.get(name, 0) for name in TARGET_PERCENT}


BRANCH_MOVED = "branch_moved: the branch head is not the one that was checked"


def run_build(ops: Operations, head: str, *, pinned: bool, sample: Sequence[SampleCase] | None) -> Outcome:
    """Build the table on the branch whose head is ``head``, or leave it as it is when nothing changes.

    The order: read Bronze and the table at ``head``; validate the table that is inherited; freeze (compare with it); check the rows to write; write only
    while the branch still has ``head``, after the namespace of a new table is committed on it; take the hash of the write from the branch's log (the head,
    one commit directly on the hash that the write was made on); read the table and Bronze at that hash and check them against what was written; end only
    when the branch still has that hash.
    """
    source = ops.read_source(head)
    assignments = build_assignments(source.patients)
    rows = [SplitRow(subjectid, assignments[subjectid], RULE_VERSION, source.snapshot_id) for subjectid in sorted(assignments, key=int)]
    state = ops.read_table(head)
    problems = frozen_problems(None if state is None else state.rows, assignments, pinned=pinned) + checks(rows, source, sample, pinned=pinned)
    if problems:
        raise PatientSplitError(*problems)
    unchanged = state is not None and {(row.subjectid, row.set, row.source_snapshot_id) for row in state.rows} == {
        (row.subjectid, row.set, row.source_snapshot_id) for row in rows
    }
    at = head
    written: str | None = None
    if not unchanged:
        if ops.head() != head:
            raise PatientSplitError(BRANCH_MOVED)
        on = ops.prepare(head)
        ops.write(rows, exists=state is not None)
        written = ops.write_hash(on)
        if written is None:
            raise PatientSplitError(BRANCH_MOVED)
        at = written
    return finish(ops, (head, written, at), rows, pinned=pinned, sample=sample, outcome="unchanged" if unchanged else "written")


def finish(
    ops: Operations, hashes: tuple[str, str | None, str], rows: Sequence[SplitRow], *, pinned: bool, sample: Sequence[SampleCase] | None, outcome: str
) -> Outcome:
    """Read the table and Bronze at the last of ``hashes`` (checked, write, read), check them against ``rows`` and confirm that the branch still has it."""
    checked, written, at = hashes
    source = ops.read_source(at)
    state = ops.read_table(at)
    if state is None:
        raise PatientSplitError(f"table_missing: {unit(0, 'table')}")
    problems = checks(state.rows, source, sample, pinned=pinned)
    if sorted(state.rows, key=row_order) != sorted(rows, key=row_order):
        problems.append("readback_differs: the table that the branch holds is not the table that was written")
    if problems:
        raise PatientSplitError(*problems)
    if ops.head() != at:
        raise PatientSplitError(BRANCH_MOVED)
    names = check_names(pinned=pinned, sample=sample)
    sample_cases = None if sample is None else len(sample)
    return Outcome(outcome, at, sorted(state.rows, key=row_order), source, state.snapshot_id, names, sample_cases, checked, written)


def run_verify(ops: Operations, head: str, *, pinned: bool, sample: Sequence[SampleCase] | None) -> Outcome:
    """Run every check on the table that the branch holds at ``head``, and confirm that the branch still has ``head``. Nothing is written."""
    source = ops.read_source(head)
    state = ops.read_table(head)
    if state is None:
        raise PatientSplitError(f"table_missing: {unit(0, 'table')}")
    problems = checks(state.rows, source, sample, pinned=pinned)
    if problems:
        raise PatientSplitError(*problems)
    if ops.head() != head:
        raise PatientSplitError(BRANCH_MOVED)
    names = check_names(pinned=pinned, sample=sample)
    sample_cases = None if sample is None else len(sample)
    return Outcome("verified", head, list(state.rows), source, state.snapshot_id, names, sample_cases, head, None)


def values_query(rows: Sequence[SplitRow]) -> str:
    """One ``VALUES`` query that holds the rows as literals in the order of ``COLUMNS``, so that Spark builds the frame in the JVM.

    No Python worker process starts: Spark's Python workers hang from time to time under the amd64 emulation of the test computer. Every value is
    checked before it enters the text: a patient number in its published form, one of the three set names, a rule version of plain characters and a
    whole number. A value outside that is an error that names no value.
    """
    if not rows:
        raise PatientSplitError("unwritable_row: no row to write")
    tuples = []
    for row in rows:
        patient_number(row.subjectid)
        if row.set not in TARGET_PERCENT or not SAFE_VERSION.fullmatch(row.rule_version) or type(row.source_snapshot_id) is not int:
            raise PatientSplitError(f"unwritable_row: {unit(1, 'row')}")
        tuples.append(f"('{row.subjectid}', '{row.set}', '{row.rule_version}', {row.source_snapshot_id}L)")
    return f"VALUES {', '.join(tuples)}"


def write_commit(entries: Sequence[Mapping[str, Any]], checked: str, allowed: Sequence[tuple[str, ...]]) -> str | None:
    """The hash of the write, from the branch's log (newest first, with each commit's operations), or ``None``.

    The write is the head when it is one commit directly on the checked hash and it only puts the ``allowed`` keys. Any other head, such as a commit by someone
    else, two commits, or a commit that changes another key, is not the write.
    """
    if not entries:
        return None
    entry = entries[0]
    commit = (entry.get("commitMeta") or {}).get("hash")
    operations = entry.get("operations") or []
    if not isinstance(commit, str) or entry.get("parentCommitHash") != checked or not operations:
        return None
    for operation in operations:
        if operation.get("type") != "PUT" or tuple((operation.get("key") or {}).get("elements") or ()) not in allowed:
            return None
    return commit


def catalog_conf(name: str, *, nessie_uri: str, warehouse: str, region: str, branch: str, at: str) -> dict[str, str]:
    """The Spark settings of the read-only catalog ``name`` on ``branch``, pinned to the hash ``at``."""
    if HASH.fullmatch(at) is None:
        raise PatientSplitError("catalog_hash: the hash is not a Nessie hash")
    options = catalog_options(nessie_uri=nessie_uri, warehouse=warehouse, ref=branch, region=region)
    conf = {key.replace(f"spark.sql.catalog.{CATALOG}", f"spark.sql.catalog.{name}", 1): value for key, value in options.items()}
    conf[f"spark.sql.catalog.{name}.ref.hash"] = at
    return conf
