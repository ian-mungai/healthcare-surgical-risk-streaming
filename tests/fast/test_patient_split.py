"""The patient split table: its rows, its checks, its freeze and the order of a build, in plain data with no Spark.

Failure modes covered here:

- The job carries its own copy of the assignment rule, so the table and the stream can disagree about a patient.
- A patient number that is not in its published form is accepted and gets a set.
- A message, a result or a record names a patient or a case, or a set beside one. The test set is sealed: only codes and flags may leave the job.
- A patient is in two sets, or twice in one, and the table is still taken as good.
- A table that differs from the rule for one patient passes, or the pinned release's shares and exact counts are not held, or they are demanded of a
  sample-sized source.
- A rebuild changes a patient's set, drops a patient, adds one to a pinned release or uses another rule version and the freeze does not stop it, or the
  inherited table is empty, holds a patient twice or holds two rule versions, and the rebuild overwrites it.
- A fixed-sample case is in the wrong set, is missing from Bronze or is listed under another patient, the check passes on nothing, or it passes on a
  candidate file that is not the complete tracked one.
- A build reads or checks a branch head other than the one it resolved, writes when the branch has moved, names a hash it did not check, or finishes
  after the branch moved.
- A step of the build (validate the inherited table, freeze, write, read back, check, confirm the head) is left out and no test notices.
- The table was built from another Bronze snapshot or another set of patients than the branch holds now, or has other columns than the card's.

The real runs on Spark, Nessie and the full clinical table are in ``tests/e2e/silver_patient_split.py``.
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib
import io
import json
import os
import re
import tempfile
import unittest
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any, get_args
from unittest import mock
from urllib.parse import unquote

from scripts.sample.candidates import CANDIDATES_FILE, SLOTS, read_candidates
from scripts.sample.split import Part, patient_part
from scripts.stack.nessie import NessieClient
from spark import split_cli, split_table
from spark.split_table import (
    COLUMNS,
    PINNED_COUNTS,
    RULE_VERSION,
    SET_NAMES,
    PatientSplitError,
    SampleCase,
    SplitRow,
    build_assignments,
    fixed_sample_problems,
    frozen_problems,
    patient_number,
    read_fixed_sample,
    schema_problems,
    share_problems,
    source_cases,
    source_problems,
    table_problems,
    values_query,
)

JOB_FILE = Path(split_table.__file__).parent / "jobs" / "patient_split.py"
COHORT = [str(number) for number in range(1, 6091)]
FULL = build_assignments(COHORT)
SNAPSHOT = 7_000_000_000_000_000_001
PLANTED_PATIENT = "424242"
PLANTED_CASE = "737373"
MESSAGE_FORM = re.compile(r"[a-z_]+: ")


def rows_of(assignments: dict[str, str], version: str = RULE_VERSION, snapshot: int = SNAPSHOT) -> list[SplitRow]:
    return [SplitRow(subjectid, name, version, snapshot) for subjectid, name in assignments.items()]


def other_set(name: str) -> str:
    return "test" if name != "test" else "training"


def tracked_sample() -> list[SampleCase]:
    return read_fixed_sample(CANDIDATES_FILE)


def sample_cohort(sample: list[SampleCase]) -> tuple[dict[str, str], dict[int, str]]:
    """The assignments of the sample's own patients and the case-to-patient map that Bronze would give."""
    return build_assignments(str(item.patient) for item in sample), {item.case: str(item.patient) for item in sample}


def source_of(patients: dict[str, str], snapshot: int = 11, cases: dict[int, str] | None = None) -> Any:
    """A stand-in source: one case for each patient, with the same number, unless the cases are given."""
    return split_table.Source(cases or {int(subjectid): subjectid for subjectid in patients}, set(patients), snapshot)


class AssignmentTest(unittest.TestCase):
    def test_each_set_is_the_part_that_the_one_rule_gives(self) -> None:
        assignments = build_assignments(str(number) for number in range(1, 2000))
        self.assertEqual({str(number): SET_NAMES[patient_part(number)] for number in range(1, 2000)}, assignments)

    def test_the_job_holds_no_second_copy_of_the_rule(self) -> None:
        for path in (Path(split_table.__file__), JOB_FILE):
            text = path.read_text()
            for marker in ("% BUCKETS", "% 100", ", 16)", "[:HEX_DIGITS]", "[:15]", "patient_bucket"):
                with self.subTest(file=path.name, marker=marker):
                    self.assertNotIn(marker, text)

    def test_every_part_has_one_set_name_and_the_names_are_the_three_of_the_card(self) -> None:
        self.assertEqual(set(get_args(Part)), set(SET_NAMES))
        self.assertEqual({"training", "validation", "test"}, set(SET_NAMES.values()))

    def test_the_pinned_release_gives_the_frozen_counts(self) -> None:
        self.assertEqual({"training": 4287, "validation": 594, "test": 1209}, PINNED_COUNTS)
        self.assertEqual(PINNED_COUNTS, dict(Counter(FULL.values())))

    def test_the_rule_version_is_frozen(self) -> None:
        self.assertEqual("patient_split:seed=1:sha256_hex15_mod100:training<70:validation<80:test", RULE_VERSION)

    def test_a_subject_id_keeps_its_published_form_or_is_refused_without_naming_it(self) -> None:
        self.assertEqual(6090, patient_number("6090"))
        for text in ("", "0", "007", " 7", "7 ", "7.0", "-3", "abc", "1e3", "١٢", "+5"):
            with self.subTest(text=text), self.assertRaises(PatientSplitError) as caught:
                patient_number(text)
            self.assertEqual(["bad_subjectid: failed"], caught.exception.problems)

    def test_a_cohort_with_a_bad_subject_id_is_refused_with_the_count_only(self) -> None:
        with self.assertRaises(PatientSplitError) as caught:
            build_assignments(["1", "007", "3", "0"])
        self.assertEqual(["bad_subjectid: failed"], caught.exception.problems)


class TableTest(unittest.TestCase):
    def test_the_full_cohort_table_has_no_problem(self) -> None:
        self.assertEqual([], table_problems(rows_of(FULL), pinned=True))

    def test_a_patient_in_two_sets_is_counted(self) -> None:
        rows = rows_of(FULL) + [SplitRow("25", other_set(FULL["25"]), RULE_VERSION, SNAPSHOT)]
        self.assertIn("two_sets: failed", table_problems(rows, pinned=False))

    def test_a_patient_twice_in_one_set_is_counted(self) -> None:
        rows = rows_of(FULL) + [SplitRow("25", FULL["25"], RULE_VERSION, SNAPSHOT)]
        self.assertIn("repeated_patient: failed", table_problems(rows, pinned=False))

    def test_a_set_the_card_does_not_name_is_counted(self) -> None:
        rows = [SplitRow("1", "train", RULE_VERSION, SNAPSHOT), *rows_of(FULL)[1:]]
        self.assertIn("unknown_set: failed", table_problems(rows, pinned=False))

    def test_a_patient_whose_set_differs_from_the_rule_is_counted_even_when_the_counts_hold(self) -> None:
        # Swap the sets of one training and one test patient: every count and share stays the same.
        training = next(subjectid for subjectid, name in FULL.items() if name == "training")
        test = next(subjectid for subjectid, name in FULL.items() if name == "test")
        problems = table_problems(rows_of({**FULL, training: "test", test: "training"}), pinned=True)
        self.assertEqual(["rule_mismatch: failed"], problems)

    def test_a_table_built_with_another_version_of_the_rule_is_counted(self) -> None:
        problems = table_problems(rows_of(FULL, version="patient_split:seed=2"), pinned=True)
        self.assertEqual(["rule_version: failed"], problems)

    def test_a_subject_id_not_in_its_published_form_is_counted(self) -> None:
        rows = [*rows_of(FULL), SplitRow("007", "training", RULE_VERSION, SNAPSHOT)]
        self.assertIn("bad_subjectid: failed", table_problems(rows, pinned=False))

    def test_an_empty_table_is_a_problem(self) -> None:
        self.assertEqual(["empty_table: failed"], table_problems([], pinned=False))

    def test_a_table_from_more_than_one_snapshot_is_counted(self) -> None:
        rows = [SplitRow("1", FULL["1"], RULE_VERSION, 1), SplitRow("2", FULL["2"], RULE_VERSION, 2)]
        self.assertIn("mixed_snapshots: failed", table_problems(rows, pinned=False))

    def test_the_pinned_release_needs_its_exact_counts(self) -> None:
        one_missing = {subjectid: name for subjectid, name in FULL.items() if subjectid != "1"}
        self.assertEqual([], table_problems(rows_of(one_missing), pinned=False))
        self.assertTrue([problem for problem in table_problems(rows_of(one_missing), pinned=True) if problem.startswith("pinned_counts: ")])

    def test_the_shares_are_held_within_one_percentage_point_in_whole_numbers(self) -> None:
        self.assertEqual([], share_problems({"training": 700, "validation": 100, "test": 200}))
        self.assertEqual([], share_problems({"training": 710, "validation": 100, "test": 190}))
        self.assertEqual([], share_problems({"training": 690, "validation": 110, "test": 200}))
        for counts in (
            {"training": 711, "validation": 100, "test": 189},
            {"training": 700, "validation": 89, "test": 211},
            {"training": 700, "validation": 111, "test": 189},
            {"training": 1000, "validation": 0, "test": 0},
        ):
            with self.subTest(counts=counts):
                self.assertTrue(share_problems(counts))

    def test_the_shares_and_the_exact_counts_are_demanded_of_the_pinned_cohort_only(self) -> None:
        # The tracked fixed sample as a source: 7, 0 and 3 patients in the three sets, nowhere near 70, 10 and 20 percent.
        assignments, _ = sample_cohort(tracked_sample())
        rows = rows_of(assignments)
        self.assertEqual([], table_problems(rows, pinned=False))
        self.assertTrue([problem for problem in table_problems(rows, pinned=True) if problem.startswith(("shares: ", "pinned_counts: "))])

    def test_the_result_names_the_checks_that_ran(self) -> None:
        sample = tracked_sample()
        sample_checks = ["two_sets", "rule_match", "rule_version", "source_snapshot", "source_patients"]
        self.assertEqual(sample_checks, split_table.check_names(pinned=False, sample=None))
        self.assertEqual([*sample_checks, "shares", "pinned_counts"], split_table.check_names(pinned=True, sample=None))
        self.assertEqual([*sample_checks, "fixed_sample"], split_table.check_names(pinned=False, sample=sample))
        self.assertEqual([*sample_checks, "shares", "pinned_counts", "fixed_sample"], split_table.check_names(pinned=True, sample=sample))


class InheritedTest(unittest.TestCase):
    """The table that a rebuild inherits must be valid on its own before it is compared with anything."""

    def test_a_valid_inherited_table_has_no_problem(self) -> None:
        self.assertEqual([], split_table.inherited_problems(rows_of(FULL)))

    def test_an_empty_inherited_table_is_refused(self) -> None:
        self.assertEqual(["inherited_empty: failed"], split_table.inherited_problems([]))

    def test_a_patient_twice_in_the_inherited_table_is_refused_whatever_the_order(self) -> None:
        wrong, right = other_set(FULL["25"]), FULL["25"]
        for order in ((wrong, right), (right, wrong), (right, right)):
            with self.subTest(order=order):
                rows = [row for row in rows_of(FULL) if row.subjectid != "25"]
                rows += [SplitRow("25", name, RULE_VERSION, SNAPSHOT) for name in order]
                self.assertEqual(["inherited_duplicates: failed"], split_table.inherited_problems(rows))

    def test_an_inherited_table_of_two_rule_versions_is_refused(self) -> None:
        rows = [*rows_of(FULL)[:-1], SplitRow("6090", FULL["6090"], "patient_split:seed=2", SNAPSHOT)]
        self.assertEqual(["inherited_versions: failed"], split_table.inherited_problems(rows))

    def test_the_freeze_runs_the_inherited_checks_itself(self) -> None:
        self.assertEqual(["inherited_empty: failed"], frozen_problems([], FULL, pinned=True))
        rows = [*rows_of(FULL), SplitRow("25", other_set(FULL["25"]), RULE_VERSION, SNAPSHOT)]
        self.assertEqual(["inherited_duplicates: failed"], frozen_problems(rows, FULL, pinned=True))


class FrozenTest(unittest.TestCase):
    def test_a_rebuild_that_changes_nothing_has_no_problem(self) -> None:
        self.assertEqual([], frozen_problems(rows_of(FULL), FULL, pinned=True))

    def test_a_changed_set_is_counted_without_naming_the_patient_or_the_sets(self) -> None:
        existing = rows_of({**FULL, "30": other_set(FULL["30"]), "31": other_set(FULL["31"])})
        self.assertEqual(["changed_set: failed"], frozen_problems(existing, FULL, pinned=False))

    def test_a_patient_that_left_the_source_is_counted(self) -> None:
        rebuilt = {subjectid: name for subjectid, name in FULL.items() if subjectid != "5"}
        self.assertEqual(["left_source: failed"], frozen_problems(rows_of(FULL), rebuilt, pinned=False))

    def test_a_new_patient_is_refused_for_a_pinned_release_and_allowed_otherwise(self) -> None:
        existing = {subjectid: name for subjectid, name in FULL.items() if subjectid != "5"}
        self.assertEqual(["new_patient: failed"], frozen_problems(rows_of(existing), FULL, pinned=True))
        self.assertEqual([], frozen_problems(rows_of(existing), FULL, pinned=False))

    def test_a_first_build_has_nothing_to_freeze_even_for_a_pinned_release(self) -> None:
        self.assertEqual([], frozen_problems(None, FULL, pinned=True))

    def test_an_existing_table_of_another_rule_version_is_counted(self) -> None:
        problems = frozen_problems(rows_of(FULL, version="patient_split:seed=2"), FULL, pinned=False)
        self.assertEqual(["inherited_rule_version: failed"], problems)


class ValuesQueryTest(unittest.TestCase):
    """The rows reach Spark as one query built in the JVM, because Spark's Python worker processes hang under the amd64 emulation of the test computer."""

    def test_each_row_is_one_tuple_in_the_order_given_with_the_snapshot_ID_typed_as_a_long(self) -> None:
        query = values_query([SplitRow("1", "test", RULE_VERSION, SNAPSHOT), SplitRow("20", "training", RULE_VERSION, SNAPSHOT)])
        self.assertEqual(f"VALUES ('1', 'test', '{RULE_VERSION}', {SNAPSHOT}L), ('20', 'training', '{RULE_VERSION}', {SNAPSHOT}L)", query)

    def test_a_value_that_could_change_the_query_is_refused_without_naming_it(self) -> None:
        for row in (
            SplitRow("1'); DROP TABLE x; --", "test", RULE_VERSION, SNAPSHOT),
            SplitRow("1", "test') --", RULE_VERSION, SNAPSHOT),
            SplitRow("1", "test", "v'1", SNAPSHOT),
            SplitRow("007", "test", RULE_VERSION, SNAPSHOT),
        ):
            with self.subTest(row=row), self.assertRaises(PatientSplitError) as caught:
                values_query([row])
            self.assertNotIn("DROP", str(caught.exception))

    def test_no_row_is_refused_because_a_table_of_nothing_is_never_written(self) -> None:
        with self.assertRaises(PatientSplitError):
            values_query([])


class SourceTest(unittest.TestCase):
    def test_a_table_built_from_the_current_source_has_no_problem(self) -> None:
        self.assertEqual([], source_problems(rows_of(FULL), set(FULL), SNAPSHOT))

    def test_a_table_built_from_another_snapshot_is_counted(self) -> None:
        self.assertEqual(["source_snapshot: failed"], source_problems(rows_of(FULL), set(FULL), SNAPSHOT + 1))

    def test_a_patient_of_the_source_with_no_row_and_a_row_with_no_patient_are_counted(self) -> None:
        short = {subjectid: name for subjectid, name in FULL.items() if subjectid != "9"}
        self.assertEqual(["source_missing: failed"], source_problems(rows_of(short), set(FULL), SNAPSHOT))
        self.assertEqual(["source_extra: failed"], source_problems(rows_of(FULL), set(short), SNAPSHOT))

    def test_the_columns_are_the_four_of_the_card_in_order(self) -> None:
        self.assertEqual([], schema_problems(list(COLUMNS), COLUMNS))
        self.assertEqual((("subjectid", "string"), ("set", "string"), ("rule_version", "string"), ("source_snapshot_id", "bigint")), COLUMNS)
        self.assertEqual(["schema: failed"], schema_problems([("subjectid", "bigint"), *COLUMNS[1:]], COLUMNS))
        self.assertEqual(["schema: failed"], schema_problems(list(COLUMNS[:3]), COLUMNS))

    def test_source_rows_give_the_patient_of_each_case_and_refuse_bad_rows_with_counts_only(self) -> None:
        cases, patients = source_cases([("1", "10"), ("2", "10"), ("3", "11")])
        self.assertEqual(({1: "10", 2: "10", 3: "11"}, {"10", "11"}), (cases, patients))
        for rows, code in (
            ([("1", "10"), ("1", "11")], "case_two_patients: failed"),
            ([("1", None)], "source_row: failed"),
            ([(None, "10")], "source_row: failed"),
            ([("x", "10"), ("y", "11")], "source_row: failed"),
        ):
            with self.subTest(rows=rows), self.assertRaises(PatientSplitError) as caught:
                source_cases(rows)
            self.assertEqual([code], caught.exception.problems)


class FixedSampleTest(unittest.TestCase):
    def test_the_tracked_candidates_match_the_rule_and_cover_both_kinds_of_case(self) -> None:
        sample = tracked_sample()
        assignments, cases = sample_cohort(sample)
        self.assertEqual([], fixed_sample_problems(assignments, cases, sample))
        kinds = {item.slot.rsplit("_", 1)[1] for item in sample}
        self.assertEqual({"development", "test"}, kinds)
        self.assertEqual({slot.name for slot in SLOTS}, {item.slot for item in sample})

    def test_the_pin_is_the_row_count_and_the_digest_of_the_tracked_file(self) -> None:
        text = CANDIDATES_FILE.read_bytes()
        self.assertEqual(len(text.splitlines()) - 1, split_table.FIXED_SAMPLE_ROWS)
        self.assertEqual(hashlib.sha256(text).hexdigest(), split_table.FIXED_SAMPLE_SHA256)

    def test_a_development_case_in_test_is_counted(self) -> None:
        sample = tracked_sample()
        assignments, cases = sample_cohort(sample)
        assignments[str(next(item for item in sample if item.slot == "short_development").patient)] = "test"
        self.assertEqual(["sample_mismatch: failed"], fixed_sample_problems(assignments, cases, sample))

    def test_a_test_case_in_training_is_counted(self) -> None:
        sample = tracked_sample()
        assignments, cases = sample_cohort(sample)
        assignments[str(next(item for item in sample if item.slot == "arterial_test").patient)] = "training"
        self.assertEqual(["sample_mismatch: failed"], fixed_sample_problems(assignments, cases, sample))

    def test_a_case_that_bronze_lacks_is_counted(self) -> None:
        sample = tracked_sample()
        assignments, cases = sample_cohort(sample)
        del cases[sample[0].case]
        self.assertEqual(["sample_missing_case: failed"], fixed_sample_problems(assignments, cases, sample))

    def test_a_case_listed_under_another_patient_is_counted(self) -> None:
        sample = tracked_sample()
        assignments, cases = sample_cohort(sample)
        cases[sample[0].case] = str(sample[0].patient + 1)
        self.assertEqual(["sample_wrong_patient: failed"], fixed_sample_problems(assignments, cases, sample))

    def test_a_patient_missing_from_the_table_is_counted(self) -> None:
        sample = tracked_sample()
        assignments, cases = sample_cohort(sample)
        del assignments[str(sample[0].patient)]
        self.assertEqual(["sample_missing_patient: failed"], fixed_sample_problems(assignments, cases, sample))

    def test_a_slot_of_no_known_kind_is_counted(self) -> None:
        self.assertEqual(["sample_bad_slot: failed"], fixed_sample_problems({"1": "training"}, {1: "1"}, [SampleCase(case=1, patient=1, slot="odd_slot")]))

    def test_an_empty_sample_is_a_problem_so_that_the_check_never_passes_on_nothing(self) -> None:
        self.assertEqual(["sample_empty: failed"], fixed_sample_problems(FULL, {}, []))

    def test_only_the_complete_tracked_file_is_accepted(self) -> None:
        lines = CANDIDATES_FILE.read_text().splitlines()
        header, first = lines[0], lines[1]
        slot_changed = [line.replace("\tarterial_test", "\tlargest_development") if "\tarterial_test" in line else line for line in lines]
        bad = {
            "empty": "",
            "header only": header + "\n",
            "header and one row": f"{header}\n{first}\n",
            "last row dropped": "\n".join(lines[:-1]) + "\n",
            "a slot changed": "\n".join(slot_changed) + "\n",
            "a row repeated": "\n".join([*lines, first]) + "\n",
        }
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "candidates.tsv"
            for label, text in bad.items():
                path.write_text(text)
                with self.subTest(label), self.assertRaises(PatientSplitError) as caught:
                    read_fixed_sample(path)
                self.assertEqual(1, len(caught.exception.problems))
                self.assertTrue(caught.exception.problems[0].startswith("sample_file: "), caught.exception.problems)

    def test_a_file_that_cannot_be_read_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as folder, self.assertRaises(PatientSplitError):
            read_fixed_sample(Path(folder) / "missing.tsv")

    def test_the_tracked_file_reads_as_the_candidate_reader_does(self) -> None:
        theirs = [(item.case, item.patient, item.slot) for item in read_candidates(CANDIDATES_FILE)]
        self.assertEqual(theirs, [(item.case, item.patient, item.slot) for item in tracked_sample()])


@dataclass
class Lake:
    """A stand-in for the branch's reads and its write: hashes are names, every call is recorded, and a case can make the branch move or the write go wrong."""

    patients: dict[str, str]
    existing: list[SplitRow] | None = None
    snapshot: int = 11
    cases: dict[int, str] | None = None
    calls: list[tuple[Any, ...]] = field(default_factory=list)
    script: list[str] = field(default_factory=list)
    versions: dict[str, Any] = field(default_factory=dict)
    corrupt: Callable[[list[SplitRow]], list[SplitRow]] | None = None
    source_after_write: Any = None
    write_parent: str | None = None
    unrelated: bool = False
    new_namespace: bool = False

    def __post_init__(self) -> None:
        source = source_of(self.patients, self.snapshot, self.cases)
        table = None if self.existing is None else split_table.TableState(list(self.existing), 5)
        self.versions = {"h0": (source, table)}
        self.current = "h0"
        self.parents: dict[str, str] = {}

    def head(self) -> str:
        self.calls.append(("head",))
        return self.script.pop(0) if self.script else self.current

    def read_source(self, at: str) -> Any:
        self.calls.append(("read_source", at))
        return self.versions[at][0]

    def read_table(self, at: str) -> Any:
        self.calls.append(("read_table", at))
        return self.versions[at][1]

    def write(self, rows: Sequence[SplitRow], *, exists: bool) -> None:
        self.calls.append(("write", exists))
        stored = list(rows) if self.corrupt is None else self.corrupt(list(rows))
        after = self.versions[self.current][0] if self.source_after_write is None else self.source_after_write
        self.versions["h1"] = (after, split_table.TableState(stored, 6))
        self.parents["h1"] = self.write_parent or self.current
        self.current = "h1"
        if self.unrelated:
            self.versions["h2"] = self.versions["h1"]
            self.parents["h2"] = "h1"
            self.current = "h2"

    def prepare(self, checked: str) -> str:
        self.calls.append(("prepare", checked))
        if not self.new_namespace:
            return checked
        self.versions["hn"] = self.versions[self.current]
        self.parents["hn"] = checked
        self.current = "hn"
        return "hn"

    def write_hash(self, checked: str) -> str | None:
        self.calls.append(("write_hash", checked))
        return self.current if self.parents.get(self.current) == checked else None

    def operations(self) -> Any:
        return split_table.Operations(
            head=self.head, read_source=self.read_source, read_table=self.read_table, write=self.write, write_hash=self.write_hash, prepare=self.prepare
        )

    def names(self) -> list[str]:
        return [call[0] for call in self.calls]


def build_with(lake: Lake, *, pinned: bool = True, sample: list[SampleCase] | None = None, head: str = "h0") -> Any:
    return split_table.run_build(lake.operations(), head, pinned=pinned, sample=sample)


class BuildSequenceTest(unittest.TestCase):
    """The order of a build with its Spark steps replaced: a step that is left out must fail a test."""

    def test_a_first_build_validates_writes_reads_back_at_the_write_hash_and_confirms_the_head(self) -> None:
        lake = Lake(FULL)
        outcome = build_with(lake)
        self.assertEqual(
            [
                ("read_source", "h0"),
                ("read_table", "h0"),
                ("head",),
                ("prepare", "h0"),
                ("write", False),
                ("write_hash", "h0"),
                ("read_source", "h1"),
                ("read_table", "h1"),
                ("head",),
            ],
            lake.calls,
        )
        self.assertEqual(("h1", 6), (outcome.head, outcome.table_snapshot_id))
        self.assertIn(("write", False), lake.calls)
        self.assertEqual(rows_of(FULL, snapshot=11), outcome.rows)

    def test_the_namespace_is_prepared_after_every_check_and_the_head_check_and_before_the_write(self) -> None:
        lake = Lake(FULL, new_namespace=True)
        facts = build_with(lake).facts()
        self.assertEqual(["read_source", "read_table", "head", "prepare", "write", "write_hash", "read_source", "read_table", "head"], lake.names())
        self.assertIn(("write_hash", "hn"), lake.calls)
        self.assertEqual(("h0", "h1"), (facts["checked_hash"], facts["write_hash"]))

    def test_a_write_that_is_not_on_the_namespace_commit_is_not_the_write(self) -> None:
        lake = Lake(FULL, new_namespace=True, write_parent="h0")
        with self.assertRaises(PatientSplitError) as caught:
            build_with(lake)
        self.assertEqual(["branch_moved: failed"], caught.exception.problems)

    def test_nothing_is_prepared_or_written_when_a_check_fails_the_branch_moved_or_nothing_changes(self) -> None:
        cases = {
            "a failed check": Lake({subjectid: name for subjectid, name in FULL.items() if subjectid != "1"}, new_namespace=True),
            "the branch moved": Lake(FULL, new_namespace=True, script=["h9"]),
            "nothing changes": Lake(FULL, existing=rows_of(FULL, snapshot=11), new_namespace=True),
            "a changed set": Lake(FULL, existing=rows_of({**FULL, "30": other_set(FULL["30"])}, snapshot=11), new_namespace=True),
        }
        for label, lake in cases.items():
            with self.subTest(label):
                with contextlib.suppress(PatientSplitError):
                    build_with(lake)
                self.assertEqual([], [name for name in lake.names() if name in ("prepare", "write")])

    def test_a_rebuild_that_changes_nothing_writes_nothing_and_names_the_hash_it_checked(self) -> None:
        lake = Lake(FULL, existing=rows_of(FULL, snapshot=11))
        outcome = build_with(lake)
        self.assertNotIn("write", lake.names())
        self.assertEqual(("h0", None), (outcome.head, outcome.write_hash))
        self.assertEqual({("read_source", "h0"), ("read_table", "h0")}, {call for call in lake.calls if call[0] != "head"})

    def test_a_rebuild_over_a_new_source_snapshot_overwrites_the_table(self) -> None:
        lake = Lake(FULL, existing=rows_of(FULL, snapshot=3))
        outcome = build_with(lake)
        self.assertIn(("write", True), lake.calls)
        self.assertEqual("h1", outcome.write_hash)

    def test_every_read_before_the_write_is_at_the_resolved_hash_and_every_read_after_it_at_the_write_hash(self) -> None:
        lake = Lake(FULL)
        build_with(lake)
        write_at = lake.names().index("write")
        before = [call[1] for call in lake.calls[:write_at] if call[0].startswith("read")]
        after = [call[1] for call in lake.calls[write_at:] if call[0].startswith("read")]
        self.assertEqual((["h0", "h0"], ["h1", "h1"]), (before, after))

    def test_a_changed_set_in_the_inherited_table_refuses_before_any_write(self) -> None:
        lake = Lake(FULL, existing=rows_of({**FULL, "30": other_set(FULL["30"])}, snapshot=11))
        with self.assertRaises(PatientSplitError) as caught:
            build_with(lake)
        self.assertEqual(["changed_set: failed"], caught.exception.problems)
        self.assertNotIn("write", lake.names())

    def test_an_empty_inherited_table_refuses_a_pinned_cohort_with_a_patient_replaced(self) -> None:
        # A replacement from the same set keeps every count exact, so only the empty table can refuse the cohort.
        replacement = next(str(number) for number in range(6091, 7000) if patient_part(number) == "test")
        replaced = {subjectid: name for subjectid, name in FULL.items() if subjectid != "1"} | {replacement: "test"}
        self.assertEqual(PINNED_COUNTS, dict(Counter(replaced.values())))
        lake = Lake(replaced, existing=[])
        with self.assertRaises(PatientSplitError) as caught:
            build_with(lake)
        self.assertEqual(["inherited_empty: failed"], caught.exception.problems)
        self.assertNotIn("write", lake.names())

    def test_an_inherited_table_with_a_wrong_row_then_the_right_one_refuses_before_any_write(self) -> None:
        existing = [*rows_of(FULL, snapshot=11), SplitRow("25", FULL["25"], RULE_VERSION, 11)]
        existing[24] = SplitRow("25", other_set(FULL["25"]), RULE_VERSION, 11)
        lake = Lake(FULL, existing=existing)
        with self.assertRaises(PatientSplitError) as caught:
            build_with(lake)
        self.assertEqual(["inherited_duplicates: failed"], caught.exception.problems)
        self.assertNotIn("write", lake.names())

    def test_a_failed_check_of_the_rows_to_write_refuses_before_any_write(self) -> None:
        lake = Lake({subjectid: name for subjectid, name in FULL.items() if subjectid != "1"})
        with self.assertRaises(PatientSplitError) as caught:
            build_with(lake)
        self.assertTrue(caught.exception.problems[0].startswith("pinned_counts: "))
        self.assertNotIn("write", lake.names())

    def test_a_branch_that_moved_before_the_write_refuses_with_no_write(self) -> None:
        lake = Lake(FULL, script=["h9"])
        with self.assertRaises(PatientSplitError) as caught:
            build_with(lake)
        self.assertEqual(["branch_moved: failed"], caught.exception.problems)
        self.assertNotIn("write", lake.names())

    def test_a_table_that_reads_back_other_than_it_was_written_is_refused(self) -> None:
        for label, corrupt in (
            ("a patient dropped", lambda rows: rows[1:]),
            ("a set changed", lambda rows: [SplitRow(rows[0].subjectid, other_set(rows[0].set), RULE_VERSION, 11), *rows[1:]]),
            ("a patient twice", lambda rows: [*rows, rows[0]]),
            ("a snapshot changed", lambda rows: [SplitRow(row.subjectid, row.set, row.rule_version, 12) for row in rows]),
        ):
            lake = Lake(FULL, corrupt=corrupt)
            with self.subTest(label), self.assertRaises(PatientSplitError) as caught:
                build_with(lake)
            self.assertTrue(caught.exception.problems)

    def test_a_read_back_that_differs_only_in_order_is_the_same_table(self) -> None:
        lake = Lake(FULL, corrupt=lambda rows: list(reversed(rows)))
        self.assertEqual("h1", build_with(lake).write_hash)

    def test_an_unrelated_commit_after_the_write_is_not_taken_as_the_write_and_nothing_is_read_at_it(self) -> None:
        lake = Lake(FULL, unrelated=True)
        with self.assertRaises(PatientSplitError) as caught:
            build_with(lake)
        self.assertEqual(["branch_moved: failed"], caught.exception.problems)
        self.assertEqual([], [call for call in lake.calls[lake.names().index("write_hash") :] if call[0].startswith("read")])

    def test_a_write_whose_commit_is_not_directly_on_the_checked_hash_is_refused(self) -> None:
        lake = Lake(FULL, write_parent="h7")
        with self.assertRaises(PatientSplitError) as caught:
            build_with(lake)
        self.assertEqual(["branch_moved: failed"], caught.exception.problems)

    def test_a_branch_that_moves_after_the_write_is_refused_whatever_the_table_holds(self) -> None:
        lake = Lake(FULL, script=["h0", "h5"])
        with self.assertRaises(PatientSplitError) as caught:
            build_with(lake)
        self.assertEqual(["branch_moved: failed"], caught.exception.problems)

    def test_a_source_that_is_not_the_one_that_was_checked_is_refused_at_the_write_hash(self) -> None:
        # The branch holds another Bronze snapshot at the hash of the write: the table that was written no longer matches the source it is checked against.
        lake = Lake(FULL, source_after_write=source_of(FULL, snapshot=12))
        with self.assertRaises(PatientSplitError) as caught:
            build_with(lake)
        self.assertEqual(["source_snapshot: failed"], caught.exception.problems)

    def test_a_fixed_sample_case_that_bronze_lists_under_another_patient_is_refused_by_the_sample_check_alone(self) -> None:
        sample = tracked_sample()
        assignments, cases = sample_cohort(sample)
        lake = Lake(assignments, cases={**cases, sample[0].case: str(sample[0].patient + 1)})
        with self.assertRaises(PatientSplitError) as caught:
            build_with(lake, pinned=False, sample=sample)
        self.assertEqual(["sample_wrong_patient: failed"], caught.exception.problems)
        self.assertNotIn("write", lake.names())

    def test_a_fixed_sample_case_in_the_wrong_set_at_the_write_hash_is_refused_by_the_sample_check_alone(self) -> None:
        # A source whose case-to-patient map changes at the hash of the write, with the table by the rule: only the fixed-sample check on the read-back sees it.
        sample = tracked_sample()
        assignments, cases = sample_cohort(sample)
        after = source_of(assignments, cases={**cases, sample[1].case: str(sample[1].patient + 1)})
        with self.assertRaises(PatientSplitError) as caught:
            build_with(Lake(assignments, cases=cases, source_after_write=after), pinned=False, sample=sample)
        self.assertEqual(["sample_wrong_patient: failed"], caught.exception.problems)

    def test_the_fixed_sample_is_checked_on_the_rows_and_on_the_table_read_back(self) -> None:
        sample = tracked_sample()
        assignments, cases = sample_cohort(sample)
        lake = Lake(assignments, cases=cases)
        outcome = build_with(lake, pinned=False, sample=sample)
        self.assertIn("fixed_sample", outcome.checks_run)
        wrong = [item for item in sample if item.slot == "short_development"][0]

        def corrupt(rows: list[SplitRow]) -> list[SplitRow]:
            return [SplitRow(row.subjectid, "test", RULE_VERSION, 11) if row.subjectid == str(wrong.patient) else row for row in rows]

        with self.assertRaises(PatientSplitError):
            build_with(Lake(assignments, cases=cases, corrupt=corrupt), pinned=False, sample=sample)

    def test_a_sample_sized_source_builds_without_the_shares_and_names_the_checks_that_ran(self) -> None:
        sample = tracked_sample()
        assignments, cases = sample_cohort(sample)
        outcome = build_with(Lake(assignments, cases=cases), pinned=False, sample=sample)
        self.assertNotIn("shares", outcome.checks_run)
        self.assertNotIn("pinned_counts", outcome.checks_run)
        self.assertNotIn("counts", outcome.facts())

    def test_the_facts_are_exactly_these_keys_and_nothing_derived_from_the_rows(self) -> None:
        facts = build_with(Lake(FULL)).facts()
        self.assertEqual(
            {
                "checked_hash",
                "write_hash",
                "branch_hash",
                "snapshot_id",
                "source_snapshot_id",
                "rule_version",
                "passed",
                "checks_run",
                "fixed_sample_cases_checked",
            },
            set(facts),
        )
        self.assertEqual(("h0", "h1", "h1"), (facts["checked_hash"], facts["write_hash"], facts["branch_hash"]))
        self.assertEqual(["two_sets", "rule_match", "rule_version", "source_snapshot", "source_patients", "shares", "pinned_counts"], facts["checks_run"])

    def test_a_result_for_a_one_patient_cohort_holds_nothing_that_tells_the_patients_apart(self) -> None:
        # Two cohorts of one patient, one patient in test and one in training: every key of the result must be equal.
        test_patient = next(subjectid for subjectid, name in FULL.items() if name == "test")
        training_patient = next(subjectid for subjectid, name in FULL.items() if name == "training")
        first = build_with(Lake({test_patient: "test"}), pinned=False).facts()
        second = build_with(Lake({training_patient: "training"}), pinned=False).facts()
        self.assertEqual(first, second)

    def test_a_result_without_a_write_has_no_write_hash(self) -> None:
        facts = build_with(Lake(FULL, existing=rows_of(FULL, snapshot=11))).facts()
        self.assertEqual((None, "h0", "h0"), (facts["write_hash"], facts["checked_hash"], facts["branch_hash"]))


class VerifySequenceTest(unittest.TestCase):
    def verify_with(self, lake: Lake, **keywords: Any) -> Any:
        return split_table.run_verify(lake.operations(), "h0", pinned=keywords.get("pinned", True), sample=keywords.get("sample"))

    def test_a_good_table_is_read_at_the_resolved_hash_and_the_head_is_confirmed(self) -> None:
        lake = Lake(FULL, existing=rows_of(FULL, snapshot=11))
        outcome = self.verify_with(lake)
        self.assertEqual([("read_source", "h0"), ("read_table", "h0"), ("head",)], lake.calls)
        self.assertEqual(("h0", 5, None), (outcome.head, outcome.table_snapshot_id, outcome.write_hash))

    def test_a_branch_with_no_table_is_refused(self) -> None:
        with self.assertRaises(PatientSplitError) as caught:
            self.verify_with(Lake(FULL))
        self.assertEqual(["table_missing: 0 tables"], caught.exception.problems)

    def test_a_branch_that_moved_during_the_check_is_refused(self) -> None:
        lake = Lake(FULL, existing=rows_of(FULL, snapshot=11), script=["h4"])
        with self.assertRaises(PatientSplitError) as caught:
            self.verify_with(lake)
        self.assertEqual(["branch_moved: failed"], caught.exception.problems)

    def test_a_fixed_sample_case_under_another_patient_is_refused_by_the_sample_check_alone(self) -> None:
        sample = tracked_sample()
        assignments, cases = sample_cohort(sample)
        lake = Lake(assignments, existing=rows_of(assignments, snapshot=11), cases={**cases, sample[0].case: str(sample[0].patient + 1)})
        with self.assertRaises(PatientSplitError) as caught:
            self.verify_with(lake, pinned=False, sample=sample)
        self.assertEqual(["sample_wrong_patient: failed"], caught.exception.problems)

    def test_each_failed_check_is_refused(self) -> None:
        cases = {
            "two sets": [*rows_of(FULL, snapshot=11), SplitRow("25", other_set(FULL["25"]), RULE_VERSION, 11)],
            "rule mismatch": rows_of({**FULL, "30": other_set(FULL["30"])}, snapshot=11),
            "another snapshot": rows_of(FULL, snapshot=99),
        }
        for label, rows in cases.items():
            with self.subTest(label), self.assertRaises(PatientSplitError):
                self.verify_with(Lake(FULL, existing=rows))


class DistinctCountTest(unittest.TestCase):
    """A message counts distinct patients, cases and subject IDs, never rows that carry the same one."""

    def test_a_bad_subject_id_that_repeats_is_one_subject_id(self) -> None:
        rows = [*rows_of(FULL), SplitRow("007", "training", RULE_VERSION, SNAPSHOT), SplitRow("007", "training", RULE_VERSION, SNAPSHOT)]
        self.assertIn("bad_subjectid: failed", table_problems(rows, pinned=False))
        with self.assertRaises(PatientSplitError) as caught:
            build_assignments(["007", "007", "1"])
        self.assertEqual(["bad_subjectid: failed"], caught.exception.problems)

    def test_a_patient_that_repeats_with_the_wrong_set_is_one_patient_off_the_rule(self) -> None:
        wrong = other_set(FULL["25"])
        rows = [row for row in rows_of(FULL) if row.subjectid != "25"] + [SplitRow("25", wrong, RULE_VERSION, SNAPSHOT) for _ in range(3)]
        self.assertIn("rule_mismatch: failed", table_problems(rows, pinned=False))
        self.assertIn("repeated_patient: failed", table_problems(rows, pinned=False))

    def test_a_case_under_three_patients_is_one_case_and_a_repeat_of_the_same_pair_is_none(self) -> None:
        with self.assertRaises(PatientSplitError) as caught:
            source_cases([("1", "10"), ("1", "11"), ("1", "12")])
        self.assertEqual(["case_two_patients: failed"], caught.exception.problems)
        self.assertEqual(({1: "10"}, {"10"}), source_cases([("1", "10"), ("1", "10")]))

    def test_a_sample_case_that_repeats_is_one_case(self) -> None:
        twice = [SampleCase(case=1, patient=1, slot="odd"), SampleCase(case=1, patient=1, slot="odd")]
        self.assertEqual(["sample_bad_slot: failed"], fixed_sample_problems({"1": "training"}, {1: "1"}, twice))
        missing = [SampleCase(case=1, patient=1, slot="repeat_test"), SampleCase(case=1, patient=1, slot="repeat_test")]
        self.assertEqual(["sample_missing_case: failed"], fixed_sample_problems({}, {}, missing))


class WriteCommitTest(unittest.TestCase):
    """The log decides which commit is the write: the head, directly on the checked hash, changing only the allowed keys."""

    TABLE = (("silver", "patient_split"),)

    @staticmethod
    def entry(commit: str, parent: str, *keys: tuple[str, ...], kind: str = "PUT") -> dict[str, Any]:
        return {"commitMeta": {"hash": commit}, "parentCommitHash": parent, "operations": [{"type": kind, "key": {"elements": list(key)}} for key in keys]}

    def test_the_head_on_the_checked_hash_that_changes_only_the_table_is_the_write(self) -> None:
        entries = [self.entry("h1", "h0", ("silver", "patient_split")), self.entry("h0", "h_1")]
        self.assertEqual("h1", split_table.write_commit(entries, "h0", self.TABLE))

    def test_anything_else_is_not_the_write(self) -> None:
        table = ("silver", "patient_split")
        cases = {
            "no log": [],
            "another parent": [self.entry("h1", "h7", table)],
            "two commits on top": [self.entry("h2", "h1", table), self.entry("h1", "h0", table)],
            "another key too": [self.entry("h1", "h0", table, ("bronze", "clinical_cases"))],
            "only another key": [self.entry("h1", "h0", ("bronze", "clinical_cases"))],
            "a delete": [self.entry("h1", "h0", table, kind="DELETE")],
            "no operations named": [self.entry("h1", "h0")],
            "no hash": [{"parentCommitHash": "h0", "operations": [{"type": "PUT", "key": {"elements": list(table)}}]}],
        }
        for label, entries in cases.items():
            with self.subTest(label):
                self.assertIsNone(split_table.write_commit(entries, "h0", self.TABLE))

    def test_the_namespace_commit_is_its_own_step_with_its_own_key(self) -> None:
        entries = [self.entry("h1", "h0", ("silver",))]
        self.assertEqual("h1", split_table.write_commit(entries, "h0", (("silver",),)))
        self.assertIsNone(split_table.write_commit(entries, "h0", self.TABLE))


class CatalogTest(unittest.TestCase):
    """A read goes through a catalog that is pinned to a hash and cannot write; the write goes through the catalog of the branch."""

    HASH = "ab" * 32
    SETTINGS = {"nessie_uri": "http://nessie:19120/api/v2", "warehouse": "file:///warehouse/", "region": ""}

    def conf(self, name: str, at: str) -> dict[str, str]:
        return split_table.catalog_conf(name, branch="b", at=at, **self.SETTINGS)

    def test_a_pinned_read_holds_the_hash_and_names_only_its_own_catalog(self) -> None:
        conf = self.conf("pin_x", self.HASH)
        self.assertEqual(self.HASH, conf["spark.sql.catalog.pin_x.ref.hash"])
        self.assertEqual("b", conf["spark.sql.catalog.pin_x.ref"])
        self.assertTrue(all(key.startswith("spark.sql.catalog.pin_x") for key in conf))

    def test_a_hash_that_is_not_a_nessie_hash_is_refused(self) -> None:
        for at in ("", "main", "ab" * 31, "AB" * 32, "ab" * 32 + "\n"):
            with self.subTest(at=at), self.assertRaises(PatientSplitError):
                self.conf("pin_x", at)

    def test_every_read_of_the_job_goes_through_a_catalog_that_was_pinned_to_the_hash_of_that_read(self) -> None:
        job = importlib.import_module("spark.jobs.patient_split")
        session = FakeSession()
        lake = job.Lake(session, NessieClient("http://127.0.0.1:1/api/v2"), "b", self.SETTINGS)
        other = "cd" * 32
        lake.read_source(self.HASH)
        first = list(session.reads)
        session.reads.clear()
        lake.read_table(other)
        second = list(session.reads)
        self.assertEqual({"exists", "source", "snapshot"}, {kind for kind, _ in first})
        self.assertEqual({"exists", "table", "snapshot"}, {kind for kind, _ in second})
        for reads, at in ((first, self.HASH), (second, other)):
            for kind, name in reads:
                catalog = name.split(".")[0]
                with self.subTest(read=kind, name=name):
                    self.assertTrue(catalog.startswith("pin_"), name)
                    self.assertEqual(at, session.conf_set[f"spark.sql.catalog.{catalog}.ref.hash"])
        self.assertFalse([key for key in session.conf_set if key.startswith("spark.sql.catalog.lake")])


class FakeFrame:
    def __init__(self, fields: Sequence[tuple[str, str]], rows: list[dict[str, Any]]) -> None:
        self.schema = SimpleNamespace(
            fields=[SimpleNamespace(name=name, dataType=SimpleNamespace(simpleString=lambda kind=kind: kind)) for name, kind in fields]
        )
        self.rows = rows

    def select(self, *_names: str) -> FakeFrame:
        return self

    def collect(self) -> list[dict[str, Any]]:
        return self.rows


class Chain:
    """A stand-in for a Spark frame that records the write that is asked of it."""

    def __init__(self, session: FakeSession) -> None:
        self.session = session

    def toDF(self, *_names: str) -> Chain:  # noqa: N802 - Spark's name
        return self

    def coalesce(self, _count: int) -> Chain:
        return self

    def writeTo(self, name: str) -> Chain:  # noqa: N802 - Spark's name
        self.session.writes.append(name)
        return self

    def tableProperty(self, *_pair: str) -> Chain:  # noqa: N802 - Spark's name
        return self

    def create(self) -> None:
        return None

    def overwritePartitions(self) -> None:  # noqa: N802 - Spark's name
        return None


class FakeSession:
    """A stand-in Spark session that records the catalog settings, every table that a read names and every statement or write that it is given."""

    def __init__(self, missing: Sequence[str] = ()) -> None:
        self.missing = tuple(missing)
        self.conf_set: dict[str, str] = {}
        self.reads: list[tuple[str, str]] = []
        self.sql_calls: list[str] = []
        self.writes: list[str] = []
        self.conf = SimpleNamespace(set=self.conf_set.__setitem__)
        self.catalog = SimpleNamespace(tableExists=self.exists)
        spark3 = SimpleNamespace(loadIcebergTable=self.load)
        self._jvm = SimpleNamespace(org=SimpleNamespace(apache=SimpleNamespace(iceberg=SimpleNamespace(spark=SimpleNamespace(Spark3Util=spark3)))))
        self._jsparkSession = object()

    def exists(self, name: str) -> bool:
        self.reads.append(("exists", name))
        return not any(name.endswith(suffix) for suffix in self.missing)

    def load(self, _session: Any, name: str) -> Any:
        self.reads.append(("snapshot", name))
        return SimpleNamespace(currentSnapshot=lambda: SimpleNamespace(snapshotId=lambda: 5))

    def stop(self) -> None:
        return None

    def sql(self, text: str) -> Chain:
        self.sql_calls.append(text)
        return Chain(self)

    def table(self, name: str) -> FakeFrame:
        if name.endswith("clinical_cases"):
            self.reads.append(("source", name))
            return FakeFrame(split_table.SOURCE_COLUMNS, [{"caseid": "1", "subjectid": "10"}])
        self.reads.append(("table", name))
        return FakeFrame(split_table.COLUMNS, [{"subjectid": "10", "set": "training", "rule_version": RULE_VERSION, "source_snapshot_id": 5}])


class AnsweringNessie(NessieClient):
    """The real Nessie client with only the wire replaced: each path gets a scripted answer, so the job's own adapter code runs."""

    def __init__(self, head: str, *, log: list[dict[str, Any]] | None = None, namespace: bool = False) -> None:
        super().__init__("http://127.0.0.1:1/api/v2")
        self.head_hash = head
        self.log = log or []
        self.namespace = namespace

    def send(self, method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
        path = target.split("?")[0]
        if path.endswith("/history"):
            return 200, json.dumps({"logEntries": self.log}).encode()
        if path.endswith("/entries"):
            entries = [{"type": "NAMESPACE", "name": {"elements": ["silver"]}}] if self.namespace else []
            return 200, json.dumps({"entries": entries, "effectiveReference": answered_reference(path)}).encode()
        return 200, json.dumps({"reference": {"type": "BRANCH", "name": "b", "hash": self.head_hash}}).encode()


def answered_reference(path: str) -> dict[str, str]:
    """The reference that Nessie names in its answer to an entries request: the branch and the hash the path asked for (``/trees/<branch>@<hash>/entries``)."""
    asked = unquote(path.split("/trees/", 1)[1].rsplit("/entries", 1)[0])
    name, _, hash_value = asked.partition("@")
    return {"type": "BRANCH", "name": name, "hash": hash_value}


BASE = "ab" * 32
NAMESPACE_COMMIT = "cd" * 32
TABLE_COMMIT = "ef" * 32


def log_entry(commit: str, parent: str, *keys: tuple[str, ...]) -> dict[str, Any]:
    return {"commitMeta": {"hash": commit}, "parentCommitHash": parent, "operations": [{"type": "PUT", "key": {"elements": list(key)}} for key in keys]}


class LakeAdapterTest(unittest.TestCase):
    """The job's own adapter against a stand-in wire: what it asks Nessie, what it sends Spark, and when it sends nothing."""

    SETTINGS = CatalogTest.SETTINGS

    def lake(self, session: FakeSession, client: AnsweringNessie) -> Any:
        return importlib.import_module("spark.jobs.patient_split").Lake(session, client, "b", self.SETTINGS)

    def test_the_write_hash_is_the_log_head_only_when_it_is_one_table_commit_on_the_checked_hash(self) -> None:
        table = ("silver", "patient_split")
        lake = self.lake(FakeSession(), AnsweringNessie(TABLE_COMMIT, log=[log_entry(TABLE_COMMIT, NAMESPACE_COMMIT, table)]))
        self.assertEqual(TABLE_COMMIT, lake.write_hash(NAMESPACE_COMMIT))
        self.assertIsNone(lake.write_hash(BASE))
        other = self.lake(FakeSession(), AnsweringNessie(TABLE_COMMIT, log=[log_entry(TABLE_COMMIT, NAMESPACE_COMMIT, table, ("bronze", "clinical_cases"))]))
        self.assertIsNone(other.write_hash(NAMESPACE_COMMIT))

    def test_a_missing_namespace_is_created_once_and_its_commit_is_the_next_checked_hash(self) -> None:
        session = FakeSession()
        lake = self.lake(session, AnsweringNessie(NAMESPACE_COMMIT, log=[log_entry(NAMESPACE_COMMIT, BASE, ("silver",))]))
        self.assertEqual(NAMESPACE_COMMIT, lake.prepare(BASE))
        self.assertEqual(["CREATE NAMESPACE IF NOT EXISTS lake.silver"], session.sql_calls)

    def test_an_existing_namespace_is_left_alone_and_the_checked_hash_stays(self) -> None:
        session = FakeSession()
        lake = self.lake(session, AnsweringNessie(BASE, namespace=True))
        self.assertEqual(BASE, lake.prepare(BASE))
        self.assertEqual([], session.sql_calls)

    def test_a_namespace_commit_that_is_not_one_commit_on_the_base_ends_with_branch_moved(self) -> None:
        for label, log in (
            ("another parent", [log_entry(NAMESPACE_COMMIT, TABLE_COMMIT, ("silver",))]),
            ("another key", [log_entry(NAMESPACE_COMMIT, BASE, ("silver",), ("bronze", "clinical_cases"))]),
            ("no commit", []),
        ):
            lake = self.lake(FakeSession(), AnsweringNessie(NAMESPACE_COMMIT, log=log))
            with self.subTest(label), self.assertRaises(PatientSplitError) as caught:
                lake.prepare(BASE)
            self.assertEqual(["branch_moved: failed"], caught.exception.problems)

    def build(self, session: FakeSession, client: AnsweringNessie) -> None:
        lake = self.lake(session, client)
        operations = split_table.Operations(lake.head, lake.read_source, lake.read_table, lake.write, lake.write_hash, lake.prepare)
        split_table.run_build(operations, BASE, pinned=False, sample=None)

    def test_a_new_branch_without_bronze_is_refused_before_any_statement_or_write(self) -> None:
        session = FakeSession(missing=("clinical_cases",))
        with self.assertRaises(PatientSplitError) as caught:
            self.build(session, AnsweringNessie(BASE))
        self.assertEqual(["source_table: 0 tables"], caught.exception.problems)
        self.assertEqual(([], []), (session.sql_calls, session.writes))

    def test_a_branch_that_moved_before_the_write_is_refused_before_any_statement_or_write(self) -> None:
        session = FakeSession(missing=("patient_split",))
        with self.assertRaises(PatientSplitError) as caught:
            self.build(session, AnsweringNessie("12" * 32))
        self.assertEqual(["branch_moved: failed"], caught.exception.problems)
        self.assertEqual(([], []), (session.sql_calls, session.writes))

    def test_a_failed_check_of_the_inherited_table_is_refused_before_any_statement_or_write(self) -> None:
        session = FakeSession()
        session.table = lambda name: FakeFrame(  # type: ignore[method-assign]
            split_table.SOURCE_COLUMNS if name.endswith("clinical_cases") else split_table.COLUMNS,
            [{"caseid": "1", "subjectid": "10"}] if name.endswith("clinical_cases") else [],
        )
        with self.assertRaises(PatientSplitError) as caught:
            self.build(session, AnsweringNessie(BASE))
        self.assertEqual(["inherited_empty: failed"], caught.exception.problems)
        self.assertEqual(([], []), (session.sql_calls, session.writes))


GOOD_SETTINGS = {
    "NESSIE_URI": "http://nessie:19120/api/v2",
    "WAREHOUSE_LOCATION": "file:///warehouse/",
    "JOB_DEADLINE_SECONDS": "100",
    "NESSIE_TIMEOUT_SECONDS": "5",
}
NOT_A_NUMBER = ["MARKER_8731", "0", "-1", "nan", "inf", "1e100", "", "1e9999"]
S3 = "s3://bucket/prefix/"
# One list of invalid inputs for each setting that the job reads: a value that is missing, empty, malformed or out of range.
INVALID_SETTINGS: dict[str, list[dict[str, str | None]]] = {
    "NESSIE_URI": [
        {"NESSIE_URI": value}
        for value in (None, "", "https://nessie:19120/api/v2", "http://nessie:MARKER_8731/api/v2", "http://nessie:19120/api/v1", "ftp://nessie/api/v2")
    ],
    "WAREHOUSE_LOCATION": [{"WAREHOUSE_LOCATION": value} for value in (None, "", "relative/path", "s3://", "http://bucket/prefix/")],
    "AWS_REGION": [{"WAREHOUSE_LOCATION": S3, "AWS_REGION": ""}, {"WAREHOUSE_LOCATION": S3, "AWS_REGION": None}],
    "JOB_DEADLINE_SECONDS": [{"JOB_DEADLINE_SECONDS": value} for value in NOT_A_NUMBER],
    "NESSIE_TIMEOUT_SECONDS": [{"NESSIE_TIMEOUT_SECONDS": value} for value in NOT_A_NUMBER],
}


class Wire:
    """The Nessie wire of the entry point: every request is recorded and the few that a build makes are answered."""

    def __init__(self, events: list[Any], heads: Sequence[str] = (BASE,)) -> None:
        self.events = events
        self.heads = list(heads)
        self.requests: list[tuple[str, str]] = []

    def send(self, _client: Any, method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
        path = target.split("?")[0]
        self.requests.append((method, path))
        self.events.append(("wire", method, path))
        if path.endswith("/config"):
            return 200, b"{}"
        if path.endswith("/entries"):
            return 200, json.dumps({"entries": [], "effectiveReference": answered_reference(path)}).encode()
        if path.endswith("/history"):
            return 200, json.dumps({"logEntries": []}).encode()
        if method == "POST":
            return 200, json.dumps({"reference": {"type": "BRANCH", "name": "b", "hash": BASE}}).encode()
        head = self.heads.pop(0) if len(self.heads) > 1 else self.heads[0]
        return 200, json.dumps({"reference": {"type": "BRANCH", "name": "b", "hash": head}}).encode()


class EntryPointTest(unittest.TestCase):
    """``main`` against a stand-in wire and a stand-in session: the order of its steps, what it does with a bad setting and what it writes when it must not."""

    def drive(self, session: FakeSession, wire: Wire, argv: list[str], env: dict[str, str | None] | None = None) -> tuple[int, str, str, Any, Any]:
        job = importlib.import_module("spark.jobs.patient_split")
        settings = {**GOOD_SETTINGS, **(env or {})}
        merged = {name: value for name, value in settings.items() if value is not None}
        events = wire.events
        original = job.read_fixed_sample

        def sampled(path: Any) -> Any:
            events.append(("sample",))
            return original(path)

        def build(*_arguments: Any) -> FakeSession:
            events.append(("session",))
            return session

        with (
            mock.patch.dict(os.environ, merged, clear=False),
            mock.patch.object(NessieClient, "send", autospec=True, side_effect=wire.send),
            mock.patch.object(job, "build_session", side_effect=build) as session_built,
            mock.patch.object(job, "start_deadline", side_effect=lambda *_a, **_k: events.append(("deadline",))) as timer,
            mock.patch.object(job, "read_fixed_sample", side_effect=sampled),
        ):
            for name, value in settings.items():
                if value is None:
                    os.environ.pop(name, None)
            code, out, err = call_main(job.main, argv)
        return code, out, err, session_built, timer

    def test_every_setting_the_job_reads_is_listed_and_each_has_invalid_cases(self) -> None:
        root = Path(split_table.__file__).parent
        found: set[str] = set()
        for path in (root / "jobs" / "patient_split.py", root / "split_cli.py", root / "session.py"):
            found |= set(re.findall(r'(?:environ\.get|required_environment|required_setting|number_setting)\(\s*"([A-Z_]+)"', path.read_text()))
        self.assertEqual(set(split_cli.SETTING_NAMES), found)
        self.assertEqual(set(split_cli.SETTING_NAMES), set(INVALID_SETTINGS))
        self.assertTrue(all(INVALID_SETTINGS.values()))

    def test_each_invalid_setting_ends_as_settings_with_no_remote_call_no_session_and_no_timer(self) -> None:
        for name, cases in INVALID_SETTINGS.items():
            for case in cases:
                for mode in (["build", "--branch", "b"], ["verify", "--branch", "b"]):
                    events: list[Any] = []
                    wire = Wire(events)
                    session = FakeSession()
                    with self.subTest(setting=name, case=case, mode=mode[0]):
                        code, out, err, built, timer = self.drive(session, wire, mode, case)
                        self.assertEqual((2, "", "patient_split: settings\n"), (code, out, err))
                        self.assertEqual(([], [], False, False), (wire.requests, events, built.called, timer.called))

    def test_every_local_step_is_done_before_the_first_remote_call(self) -> None:
        events: list[Any] = []
        wire = Wire(events)
        session = FakeSession(missing=("clinical_cases",))
        sample = str(CANDIDATES_FILE)
        self.drive(session, wire, ["build", "--branch", "b", "--fixed-sample", sample], None)
        first = next(index for index, event in enumerate(events) if event[0] == "wire")
        self.assertEqual({("sample",), ("deadline",), ("session",)}, {event for event in events[:first] if event[0] != "wire"})
        self.assertEqual([], [event for event in events[first:] if event[0] in ("sample", "deadline", "session")])

    def test_a_source_table_that_is_missing_commits_no_namespace_and_writes_nothing(self) -> None:
        wire = Wire([])
        session = FakeSession(missing=("clinical_cases",))
        code, out, err, _, _ = self.drive(session, wire, ["build", "--branch", "b"])
        self.assertEqual((1, "", "patient_split: source_table: 0 tables\n"), (code, out, err))
        self.assertEqual(([], []), (session.sql_calls, session.writes))
        self.assertEqual(["POST"], [method for method, _ in wire.requests if method != "GET"])

    def test_a_branch_that_moved_commits_no_namespace_and_writes_nothing(self) -> None:
        wire = Wire([], heads=(BASE, "12" * 32))
        session = FakeSession(missing=("patient_split",))
        code, out, err, _, _ = self.drive(session, wire, ["build", "--branch", "b"])
        self.assertEqual((1, "", "patient_split: branch_moved: failed\n"), (code, out, err))
        self.assertEqual(([], []), (session.sql_calls, session.writes))


def call_main(main: Callable[[list[str]], int], argv: list[str]) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    code = 0
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = main(argv)
        except SystemExit as stop:
            code = int(stop.code or 0)
    return code, out.getvalue(), err.getvalue()


def all_messages() -> list[str]:
    """Every failure of the module, driven with the planted patient and case numbers in every input that can carry them."""
    patient, case = PLANTED_PATIENT, PLANTED_CASE
    base = {**FULL, patient: "test"}
    rows = rows_of(base)
    messages: list[str] = []

    def catching(call: Callable[[], Any]) -> None:
        try:
            call()
        except PatientSplitError as error:
            messages.extend(error.problems)

    messages += table_problems([*rows, SplitRow(patient, "training", RULE_VERSION, SNAPSHOT), SplitRow(patient, "training", "v9", 1)], pinned=True)
    messages += table_problems([SplitRow(patient, "train", RULE_VERSION, SNAPSHOT), SplitRow("0" + patient, "test", RULE_VERSION, SNAPSHOT)], pinned=True)
    messages += table_problems([SplitRow(patient, other_set(base[patient]), RULE_VERSION, SNAPSHOT)], pinned=True)
    messages += table_problems([], pinned=True)
    messages += split_table.inherited_problems([*rows, SplitRow(patient, "training", "v9", SNAPSHOT)])
    messages += split_table.inherited_problems([])
    messages += frozen_problems(rows_of({**base, patient: "training"}), {k: v for k, v in base.items() if k != "5"} | {"x": "test"}, pinned=True)
    messages += frozen_problems(rows_of({k: v for k, v in base.items() if k != patient}), base, pinned=True)
    messages += frozen_problems(rows_of(base, version="v9"), base, pinned=True)
    messages += source_problems(rows, set(base) - {patient}, SNAPSHOT + 1)
    messages += source_problems(rows, set(base) | {"9" + patient}, SNAPSHOT)
    messages += schema_problems([(patient, "string")], COLUMNS)
    sample = [
        SampleCase(int(case), int(patient), "repeat_test"),
        SampleCase(int(case) + 1, int(patient) + 1, "odd"),
        SampleCase(int(case) + 2, int(patient) + 2, "short_development"),
    ]
    messages += fixed_sample_problems({patient: "training"}, {int(case): patient}, sample)
    messages += fixed_sample_problems({}, {int(case): "1"}, sample[:1])
    messages += fixed_sample_problems({}, {int(case): patient}, sample[:1])
    messages += table_problems([SplitRow(patient, "test", RULE_VERSION, SNAPSHOT), SplitRow(patient, "test", RULE_VERSION, SNAPSHOT)], pinned=False)
    messages += fixed_sample_problems({}, {}, [])
    catching(lambda: build_assignments([patient, "0" + patient]))
    catching(lambda: patient_number("0" + patient))
    catching(lambda: source_cases([(case, patient), (case, "1"), (None, patient), (case, None), ("x" + case, patient)]))
    catching(lambda: values_query([SplitRow("0" + patient, "test", RULE_VERSION, 1)]))
    catching(lambda: values_query([SplitRow(patient, "test" + patient, RULE_VERSION, 1)]))
    catching(lambda: values_query([]))
    catching(lambda: split_table.catalog_conf("pin_x", nessie_uri="http://n:1/api/v2", warehouse="file:///w/", region="", branch="b", at=patient))
    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / f"candidates_{patient}.tsv"
        path.write_text(f"case\tpatient\n{case}\t{patient}\n")
        catching(lambda: read_fixed_sample(path))
        catching(lambda: read_fixed_sample(Path(folder) / f"{case}.tsv"))
    for lake in (
        Lake(base, existing=rows_of({**base, patient: "training"}, snapshot=11)),
        Lake(base, existing=[]),
        Lake(base, existing=[*rows_of(base, snapshot=11), SplitRow(patient, "training", RULE_VERSION, 11)]),
        Lake(base, script=["h9"]),
        Lake(base, script=["h0", "h5"]),
        Lake(base, unrelated=True),
        Lake(base, write_parent="h7"),
        Lake(base, corrupt=lambda rows: rows[1:]),
    ):
        catching(lambda lake=lake: build_with(lake, pinned=False))  # type: ignore[misc]
    catching(lambda: split_table.run_verify(Lake(base).operations(), "h0", pinned=False, sample=None))
    return messages


class MessageTest(unittest.TestCase):
    """The test set is sealed until the choice record is frozen: a message may carry a code and a count, never a patient, a case or a set beside one."""

    def test_every_failure_the_module_can_give_is_a_code_with_no_identifier(self) -> None:
        messages = all_messages()
        self.assertGreater(len(messages), 30)
        for message in messages:
            with self.subTest(message=message):
                self.assertRegex(message, MESSAGE_FORM)
                self.assertIn(message.split(": ")[0], split_table.CODES)
                for identifier in (PLANTED_PATIENT, PLANTED_CASE, "0" + PLANTED_PATIENT, "9" + PLANTED_PATIENT):
                    self.assertNotIn(identifier, message)

    def test_no_message_puts_a_set_name_in_a_sentence_about_one_patient_or_case(self) -> None:
        for message in all_messages():
            with self.subTest(message=message):
                self.assertNotRegex(message, r"(?i)\b(patient|case|subjectid)\b[^:]*\b(training|validation|test)\b")

    def test_every_code_is_driven_at_least_once(self) -> None:
        seen = {message.split(": ")[0] for message in all_messages()}
        self.assertEqual(set(), set(split_table.CODES) - seen - split_table.CODES_OF_THE_JOB)

    def test_the_job_never_prints_a_row_and_writes_only_problems_and_its_result(self) -> None:
        text = JOB_FILE.read_text()
        for forbidden in ("print(", "logging", "show(", ".collect()[", "toPandas"):
            self.assertNotIn(forbidden, text)


if __name__ == "__main__":
    unittest.main()
