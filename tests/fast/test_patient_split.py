"""The patient split table: its rows, its checks and its freeze, in plain data with no Spark.

Failure modes covered here:

- The job carries its own copy of the assignment rule, so the table and the stream can disagree about a patient.
- A patient number that is not in its published form (leading zeros, spaces, a decimal point, non-ASCII digits) is accepted and gets a set.
- A patient is in two sets, or twice in one, and the table is still taken as good.
- A table that differs from the rule for one patient passes, or the shares drift past one percentage point, or the pinned release's exact counts are not held.
- A rebuild changes a patient's set, drops a patient, adds one to a pinned release or uses another rule version, and the freeze does not stop it.
- A fixed-sample case is in the wrong set (a development case in test, a test case in training), is missing from Bronze or is listed under another patient,
  or the check passes because it checked nothing.
- The table was built from another Bronze snapshot or another set of patients than the branch holds now, or has other columns than the card's.

The real runs on Spark, Nessie and the full clinical table are in ``tests/e2e/silver_patient_split.py``.
"""

from __future__ import annotations

import tempfile
import unittest
from collections import Counter
from pathlib import Path
from typing import get_args

from scripts.sample.candidates import CANDIDATES_FILE, SLOTS, read_candidates
from scripts.sample.split import Part, patient_part
from spark import split_table
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


def rows_of(assignments: dict[str, str], version: str = RULE_VERSION) -> list[SplitRow]:
    return [SplitRow(subjectid, name, version, SNAPSHOT) for subjectid, name in assignments.items()]


def other_set(name: str) -> str:
    return "test" if name != "test" else "training"


def tracked_sample() -> list[SampleCase]:
    return read_fixed_sample(CANDIDATES_FILE)


def sample_cohort(sample: list[SampleCase]) -> tuple[dict[str, str], dict[int, str]]:
    """The assignments of the sample's own patients and the case-to-patient map that Bronze would give."""
    return build_assignments(str(item.patient) for item in sample), {item.case: str(item.patient) for item in sample}


class AssignmentTest(unittest.TestCase):
    def test_each_set_is_the_part_that_the_one_rule_gives(self) -> None:
        assignments = build_assignments(str(number) for number in range(1, 2000))
        self.assertEqual({str(number): SET_NAMES[patient_part(number)] for number in range(1, 2000)}, assignments)

    def test_the_job_holds_no_second_copy_of_the_rule(self) -> None:
        for path in (Path(split_table.__file__), JOB_FILE):
            text = path.read_text()
            for marker in ("hashlib", "hexdigest", "sha256("):
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

    def test_a_subject_id_keeps_its_published_form_or_is_refused(self) -> None:
        self.assertEqual(6090, patient_number("6090"))
        for text in ("", "0", "007", " 7", "7 ", "7.0", "-3", "abc", "1e3", "١٢", "+5"):
            with self.subTest(text=text), self.assertRaises(PatientSplitError):
                patient_number(text)

    def test_a_cohort_with_a_bad_subject_id_is_refused_naming_it(self) -> None:
        with self.assertRaisesRegex(PatientSplitError, "007"):
            build_assignments(["1", "007", "3"])


class TableTest(unittest.TestCase):
    def test_the_full_cohort_table_has_no_problem(self) -> None:
        self.assertEqual([], table_problems(rows_of(FULL), pinned=True))

    def test_a_patient_in_two_sets_is_named(self) -> None:
        rows = rows_of(FULL) + [SplitRow("25", other_set(FULL["25"]), RULE_VERSION, SNAPSHOT)]
        problems = table_problems(rows, pinned=False)
        self.assertTrue([problem for problem in problems if "in two sets" in problem and "25" in problem], problems)

    def test_a_patient_twice_in_one_set_is_named(self) -> None:
        rows = rows_of(FULL) + [SplitRow("25", FULL["25"], RULE_VERSION, SNAPSHOT)]
        self.assertTrue([problem for problem in table_problems(rows, pinned=False) if "more than once" in problem and "25" in problem])

    def test_a_set_the_card_does_not_name_is_named(self) -> None:
        rows = [SplitRow("1", "train", RULE_VERSION, SNAPSHOT), *rows_of(FULL)[1:]]
        self.assertTrue([problem for problem in table_problems(rows, pinned=False) if "not one of" in problem and "'train'" in problem])

    def test_a_patient_whose_set_differs_from_the_rule_is_named_even_when_the_counts_hold(self) -> None:
        # Swap the sets of one training and one test patient: every count and share stays the same.
        training = next(subjectid for subjectid, name in FULL.items() if name == "training")
        test = next(subjectid for subjectid, name in FULL.items() if name == "test")
        swapped = {**FULL, training: "test", test: "training"}
        problems = table_problems(rows_of(swapped), pinned=True)
        self.assertEqual(2, len([problem for problem in problems if "does not match the rule" in problem]), problems)
        self.assertFalse([problem for problem in problems if "share" in problem or "exactly" in problem], problems)

    def test_a_table_built_with_another_version_of_the_rule_is_named(self) -> None:
        problems = table_problems(rows_of(FULL, version="patient_split:seed=2"), pinned=True)
        self.assertTrue([problem for problem in problems if "rule version" in problem])

    def test_a_subject_id_not_in_its_published_form_is_named(self) -> None:
        rows = [*rows_of(FULL), SplitRow("007", "training", RULE_VERSION, SNAPSHOT)]
        self.assertTrue([problem for problem in table_problems(rows, pinned=False) if "007" in problem])

    def test_an_empty_table_is_a_problem(self) -> None:
        self.assertTrue(table_problems([], pinned=False))

    def test_the_pinned_release_needs_its_exact_counts(self) -> None:
        one_missing = {subjectid: name for subjectid, name in FULL.items() if subjectid != "1"}
        self.assertEqual([], table_problems(rows_of(one_missing), pinned=False))
        self.assertTrue([problem for problem in table_problems(rows_of(one_missing), pinned=True) if "exactly" in problem])

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

    def test_a_cohort_of_training_patients_only_fails_the_shares(self) -> None:
        only_training = {subjectid: name for subjectid, name in FULL.items() if name == "training"}
        self.assertTrue([problem for problem in table_problems(rows_of(only_training), pinned=False) if "share" in problem])


class FrozenTest(unittest.TestCase):
    def test_a_rebuild_that_changes_nothing_has_no_problem(self) -> None:
        self.assertEqual([], frozen_problems(rows_of(FULL), FULL, pinned=True))

    def test_a_changed_set_is_named_with_both_sets(self) -> None:
        existing = rows_of({**FULL, "30": other_set(FULL["30"])})
        problems = frozen_problems(existing, FULL, pinned=False)
        self.assertEqual(1, len(problems))
        self.assertIn("30", problems[0])
        self.assertIn(other_set(FULL["30"]), problems[0])
        self.assertIn(FULL["30"], problems[0])

    def test_a_patient_that_left_the_source_is_named(self) -> None:
        rebuilt = {subjectid: name for subjectid, name in FULL.items() if subjectid != "5"}
        self.assertTrue([problem for problem in frozen_problems(rows_of(FULL), rebuilt, pinned=False) if "5" in problem and "no longer" in problem])

    def test_a_new_patient_is_refused_for_a_pinned_release_and_allowed_otherwise(self) -> None:
        existing = {subjectid: name for subjectid, name in FULL.items() if subjectid != "5"}
        self.assertTrue([problem for problem in frozen_problems(rows_of(existing), FULL, pinned=True) if "new patient" in problem and "5" in problem])
        self.assertEqual([], frozen_problems(rows_of(existing), FULL, pinned=False))

    def test_a_first_build_has_nothing_to_freeze_even_for_a_pinned_release(self) -> None:
        self.assertEqual([], frozen_problems([], FULL, pinned=True))

    def test_an_existing_table_of_another_rule_version_is_named(self) -> None:
        problems = frozen_problems(rows_of(FULL, version="patient_split:seed=2"), FULL, pinned=False)
        self.assertTrue([problem for problem in problems if "rule version" in problem])


class SourceTest(unittest.TestCase):
    def test_a_table_built_from_the_current_source_has_no_problem(self) -> None:
        self.assertEqual([], source_problems(rows_of(FULL), set(FULL), SNAPSHOT))

    def test_a_table_built_from_another_snapshot_is_named(self) -> None:
        self.assertTrue([problem for problem in source_problems(rows_of(FULL), set(FULL), SNAPSHOT + 1) if "snapshot" in problem])

    def test_a_patient_of_the_source_with_no_row_and_a_row_with_no_patient_are_named(self) -> None:
        short = {subjectid: name for subjectid, name in FULL.items() if subjectid != "9"}
        self.assertTrue([problem for problem in source_problems(rows_of(short), set(FULL), SNAPSHOT) if "have no row" in problem and "'9'" in problem])
        self.assertTrue([problem for problem in source_problems(rows_of(FULL), set(short), SNAPSHOT) if "not in the source" in problem and "'9'" in problem])

    def test_the_columns_are_the_four_of_the_card_in_order(self) -> None:
        self.assertEqual([], schema_problems(list(COLUMNS), COLUMNS, "table"))
        self.assertEqual(
            (("subjectid", "string"), ("set", "string"), ("rule_version", "string"), ("source_snapshot_id", "bigint")),
            COLUMNS,
        )
        self.assertTrue(schema_problems([("subjectid", "bigint"), *COLUMNS[1:]], COLUMNS, "table"))
        self.assertTrue(schema_problems(list(COLUMNS[:3]), COLUMNS, "table"))

    def test_source_rows_give_the_patient_of_each_case_and_refuse_a_case_under_two_patients(self) -> None:
        cases, patients = source_cases([("1", "10"), ("2", "10"), ("3", "11")])
        self.assertEqual(({1: "10", 2: "10", 3: "11"}, {"10", "11"}), (cases, patients))
        for rows in ([("1", "10"), ("1", "11")], [("1", None)], [(None, "10")], [("x", "10")]):
            with self.subTest(rows=rows), self.assertRaises(PatientSplitError):
                source_cases(rows)


class ValuesQueryTest(unittest.TestCase):
    """The rows reach Spark as one query built in the JVM, because Spark's Python worker processes hang under the amd64 emulation of the test computer."""

    def test_each_row_is_one_tuple_in_the_order_given_with_the_four_columns_typed(self) -> None:
        query = values_query([SplitRow("1", "test", RULE_VERSION, SNAPSHOT), SplitRow("20", "training", RULE_VERSION, SNAPSHOT)])
        self.assertEqual(f"VALUES ('1', 'test', '{RULE_VERSION}', {SNAPSHOT}L), ('20', 'training', '{RULE_VERSION}', {SNAPSHOT}L)", query)

    def test_a_value_that_could_change_the_query_is_refused(self) -> None:
        for row in (
            SplitRow("1'); DROP TABLE x; --", "test", RULE_VERSION, SNAPSHOT),
            SplitRow("1", "test') --", RULE_VERSION, SNAPSHOT),
            SplitRow("1", "test", "v'1", SNAPSHOT),
            SplitRow("007", "test", RULE_VERSION, SNAPSHOT),
        ):
            with self.subTest(row=row), self.assertRaises(PatientSplitError):
                values_query([row])

    def test_no_row_is_refused_because_a_table_of_nothing_is_never_written(self) -> None:
        with self.assertRaises(PatientSplitError):
            values_query([])


class FixedSampleTest(unittest.TestCase):
    def test_the_tracked_candidates_match_the_rule_and_cover_both_kinds_of_case(self) -> None:
        sample = tracked_sample()
        assignments, cases = sample_cohort(sample)
        self.assertEqual([], fixed_sample_problems(assignments, cases, sample))
        kinds = {item.slot.rsplit("_", 1)[1] for item in sample}
        self.assertEqual({"development", "test"}, kinds)
        self.assertEqual({slot.name for slot in SLOTS}, {item.slot for item in sample})

    def test_a_development_case_in_test_is_named_by_case_and_slot(self) -> None:
        sample = tracked_sample()
        assignments, cases = sample_cohort(sample)
        chosen = next(item for item in sample if item.slot == "short_development")
        assignments[str(chosen.patient)] = "test"
        problems = fixed_sample_problems(assignments, cases, sample)
        self.assertTrue([problem for problem in problems if f"case {chosen.case}" in problem and "short_development" in problem], problems)

    def test_a_test_case_in_training_is_named(self) -> None:
        sample = tracked_sample()
        assignments, cases = sample_cohort(sample)
        chosen = next(item for item in sample if item.slot == "arterial_test")
        assignments[str(chosen.patient)] = "training"
        self.assertTrue([problem for problem in fixed_sample_problems(assignments, cases, sample) if f"case {chosen.case}" in problem])

    def test_a_case_that_bronze_lacks_is_named(self) -> None:
        sample = tracked_sample()
        assignments, cases = sample_cohort(sample)
        del cases[sample[0].case]
        self.assertTrue(
            [problem for problem in fixed_sample_problems(assignments, cases, sample) if f"case {sample[0].case}" in problem and "no row" in problem]
        )

    def test_a_case_listed_under_another_patient_is_named(self) -> None:
        sample = tracked_sample()
        assignments, cases = sample_cohort(sample)
        cases[sample[0].case] = str(sample[0].patient + 1)
        self.assertTrue(
            [problem for problem in fixed_sample_problems(assignments, cases, sample) if f"case {sample[0].case}" in problem and "patient" in problem]
        )

    def test_a_patient_missing_from_the_table_is_named(self) -> None:
        sample = tracked_sample()
        assignments, cases = sample_cohort(sample)
        del assignments[str(sample[0].patient)]
        self.assertTrue([problem for problem in fixed_sample_problems(assignments, cases, sample) if f"case {sample[0].case}" in problem])

    def test_a_slot_of_no_known_kind_is_named(self) -> None:
        sample = [SampleCase(case=1, patient=1, slot="odd_slot")]
        self.assertTrue(fixed_sample_problems({"1": "training"}, {1: "1"}, sample))

    def test_an_empty_sample_is_a_problem_so_that_the_check_never_passes_on_nothing(self) -> None:
        self.assertTrue(fixed_sample_problems(FULL, {}, []))

    def test_the_file_must_have_the_candidate_header_and_whole_numbers(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "candidates.tsv"
            for text in ("", "case\tpatient\n1\t2\n", "case\tpatient\tkey\tsize_bytes\tsha256\tarterial_waveform\tslot\nx\t2\tk\t1\ts\ttrue\trepeat_test\n"):
                path.write_text(text)
                with self.subTest(text=text[:20]), self.assertRaises(PatientSplitError):
                    read_fixed_sample(path)

    def test_the_tracked_file_reads_as_the_candidate_reader_does(self) -> None:
        theirs = [(item.case, item.patient, item.slot) for item in read_candidates(CANDIDATES_FILE)]
        self.assertEqual(theirs, [(item.case, item.patient, item.slot) for item in tracked_sample()])


if __name__ == "__main__":
    unittest.main()
