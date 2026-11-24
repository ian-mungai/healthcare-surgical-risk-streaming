"""The tables, the prespecified source counts and the checks of the signal profile, on plain counts. No Spark runs here.

Failure modes covered here:

- A profile table whose causes and valid rows do not add up to its rows, whose share is not valid rows over rows, whose key is twice, or whose rows differ
  from the development rows of Silver, is accepted.
- A table is built with a column that is not the declared one, or the cause columns drift from the causes of the validity definition.
- The unscored row is not one row, is out of order (more unscored than cutoffs, either below one limit), has a share that is not its count over its cutoffs,
  counts more cases with no grid than cases, or holds a rule that is not the registry's and the warning grid rule's (step, first cutoff, last margin).
- A column of the unscored table is not read back by the check, so a change of it passes.
- A prespecified count is changed without a test failing, a count of the full cohort is saved with its value (the check is sealed: a flag only) or a count of
  development data is saved without its value.
- A count that matches is judged as not matching, or the reverse, or a rounded count is compared exactly.
- A check names a case, a track or a value in a message.
"""

from __future__ import annotations

import dataclasses
import unittest
from typing import Any

from features.registry import MAX_INVALID_NUMERIC_PERCENT, MAX_INVALID_WAVEFORM_SECONDS, NUMERIC_WINDOW_SECONDS, WAVEFORM_WINDOW_SECONDS
from features.signal_validity import ARTERIAL_WAVEFORM, INVALID_CAUSES
from spark.profile.checks import (
    CHECK_NAMES,
    CODES,
    CountsFacts,
    InputFacts,
    ProfileFacts,
    UnscoredFacts,
    counts_problems,
    profile_problems,
    unscored_problems,
)
from spark.profile.counts import ARTERIAL_TRACK, BY_NAME, DEVELOPMENT, FULL_COHORT, NONINVASIVE_TRACK, PRESPECIFIED, count_rows, judged
from spark.profile.grid_rule import FIRST_CUTOFF_OFFSET_SECONDS, GRID_STEP_SECONDS, LAST_CUTOFF_MARGIN_SECONDS
from spark.profile.tables import CAUSE_COLUMNS, COUNTS, PROFILE, PROFILE_TABLES, UNSCORED

INPUT = InputFacts(numeric_rows=500, waveform_rows=300, tracks=12, arterial_cases=3)
GOOD = ProfileFacts(rows=12, duplicate_keys=0, partition_differs=0, share_differs=0, unknown_kind=0, numeric_rows=500, waveform_rows=300, tracks=12)


def names(problems: list[str]) -> list[str]:
    return [problem.split(":")[0] for problem in problems]


class Tables(unittest.TestCase):
    def test_the_cause_columns_are_the_causes_of_the_validity_definition_in_order(self) -> None:
        self.assertEqual(tuple(f"cause_{cause}" for cause in INVALID_CAUSES), CAUSE_COLUMNS)
        self.assertEqual(CAUSE_COLUMNS, tuple(name for name, _ in PROFILE.columns if name.startswith("cause_")))

    def test_each_table_has_its_key_in_its_columns_and_no_column_twice(self) -> None:
        self.assertEqual({"signal_profile", "unscored_share", "source_counts"}, set(PROFILE_TABLES))
        for table in PROFILE_TABLES.values():
            with self.subTest(table=table.name):
                columns = [name for name, _ in table.columns]
                self.assertEqual(len(columns), len(set(columns)))
                self.assertTrue(set(table.key) <= set(columns))

    def test_the_declared_columns(self) -> None:
        self.assertEqual(("kind", "track"), PROFILE.key)
        self.assertEqual(("scope",), UNSCORED.key)
        self.assertEqual(("name",), COUNTS.key)
        self.assertEqual(
            [("kind", "string"), ("track", "string"), ("cases", "bigint"), ("rows", "bigint"), ("valid_rows", "bigint"), ("valid_share", "double")],
            list(PROFILE.columns[:6]),
        )


class ProfileChecks(unittest.TestCase):
    def test_a_profile_that_reconciles_has_no_problem(self) -> None:
        self.assertEqual([], profile_problems(GOOD, INPUT))

    def test_each_defect_is_named_with_a_count_and_nothing_else(self) -> None:
        cases = {
            "profile_duplicate_key": ProfileFacts(12, 2, 0, 0, 0, 500, 300, 12),
            "profile_partition": ProfileFacts(12, 0, 3, 0, 0, 500, 300, 12),
            "profile_share": ProfileFacts(12, 0, 0, 1, 0, 500, 300, 12),
            "profile_kind": ProfileFacts(12, 0, 0, 0, 1, 500, 300, 12),
            "profile_rows_differ": ProfileFacts(12, 0, 0, 0, 0, 499, 300, 12),
            "profile_tracks_differ": ProfileFacts(11, 0, 0, 0, 0, 500, 300, 11),
        }
        for code, facts in cases.items():
            with self.subTest(code=code):
                problems = profile_problems(facts, INPUT)
                self.assertEqual([code], names(problems))
                self.assertRegex(problems[0], rf"^{code}: \d+ [a-z ]+$")

    def test_a_waveform_count_that_differs_is_named_as_rows_too(self) -> None:
        self.assertEqual(["profile_rows_differ"], names(profile_problems(ProfileFacts(12, 0, 0, 0, 0, 500, 301, 12), INPUT)))

    def test_an_empty_profile_over_a_development_input_that_holds_rows_is_named(self) -> None:
        self.assertIn("profile_empty", names(profile_problems(ProfileFacts(0, 0, 0, 0, 0, 0, 0, 0), INPUT)))

    def test_an_empty_input_and_an_empty_profile_are_not_a_profile(self) -> None:
        self.assertEqual(["input_empty"], names(profile_problems(ProfileFacts(0, 0, 0, 0, 0, 0, 0, 0), InputFacts(0, 0, 0, 0))))


def unscored(**changes: float | None) -> UnscoredFacts:
    base: dict[str, Any]
    base = {
        "rows": 1,
        "cutoffs": 100,
        "waveform": 10,
        "numeric": 5,
        "either": 12,
        "share": 0.12,
        "cases": 3,
        "no_grid": 1,
        "cutoff_step": GRID_STEP_SECONDS,
        "cutoff_offset": FIRST_CUTOFF_OFFSET_SECONDS,
        "last_margin": LAST_CUTOFF_MARGIN_SECONDS,
        "waveform_window": WAVEFORM_WINDOW_SECONDS,
        "waveform_limit": MAX_INVALID_WAVEFORM_SECONDS,
        "numeric_window": NUMERIC_WINDOW_SECONDS,
        "numeric_limit": MAX_INVALID_NUMERIC_PERCENT,
    }
    return UnscoredFacts(**(base | changes))


class UnscoredChecks(unittest.TestCase):
    def test_a_consistent_row_has_no_problem(self) -> None:
        self.assertEqual([], unscored_problems(unscored(), INPUT))

    def test_a_row_with_no_cutoff_has_no_share(self) -> None:
        self.assertEqual([], unscored_problems(unscored(cutoffs=0, waveform=0, numeric=0, either=0, share=None), INPUT))
        self.assertEqual(["unscored_share"], names(unscored_problems(unscored(cutoffs=0, waveform=0, numeric=0, either=0, share=0.0), INPUT)))

    def test_each_defect_is_named(self) -> None:
        cases = {
            "unscored_rows": unscored(rows=2),
            "unscored_order": unscored(either=101, share=1.01),
            "unscored_share": unscored(share=0.13),
            "unscored_rule": unscored(waveform_limit=1),
            "unscored_cases": unscored(cases=4),
            "unscored_no_grid": unscored(no_grid=4),
        }
        for code, facts in cases.items():
            with self.subTest(code=code):
                self.assertEqual([code], names(unscored_problems(facts, INPUT)))

    def test_either_is_between_the_larger_limit_and_the_sum_of_both(self) -> None:
        for either in (9, 16):
            with self.subTest(either=either):
                self.assertEqual(["unscored_order"], names(unscored_problems(unscored(either=either, share=either / 100), INPUT)))
        for either in (10, 15):
            with self.subTest(either=either):
                self.assertEqual([], unscored_problems(unscored(either=either, share=either / 100), INPUT))

    def test_every_rule_column_is_held_to_the_registry(self) -> None:
        for column in ("cutoff_step", "cutoff_offset", "last_margin", "waveform_window", "waveform_limit", "numeric_window", "numeric_limit"):
            with self.subTest(column=column):
                self.assertEqual(["unscored_rule"], names(unscored_problems(unscored(**{column: 7}), INPUT)))

    def test_a_case_with_no_grid_is_one_of_the_cases_and_none_is_allowed_too(self) -> None:
        self.assertEqual([], unscored_problems(unscored(no_grid=0), INPUT))
        self.assertEqual([], unscored_problems(unscored(no_grid=3), INPUT))
        self.assertEqual([], unscored_problems(unscored(cutoffs=0, waveform=0, numeric=0, either=0, share=None, no_grid=3), INPUT))
        self.assertEqual(["unscored_no_grid"], names(unscored_problems(unscored(no_grid=4), INPUT)))

    def test_every_column_of_the_unscored_table_is_a_fact_that_the_check_reads(self) -> None:
        table = {name for name, _ in UNSCORED.columns} - {"scope", "unscored_share"}
        facts = {field.name for field in dataclasses.fields(UnscoredFacts)} - {"rows", "share"}
        self.assertEqual(table, facts)

    def test_the_check_names_include_every_unscored_check(self) -> None:
        for code in ("unscored_rows", "unscored_order", "unscored_share", "unscored_rule", "unscored_cases", "unscored_no_grid"):
            with self.subTest(code=code):
                self.assertIn(code, CHECK_NAMES)


class SourceCounts(unittest.TestCase):
    def test_the_prespecified_values_are_those_of_the_guide_and_the_standard(self) -> None:
        held = {count.name: (count.scope, count.expected) for count in PRESPECIFIED}
        self.assertEqual(
            {
                "development_cases": (DEVELOPMENT, 5125),
                "development_patients_training": (DEVELOPMENT, 4287),
                "development_patients_validation": (DEVELOPMENT, 594),
                "cases": (FULL_COHORT, 6388),
                "patients": (FULL_COHORT, 6090),
                "tracks": (FULL_COHORT, 486449),
                "cases_with_arterial_waveform": (FULL_COHORT, 3645),
                "cases_with_noninvasive_pressure": (FULL_COHORT, 5763),
                "recorded_hours": (FULL_COHORT, 20138),
                "mean_case_hours": (FULL_COHORT, 3.15),
                "median_case_hours": (FULL_COHORT, 2.76),
            },
            held,
        )

    def test_the_signal_names_of_the_counts_are_the_publisher_names_and_are_in_the_method(self) -> None:
        self.assertEqual(ARTERIAL_WAVEFORM, ARTERIAL_TRACK)
        self.assertEqual("Solar8000/NIBP_MBP", NONINVASIVE_TRACK)
        self.assertIn(ARTERIAL_TRACK, BY_NAME["cases_with_arterial_waveform"].method)
        self.assertIn(NONINVASIVE_TRACK, BY_NAME["cases_with_noninvasive_pressure"].method)

    def test_the_track_counts_come_from_the_publishers_track_list_that_the_guide_names(self) -> None:
        for name in ("tracks", "cases_with_arterial_waveform", "cases_with_noninvasive_pressure"):
            with self.subTest(count=name):
                self.assertEqual("raw_records.track_list", BY_NAME[name].input)
                self.assertIn("track list", BY_NAME[name].method)

    def test_every_count_names_its_input_method_and_denominator(self) -> None:
        for count in PRESPECIFIED:
            with self.subTest(count=count.name):
                self.assertTrue(count.input and count.method and count.denominator)

    def test_a_count_matches_exactly_or_after_its_stated_rounding(self) -> None:
        by_name = {count.name: count for count in PRESPECIFIED}
        self.assertTrue(judged(by_name["cases"], 6388.0))
        self.assertFalse(judged(by_name["cases"], 6387.0))
        self.assertFalse(judged(by_name["cases"], 6388.4))
        self.assertTrue(judged(by_name["recorded_hours"], 20137.6033))
        self.assertFalse(judged(by_name["recorded_hours"], 20137.4))
        self.assertTrue(judged(by_name["mean_case_hours"], 3.152411))
        self.assertFalse(judged(by_name["mean_case_hours"], 3.16))
        self.assertTrue(judged(by_name["median_case_hours"], 2.756805))
        self.assertFalse(judged(by_name["median_case_hours"], 2.7549))

    def values(self, **changes: float | None) -> dict[str, float | None]:
        held: dict[str, float | None] = {count.name: float(count.expected) for count in PRESPECIFIED}
        held["recorded_hours"], held["mean_case_hours"], held["median_case_hours"] = 20137.6, 3.152, 2.7568
        return held | changes

    def test_a_pinned_run_saves_a_flag_for_every_count_and_a_value_for_development_counts_only(self) -> None:
        rows = {row["name"]: row for row in count_rows(self.values(), compared=True)}
        self.assertEqual({count.name for count in PRESPECIFIED}, set(rows))
        for count in PRESPECIFIED:
            row = rows[count.name]
            with self.subTest(count=count.name):
                self.assertIs(True, row["matches"])
                self.assertEqual(count.scope == FULL_COHORT, row["sealed"])
                self.assertEqual(None if count.scope == FULL_COHORT else float(count.expected), row["value"])

    def test_a_count_that_differs_is_a_false_flag_and_its_value_is_still_not_saved_for_the_full_cohort(self) -> None:
        rows = {row["name"]: row for row in count_rows(self.values(tracks=486451.0, development_cases=5124.0), compared=True)}
        self.assertIs(False, rows["tracks"]["matches"])
        self.assertIsNone(rows["tracks"]["value"])
        self.assertIs(False, rows["development_cases"]["matches"])
        self.assertEqual(5124.0, rows["development_cases"]["value"])
        self.assertEqual({"tracks", "development_cases"}, {name for name, row in rows.items() if row["matches"] is False})

    def test_a_count_with_no_value_is_not_a_match(self) -> None:
        rows = {row["name"]: row for row in count_rows(self.values(patients=None), compared=True)}
        self.assertIs(False, rows["patients"]["matches"])

    def test_an_unpinned_run_compares_nothing_and_saves_the_development_values_only(self) -> None:
        for row in count_rows({"development_cases": 7.0}, compared=False):
            with self.subTest(count=row["name"]):
                self.assertIsNone(row["matches"])
                self.assertEqual(7.0 if row["name"] == "development_cases" else None, row["value"])


def counts(**changes: int) -> CountsFacts:
    base = {"rows": len(PRESPECIFIED), "names_differ": 0, "sealed_with_value": 0, "expected_differs": 0, "not_judged": 0, "judged_unpinned": 0}
    return CountsFacts(**(base | changes))


class CountsChecks(unittest.TestCase):
    def test_a_table_of_the_prespecified_counts_has_no_problem(self) -> None:
        self.assertEqual([], counts_problems(counts(), pinned=True))
        self.assertEqual([], counts_problems(counts(), pinned=False))

    def test_a_table_with_a_row_too_many_or_too_few_is_named(self) -> None:
        for rows in (len(PRESPECIFIED) + 1, len(PRESPECIFIED) - 1, 0):
            with self.subTest(rows=rows):
                self.assertEqual(["counts_rows"], names(counts_problems(counts(rows=rows), pinned=True)))

    def test_each_defect_is_named(self) -> None:
        cases = {
            "counts_names": counts(names_differ=1),
            "counts_sealed_value": counts(sealed_with_value=2),
            "counts_expected": counts(expected_differs=1),
        }
        for code, facts in cases.items():
            with self.subTest(code=code):
                self.assertEqual([code], names(counts_problems(facts, pinned=True)))

    def test_a_pinned_table_judges_every_count_and_an_unpinned_one_none(self) -> None:
        self.assertEqual(["counts_not_judged"], names(counts_problems(counts(not_judged=3), pinned=True)))
        self.assertEqual([], counts_problems(counts(not_judged=3), pinned=False))
        self.assertEqual(["counts_judged_unpinned"], names(counts_problems(counts(judged_unpinned=2), pinned=False)))
        self.assertEqual([], counts_problems(counts(judged_unpinned=2), pinned=True))


class Codes(unittest.TestCase):
    def test_every_code_that_the_build_and_the_verify_raise_is_in_the_code_list(self) -> None:
        for code in (
            "plan_differs",
            "readback_differs",
            "table_missing",
            "branch_moved",
            "source_changed",
            "input_empty",
            "source_table",
            "source_no_snapshot",
            "table_schema",
            "grid_rule_changed",
            "grid_rule_rows",
        ):
            with self.subTest(code=code):
                self.assertIn(code, CODES)

    def test_every_problem_code_is_in_the_code_list(self) -> None:
        produced = set()
        produced |= set(names(profile_problems(ProfileFacts(0, 1, 1, 1, 1, 9, 9, 9), InputFacts(1, 1, 1, 1))))
        produced |= set(names(profile_problems(ProfileFacts(0, 0, 0, 0, 0, 0, 0, 0), InputFacts(0, 0, 0, 0))))
        produced |= set(names(unscored_problems(unscored(rows=2, either=101, share=0.5, waveform_limit=9, cases=9), INPUT)))
        produced |= set(names(counts_problems(counts(names_differ=1, sealed_with_value=1, expected_differs=1, not_judged=1), pinned=True)))
        produced |= set(names(counts_problems(counts(judged_unpinned=1), pinned=False)))
        self.assertTrue(produced)
        self.assertEqual(set(), produced - CODES)


if __name__ == "__main__":
    unittest.main()
