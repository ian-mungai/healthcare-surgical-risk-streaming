"""The facts known before surgery, used only when their availability time is at or before the cutoff.

Failure modes covered here:

- A value is used when it became available after the cutoff, or when its availability time is not known.
- A value available exactly at the cutoff is dropped.
- A field the plan forbids (a duration, an end time, a stay, an outcome, an intraoperative total) reaches the features because the table holds it.
- A sensitivity-only field is a feature of the main set.
- A pre-operative result or condition is dropped, or the fixed fields are missing from the output when the table has no value for them.
"""

from __future__ import annotations

import math
import unittest
from typing import Any

from features.clinical import clinical_group
from features.leakage import FORBIDDEN_FIELDS, FORBIDDEN_PREFIXES
from features.model import ClinicalValue, FeatureInputError
from features.registry import CLINICAL_FIELDS, PREOP_COLUMNS, SENSITIVITY_FIELDS

CUTOFF = 1_000_000
MAIN = ("age", "sex", "height", "weight", "bmi", "asa", "emop", "department", "optype")
# The pre-operative columns of the header of the pinned clinical table, in their order.
PREOP = (
    "preop_htn", "preop_dm", "preop_ecg", "preop_pft", "preop_hb", "preop_plt", "preop_pt", "preop_aptt", "preop_na", "preop_k", "preop_gluc",
    "preop_alb", "preop_ast", "preop_alt", "preop_bun", "preop_cr", "preop_ph", "preop_hco3", "preop_be", "preop_pao2", "preop_paco2", "preop_sao2",
)  # fmt: skip
FIXED = (*MAIN, *PREOP)
# The header of the pinned clinical_data.csv of the release (tests/fixtures/vitaldb/release.json pins its size and SHA-256), in its order.
PINNED_HEADER = (
    "caseid", "subjectid", "casestart", "caseend", "anestart", "aneend", "opstart", "opend", "adm", "dis", "icu_days", "death_inhosp", "age", "sex",
    "height", "weight", "bmi", "asa", "emop", "department", "optype", "dx", "opname", "approach", "position", "ane_type", "preop_htn", "preop_dm",
    "preop_ecg", "preop_pft", "preop_hb", "preop_plt", "preop_pt", "preop_aptt", "preop_na", "preop_k", "preop_gluc", "preop_alb", "preop_ast",
    "preop_alt", "preop_bun", "preop_cr", "preop_ph", "preop_hco3", "preop_be", "preop_pao2", "preop_paco2", "preop_sao2", "cormack", "airway",
    "tubesize", "dltubesize", "lmasize", "iv1", "iv2", "aline1", "aline2", "cline1", "cline2", "intraop_ebl", "intraop_uo", "intraop_rbc", "intraop_ffp",
    "intraop_crystalloid", "intraop_colloid", "intraop_ppf", "intraop_mdz", "intraop_ftn", "intraop_rocu", "intraop_vecu", "intraop_eph", "intraop_phe",
    "intraop_epi", "intraop_ca"
)  # fmt: skip
# The columns of the release that are neither features nor forbidden: the caller drops them before the features, and the features refuse them by name.
DROPPED = (
    "caseid", "subjectid", "casestart", "anestart", "opstart", "adm", "dx", "opname", "cormack", "airway", "tubesize", "dltubesize", "lmasize",
    "iv1", "iv2", "aline1", "aline2", "cline1", "cline2",
)  # fmt: skip
# The forbidden names that are not columns of the release: quantities derived from forbidden columns. The list holds none.
DERIVED_NAMES: tuple[str, ...] = ()


def known(**values: float | int | str | None) -> dict[str, ClinicalValue]:
    return {name: ClinicalValue(value, 0) for name, value in values.items()}


class Availability(unittest.TestCase):
    def test_a_value_available_at_or_before_the_cutoff_is_used_and_one_after_it_is_missing(self) -> None:
        table = {"age": ClinicalValue(63, CUTOFF), "sex": ClinicalValue("F", CUTOFF + 1), "asa": ClinicalValue(2, 0), "weight": ClinicalValue(70.5, CUTOFF - 1)}
        group = clinical_group(table, CUTOFF, include_sensitivity=False)
        self.assertEqual(63, group.values["age"])
        self.assertIsNone(group.values["sex"])
        self.assertEqual(2, group.values["asa"])
        self.assertEqual(70.5, group.values["weight"])
        self.assertEqual(CUTOFF, group.latest_us)

    def test_a_value_with_no_availability_time_is_missing(self) -> None:
        group = clinical_group({"age": ClinicalValue(63, None), "preop_hb": ClinicalValue(12.1, None)}, CUTOFF, include_sensitivity=False)
        self.assertIsNone(group.values["age"])
        self.assertIsNone(group.values["preop_hb"])
        self.assertIsNone(group.latest_us)

    def test_the_latest_time_is_that_of_the_values_used(self) -> None:
        table = {"age": ClinicalValue(63, 10), "asa": ClinicalValue(2, 400), "sex": ClinicalValue("F", CUTOFF + 5)}
        self.assertEqual(400, clinical_group(table, CUTOFF, include_sensitivity=False).latest_us)

    def test_a_value_that_is_not_there_is_missing_and_the_fixed_fields_are_always_in_the_output(self) -> None:
        group = clinical_group({}, CUTOFF, include_sensitivity=False)
        self.assertEqual(list(FIXED), list(group.values))
        self.assertEqual({None}, set(group.values.values()))


class Types(unittest.TestCase):
    def test_a_number_becomes_a_float_and_a_text_stays_a_text(self) -> None:
        group = clinical_group(known(age=63, height=170.5, sex="F", preop_hb=12), CUTOFF, include_sensitivity=False)
        for name in ("age", "height", "preop_hb"):
            self.assertIsInstance(group.values[name], float, name)
        self.assertEqual(63.0, group.values["age"])
        self.assertEqual("F", group.values["sex"])

    def test_a_value_that_is_not_a_finite_number_or_a_text_is_refused_with_its_field(self) -> None:
        for field in ("age", "preop_hb", "approach"):
            bad: Any
            for bad in (math.nan, math.inf, -math.inf, True, [1.0], {"a": 1}):
                with self.subTest(field=field, bad=bad), self.assertRaisesRegex(FeatureInputError, field):
                    clinical_group({field: ClinicalValue(bad, 0)}, CUTOFF, include_sensitivity=True)

    def test_a_value_after_the_cutoff_that_is_not_finite_is_refused_too(self) -> None:
        with self.assertRaisesRegex(FeatureInputError, "age"):
            clinical_group({"age": ClinicalValue(math.nan, CUTOFF + 1)}, CUTOFF, include_sensitivity=False)

    def test_a_number_too_large_for_a_float_is_refused_with_its_field(self) -> None:
        with self.assertRaisesRegex(FeatureInputError, "age"):
            clinical_group(known(age=10**400), CUTOFF, include_sensitivity=False)

    def test_an_availability_time_that_is_not_a_whole_number_of_microseconds_is_refused_with_its_field(self) -> None:
        for bad in (CUTOFF + 0.9, float(CUTOFF), True, "0", math.nan):
            with self.subTest(bad=bad), self.assertRaisesRegex(FeatureInputError, "preop_hb"):
                clinical_group({"preop_hb": ClinicalValue(12.1, bad)}, CUTOFF, include_sensitivity=False)  # type: ignore[arg-type]

    def test_a_cutoff_that_is_not_a_whole_number_of_microseconds_is_refused(self) -> None:
        bad_cutoff: Any = CUTOFF + 0.5
        with self.assertRaisesRegex(FeatureInputError, "cutoff"):
            clinical_group({}, bad_cutoff, include_sensitivity=False)


class Fields(unittest.TestCase):
    def test_every_pre_operative_result_and_condition_is_a_feature_under_its_own_name(self) -> None:
        group = clinical_group(known(preop_hb=12.1, preop_htn=1, preop_gluc=None, preop_ecg="Normal Sinus Rhythm"), CUTOFF, include_sensitivity=False)
        self.assertEqual(12.1, group.values["preop_hb"])
        self.assertEqual(1, group.values["preop_htn"])
        self.assertIsNone(group.values["preop_gluc"])
        self.assertEqual("Normal Sinus Rhythm", group.values["preop_ecg"])

    def test_a_forbidden_field_is_refused_as_forbidden_by_name_whatever_its_value_and_availability(self) -> None:
        for name in (*sorted(FORBIDDEN_FIELDS), "intraop_ebl", "intraop_uo"):
            for item in (ClinicalValue(1.0, 0), ClinicalValue(math.nan, CUTOFF + 1), ClinicalValue("x", None)):
                with self.subTest(name=name, item=item), self.assertRaisesRegex(FeatureInputError, f"{name}: a forbidden clinical field"):
                    clinical_group(known(age=63) | {name: item}, CUTOFF, include_sensitivity=True)

    def test_icu_days_is_refused_as_forbidden_and_not_as_unregistered(self) -> None:
        with self.assertRaises(FeatureInputError) as caught:
            clinical_group(known(age=63, icu_days=2), CUTOFF, include_sensitivity=False)
        self.assertIn("icu_days: a forbidden clinical field", str(caught.exception))
        self.assertNotIn("neither", str(caught.exception))

    def test_a_field_that_is_not_in_the_registry_and_not_forbidden_is_refused_by_name(self) -> None:
        for name in ("caseid", "subjectid", "opname", "dx", "preop_made_up", "mystery", "opdur", "los_icu"):
            with self.subTest(name=name), self.assertRaisesRegex(FeatureInputError, name):
                clinical_group(known(age=63) | known(**{name: 1.0}), CUTOFF, include_sensitivity=False)

    def test_the_keys_come_from_the_registry_and_not_from_the_input(self) -> None:
        # A record that becomes available after the cutoff, or is absent, leaves every key where it was, in the same order.
        without = clinical_group(known(age=63), CUTOFF, include_sensitivity=False)
        future = clinical_group(
            known(age=63) | {"preop_hb": ClinicalValue(12.1, CUTOFF + 1), "preop_k": ClinicalValue(4.0, None)}, CUTOFF, include_sensitivity=False
        )
        self.assertEqual(list(FIXED), list(without.values))
        self.assertEqual(list(without.values), list(future.values))
        self.assertEqual(without, future)

    def test_the_sensitivity_fields_are_features_only_when_asked_for(self) -> None:
        table = known(age=63, approach="Open", position="Supine", ane_type="General")
        self.assertEqual(list(FIXED), list(clinical_group(table, CUTOFF, include_sensitivity=False).values))
        with_sensitivity = clinical_group(table, CUTOFF, include_sensitivity=True)
        self.assertEqual([*FIXED, "approach", "position", "ane_type"], list(with_sensitivity.values))
        self.assertEqual("Open", with_sensitivity.values["approach"])

    def test_a_sensitivity_field_with_no_availability_time_is_missing(self) -> None:
        group = clinical_group({"approach": ClinicalValue("Open", None)}, CUTOFF, include_sensitivity=True)
        self.assertIsNone(group.values["approach"])


class PinnedHeader(unittest.TestCase):
    def test_every_forbidden_release_field_is_a_column_of_the_pinned_header_and_the_derived_names_are_listed_apart(self) -> None:
        self.assertEqual(set(DERIVED_NAMES), set(FORBIDDEN_FIELDS) - set(PINNED_HEADER))
        self.assertEqual({"caseend", "opend", "aneend", "dis", "icu_days", "death_inhosp"}, set(FORBIDDEN_FIELDS))
        for prefix in FORBIDDEN_PREFIXES:
            self.assertTrue([column for column in PINNED_HEADER if column.startswith(prefix)], prefix)

    def test_every_registered_clinical_field_is_a_column_of_the_pinned_header(self) -> None:
        self.assertEqual([], [name for name in (*CLINICAL_FIELDS, *PREOP_COLUMNS, *SENSITIVITY_FIELDS) if name not in PINNED_HEADER])

    def test_every_column_of_the_pinned_header_is_registered_or_forbidden_or_dropped_and_none_is_two_of_these(self) -> None:
        registered = {*CLINICAL_FIELDS, *PREOP_COLUMNS, *SENSITIVITY_FIELDS}
        forbidden = {column for column in PINNED_HEADER if column in FORBIDDEN_FIELDS or column.startswith(FORBIDDEN_PREFIXES)}
        dropped = set(DROPPED)
        self.assertEqual(len(PINNED_HEADER), len(registered) + len(forbidden) + len(dropped))
        self.assertEqual(set(PINNED_HEADER), registered | forbidden | dropped)
        self.assertEqual(set(), registered & forbidden | registered & dropped | forbidden & dropped)


if __name__ == "__main__":
    unittest.main()
