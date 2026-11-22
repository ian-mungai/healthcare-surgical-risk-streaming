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
from features.model import ClinicalValue, FeatureInputError

CUTOFF = 1_000_000
MAIN = ("age", "sex", "height", "weight", "bmi", "asa", "emop", "department", "optype")


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
        self.assertEqual(set(MAIN), set(group.values))
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

    def test_a_field_the_plan_does_not_use_is_not_examined(self) -> None:
        group = clinical_group(known(age=63, opdur=math.nan, caseid=math.inf), CUTOFF, include_sensitivity=False)
        self.assertEqual(63.0, group.values["age"])

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

    def test_a_forbidden_field_in_the_table_is_never_a_feature(self) -> None:
        forbidden = known(
            opdur=3.0, anedur=4.0, caseend=100, opend=90, aneend=95, dis=1, los_postop=5, los_icu=2, death_inhosp=0, intraop_ebl=100, intraop_uo=300, caseid=1
        )
        group = clinical_group(known(age=63) | forbidden, CUTOFF, include_sensitivity=True)
        self.assertEqual(set(), set(forbidden) & set(group.values))
        self.assertEqual(63, group.values["age"])

    def test_a_field_the_plan_does_not_name_is_not_a_feature(self) -> None:
        group = clinical_group(known(age=63, opname="a name", dx="a diagnosis"), CUTOFF, include_sensitivity=False)
        self.assertEqual(set(MAIN), set(group.values))

    def test_the_sensitivity_fields_are_features_only_when_asked_for(self) -> None:
        table = known(age=63, approach="Open", position="Supine", ane_type="General")
        self.assertEqual(set(MAIN), set(clinical_group(table, CUTOFF, include_sensitivity=False).values))
        with_sensitivity = clinical_group(table, CUTOFF, include_sensitivity=True)
        self.assertEqual(set(MAIN) | {"approach", "position", "ane_type"}, set(with_sensitivity.values))
        self.assertEqual("Open", with_sensitivity.values["approach"])

    def test_a_sensitivity_field_with_no_availability_time_is_missing(self) -> None:
        group = clinical_group({"approach": ClinicalValue("Open", None)}, CUTOFF, include_sensitivity=True)
        self.assertIsNone(group.values["approach"])


if __name__ == "__main__":
    unittest.main()
