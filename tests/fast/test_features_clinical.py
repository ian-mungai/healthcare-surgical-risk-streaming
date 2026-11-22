"""The facts known before surgery, used only when their availability time is at or before the cutoff.

Failure modes covered here:

- A value is used when it became available after the cutoff, or when its availability time is not known.
- A value available exactly at the cutoff is dropped.
- A field the plan forbids (a duration, an end time, a stay, an outcome, an intraoperative total) reaches the features because the table holds it.
- A sensitivity-only field is a feature of the main set.
- A pre-operative result or condition is dropped, or the fixed fields are missing from the output when the table has no value for them.
"""

from __future__ import annotations

import unittest

from features.clinical import clinical_group
from features.model import ClinicalValue

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
