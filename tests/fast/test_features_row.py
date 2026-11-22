"""The registry against the feature list, the feature row and the leakage check.

Failure modes covered here:

- A feature of the list has no registry entry, an entry lacks its unit or its missing rule, or the registry holds a feature that the list does not.
- The row holds a name the registry does not know, or lacks one it holds, so the training table and the stream disagree on the columns.
- The row is computed from a reading, a waveform second or a clinical value after its cutoff, or its recorded latest times are wrong.
- The leakage check misses a latest reading time after the cutoff, a clinical availability time after the cutoff, a forbidden field or an unknown field.
- A fitted step (scaling or filling) hides in the row: a missing value is a zero or a fill.
"""

from __future__ import annotations

import unittest
from dataclasses import replace
from typing import Any

from features.leakage import FORBIDDEN_FIELDS, row_problems
from features.model import ClinicalValue, FeatureRow, Reading, WaveformSecond, WaveformWindow, WindowInput
from features.registry import NUMERIC_TRACKS, WAVEFORM_STATUS, WAVEFORM_TRACKS, feature_specs, is_registered
from features.row import build_row
from tests.fast.features_support import ARTERIAL_MEAN, CUTOFF_US, HEART_RATE, SECOND, SERIES, alternating, arithmetic, numeric_window, waveform_window

# The draft feature list, written out independently of the registry.
LIST_NUMERIC_TRACKS = {
    "arterial": ["Solar8000/ART_MBP", "Solar8000/ART_SBP", "Solar8000/ART_DBP"],
    "non_invasive": ["Solar8000/NIBP_MBP"],
    "heart_rate": ["Solar8000/HR"],
    "oxygen_saturation": ["Solar8000/PLETH_SPO2"],
    "end_tidal_co2": ["Solar8000/ETCO2"],
    "minute_ventilation": ["Primus/MV"],
    "propofol": ["Orchestra/PPF20_RATE"],
    "remifentanil": ["Orchestra/RFTN20_RATE"],
    "vasopressor": ["Orchestra/NEPI_RATE", "Orchestra/EPI_RATE", "Orchestra/PHEN_RATE", "Orchestra/VASO_RATE", "Orchestra/DOPA_RATE"],
}
LIST_SOURCE_STATS = ["last", "mean", "median", "min", "max", "std", "slope_per_min", "valid_count", "seconds_since_last"]
LIST_SERIES = "Silver/ART_MBP_1S"
LIST_SERIES_STATS = ["valid_count", "coverage", "minutes_below_65", "minutes_below_60", "seconds_since_below_65", "projection_5min"]
LIST_WAVEFORM_STATS = {
    "SNUADC/ART": ["mean", "std", "pulse_pressure_mean", "dominant_freq_hz", "valid_seconds", "coverage"],
    "SNUADC/ECG_II": ["std", "dominant_freq_hz", "valid_seconds", "coverage"],
    "SNUADC/PLETH": ["amplitude_mean", "amplitude_std", "dominant_freq_hz", "valid_seconds", "coverage"],
}
LIST_CLINICAL = ["age", "sex", "height", "weight", "bmi", "asa", "emop", "department", "optype"]
LIST_SENSITIVITY = ["approach", "position", "ane_type"]
LIST_NEVER = ["opdur", "anedur", "caseend", "opend", "aneend", "dis", "los_postop", "los_icu", "death_inhosp"]


def slug(track: str) -> str:
    return track.lower().replace("/", "_")


def expected_names() -> set[str]:
    names = set(LIST_CLINICAL)
    for tracks in LIST_NUMERIC_TRACKS.values():
        for track in tracks:
            names |= {f"{slug(track)}__{stat}" for stat in LIST_SOURCE_STATS}
    names |= {f"{slug(LIST_SERIES)}__{stat}" for stat in LIST_SERIES_STATS}
    for track, stats in LIST_WAVEFORM_STATS.items():
        names |= {f"{slug(track)}__{stat}" for stat in stats}
    return names


def empty_window(cutoff_us: int = CUTOFF_US) -> WindowInput:
    return WindowInput(cutoff_us, {}, {}, {})


class Registry(unittest.TestCase):
    def test_the_registry_holds_exactly_the_features_of_the_list(self) -> None:
        fixed = {spec.name for spec in feature_specs() if not spec.sensitivity_only}
        self.assertEqual(expected_names(), fixed)
        self.assertEqual(set(LIST_SENSITIVITY), {spec.name for spec in feature_specs() if spec.sensitivity_only})

    def test_the_tracks_of_the_registry_are_those_of_the_list(self) -> None:
        self.assertEqual({track for tracks in LIST_NUMERIC_TRACKS.values() for track in tracks} | {LIST_SERIES}, set(NUMERIC_TRACKS))
        self.assertEqual(set(LIST_WAVEFORM_STATS), set(WAVEFORM_TRACKS))

    def test_every_feature_has_a_unit_a_missing_rule_and_a_definition_and_no_name_repeats(self) -> None:
        specs = feature_specs()
        self.assertEqual(len(specs), len({spec.name for spec in specs}))
        for spec in specs:
            with self.subTest(name=spec.name):
                self.assertTrue(spec.unit.strip())
                self.assertTrue(spec.missing.strip())
                self.assertTrue(spec.definition.strip())

    def test_every_feature_has_its_input_kind_and_the_registry_keeps_both_kinds_of_numeric_input(self) -> None:
        kinds = {spec.group: spec.input_kind for spec in feature_specs()}
        self.assertEqual("per_second", kinds[SERIES])
        self.assertEqual("source_time", kinds[ARTERIAL_MEAN])
        self.assertEqual("source_time", kinds[HEART_RATE])
        self.assertEqual("waveform", kinds["SNUADC/ART"])
        self.assertEqual("clinical", kinds["clinical"])
        self.assertEqual({"source_time", "per_second", "waveform", "clinical"}, set(kinds.values()))
        self.assertEqual({SERIES}, {track for track, spec in NUMERIC_TRACKS.items() if spec.kind == "per_second"})

    def test_a_track_marked_confirmed_has_an_evidence_note_and_an_unconfirmed_one_says_what_to_confirm(self) -> None:
        statuses = {track: spec.status for track, spec in NUMERIC_TRACKS.items()} | dict(WAVEFORM_STATUS)
        self.assertEqual(set(NUMERIC_TRACKS) | set(WAVEFORM_TRACKS), set(statuses))
        for track, status in statuses.items():
            with self.subTest(track=track):
                self.assertTrue(status.evidence.strip())
                if not status.confirmed:
                    self.assertIn("to confirm", status.evidence)
        for track in ("SNUADC/ART", "Solar8000/ART_MBP"):
            self.assertTrue(statuses[track].confirmed)
        for track in ("Solar8000/HR", "SNUADC/ECG_II", "SNUADC/PLETH", "Orchestra/PPF20_RATE", "Primus/MV"):
            self.assertFalse(statuses[track].confirmed)
            self.assertIn("bronze.tracks", statuses[track].evidence)

    def test_every_feature_carries_the_status_of_its_track_and_the_projection_names_its_source_to_confirm(self) -> None:
        by_name = {spec.name: spec for spec in feature_specs()}
        for spec in by_name.values():
            with self.subTest(name=spec.name):
                self.assertIn(spec.status, ("confirmed", "to_confirm"))
                self.assertTrue(spec.note.strip())
                if spec.status == "confirmed":
                    self.assertNotIn("to confirm", spec.note)
        self.assertEqual("confirmed", by_name["solar8000_art_mbp__mean"].status)
        self.assertEqual("to_confirm", by_name["solar8000_hr__mean"].status)
        self.assertEqual("to_confirm", by_name["snuadc_pleth__amplitude_std"].status)
        self.assertEqual("to_confirm", by_name["silver_art_mbp_1s__projection_5min"].status)
        self.assertIn("Jacquet-Lagrèze 2022", by_name["silver_art_mbp_1s__projection_5min"].note)

    def test_every_standard_deviation_says_that_it_divides_by_n_minus_one(self) -> None:
        stds = [spec for spec in feature_specs() if spec.name.endswith(("__std", "__amplitude_std"))]
        self.assertEqual(len(NUMERIC_TRACKS) - 1 + 3, len(stds))
        for spec in stds:
            with self.subTest(name=spec.name):
                self.assertIn("n - 1", spec.definition)

    def test_the_registry_knows_its_names_and_the_pre_operative_family_and_nothing_else(self) -> None:
        self.assertTrue(is_registered("age"))
        self.assertTrue(is_registered("preop_hb"))
        self.assertTrue(is_registered("approach"))
        self.assertTrue(is_registered(f"{slug(ARTERIAL_MEAN)}__mean"))
        for name in ("opdur", "intraop_ebl", "mystery", "solar8000_art_mbp__mystery", "solar8000_unknown__mean", ""):
            with self.subTest(name=name):
                self.assertFalse(is_registered(name))

    def test_no_forbidden_field_is_in_the_registry(self) -> None:
        self.assertEqual(set(LIST_NEVER), set(FORBIDDEN_FIELDS))
        self.assertEqual(set(), {spec.name for spec in feature_specs()} & set(FORBIDDEN_FIELDS))
        self.assertEqual([], [spec.name for spec in feature_specs() if spec.name.startswith("intraop_")])


class Row(unittest.TestCase):
    def test_an_empty_window_gives_every_fixed_name_with_missing_values_and_zero_counts(self) -> None:
        row = build_row(empty_window())
        self.assertEqual(expected_names(), set(row.values))
        for name, value in row.values.items():
            if name.endswith(("__valid_count", "__valid_seconds")):
                self.assertEqual(0, value, name)
            elif name.endswith("__coverage"):
                self.assertEqual(0.0, value, name)
            else:
                self.assertIsNone(value, name)
        self.assertIsNone(row.latest_reading_us)
        self.assertIsNone(row.latest_clinical_us)
        self.assertEqual(CUTOFF_US, row.cutoff_us)

    def test_the_sensitivity_fields_are_added_only_when_asked_for(self) -> None:
        self.assertEqual(expected_names() | set(LIST_SENSITIVITY), set(build_row(empty_window(), include_sensitivity=True).values))

    def test_a_row_from_a_full_window_has_known_values_and_recorded_latest_times(self) -> None:
        window = WindowInput(
            CUTOFF_US,
            {
                ARTERIAL_MEAN: numeric_window(arithmetic(100.0, -0.05)),
                SERIES: numeric_window(arithmetic(100.0, -0.05)),
                HEART_RATE: numeric_window([60.0] * 890, cutoff_us=CUTOFF_US - 10 * SECOND),
            },
            {"SNUADC/ART": waveform_window(alternating)},
            {"age": ClinicalValue(63, 100), "preop_hb": ClinicalValue(12.1, 250)},
        )
        row = build_row(window)
        self.assertEqual(890, row.values["solar8000_hr__valid_count"])
        self.assertEqual(10.0, row.values["solar8000_hr__seconds_since_last"])
        self.assertEqual(900, row.values["silver_art_mbp_1s__valid_count"])
        self.assertEqual(1.0, row.values["silver_art_mbp_1s__coverage"])
        self.assertEqual(0, row.values["snuadc_ecg_ii__valid_seconds"])
        self.assertEqual(0, row.values["snuadc_pleth__valid_seconds"])
        self.assertIsNone(row.values["snuadc_ecg_ii__std"])
        self.assertEqual(CUTOFF_US, row.latest_reading_us)
        self.assertEqual(250, row.latest_clinical_us)
        self.assertEqual(900, row.values["solar8000_art_mbp__valid_count"])
        self.assertEqual(20, row.values["snuadc_art__valid_seconds"])
        self.assertEqual(63, row.values["age"])
        self.assertEqual(12.1, row.values["preop_hb"])
        self.assertEqual(0, row.values["solar8000_etco2__valid_count"])
        self.assertEqual([], row_problems(row))

    def test_a_track_the_registry_does_not_know_is_refused_in_the_row(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown track"):
            build_row(WindowInput(CUTOFF_US, {"Solar8000/NOT_A_TRACK": []}, {}, {}))
        with self.assertRaisesRegex(ValueError, "unknown track"):
            build_row(WindowInput(CUTOFF_US, {}, {"SNUADC/NOT_A_TRACK": waveform_window(alternating)}, {}))


class Leakage(unittest.TestCase):
    def window_with_future(self, future: bool) -> WindowInput:
        numeric = numeric_window(arithmetic(100.0, -0.05))
        waveform = waveform_window(alternating)
        clinical = {"age": ClinicalValue(63, 0), "sex": ClinicalValue("F", 0)}
        if future:
            numeric += [Reading(CUTOFF_US + SECOND, 0.0, True), Reading(CUTOFF_US + 100 * SECOND, 500.0, True)]
            waveform = WaveformWindow(waveform.sample_rate, waveform.seconds + (WaveformSecond(CUTOFF_US // SECOND, (999.0,) * 10, True),))
            clinical |= {
                "asa": ClinicalValue(5, CUTOFF_US + 1),
                "weight": ClinicalValue(999.0, CUTOFF_US + SECOND),
                "preop_hb": ClinicalValue(1.0, CUTOFF_US + 1),
            }
        return WindowInput(CUTOFF_US, {ARTERIAL_MEAN: numeric}, {"SNUADC/ART": waveform}, clinical)

    def test_data_after_the_cutoff_changes_no_value_and_leaves_no_latest_time_after_it(self) -> None:
        with_future = build_row(self.window_with_future(True))
        without = build_row(self.window_with_future(False))
        self.assertEqual(
            {k: v for k, v in without.values.items() if k not in ("asa", "weight", "preop_hb")},
            {k: v for k, v in with_future.values.items() if k not in ("asa", "weight", "preop_hb")},
        )
        self.assertIsNone(with_future.values["asa"])
        self.assertIsNone(with_future.values["weight"])
        self.assertIsNone(with_future.values["preop_hb"])
        self.assertEqual([], row_problems(with_future))
        self.assertIsNotNone(with_future.latest_reading_us)
        self.assertIsNotNone(with_future.latest_clinical_us)
        self.assertLessEqual(with_future.latest_reading_us or 0, with_future.cutoff_us)
        self.assertLessEqual(with_future.latest_clinical_us or 0, with_future.cutoff_us)

    def row(self, **changes: Any) -> FeatureRow:
        return replace(FeatureRow(CUTOFF_US, {"age": 63, "solar8000_hr__mean": 60.0}, CUTOFF_US, 0), **changes)

    def test_the_check_passes_a_clean_row(self) -> None:
        self.assertEqual([], row_problems(self.row()))
        self.assertEqual([], row_problems(self.row(latest_reading_us=None, latest_clinical_us=None)))

    def test_the_check_fails_a_latest_reading_time_after_the_cutoff(self) -> None:
        problems = row_problems(self.row(latest_reading_us=CUTOFF_US + 1))
        self.assertEqual(1, len(problems))
        self.assertIn("latest reading", problems[0])

    def test_the_check_fails_a_clinical_availability_time_after_the_cutoff(self) -> None:
        problems = row_problems(self.row(latest_clinical_us=CUTOFF_US + 1))
        self.assertEqual(1, len(problems))
        self.assertIn("availability", problems[0])

    def test_the_check_fails_each_forbidden_field_and_each_intraoperative_total(self) -> None:
        for name in (*LIST_NEVER, "intraop_ebl", "intraop_uo"):
            with self.subTest(name=name):
                problems = row_problems(self.row(values={"age": 63, name: 1.0}))
                self.assertEqual(1, len(problems))
                self.assertIn("forbidden", problems[0])

    def test_the_check_fails_a_name_the_registry_does_not_know(self) -> None:
        problems = row_problems(self.row(values={"age": 63, "caseid": 1}))
        self.assertEqual(1, len(problems))
        self.assertIn("unknown", problems[0])

    def test_the_check_lists_every_problem_of_a_row(self) -> None:
        problems = row_problems(self.row(values={"opdur": 1.0, "mystery": 2}, latest_reading_us=CUTOFF_US + 1, latest_clinical_us=CUTOFF_US + 2))
        self.assertEqual(4, len(problems))


if __name__ == "__main__":
    unittest.main()
