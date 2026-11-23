"""The batch entry point: windows in a plain JSON file in, feature rows in a plain JSON file out.

Failure modes covered here:

- The batch entry point computes a feature by its own code, so the stream's call of ``build_row`` and the batch disagree.
- A window of the file is read with the wrong cutoff, units or valid flag, or a missing value is written as zero or as not a number.
- A row that breaks the leakage check is written.
- A bad window leaves a half-written output file, or a second run gives another file.
"""

from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

from features import batch
from features.leakage import LeakageError
from features.model import ClinicalValue, FeatureRow, Reading, WindowInput
from features.row import build_row
from tests.fast.features_support import (
    ARTERIAL_MEAN,
    CUTOFF_US,
    HEART_RATE,
    SECOND,
    SERIES,
    alternating,
    arithmetic,
    numeric_window,
    series_window,
    waveform_window,
)


def series_row(reading: Reading) -> list[Any]:
    """The JSON reading of the series: its value, its flag and its exact mean as a count and the two parts of the sum."""
    if reading.valid:
        return [reading.time_us, reading.value, True, 1, reading.value, 0.0]
    return [reading.time_us, reading.value, False, None, None, None]


def window_document(window_id: str, future: bool = False) -> dict[str, Any]:
    readings = [[reading.time_us, reading.value, reading.valid] for reading in numeric_window(arithmetic(100.0, -0.05), valid=lambda index: index != 10)]
    series = [series_row(reading) for reading in series_window(arithmetic(100.0, -0.05), valid=lambda index: index != 10)]
    if future:
        readings += [[CUTOFF_US + SECOND, 0.0, True]]
        series += [[CUTOFF_US, 0.0, True, 1, 0.0, 0.0], [CUTOFF_US + SECOND, 0.0, True, 1, 0.0, 0.0]]
    wave = waveform_window(alternating)
    seconds = [{"second": second.second, "samples": list(second.samples), "valid": second.valid, "start_offset_us": 0} for second in wave.seconds]
    clinical: dict[str, Any] = {
        "age": {"value": 63, "available_us": 0},
        "sex": {"value": "F", "available_us": 0},
        "approach": {"value": "Open", "available_us": 0},
    }
    if future:
        clinical["asa"] = {"value": 5, "available_us": CUTOFF_US + 1}
    return {
        "window_id": window_id,
        "cutoff_us": CUTOFF_US,
        "numeric": {ARTERIAL_MEAN: readings, SERIES: series},
        "waveforms": {"SNUADC/ART": {"sample_rate": wave.sample_rate, "seconds": seconds}},
        "clinical": clinical,
    }


class Documentation(unittest.TestCase):
    @unittest.expectedFailure
    def test_the_docstring_gives_the_six_field_form_of_a_reading_of_the_arterial_series(self) -> None:
        text = " ".join((batch.__doc__ or "").split())
        self.assertIn("[time_us, value, valid, sample_count, sum_high, sum_low]", text)
        self.assertIn(SERIES, text)
        self.assertNotIn(
            "a track to a list of ``[time_us, value, valid]``)", text.replace("``numeric`` (a track to a list of ``[time_us, value, valid]``)", "X") + ""
        )


class Entry(unittest.TestCase):
    def setUp(self) -> None:
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name)

    def run_batch(self, windows: list[dict[str, Any]]) -> tuple[int, Path]:
        source, target = self.root / "windows.json", self.root / "rows.json"
        source.write_text(json.dumps({"windows": windows}))
        return batch.main([str(source), str(target)]), target

    def test_the_file_of_windows_gives_the_file_of_rows_with_the_known_values(self) -> None:
        code, target = self.run_batch([window_document("w1")])
        self.assertEqual(0, code)
        (row,) = json.loads(target.read_text())["rows"]
        self.assertEqual("w1", row["window_id"])
        self.assertEqual(CUTOFF_US, row["cutoff_us"])
        features = row["features"]
        self.assertEqual(899, features["solar8000_art_mbp__valid_count"])
        self.assertAlmostEqual(899 / 900, features["silver_art_mbp_1s__coverage"], places=12)
        self.assertEqual(20, features["snuadc_art__valid_seconds"])
        self.assertAlmostEqual(40.0, features["snuadc_art__pulse_pressure_mean"], places=9)
        self.assertEqual(63, features["age"])
        self.assertIsNone(features["solar8000_hr__mean"])
        self.assertEqual(0, features["solar8000_hr__valid_count"])
        self.assertIsNone(features["asa"])
        self.assertNotIn("approach", features)

    def test_the_batch_and_a_direct_call_give_the_same_bytes_whatever_the_number_types(self) -> None:
        # The JSON holds whole numbers where the direct call holds floats, and the other way round.
        direct = WindowInput(
            CUTOFF_US,
            {HEART_RATE: [Reading(CUTOFF_US - 2 * SECOND, 60.0, True), Reading(CUTOFF_US, 62, True)]},
            {},
            {"age": ClinicalValue(73.0, 0), "preop_hb": ClinicalValue(12, 0)},
        )
        document = {
            "window_id": "w",
            "cutoff_us": CUTOFF_US,
            "numeric": {HEART_RATE: [[CUTOFF_US - 2 * SECOND, 60, True], [CUTOFF_US, 62.0, True]]},
            "waveforms": {},
            "clinical": {"age": {"value": 73, "available_us": 0}, "preop_hb": {"value": 12.0, "available_us": 0}},
        }
        code, target = self.run_batch([document])
        self.assertEqual(0, code)
        expected = batch.serialise([("w", build_row(direct))])
        self.assertEqual(expected, target.read_text())
        self.assertIn('"age": 73.0', expected)
        self.assertIn('"preop_hb": 12.0', expected)
        self.assertIn('"solar8000_hr__valid_count": 2,', expected)

    def test_the_parser_honours_the_valid_flags_of_readings_and_seconds(self) -> None:
        document = window_document("w")
        document["waveforms"]["SNUADC/ART"]["seconds"][4]["valid"] = False
        document["numeric"][HEART_RATE] = [[CUTOFF_US - 2 * SECOND, 60.0, False], [CUTOFF_US - SECOND, 70.0, True]]
        code, target = self.run_batch([document])
        self.assertEqual(0, code)
        features = json.loads(target.read_text())["rows"][0]["features"]
        self.assertEqual(19, features["snuadc_art__valid_seconds"])
        self.assertEqual(1, features["solar8000_hr__valid_count"])
        self.assertEqual(70.0, features["solar8000_hr__last"])

    def test_a_malformed_window_is_refused_with_a_message_and_no_output(self) -> None:
        bad_cases: dict[str, Any] = {}
        flag = window_document("w")
        flag["numeric"][HEART_RATE] = [[CUTOFF_US, 70.0, "false"]]
        bad_cases["valid flag"] = flag
        wave_flag = window_document("w")
        wave_flag["waveforms"]["SNUADC/ART"]["seconds"][4]["valid"] = "false"
        bad_cases["valid flag "] = wave_flag
        time = window_document("w")
        time["numeric"][HEART_RATE] = [[CUTOFF_US + 0.9, 70.0, True]]
        bad_cases["time"] = time
        cutoff = window_document("w")
        cutoff["cutoff_us"] = CUTOFF_US + 0.9
        bad_cases["cutoff"] = cutoff
        availability = window_document("w")
        availability["clinical"]["preop_hb"] = {"value": 1.0, "available_us": CUTOFF_US + 0.9}
        bad_cases["preop_hb"] = availability
        wrong_map = window_document("w")
        wrong_map["numeric"]["SNUADC/ART"] = []
        bad_cases["SNUADC/ART"] = wrong_map
        offset = window_document("w")
        offset["waveforms"]["SNUADC/ART"]["seconds"][19]["start_offset_us"] = 990_001
        bad_cases["whole second"] = offset
        for pattern, document in bad_cases.items():
            with self.subTest(pattern=pattern):
                stderr = io.StringIO()
                with contextlib.redirect_stderr(stderr):
                    code, target = self.run_batch([document])
                self.assertEqual(2, code)
                self.assertIn(pattern.strip(), stderr.getvalue())
                self.assertFalse(target.exists())

    def test_the_rows_keep_the_order_of_the_windows_and_data_after_the_cutoff_changes_nothing(self) -> None:
        code, target = self.run_batch([window_document("a"), window_document("b", future=True)])
        self.assertEqual(0, code)
        first, second = json.loads(target.read_text())["rows"]
        self.assertEqual(["a", "b"], [first["window_id"], second["window_id"]])
        self.assertEqual(first["features"], second["features"])
        self.assertLessEqual(second["latest_reading_us"], second["cutoff_us"])
        self.assertLessEqual(second["latest_clinical_us"], second["cutoff_us"])

    def test_a_second_run_writes_the_same_bytes(self) -> None:
        windows = [window_document("a"), window_document("b", future=True)]
        _, target = self.run_batch(windows)
        first = target.read_bytes()
        target.unlink()
        self.run_batch(windows)
        self.assertEqual(first, target.read_bytes())

    def test_the_batch_calls_the_one_function_the_stream_calls(self) -> None:
        window = WindowInput(CUTOFF_US, {ARTERIAL_MEAN: numeric_window(arithmetic(100.0, -0.05))}, {}, {})
        self.assertEqual([build_row(window)], batch.compute_rows([window]))
        self.assertIs(build_row, vars(batch)["build_row"])

    def test_a_row_that_breaks_the_leakage_check_is_not_written(self) -> None:
        bad = FeatureRow(CUTOFF_US, {"age": 63}, CUTOFF_US + 1, None)
        with mock.patch.object(batch, "build_row", return_value=bad), self.assertRaisesRegex(LeakageError, "latest reading"):
            batch.compute_rows([WindowInput(CUTOFF_US, {}, {}, {})])

    def run_text(self, text: str) -> tuple[int, str, Path]:
        """Run the batch entry point on the text of a windows file, with an earlier result already at the target."""
        source, target = self.root / "windows.json", self.root / "rows.json"
        source.write_text(text)
        target.write_text("OLD")
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            code = batch.main([str(source), str(target)])
        return code, stderr.getvalue(), target

    def test_a_shape_that_is_not_the_documented_one_is_refused_by_name_and_leaves_the_earlier_result(self) -> None:
        def edited(change: Callable[[dict[str, Any]], None]) -> str:
            document = window_document("w")
            change(document)
            return json.dumps({"windows": [document]})

        good = window_document("w")
        cases: dict[str, str] = {
            "windows": json.dumps({"windows": {}}),
            "windows ": json.dumps({"windows": []}),
            "windows  ": json.dumps({"windows": "x"}),
            "windows   ": json.dumps({}),
            "top level": json.dumps([good]),
            "extra": json.dumps({"windows": [good], "extra": 1}),
            "windows[0]": json.dumps({"windows": [[]]}),
            "mystery": edited(lambda d: d.update(mystery=1)),
            "cutoff_us": edited(lambda d: d.pop("cutoff_us")),
            "numeric": edited(lambda d: d.update(numeric=[])),
            f"numeric[{HEART_RATE}]": edited(lambda d: d["numeric"].update({HEART_RATE: {}})),
            f"numeric[{HEART_RATE}] ": edited(lambda d: d["numeric"].update({HEART_RATE: [{"t": 1}]})),
            f"numeric[{HEART_RATE}]  ": edited(lambda d: d["numeric"].update({HEART_RATE: [[1, 2]]})),
            "waveforms": edited(lambda d: d.update(waveforms=[])),
            "waveforms[SNUADC/ART]": edited(lambda d: d["waveforms"].update({"SNUADC/ART": []})),
            "seconds": edited(lambda d: d["waveforms"]["SNUADC/ART"].update(seconds={})),
            "seconds ": edited(lambda d: d["waveforms"]["SNUADC/ART"].pop("seconds")),
            "seconds[0]": edited(lambda d: d["waveforms"]["SNUADC/ART"].update(seconds=[[]])),
            "valid": edited(lambda d: d["waveforms"]["SNUADC/ART"]["seconds"][0].pop("valid")),
            "clinical": edited(lambda d: d.update(clinical=[])),
            "clinical[age]": edited(lambda d: d["clinical"].update(age=5)),
            "clinical[age] ": edited(lambda d: d["clinical"].update(age={"value": 1})),
        }
        for pattern, text in cases.items():
            with self.subTest(pattern=pattern):
                code, message, target = self.run_text(text)
                self.assertEqual(2, code)
                self.assertIn(pattern.strip(), message)
                self.assertEqual("OLD", target.read_text())

    def test_a_file_that_is_not_json_is_refused_and_leaves_the_earlier_result(self) -> None:
        code, message, target = self.run_text("not json")
        self.assertEqual(2, code)
        self.assertIn("features.batch", message)
        self.assertEqual("OLD", target.read_text())

    def test_a_number_too_large_for_a_float_is_a_named_error_and_not_a_traceback(self) -> None:
        document = window_document("w")
        document["numeric"][HEART_RATE] = [[CUTOFF_US, 10**400, True]]
        code, message, target = self.run_text(json.dumps({"windows": [document]}))
        self.assertEqual(2, code)
        self.assertIn(HEART_RATE, message)
        self.assertEqual("OLD", target.read_text())

    def test_a_row_that_breaks_the_leakage_check_never_reaches_the_file_through_the_entry_point(self) -> None:
        # A run that called build_row and wrote its rows without the check would write the file here, and this test would fail.
        document = json.dumps({"windows": [window_document("w")]})
        bad_rows = {
            "latest reading": FeatureRow(CUTOFF_US, {"age": 63.0}, CUTOFF_US + 1, None),
            "availability": FeatureRow(CUTOFF_US, {"age": 63.0}, None, CUTOFF_US + 1),
            "forbidden": FeatureRow(CUTOFF_US, {"icu_days": 1.0}, None, None),
            "unknown": FeatureRow(CUTOFF_US, {"mystery": 1.0}, None, None),
        }
        for pattern, bad in bad_rows.items():
            with self.subTest(pattern=pattern), mock.patch.object(batch, "build_row", return_value=bad):
                code, message, target = self.run_text(document)
                self.assertEqual(2, code)
                self.assertIn(pattern, message)
                self.assertEqual("OLD", target.read_text())

    def test_a_bad_window_stops_the_run_and_leaves_no_output(self) -> None:
        bad = window_document("bad")
        bad["numeric"] = {"Solar8000/NOT_A_TRACK": []}
        code, target = self.run_batch([window_document("good"), bad])
        self.assertNotEqual(0, code)
        self.assertFalse(target.exists())
        self.assertEqual([], [path.name for path in self.root.iterdir() if path.name not in ("windows.json", "rows.json")])


if __name__ == "__main__":
    unittest.main()
