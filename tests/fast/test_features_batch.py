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


def window_document(window_id: str, future: bool = False) -> dict[str, Any]:
    readings = [[reading.time_us, reading.value, reading.valid] for reading in numeric_window(arithmetic(100.0, -0.05), valid=lambda index: index != 10)]
    series = [[reading.time_us, reading.value, reading.valid] for reading in series_window(arithmetic(100.0, -0.05), valid=lambda index: index != 10)]
    if future:
        readings += [[CUTOFF_US + SECOND, 0.0, True]]
        series += [[CUTOFF_US, 0.0, True], [CUTOFF_US + SECOND, 0.0, True]]
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

    def test_a_bad_window_stops_the_run_and_leaves_no_output(self) -> None:
        bad = window_document("bad")
        bad["numeric"] = {"Solar8000/NOT_A_TRACK": []}
        code, target = self.run_batch([window_document("good"), bad])
        self.assertNotEqual(0, code)
        self.assertFalse(target.exists())
        self.assertEqual([], [path.name for path in self.root.iterdir() if path.name not in ("windows.json", "rows.json")])


if __name__ == "__main__":
    unittest.main()
