"""The row-count comparisons of the Silver signal and hypotension checks run in Great Expectations, and the facts hold one flag for each. No Spark runs here.

The Bronze signal tables keep their row counts, but a count that reaches a Python comparison can reach a message or a result. The comparison belongs to the
expectation (``spark/bronze/quality.py``), and the checks read the flag that it gives.

Failure modes covered here:

- A check compares a count of rows with a count of Bronze in Python, so the count has to be in the facts.
- The facts function keeps the count, or turns the answer of the expectation the wrong way round.
- ``row_count_is`` widens the expected range, ignores the answer or returns a count.
"""

from __future__ import annotations

import dataclasses
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from spark.bronze import quality
from spark.silver import checks as signal_checks
from spark.silver import frames as signal_frames
from spark.silver import hypotension_checks, hypotension_frames

ROOT = Path(__file__).resolve().parents[2]


class RowCountExpectation(unittest.TestCase):
    """``row_count_is`` asks Great Expectations whether a table has exactly the expected rows, and the Silver facts hold the flag that it gives."""

    class Batch:
        def __init__(self, success: bool, asked: list[Any]) -> None:
            self.success, self.asked = success, asked

        def validate(self, expectation: Any, result_format: Any) -> Any:
            self.asked.append((expectation, result_format))
            return mock.Mock(success=self.success)

    def ask(self, success: bool, expected: int) -> tuple[bool, list[Any]]:
        asked: list[Any] = []
        batch = self.Batch(success, asked)
        asset = mock.Mock()
        asset.add_batch_definition_whole_dataframe.return_value.get_batch.return_value = batch
        source = mock.Mock()
        source.add_dataframe_asset.return_value = asset
        context = mock.Mock()
        context.data_sources.add_spark.return_value = source
        library = mock.Mock()
        with mock.patch("spark.bronze.quality.importlib.import_module", return_value=library):
            found = quality.row_count_is("frame", expected, context)
        return found, [library, *asked]

    def test_the_expectation_is_the_row_count_between_the_expected_number_and_itself(self) -> None:
        found, (library, (_, result_format)) = self.ask(True, 990)
        self.assertTrue(found)
        library.expectations.ExpectTableRowCountToBeBetween.assert_called_once_with(min_value=990, max_value=990)
        self.assertEqual(quality.RESULT_FORMAT, result_format)

    def test_a_failed_expectation_is_false_and_no_count_comes_back(self) -> None:
        found, _ = self.ask(False, 990)
        self.assertIs(False, found)

    def test_each_facts_function_turns_the_answer_into_one_flag_and_keeps_no_count(self) -> None:
        for path, function in (
            ("spark/silver/frames.py", "numeric_facts"),
            ("spark/silver/frames.py", "waveform_facts"),
            ("spark/silver/hypotension_frames.py", "series_facts"),
        ):
            text = (ROOT / path).read_text()
            body = text[text.index(f"def {function}(") :].split("\ndef ")[0]
            self.assertRegex(body, r"int\(not (every_reading|row_count_is\()", f"{path} {function}")
            self.assertNotIn('alias("rows")', body, f"{path} {function}")


class SignalRowCounts(unittest.TestCase):
    """The row-count comparisons of the Silver signal checks run in Great Expectations, and the checks read one flag."""

    def test_the_numeric_facts_hold_a_flag_and_no_row_count(self) -> None:
        names = {field.name for field in dataclasses.fields(signal_checks.NumericFacts)}
        self.assertNotIn("rows", names)
        self.assertNotIn("ledger_total", names)
        self.assertIn("row_count_differs", names)

    def test_the_waveform_series_facts_hold_a_flag_and_no_row_count(self) -> None:
        self.assertNotIn("rows", {field.name for field in dataclasses.fields(signal_checks.WaveformFacts)})
        self.assertIn("row_count_differs", {field.name for field in dataclasses.fields(signal_checks.WaveformFacts)})
        self.assertNotIn("rows", {field.name for field in dataclasses.fields(hypotension_checks.SeriesFacts)})
        self.assertIn("row_count_differs", {field.name for field in dataclasses.fields(hypotension_checks.SeriesFacts)})

    def test_a_row_count_flag_is_one_code_and_a_flag(self) -> None:
        numeric = dataclasses.replace(self.numeric(), row_count_differs=1)
        self.assertEqual(["numeric_not_every_reading: failed"], signal_checks.numeric_problems(numeric))
        waveform = signal_checks.WaveformFacts(0, 0, 0, 0, 0, 0, 0, 0, row_count_differs=1)
        self.assertEqual(["waveform_not_every_second: failed"], signal_checks.waveform_problems(waveform))
        series = hypotension_checks.SeriesFacts(0, 0, 0, 0, 0, 0, 0, row_count_differs=1)
        self.assertEqual(["series_not_every_second: failed"], hypotension_checks.series_problems(series))

    def numeric(self) -> signal_checks.NumericFacts:
        return signal_checks.NumericFacts(0, 0, 0, 0, 0, 0, 0, 0)

    def test_no_check_function_compares_a_count_of_rows_with_a_count_of_bronze(self) -> None:
        for path in ("spark/silver/checks.py", "spark/silver/hypotension_checks.py"):
            text = (ROOT / path).read_text()
            self.assertNotRegex(text, r"bronze_rows\s*!=|art_seconds\s*!=", path)

    def test_the_frames_ask_great_expectations_for_each_comparison(self) -> None:
        for path in ("spark/silver/frames.py", "spark/silver/hypotension_frames.py"):
            self.assertIn("row_count_is", (ROOT / path).read_text(), path)
        self.assertTrue(callable(quality.row_count_is))
        self.assertTrue(callable(signal_frames.functions))
        self.assertTrue(hasattr(hypotension_frames, "series_facts"))


if __name__ == "__main__":
    unittest.main()
