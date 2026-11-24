"""The row-count comparisons of the Silver signal and hypotension checks run in Great Expectations, and the facts hold one flag for each. No Spark runs here.

The Bronze signal tables keep their row counts, but a count that reaches a Python comparison can reach a message or a result. The comparison belongs to the
expectation (``spark/bronze/quality.py``), and the checks read the flag that it gives.

Failure modes covered here:

- A check compares a count of rows with a count of Bronze in Python, so the count has to be in the facts. The input-empty check of both jobs is one of them: it
  compares a count with zero in Python.
- The numeric ledger is added to the Bronze rows instead of taken from them, so a table that dropped duplicates is judged against too many rows (or too few).
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
from spark.silver.hypotension_checks import InputFacts

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


class AtLeastExpectation(unittest.TestCase):
    """``row_count_at_least`` asks Great Expectations whether a table has at least the given number of rows, and returns a flag and no count."""

    def ask(self, success: bool, minimum: int = 1) -> tuple[bool, Any]:
        batch = mock.Mock()
        batch.validate.return_value = mock.Mock(success=success)
        asset = mock.Mock()
        asset.add_batch_definition_whole_dataframe.return_value.get_batch.return_value = batch
        context = mock.Mock()
        context.data_sources.add_spark.return_value.add_dataframe_asset.return_value = asset
        library = mock.Mock()
        with mock.patch("spark.bronze.quality.importlib.import_module", return_value=library):
            found = quality.row_count_at_least("frame", minimum, context)
        return found, library

    def test_the_expectation_is_a_row_count_of_at_least_one_with_no_upper_bound(self) -> None:
        found, library = self.ask(True)
        self.assertIs(True, found)
        library.expectations.ExpectTableRowCountToBeBetween.assert_called_once_with(min_value=1)

    def test_a_failed_expectation_is_false(self) -> None:
        self.assertIs(False, self.ask(False)[0])

    def test_the_minimum_is_the_one_asked_for(self) -> None:
        _, library = self.ask(True, 5)
        library.expectations.ExpectTableRowCountToBeBetween.assert_called_once_with(min_value=5)


class Chain:
    """A stand-in for ``pyspark.sql.functions`` and its columns: every attribute, call, index, operator and comparison gives another stand-in."""

    def __getattr__(self, name: str) -> Chain:
        return Chain()

    def __call__(self, *arguments: Any, **keywords: Any) -> Chain:
        return Chain()

    def __getitem__(self, key: Any) -> Chain:
        return Chain()

    def __gt__(self, other: Any) -> Chain:
        return Chain()

    def __ne__(self, other: Any) -> Chain:  # type: ignore[override]
        return Chain()

    def __eq__(self, other: Any) -> Chain:  # type: ignore[override]
        return Chain()

    def __or__(self, other: Any) -> Chain:
        return Chain()

    def __and__(self, other: Any) -> Chain:
        return Chain()

    def __sub__(self, other: Any) -> Chain:
        return Chain()

    def __add__(self, other: Any) -> Chain:
        return Chain()

    def __invert__(self) -> Chain:
        return Chain()

    __hash__ = object.__hash__


class FrameFacts(unittest.TestCase):
    """The facts functions of the frames, run on stand-ins for Spark: the comparisons go to Great Expectations and the facts hold flags.

    The frames, the column functions and the expectation are stand-ins, so what is judged is the arithmetic and the wiring, not Spark's answers.
    """

    def frame(self, row: dict[str, Any]) -> mock.MagicMock:
        frame = mock.MagicMock(name="frame")
        for chained in (frame.withColumn.return_value, frame):
            chained.agg.return_value.first.return_value = row
        return frame

    def numeric(self, ledger: int | None, bronze_rows: int, every_reading: bool = True) -> tuple[signal_checks.NumericFacts, list[Any]]:
        row = {
            "duplicate_keys": 0,
            "ledger_total": ledger,
            "not_first": 0,
            "null": 0,
            "valid_with_cause": 0,
            "invalid_without_cause": 0,
            "unknown": 0,
            "differs": 0,
        }
        asked: list[Any] = []

        def row_count_is(frame: Any, expected: int) -> bool:
            asked.append(expected)
            return every_reading

        bronze = mock.MagicMock()
        bronze.count.return_value = bronze_rows
        with (
            mock.patch.object(signal_frames, "functions", return_value=Chain()),
            mock.patch.object(signal_frames, "numeric_udf"),
            mock.patch.object(signal_frames, "numeric_kept"),
            mock.patch.object(signal_frames, "source_differences", return_value=0),
            mock.patch.object(signal_frames, "row_count_is", row_count_is),
        ):
            return signal_frames.numeric_facts(self.frame(row), bronze), asked

    def test_the_stored_numeric_rows_are_the_bronze_rows_less_the_dropped_duplicates_of_the_ledger(self) -> None:
        for ledger, bronze_rows, expected in ((3, 10, 7), (0, 10, 10), (None, 10, 10), (9, 10, 1)):
            with self.subTest(ledger=ledger):
                self.assertEqual([expected], self.numeric(ledger, bronze_rows)[1])

    def test_the_numeric_flag_is_the_answer_of_the_expectation_turned_into_a_flag(self) -> None:
        self.assertEqual(0, self.numeric(2, 10, True)[0].row_count_differs)
        self.assertEqual(1, self.numeric(2, 10, False)[0].row_count_differs)

    def test_the_stored_waveform_rows_are_the_bronze_rows_and_the_flag_is_the_answer(self) -> None:
        row = {
            "duplicate_keys": 0,
            "shape": 0,
            "null": 0,
            "valid_with_cause": 0,
            "invalid_without_cause": 0,
            "unknown": 0,
            "differs": 0,
        }
        for answer in (True, False):
            asked: list[Any] = []

            def row_count_is(frame: Any, expected: int, asked: list[Any] = asked, answer: bool = answer) -> bool:
                asked.append(expected)
                return answer

            bronze = mock.MagicMock()
            bronze.count.return_value = 40
            with (
                mock.patch.object(signal_frames, "functions", return_value=Chain()),
                mock.patch.object(signal_frames, "judged", return_value=self.frame(row)),
                mock.patch.object(signal_frames, "waveform_physical"),
                mock.patch.object(signal_frames, "source_differences", return_value=0),
                mock.patch.object(signal_frames, "row_count_is", row_count_is),
            ):
                facts = signal_frames.waveform_facts(mock.MagicMock(), bronze, mock.MagicMock())
            self.assertEqual(([40], int(not answer)), (asked, facts.row_count_differs))

    def bronze_inputs(self, numeric_has: bool, waveform_has: bool) -> signal_checks.BronzeFacts:
        numeric, waveform, tracks = mock.MagicMock(name="numeric"), mock.MagicMock(name="waveform"), mock.MagicMock(name="tracks")

        def at_least_one(frame: Any, minimum: int) -> bool:
            self.assertEqual(1, minimum)
            return {id(numeric): numeric_has, id(waveform): waveform_has}[id(frame)]

        with mock.patch.object(signal_frames, "functions", return_value=Chain()), mock.patch.object(signal_frames, "row_count_at_least", at_least_one):
            return signal_frames.bronze_facts(numeric, waveform, tracks)

    def test_the_bronze_input_is_empty_only_when_neither_signal_table_has_a_row(self) -> None:
        self.assertEqual(
            {(True, True): 0, (True, False): 0, (False, True): 0, (False, False): 1},
            {(a, b): self.bronze_inputs(a, b).input_empty for a in (True, False) for b in (True, False)},
        )

    def test_the_hypotension_input_is_empty_when_the_arterial_seconds_have_no_row(self) -> None:
        for has_rows in (True, False):
            waveform, clinical, split = mock.MagicMock(), mock.MagicMock(), mock.MagicMock()
            art = mock.MagicMock()
            art.agg.return_value.first.return_value = {"rows": 9, "null_keys": 0, "distinct": 9, "no_verdict": 0}
            asked: list[Any] = []

            def at_least_one(frame: Any, minimum: int, art: Any = art, has_rows: bool = has_rows, asked: list[Any] = asked) -> bool:
                asked.append((frame is art, minimum))
                return has_rows

            with (
                mock.patch.object(hypotension_frames, "functions", return_value=Chain()),
                mock.patch.object(signal_frames, "functions", return_value=Chain()),
                mock.patch.object(hypotension_frames, "arterial", return_value=art),
                mock.patch.object(hypotension_frames, "operations"),
                mock.patch.object(hypotension_frames, "exists", return_value=0),
                mock.patch.object(hypotension_frames, "row_count_at_least", at_least_one),
            ):
                facts = hypotension_frames.input_facts(waveform, clinical, split)
            self.assertEqual(([(True, 1)], int(not has_rows)), (asked, facts.input_empty))


class InputEmpty(unittest.TestCase):
    """The checks read the flag and never compare a count with zero."""

    def test_the_flag_is_one_code_and_a_flag_in_both_jobs(self) -> None:
        bronze = signal_checks.BronzeFacts(1, 0, 0, 0, 0, 0, 0, 0)
        self.assertEqual(["input_empty: failed"], signal_checks.bronze_problems(bronze))
        self.assertEqual([], signal_checks.bronze_problems(dataclasses.replace(bronze, input_empty=0)))
        hypotension = InputFacts(input_empty=1, null_keys=0, duplicate_seconds=0, ambiguous_owner=0, cases_without_split=0, split_problems=())
        self.assertEqual(["input_empty: failed"], hypotension_checks.input_problems(hypotension))
        self.assertEqual([], hypotension_checks.input_problems(dataclasses.replace(hypotension, input_empty=0)))

    def test_the_facts_hold_no_count_of_input_rows_and_each_holds_the_flag(self) -> None:
        for cls in (signal_checks.BronzeFacts, InputFacts):
            names = {field.name for field in dataclasses.fields(cls)}
            self.assertEqual(set(), {"numeric_rows", "waveform_rows", "art_seconds"} & names, cls.__name__)
            self.assertIn("input_empty", names, cls.__name__)

    def test_no_check_compares_a_count_with_zero(self) -> None:
        for path in ("spark/silver/checks.py", "spark/silver/hypotension_checks.py"):
            self.assertNotRegex((ROOT / path).read_text(), r"rows\s*==\s*0|seconds\s*==\s*0", path)


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
