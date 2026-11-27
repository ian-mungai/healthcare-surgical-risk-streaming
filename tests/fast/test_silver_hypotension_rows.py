"""The row functions that the Spark job passes to its Python worker, and the job's command line: thin adapters over the two definitions.

Added after the code, by the self-review of the slice, because a planted fault in each adapter was not caught by any other test.

Failure modes covered here:

- The worker's row function reads the row before as the second ``s - 1`` without asking whether it is, or treats a null verdict as valid.
- The event row function gives events for a case whose operation start or end is null, not finite or after its end, or reads the points in the wrong columns.
- The command line prints the name of another job on its codes, so a failed check of this job is named as the Silver signals job's, also for a bad subcommand.
- The result file names another job than the one that ran, so a refusal of this job reads as the Silver signals job's.
"""

from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from fractions import Fraction
from pathlib import Path

from features.signal_validity import FLAT, MISSING
from spark.silver.checks import SilverSignalsError
from spark.silver.cli import run
from spark.silver.hypotension_frames import events_row, facts_row, reading_row

NAN, INFINITY = float("nan"), float("inf")


def points(first: int, count: int, value: float = 60.0) -> list[dict[str, object]]:
    return [{"case_second": second, "valid": True, "sample_count": 4, "sum_high": 4 * value, "sum_low": 0.0} for second in range(first, first + count)]


class Facts(unittest.TestCase):
    def test_a_valid_second_gives_its_count_its_sum_and_the_error_of_the_sum_and_an_invalid_one_nothing(self) -> None:
        self.assertEqual((4, 200.0, 0.0), facts_row(True, None, [40.0, 50.0, 60.0, 50.0]))
        self.assertEqual((0, 0.0, 0.0), facts_row(False, FLAT, None))

    def test_the_two_floats_of_a_second_hold_its_exact_sum(self) -> None:
        samples = [100.0 + index * 1e-9 for index in range(500)]
        count, high, low = facts_row(True, None, samples)
        self.assertEqual((500, sum((Fraction(x) for x in samples), Fraction(0))), (count, Fraction(high) + Fraction(low)))

    def test_a_second_with_no_verdict_is_refused_and_so_is_a_valid_one_without_samples(self) -> None:
        for arguments in ((None, None, [1.0]), (True, None, None), (True, None, [1.0, None])):
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                facts_row(*arguments)


class Reading(unittest.TestCase):
    def test_the_row_before_is_used_only_when_it_is_the_second_before(self) -> None:
        self.assertEqual((True, None, 60.0, 8, 480.0, 0.0), reading_row(10, True, None, 4, 240.0, 0.0, 9, True, None, 4, 240.0, 0.0))
        self.assertEqual((False, MISSING, None, None, None, None), reading_row(10, True, None, 4, 240.0, 0.0, 8, True, None, 4, 240.0, 0.0))
        self.assertEqual((False, MISSING, None, None, None, None), reading_row(10, True, None, 4, 240.0, 0.0, None, None, None, None, None, None))

    def test_an_invalid_row_before_hands_on_its_cause_and_an_invalid_row_keeps_its_own(self) -> None:
        self.assertEqual((False, FLAT, None, None, None, None), reading_row(10, True, None, 4, 240.0, 0.0, 9, False, FLAT, 0, 0.0, 0.0))
        self.assertEqual((False, MISSING, None, None, None, None), reading_row(10, False, MISSING, 0, 0.0, 0.0, 9, False, FLAT, 0, 0.0, 0.0))

    def test_the_stored_sum_is_the_exact_sum_of_both_seconds_so_a_mean_that_rounds_up_to_65_is_still_below_it(self) -> None:
        low_second = 4 * 64.99999999999999
        valid, cause, value, count, high, low = reading_row(10, True, None, 4, low_second, 0.0, 9, True, None, 4, 260.0, 0.0)
        self.assertEqual((True, None, 65.0, 8), (valid, cause, value, count))
        self.assertLess((Fraction(high or 0.0) + Fraction(low or 0.0)) / (count or 1), 65)

    def test_the_error_of_the_sum_of_each_of_the_two_seconds_is_in_the_stored_sum(self) -> None:
        _, _, _, count, high, low = reading_row(10, True, None, 4, 240.0, 2.0**-48, 9, True, None, 4, 260.0, -3 * 2.0**-48)
        self.assertEqual(Fraction(500) - Fraction(2, 2**48), Fraction(high or 0.0) + Fraction(low or 0.0))
        self.assertEqual(8, count)

    def test_a_row_with_no_verdict_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            reading_row(10, None, None, 0, 0.0, 0.0, None, None, None, None, None, None)


class Events(unittest.TestCase):
    def test_the_events_of_a_case_come_with_their_times(self) -> None:
        self.assertEqual([(201, 202_000_000, 261_000_000, 260, 261_000_000, 60, "operation_end")], events_row(points(201, 60), 100.0, 261.0))

    def test_a_second_is_low_by_its_exact_mean_and_not_by_the_rounded_value(self) -> None:
        rows = [{"case_second": second, "valid": True, "sample_count": 8, "sum_high": 520.0, "sum_low": -7.1054273576010019e-15} for second in range(201, 261)]
        self.assertEqual(1, len(events_row(rows, 100.0, 261.0)))

    def test_a_case_without_usable_operation_times_has_no_event(self) -> None:
        for start, end in ((None, 500.0), (100.0, None), (NAN, 500.0), (100.0, INFINITY), (-INFINITY, 500.0), (500.0, 100.0)):
            with self.subTest(start=start, end=end):
                self.assertEqual([], events_row(points(201, 60), start, end))

    def test_a_case_with_no_points_has_no_event(self) -> None:
        self.assertEqual([], events_row(None, 100.0, 500.0))
        self.assertEqual([], events_row([], 100.0, 500.0))


class CommandLine(unittest.TestCase):
    def test_the_codes_carry_the_name_of_the_job_that_the_caller_gives(self) -> None:
        err = io.StringIO()
        with contextlib.redirect_stderr(err), self.assertRaises(SystemExit) as stop:
            run(["build"], lambda arguments: {}, "silver_hypotension")
        self.assertEqual(2, stop.exception.code)
        self.assertEqual("silver_hypotension: usage\n", err.getvalue())

    def test_a_failed_check_is_printed_with_the_name_of_the_job_that_the_caller_gives_and_is_in_the_result_file(self) -> None:
        def failing(arguments: object) -> dict[str, object]:
            raise SilverSignalsError("series_differs: failed")

        err = io.StringIO()
        with tempfile.TemporaryDirectory() as folder, contextlib.redirect_stderr(err):
            path = Path(folder) / "result.json"
            code = run(["verify", "--branch", "b", "--result", str(path)], failing, "silver_hypotension")
            document = json.loads(path.read_text())
        self.assertEqual((1, "silver_hypotension: series_differs: failed\n"), (code, err.getvalue()))
        self.assertEqual({"job": "silver_hypotension", "mode": "verify", "refusal": ["series_differs: failed"]}, document)

    def test_a_good_call_writes_the_result_to_the_file_and_prints_nothing(self) -> None:
        out, err = io.StringIO(), io.StringIO()
        with tempfile.TemporaryDirectory() as folder, contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            path = Path(folder) / "result.json"
            code = run(
                ["build", "--branch", "b", "--result", str(path)], lambda arguments: {"job": "silver_hypotension", "mode": arguments.mode}, "silver_hypotension"
            )
            document = json.loads(path.read_text())
        self.assertEqual((0, "", ""), (code, out.getvalue(), err.getvalue()))
        self.assertEqual({"job": "silver_hypotension", "mode": "build"}, document)


if __name__ == "__main__":
    unittest.main()
