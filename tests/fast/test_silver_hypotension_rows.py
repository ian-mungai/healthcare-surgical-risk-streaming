"""The row functions that the Spark job passes to its Python worker, and the job's command line: thin adapters over the two definitions.

Added after the code, by the self-review of the slice, because a planted fault in each adapter was not caught by any other test.

Failure modes covered here:

- The worker's row function reads the row before as the second ``s - 1`` without asking whether it is, or treats a null verdict as valid.
- The event row function gives events for a case whose operation start or end is null, not finite or after its end, or reads the points in the wrong columns.
- The command line prints the name of another job on its codes, so a failed check of this job is named as the Silver signals job's, also for a bad subcommand.
"""

from __future__ import annotations

import contextlib
import io
import unittest

from features.signal_validity import FLAT, MISSING
from spark.silver.checks import SilverSignalsError
from spark.silver.cli import run
from spark.silver.hypotension_frames import events_row, facts_row, reading_row

NAN, INFINITY = float("nan"), float("inf")


def points(first: int, count: int, value: float = 60.0) -> list[dict[str, object]]:
    return [{"case_second": second, "value": value, "valid": True} for second in range(first, first + count)]


class Facts(unittest.TestCase):
    def test_a_valid_second_gives_its_count_and_sum_and_an_invalid_one_nothing(self) -> None:
        self.assertEqual((4, 200.0), facts_row(True, None, [40.0, 50.0, 60.0, 50.0]))
        self.assertEqual((0, 0.0), facts_row(False, FLAT, None))

    def test_a_second_with_no_verdict_is_refused_and_so_is_a_valid_one_without_samples(self) -> None:
        for arguments in ((None, None, [1.0]), (True, None, None), (True, None, [1.0, None])):
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                facts_row(*arguments)


class Reading(unittest.TestCase):
    def test_the_row_before_is_used_only_when_it_is_the_second_before(self) -> None:
        self.assertEqual((60.0, True, None), reading_row(10, True, None, 4, 240.0, 9, True, None, 4, 240.0))
        self.assertEqual((None, False, MISSING), reading_row(10, True, None, 4, 240.0, 8, True, None, 4, 240.0))
        self.assertEqual((None, False, MISSING), reading_row(10, True, None, 4, 240.0, None, None, None, None, None))

    def test_an_invalid_row_before_hands_on_its_cause_and_an_invalid_row_keeps_its_own(self) -> None:
        self.assertEqual((None, False, FLAT), reading_row(10, True, None, 4, 240.0, 9, False, FLAT, 0, 0.0))
        self.assertEqual((None, False, MISSING), reading_row(10, False, MISSING, 0, 0.0, 9, False, FLAT, 0, 0.0))

    def test_a_row_with_no_verdict_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            reading_row(10, None, None, 0, 0.0, None, None, None, None, None)


class Events(unittest.TestCase):
    def test_the_events_of_a_case_come_with_their_times(self) -> None:
        self.assertEqual([(201, 202_000_000, 261_000_000, 260, 261_000_000, 60, "operation_end")], events_row(points(201, 60), 100.0, 261.0))

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

    def test_a_failed_check_is_printed_with_the_name_of_the_job_that_the_caller_gives(self) -> None:
        def failing(arguments: object) -> dict[str, object]:
            raise SilverSignalsError("series_differs: 1 row")

        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            code = run(["verify", "--branch", "b"], failing, "silver_hypotension")
        self.assertEqual((1, "silver_hypotension: series_differs: 1 row\n"), (code, err.getvalue()))


if __name__ == "__main__":
    unittest.main()
