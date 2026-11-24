"""The traceback helper of the tests hands a check the whole text of an error with its frames, and fails when the frames are missing.

Failure modes covered:

- ``assertRaises`` clears the traceback of the exception it keeps, so a check that formats it afterwards reads the message and the cause chain only.
- A call that raises nothing, or an error of another type, must not pass as the expected error.
- A text without the traceback header, or without a frame that the check named, must fail the check that reads it.
"""

from __future__ import annotations

import traceback
import unittest

from tests.fast.traceback_support import HEADER, raised_text, traceback_text


def refuse(value: str) -> None:
    raise ValueError(f"refused {value}")


def refuse_with_cause() -> None:
    try:
        refuse("inner")
    except ValueError as error:
        raise KeyError("outer") from error


class AssertRaisesClearsTheTracebackTest(unittest.TestCase):
    def test_control_an_error_kept_by_assert_raises_has_no_traceback_text(self) -> None:
        with self.assertRaises(ValueError) as kept:
            refuse("x")
        self.assertIsNone(kept.exception.__traceback__)
        self.assertNotIn(HEADER, "".join(traceback.format_exception(kept.exception)))


class RaisedTextTest(unittest.TestCase):
    def test_the_text_is_formatted_inside_the_handler_and_names_the_raising_function(self) -> None:
        error, text = raised_text(lambda: refuse("x"), ValueError, "refuse")
        self.assertEqual("refused x", str(error))
        self.assertIn(HEADER, text)
        self.assertIn(", in refuse\n", text)
        self.assertTrue(text.rstrip().endswith("ValueError: refused x"))

    def test_the_text_holds_the_whole_chain(self) -> None:
        _, text = raised_text(refuse_with_cause, KeyError, "refuse_with_cause", "refuse")
        self.assertIn("ValueError: refused inner", text)
        self.assertIn("KeyError: 'outer'", text)

    def test_a_call_that_raises_nothing_fails(self) -> None:
        with self.assertRaisesRegex(AssertionError, "raised no ValueError"):
            raised_text(lambda: None, ValueError)

    def test_an_error_of_another_type_goes_through_and_is_not_taken_for_the_expected_one(self) -> None:
        with self.assertRaises(ValueError):
            raised_text(lambda: refuse("x"), KeyError)

    def test_a_frame_that_the_check_names_but_the_traceback_lacks_fails(self) -> None:
        with self.assertRaisesRegex(AssertionError, "no_such_function"):
            raised_text(lambda: refuse("x"), ValueError, "no_such_function")

    def test_a_frame_name_is_a_whole_name_and_not_a_part_of_one(self) -> None:
        with self.assertRaisesRegex(AssertionError, "refus"):
            raised_text(lambda: refuse("x"), ValueError, "refus")


class TracebackTextTest(unittest.TestCase):
    def test_an_error_whose_traceback_was_cleared_fails_even_with_no_frame_named(self) -> None:
        with self.assertRaises(ValueError) as kept:
            refuse("x")
        with self.assertRaisesRegex(AssertionError, "no traceback"):
            traceback_text(kept.exception)

    def test_an_error_that_was_never_raised_has_no_traceback_and_fails(self) -> None:
        with self.assertRaisesRegex(AssertionError, "no traceback"):
            traceback_text(ValueError("never raised"))

    def test_an_error_caught_by_a_handler_that_kept_it_has_its_traceback(self) -> None:
        try:
            refuse("x")
        except ValueError as error:
            kept = error
        self.assertIn(", in refuse\n", traceback_text(kept, "refuse"))

    def test_a_named_frame_that_is_missing_fails(self) -> None:
        try:
            refuse("x")
        except ValueError as error:
            kept = error
        with self.assertRaisesRegex(AssertionError, "other_function"):
            traceback_text(kept, "other_function")


if __name__ == "__main__":
    unittest.main()
