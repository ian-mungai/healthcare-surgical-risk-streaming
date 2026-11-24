"""The traceback helper of the tests hands a check the whole text of an error with its frames, and fails when the frames are missing.

Failure modes covered:

- ``assertRaises`` clears the traceback of the exception it keeps, so a check that formats it afterwards reads the message and the cause chain only.
- A call that raises nothing, or an error of another type, must not pass as the expected error.
- A text without the traceback header, or without a frame that the check named, must fail the check that reads it.
- An error whose own traceback was cleared must fail even when its cause still holds the named frames, because the check reads the error and not its cause.
- A function name that only a note of an exception shows (``add_note``) is text and not a frame: it must not count as a frame.
- A frame of an exception that the terminal does not show (a context that ``from None`` hides) must not count.
"""

from __future__ import annotations

import traceback
import unittest

from tests.fast.traceback_support import HEADER, raised_text, traceback_text


def refuse(value: str) -> None:
    raise ValueError(f"refused {value}")


def refuse_with_implicit_context() -> None:
    try:
        refuse("inner")
    except ValueError:
        raise KeyError("outer")  # noqa: B904


def refuse_with_hidden_context() -> None:
    try:
        refuse("inner")
    except ValueError:
        raise KeyError("outer") from None


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


class FramesAreReadFromTheStructureTest(unittest.TestCase):
    def test_an_error_with_a_cleared_traceback_fails_even_when_its_cause_holds_both_names(self) -> None:
        with self.assertRaises(KeyError) as kept:
            refuse_with_cause()
        self.assertIsNone(kept.exception.__traceback__)
        cause = kept.exception.__cause__
        if cause is None:
            self.fail("control: the error has a cause")
        self.assertIsNotNone(cause.__traceback__)
        with self.assertRaisesRegex(AssertionError, "no traceback"):
            traceback_text(kept.exception, "refuse_with_cause", "refuse")

    def test_a_function_name_that_only_a_note_gives_is_not_a_frame(self) -> None:
        try:
            refuse("x")
        except ValueError as error:
            error.add_note("  File x.py, line 1, in note_only_function")
            kept = error
        self.assertIn(", in note_only_function\n", "".join(traceback.format_exception(kept)))
        with self.assertRaisesRegex(AssertionError, "note_only_function"):
            traceback_text(kept, "note_only_function")

    def test_a_note_does_not_stop_a_real_frame_from_counting(self) -> None:
        try:
            refuse("x")
        except ValueError as error:
            error.add_note("a note")
            kept = error
        self.assertIn("a note", traceback_text(kept, "refuse"))

    def test_a_frame_of_the_cause_counts(self) -> None:
        try:
            refuse_with_cause()
        except KeyError as error:
            kept = error
        traceback_text(kept, "refuse_with_cause", "refuse")

    def test_a_frame_of_an_implicit_context_counts_because_the_terminal_shows_it(self) -> None:
        try:
            refuse_with_implicit_context()
        except KeyError as error:
            kept = error
        traceback_text(kept, "refuse")

    def test_a_frame_of_a_context_hidden_by_from_none_does_not_count(self) -> None:
        try:
            refuse_with_hidden_context()
        except KeyError as error:
            kept = error
        with self.assertRaisesRegex(AssertionError, "refuse"):
            traceback_text(kept, "refuse")
        self.assertNotIn(", in refuse\n", traceback_text(kept, "refuse_with_hidden_context"))

    def test_every_named_frame_is_required_and_not_only_the_first(self) -> None:
        with self.assertRaisesRegex(AssertionError, "no_such_function"):
            raised_text(refuse_with_cause, KeyError, "refuse", "no_such_function")
        with self.assertRaisesRegex(AssertionError, "no_such_function"):
            raised_text(refuse_with_cause, KeyError, "no_such_function", "refuse")

    def test_a_chain_that_loops_back_to_an_error_ends_and_counts_each_frame_once(self) -> None:
        try:
            refuse("x")
        except ValueError as first:
            try:
                refuse_with_cause()
            except KeyError as second:
                first.__cause__ = second
                second.__cause__ = first
                kept = second
        traceback_text(kept, "refuse", "refuse_with_cause")

    def test_the_text_still_holds_the_notes_for_the_secret_check(self) -> None:
        try:
            refuse("x")
        except ValueError as error:
            error.add_note("secret_in_a_note")
            kept = error
        self.assertIn("secret_in_a_note", traceback_text(kept, "refuse"))


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
