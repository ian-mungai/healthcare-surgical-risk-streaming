"""The shared pieces of the header-secret cases, held so that the cases that use them cannot pass with nothing to check."""

from __future__ import annotations

import unittest

from tests.fast.header_secret_support import everything_said_about, raised_text, unsafe_values


class UnsafeValuesTest(unittest.TestCase):
    def test_the_values_hold_a_line_break_a_space_a_nul_and_a_non_ascii_character_after_the_marker(self) -> None:
        values = unsafe_values("marker")
        self.assertTrue({"line break", "carriage return", "space", "NUL", "non-ASCII"} <= set(values))
        self.assertIn("\n", values["line break"])
        self.assertIn(" ", values["space"])
        self.assertIn("\x00", values["NUL"])
        self.assertFalse(values["non-ASCII"].isascii())
        for name, value in values.items():
            with self.subTest(character=name):
                self.assertIn("marker", value)
                self.assertGreater(len(value), len("marker"))

    def test_the_values_hold_each_printable_character_outside_the_form_and_a_misplaced_equals_sign(self) -> None:
        """A rule widened to the printable characters, or to an ``=`` anywhere, must fail a case: these are its controls."""
        values = unsafe_values("marker")
        allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~+/=")
        for code in range(0x21, 0x7F):
            character = chr(code)
            if character not in allowed:
                with self.subTest(character=character):
                    self.assertTrue(any(character in value for value in values.values()))
        for bracket in "[]()<>{}":
            self.assertTrue(any(bracket in value for value in values.values()), bracket)
        self.assertIn("marker=tail", values.values())
        self.assertIn("=marker", values.values())
        for value in values.values():
            self.assertNotRegex(value, r"\A[A-Za-z0-9\-._~+/]+=*\Z")


class RaisedTextTest(unittest.TestCase):
    """``assertRaises`` clears the frames of the traceback before a test can read it, so the text is formatted inside the handler and must name the raiser."""

    def test_the_text_holds_the_traceback_and_names_the_function_that_raised(self) -> None:
        def refuse() -> None:
            raise ValueError("fixed")

        def caller() -> None:
            refuse()

        error, text = raised_text(caller, ValueError, "refuse")
        self.assertEqual("fixed", str(error))
        self.assertIn(", in refuse\n", text)
        self.assertIn(", in caller\n", text)

    def test_the_text_holds_the_cause_of_the_error(self) -> None:
        secret = "".join(["in-the", "-cause"])

        def fail() -> None:
            try:
                raise KeyError(secret)
            except KeyError as inner:
                raise RuntimeError("fixed") from inner

        _, text = raised_text(fail, RuntimeError, "fail")
        self.assertIn(secret, text)

    def test_a_traceback_that_does_not_name_the_expected_function_fails_the_case(self) -> None:
        def refuse() -> None:
            raise ValueError("fixed")

        with self.assertRaisesRegex(AssertionError, "names no frame of other_function"):
            raised_text(refuse, ValueError, "other_function")

    def test_a_call_that_raises_nothing_or_another_error_fails_the_case(self) -> None:
        with self.assertRaisesRegex(AssertionError, "raised nothing"):
            raised_text(lambda: None, ValueError, "anything")

        def other() -> None:
            raise KeyError("x")

        with self.assertRaises(KeyError):
            raised_text(other, ValueError, "anything")


class EverythingSaidTest(unittest.TestCase):
    def test_the_text_holds_the_message_the_representation_the_cause_the_context_and_the_traceback(self) -> None:
        secret = "".join(["in-the", "-cause"])  # not a literal, because a printed traceback shows the source lines of its frames

        def fail() -> None:
            try:
                raise ValueError(secret)
            except ValueError as error:
                raise RuntimeError("fixed") from error

        for name in ("cause", "context", "arguments"):
            with self.subTest(place=name):
                caught: BaseException
                try:
                    fail()
                except RuntimeError as error:
                    caught = error
                if name == "context":
                    caught.__cause__ = None
                if name == "arguments":
                    caught = UnicodeDecodeError("utf-8", secret.encode() + b"\xff", 12, 13, "invalid start byte")
                self.assertIn(secret, everything_said_about(caught))
        self.assertNotIn(secret, everything_said_about(RuntimeError("fixed")))


if __name__ == "__main__":
    unittest.main()
