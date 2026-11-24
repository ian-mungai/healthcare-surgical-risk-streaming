"""The shared pieces of the header-secret cases, held so that the cases that use them cannot pass with nothing to check."""

from __future__ import annotations

import unittest

from tests.fast.header_secret_support import everything_said_about, unsafe_values


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
                self.assertTrue(value.startswith("marker"))
                self.assertGreater(len(value), len("marker"))


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
