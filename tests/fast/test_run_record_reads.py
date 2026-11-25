"""The helpers that the end-to-end runners use to read a command's answer: a read that failed is a distinct failure and never an answer.

Failure modes covered here:

- A read that failed (non-zero exit, empty output, text that does not parse) is compared with another failed read and the two are equal, so a check that
  wants "unchanged" passes on two failures.
- A failed listing reads as an empty list, so a check that wants "none left" passes on a failure.
- A failed read is treated as a value (a string, an empty list) that a later check can match.
"""

from __future__ import annotations

import json
import unittest

from tests.e2e.run_record import Unread, listed_words, parsed, readable, unchanged


class ParsedRead(unittest.TestCase):
    def test_a_read_that_exited_0_with_text_is_its_stripped_text(self) -> None:
        self.assertEqual(parsed(0, " 2026-11-25T00:00:00Z\n"), "2026-11-25T00:00:00Z")

    def test_a_non_zero_exit_is_a_failure_whatever_it_printed(self) -> None:
        for code in (1, 255, -1):
            with self.subTest(code=code):
                self.assertIsInstance(parsed(code, "2026-11-25T00:00:00Z\n"), Unread)
                self.assertIsInstance(parsed(code, '{"a": 1}', json.loads), Unread)

    def test_empty_or_blank_output_is_a_failure(self) -> None:
        for text in ("", "\n", "  \n\t"):
            with self.subTest(text=text):
                self.assertIsInstance(parsed(0, text), Unread)

    def test_text_that_does_not_parse_is_a_failure_and_raises_nothing(self) -> None:
        self.assertIsInstance(parsed(0, "not json", json.loads), Unread)
        self.assertIsInstance(parsed(0, "x", int), Unread)

    def test_a_parser_gives_its_value(self) -> None:
        self.assertEqual(parsed(0, '{"a": 1}\n', json.loads), {"a": 1})


class Failures(unittest.TestCase):
    def test_a_failure_never_equals_another_failure_or_itself_or_anything_else(self) -> None:
        first, second = parsed(1, ""), parsed(1, "")
        self.assertNotEqual(first, second)
        self.assertNotEqual(first, first)
        self.assertNotEqual(first, "")
        self.assertNotEqual(first, "unreadable")
        self.assertNotEqual(first, None)
        self.assertNotEqual([first], [second])

    def test_a_failure_is_false_and_shows_its_reason_without_any_output(self) -> None:
        failure = parsed(255, "secret text")
        self.assertFalse(failure)
        self.assertEqual(str(failure), "unread (exit 255)")
        self.assertNotIn("secret", repr(failure))


class Unchanged(unittest.TestCase):
    def test_two_equal_reads_are_unchanged_and_two_different_reads_are_not(self) -> None:
        self.assertTrue(unchanged("a", "a"))
        self.assertFalse(unchanged("a", "b"))

    def test_a_failed_read_on_either_side_or_both_is_never_unchanged(self) -> None:
        good = parsed(0, "a")
        for before, after in ((parsed(1, ""), good), (good, parsed(1, "")), (parsed(1, ""), parsed(1, ""))):
            with self.subTest(before=before, after=after):
                self.assertFalse(unchanged(before, after))

    def test_a_failure_that_is_the_same_object_on_both_sides_is_not_unchanged(self) -> None:
        failure = parsed(1, "")
        self.assertFalse(unchanged(failure, failure))
        self.assertFalse(unchanged({"a": failure}, {"a": failure}))

    def test_a_table_of_reads_is_unchanged_only_when_every_read_succeeded_and_matches(self) -> None:
        self.assertTrue(unchanged({"a": "1", "b": "2"}, {"a": "1", "b": "2"}))
        self.assertFalse(unchanged({"a": "1", "b": "2"}, {"a": "1", "b": "3"}))
        self.assertFalse(unchanged({"a": "1", "b": parsed(1, "")}, {"a": "1", "b": parsed(1, "")}))
        self.assertFalse(unchanged({"a": "1"}, {"a": "1", "b": "2"}))
        self.assertFalse(unchanged({}, {}))

    def test_an_empty_text_is_not_a_read_that_can_be_unchanged(self) -> None:
        self.assertFalse(unchanged("", ""))
        self.assertFalse(unchanged(None, None))


class Readable(unittest.TestCase):
    def test_a_value_is_readable_unless_it_is_a_failure_or_holds_one(self) -> None:
        self.assertTrue(readable("a"))
        self.assertTrue(readable({"a": "1"}))
        self.assertFalse(readable(parsed(1, "")))
        self.assertFalse(readable({"a": "1", "b": parsed(1, "")}))
        self.assertFalse(readable(None))
        self.assertFalse(readable(""))

    def test_a_collection_is_readable_only_when_it_holds_a_read_and_none_that_failed(self) -> None:
        self.assertTrue(readable({"a"}))
        self.assertTrue(readable(["a", "b"]))
        self.assertFalse(readable(set()))
        self.assertFalse(readable([]))
        self.assertFalse(readable(["a", parsed(1, "")]))
        self.assertFalse(unchanged(set(), set()))
        self.assertTrue(unchanged({"a"}, {"a"}))
        self.assertFalse(unchanged({"a"}, {"b"}))


class ListedRead(unittest.TestCase):
    def test_a_listing_that_exited_0_is_its_words_and_none_printed_is_an_empty_list(self) -> None:
        self.assertEqual(listed_words(0, "a\nb c\n"), ["a", "b", "c"])
        self.assertEqual(listed_words(0, ""), [])

    def test_a_listing_that_failed_is_a_failure_and_not_an_empty_list(self) -> None:
        for code in (1, 255):
            with self.subTest(code=code):
                self.assertIsInstance(listed_words(code, ""), Unread)
                self.assertIsInstance(listed_words(code, "a\n"), Unread)


if __name__ == "__main__":
    unittest.main()
