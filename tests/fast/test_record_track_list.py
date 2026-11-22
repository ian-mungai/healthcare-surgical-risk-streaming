"""The pinned track list: the digest of its rows, the pin's form and the check of a downloaded file against the pin.

The digest is SHA-256 over the rows sorted by their full text, each row as its fields joined by a tab, the lines joined by a newline with none after the
last. The expected values below are written out by hand, so a change of the sort, the joiner or the line end is found by the planted cases.
"""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.ingestion import track_list
from scripts.ingestion.notice import Notice, render
from scripts.sample.candidates import RELEASE_FILE, release_problems

ROWS = [("2", "Solar8000/HR", "bb"), ("1", "SNUADC/ART", "aa"), ("1", "BIS/BIS", "cc")]
SORTED_TEXT = "1\tBIS/BIS\tcc\n1\tSNUADC/ART\taa\n2\tSolar8000/HR\tbb"


def texts(notices: list[Notice]) -> list[str]:
    return [render(notice) for notice in notices]


class DigestTest(unittest.TestCase):
    def test_the_digest_is_over_the_sorted_tab_joined_rows_joined_by_newlines(self) -> None:
        self.assertEqual((3, hashlib.sha256(SORTED_TEXT.encode()).hexdigest()), track_list.rows_digest(ROWS))

    def test_the_order_of_the_rows_does_not_change_it(self) -> None:
        self.assertEqual(track_list.rows_digest(ROWS), track_list.rows_digest(list(reversed(ROWS))))

    def test_a_changed_field_a_missing_row_a_repeated_row_and_an_added_row_each_change_it(self) -> None:
        wanted = track_list.rows_digest(ROWS)
        for name, rows in (
            ("a changed field", [("2", "Solar8000/HR", "bc"), *ROWS[1:]]),
            ("a missing row", ROWS[:-1]),
            ("a repeated row", [*ROWS, ROWS[0]]),
            ("an added row", [*ROWS, ("3", "x", "y")]),
        ):
            with self.subTest(name):
                self.assertNotEqual(wanted, track_list.rows_digest(rows))


class RefusedFieldTest(unittest.TestCase):
    def test_a_field_with_a_tab_or_a_line_break_is_refused_by_its_row_and_the_message_shows_no_value(self) -> None:
        for name, field in (("tab", "a\tb"), ("newline", "a\nb"), ("carriage return", "a\rb"), ("only a tab", "\t")):
            with self.subTest(name), self.assertRaises(track_list.TrackListRowError) as raised:
                track_list.rows_digest([("1", "x", "y"), ("2", field, "z")])
            self.assertIn("row 2", str(raised.exception))
            self.assertIn("tab or a line break", str(raised.exception))

    def test_it_is_a_value_error_and_a_clean_row_is_accepted(self) -> None:
        self.assertTrue(issubclass(track_list.TrackListRowError, ValueError))
        self.assertEqual(2, track_list.rows_digest([("1", "a b", "c,d"), ("2", "e", "f")])[0])

    def test_the_message_does_not_contain_the_field(self) -> None:
        with self.assertRaises(track_list.TrackListRowError) as raised:
            track_list.rows_digest([("1", "SECRETWORD\tx", "y")])
        self.assertNotIn("SECRETWORD", str(raised.exception))


class PinTest(unittest.TestCase):
    def test_the_tracked_pin_holds_the_row_count_and_the_digest_and_is_valid(self) -> None:
        pin = json.loads(RELEASE_FILE.read_text())
        self.assertEqual([], release_problems(pin))
        self.assertEqual(486449, pin["track_list"]["rows"])
        self.assertRegex(pin["track_list"]["rows_sha256"], r"[0-9a-f]{64}")
        pinned = track_list.pinned_track_list()
        self.assertEqual((486449, pin["track_list"]["rows_sha256"], "https://api.vitaldb.net/trks"), (pinned.rows, pinned.rows_sha256, pinned.url))

    def test_a_pin_without_the_rows_or_with_a_bad_value_is_found(self) -> None:
        changes: tuple[tuple[str, Callable[[dict[str, Any]], object]], ...] = (
            ("no rows", lambda pin: pin["track_list"].pop("rows")),
            ("no digest", lambda pin: pin["track_list"].pop("rows_sha256")),
            ("zero rows", lambda pin: pin["track_list"].update(rows=0)),
            ("negative rows", lambda pin: pin["track_list"].update(rows=-1)),
            ("short digest", lambda pin: pin["track_list"].update(rows_sha256="abc")),
            ("upper case digest", lambda pin: pin["track_list"].update(rows_sha256="A" * 64)),
            ("rows as text", lambda pin: pin["track_list"].update(rows="486449")),
        )
        for name, change in changes:
            with self.subTest(name):
                pin = json.loads(RELEASE_FILE.read_text())
                change(pin)
                self.assertTrue(any(text.startswith("track_list") for text in texts(release_problems(pin))))


class FileCheckTest(unittest.TestCase):
    def setUp(self) -> None:
        self.folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="track_list_test_")))

    def file_with(self, rows: list[tuple[str, str, str]], header: str = "caseid,tname,tid") -> Path:
        path = self.folder / f"track_list_{len(list(self.folder.iterdir()))}.csv"
        path.write_text(header + "\r\n" + "".join(",".join(row) + "\r\n" for row in rows), newline="")
        return path

    def pin(self, rows: list[tuple[str, str, str]] = ROWS) -> track_list.PinnedTrackList:
        count, digest = track_list.rows_digest(rows)
        return track_list.PinnedTrackList("https://api.vitaldb.net/trks", count, digest)

    def test_control_a_file_of_the_pinned_rows_in_any_order_passes(self) -> None:
        self.assertEqual([], track_list.file_problems(self.file_with(list(reversed(ROWS))), self.pin()))

    def test_a_dropped_row_a_changed_field_and_a_wrong_header_are_each_refused(self) -> None:
        for name, path in (
            ("dropped row", self.file_with(ROWS[:-1])),
            ("changed field", self.file_with([("2", "Solar8000/HR", "bc"), *ROWS[1:]])),
            ("wrong header", self.file_with(ROWS, header="caseid,tname,id")),
        ):
            with self.subTest(name):
                self.assertTrue(track_list.file_problems(path, self.pin()))

    def test_a_dropped_row_is_reported_with_both_counts(self) -> None:
        self.assertIn("the file has 2 rows, the pin 3", track_list.file_problems(self.file_with(ROWS[:-1]), self.pin()))

    def test_a_file_with_a_field_that_holds_a_line_break_or_a_tab_is_a_problem_naming_the_row_not_a_crash(self) -> None:
        path = self.folder / "quoted.csv"
        path.write_text('caseid,tname,tid\r\n1,a,b\r\n2,"x\ny",z\r\n3,"p\tq",r\r\n', newline="")
        problems = track_list.file_problems(path, self.pin())
        self.assertEqual(1, len(problems))
        self.assertIn("row 2", problems[0])

    def test_a_row_with_the_wrong_number_of_fields_is_refused_by_its_line(self) -> None:
        path = self.folder / "track_list.csv"
        path.write_text("caseid,tname,tid\r\n1,a\r\n")
        self.assertTrue(any("line 2" in problem for problem in track_list.file_problems(path, self.pin())))


if __name__ == "__main__":
    unittest.main()
