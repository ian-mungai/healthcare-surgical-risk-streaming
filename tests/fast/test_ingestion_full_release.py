"""The full release is one fixed set: the 6,390 keys of the pinned publisher list, with the pinned list digest taken from the tracked pin.

A selection named ``full_release`` is refused unless its files are exactly the two tables and the 6,388 case files and its source and checksum list are
the tracked pin's. The set cannot be shortened by a range, a missing table or another pin. A selection with another name keeps its own explicit files.
"""

from __future__ import annotations

import contextlib
import json
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.ingestion import selection
from scripts.ingestion.selection import Selection, SelectionError
from scripts.sample.candidates import RELEASE_FILE

PIN: dict[str, Any] = json.loads(Path(RELEASE_FILE).read_text())
FULL: dict[str, Any] = {
    "name": "full_release",
    "release": "1.0.0",
    "source_bucket": PIN["source"]["bucket"],
    "source_prefix": PIN["source"]["release_path"],
    "measurement_period": "2016-08 to 2017-06",
    "checksum_list": {"key": PIN["checksum_list"]["key"], "sha256": PIN["checksum_list"]["sha256"]},
    "files": ["clinical_data.csv", "lab_data.csv"],
    "case_files": {"pattern": "vital_files/{:04d}.vital", "first": 1, "last": 6388},
}


def load(change: Callable[[dict[str, Any]], None] | None = None) -> Selection:
    document = json.loads(json.dumps(FULL))
    if change:
        change(document)
    return Selection.from_json(json.dumps(document))


class FullReleaseTest(unittest.TestCase):
    def test_control_the_pinned_full_release_holds_the_6390_keys_of_the_list(self) -> None:
        selection = load()
        self.assertEqual(6390, len(selection.files))
        self.assertEqual(PIN["checksum_list"]["sha256"], selection.checksum_list_sha256)
        self.assertEqual({"clinical_data.csv", "lab_data.csv"}, {path for path in selection.files if "/" not in path})
        self.assertEqual({f"vital_files/{number:04d}.vital" for number in range(1, 6389)}, {path for path in selection.files if "/" in path})

    def test_a_shortened_reversed_or_widened_set_is_refused(self) -> None:
        changes: dict[str, Callable[[dict[str, Any]], None]] = {
            "reversed range": lambda document: document["case_files"].update(first=6388, last=1),
            "one case short": lambda document: document["case_files"].update(last=6387),
            "one case too many": lambda document: document["case_files"].update(last=6389),
            "first case dropped": lambda document: document["case_files"].update(first=2),
            "no case files": lambda document: document.pop("case_files"),
            "a table dropped": lambda document: document.update(files=["clinical_data.csv"]),
            "a table named twice": lambda document: document.update(files=["clinical_data.csv", "lab_data.csv", "lab_data.csv"]),
            "another file added": lambda document: document.update(files=["clinical_data.csv", "lab_data.csv", "SHA256SUMS.txt"]),
            "another pattern": lambda document: document["case_files"].update(pattern="vital_files/{:05d}.vital"),
        }
        for name, change in changes.items():
            with self.subTest(name), self.assertRaises(SelectionError):
                load(change)

    def test_the_list_digest_comes_from_the_tracked_pin_and_not_from_the_input(self) -> None:
        for name, change in {
            "another digest": lambda document: document["checksum_list"].update(sha256="0" * 64),
            "another list key": lambda document: document["checksum_list"].update(key="vitaldb/1.0.0/clinical_data.csv"),
            "another bucket": lambda document: document.update(source_bucket="another-bucket"),
            "another release path": lambda document: document.update(source_prefix="vitaldb/2.0.0/"),
            "another release": lambda document: document.update(release="2.0.0"),
        }.items():
            with self.subTest(name), self.assertRaises(SelectionError):
                load(change)

    def test_a_refusal_names_the_field_and_never_a_value(self) -> None:
        with self.assertRaises(SelectionError) as raised:
            load(lambda document: document["checksum_list"].update(sha256="f" * 64))
        self.assertIn("checksum_list", str(raised.exception))
        self.assertNotIn("f" * 64, str(raised.exception))

    def test_a_selection_with_another_name_keeps_its_own_files_and_pin(self) -> None:
        selection = load(
            lambda document: document.update(
                name="probe_set", files=["clinical_data.csv"], case_files={"pattern": "vital_files/{:04d}.vital", "first": 1, "last": 2}
            )
        )
        self.assertEqual(3, len(selection.files))

    def test_a_caller_that_edits_the_returned_pin_does_not_change_the_pin_the_full_release_is_held_to(self) -> None:
        self.addCleanup(selection.pinned_release.cache_clear)
        other = "0" * 64
        pin: Any = selection.pinned_release()
        edits: tuple[Callable[[], object], ...] = (
            lambda: pin["checksum_list"].update(sha256=other),
            lambda: pin.__setitem__("checksum_list", {"key": PIN["checksum_list"]["key"], "sha256": other}),
            lambda: setattr(pin, "checksum_list_sha256", other),
        )
        for edit in edits:
            with contextlib.suppress(TypeError, AttributeError):
                edit()
        with self.assertRaises(SelectionError):
            load(lambda document: document["checksum_list"].update(sha256=other))
        self.assertEqual(6390, len(load().files))

    def test_a_full_release_built_in_code_is_held_to_the_same_rule(self) -> None:
        with self.assertRaises(SelectionError):
            Selection(
                "full_release",
                "1.0.0",
                PIN["source"]["bucket"],
                PIN["source"]["release_path"],
                "p",
                PIN["checksum_list"]["key"],
                PIN["checksum_list"]["sha256"],
                ("lab_data.csv",),
            )


if __name__ == "__main__":
    unittest.main()
