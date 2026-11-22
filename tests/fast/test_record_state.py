"""The run's state file: what this run created, written whole and updated at each step, so a retry deletes only what the run itself made.

It names each object version the run wrote (read key and version ID), the load branch with the hash it was created from, and each job it started.
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.ingestion.record_state import RunState, StateError

RUN = "run_20261122_ab12cd34"
KEY = f"records_input/{RUN}/{'a' * 64}/clinical_data.csv"


class StateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="record_state_test_")))

    def path(self) -> Path:
        return self.folder / f"{RUN}.state.json"

    def test_a_run_with_no_file_has_an_empty_state_and_nothing_is_written_by_reading_it(self) -> None:
        state = RunState(self.folder, RUN)
        self.assertEqual((frozenset(), None, {}), (state.versions(KEY), state.branch, state.jobs))
        self.assertEqual(frozenset(), state.keys())
        self.assertFalse(self.path().exists())

    def test_each_recorded_version_branch_and_job_is_in_the_file_at_once_and_a_new_reader_sees_it(self) -> None:
        state = RunState(self.folder, RUN)
        state.add(KEY, "v1")
        self.assertEqual({"v1"}, set(json.loads(self.path().read_text())["copies"][KEY]))
        state.record_branch(f"load_{RUN}", "c" * 64)
        state.record_job("lab_data", 7)
        again = RunState(self.folder, RUN)
        self.assertEqual(({"v1"}, (f"load_{RUN}", "c" * 64), {"lab_data": 7}), (set(again.versions(KEY)), again.branch, again.jobs))
        self.assertEqual(frozenset({KEY}), again.keys())

    def test_a_discarded_version_and_a_forgotten_branch_leave_the_file(self) -> None:
        state = RunState(self.folder, RUN)
        state.add(KEY, "v1")
        state.add(KEY, "v2")
        state.discard(KEY, "v1")
        state.record_branch(f"load_{RUN}", "c" * 64)
        state.forget_branch()
        document = json.loads(self.path().read_text())
        self.assertEqual({KEY: ["v2"]}, document["copies"])
        self.assertIsNone(document["branch"])
        state.discard(KEY, "v2")
        self.assertEqual(frozenset(), RunState(self.folder, RUN).keys(), "a key with no version is not kept")

    def test_the_file_is_written_whole_or_not_at_all(self) -> None:
        state = RunState(self.folder, RUN)
        state.add(KEY, "v1")
        before = self.path().read_text()
        with mock.patch("scripts.ingestion.record_state.os.replace", side_effect=OSError("stopped")), self.assertRaises(OSError):
            state.add(KEY, "v2")
        self.assertEqual(before, self.path().read_text())
        self.assertEqual([self.path().name], sorted(path.name for path in self.folder.iterdir()), "no temporary file is left")

    def test_the_file_has_the_mode_of_a_record_and_the_run_s_own_name(self) -> None:
        RunState(self.folder, RUN).add(KEY, "v1")
        self.assertEqual(0o644, self.path().stat().st_mode & 0o777)

    def test_a_file_that_is_not_of_this_form_is_refused_whole_and_names_no_value(self) -> None:
        good: dict[str, Any] = {"format": "load_state_1", "run_id": RUN, "copies": {KEY: ["v1"]}, "branch": None, "jobs": {}}
        edits: list[tuple[str, Callable[[dict[str, Any]], object]]] = [
            ("another format", lambda document: document.update(format="load_state_2")),
            ("another run", lambda document: document.update(run_id="run_other")),
            ("a field added", lambda document: document.update(extra=1)),
            ("a field missing", lambda document: document.pop("jobs")),
            ("versions not a list", lambda document: document["copies"].update({KEY: "v1"})),
            ("a version that is not text", lambda document: document["copies"].update({KEY: [1]})),
            ("a branch of another form", lambda document: document.update(branch={"name": "x"})),
            ("a branch hash that is not a hash", lambda document: document.update(branch={"name": f"load_{RUN}", "created_from": "zz"})),
            ("a job that is not an integer", lambda document: document["jobs"].update(lab_data="7")),
            ("a key outside the read prefix", lambda document: document["copies"].update({"landing/x": ["v1"]})),
        ]
        for name, edit in edits:
            with self.subTest(name):
                document = json.loads(json.dumps(good))
                edit(document)
                self.path().write_text(json.dumps(document))
                with self.assertRaises(StateError) as raised:
                    RunState(self.folder, RUN)
                self.assertNotIn("v1", str(raised.exception))
        for text in ("", "[]", "{", "null"):
            with self.subTest(text):
                self.path().write_text(text)
                with self.assertRaises(StateError):
                    RunState(self.folder, RUN)

    def test_a_run_ID_that_is_not_a_plain_name_is_refused(self) -> None:
        for run in ("", "../x", "a/b", "Run", "x" * 65):
            with self.subTest(run), self.assertRaises(ValueError):
                RunState(self.folder, run)

    def test_a_branch_is_recorded_only_as_a_load_branch_with_a_hash(self) -> None:
        state = RunState(self.folder, RUN)
        for name, head in (("main", "c" * 64), ("", "c" * 64), (f"load_{RUN}", "C" * 64), (f"load_{RUN}", "short")):
            with self.subTest(name=name, head=head), self.assertRaises(StateError):
                state.record_branch(name, head)
        self.assertFalse(self.path().exists())


class FilesystemTest(unittest.TestCase):
    def test_the_state_folder_is_created_when_it_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch) / "deeper" / "records"
            RunState(folder, RUN).add(KEY, "v1")
            self.assertTrue((folder / f"{RUN}.state.json").is_file())
            self.assertEqual(0, len([name for name in os.listdir(folder) if name.startswith(".")]))


if __name__ == "__main__":
    unittest.main()
