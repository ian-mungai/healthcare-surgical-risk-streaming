"""A command never writes an output over a path that exists, in any form.

``write_new_file`` refuses an output path that is a file, a link (one that leads nowhere too), a folder or a name that differs from an existing one only in
case, and a path whose folder does not exist. It writes a private temporary file in the folder of the output, then puts it in place with ``os.link``, which
fails when the path has appeared since: nothing is replaced, and no temporary file stays. Every command that writes an output named on its command line
goes through it, or through ``check_new_output`` for an output that it appends to as it runs.
"""

from __future__ import annotations

import contextlib
import importlib
import io
import json
import os
import stat
import sys
import tempfile
import types
import unittest
from collections.abc import Callable
from pathlib import Path
from unittest import mock

from features import batch as features_batch
from scripts.ingestion import cli, copy_cost, copy_probe, staging_peak
from scripts.ingestion.closed import ClosedInputError
from scripts.ingestion.notice import Code, Notice, render
from scripts.ingestion.output_file import check_new_output, write_new_file
from scripts.sample import candidates

ORIGINAL = "original text\n"
EXISTS = render(Notice(Code.OUTPUT_EXISTS))
NO_FOLDER = render(Notice(Code.OUTPUT_FOLDER))
FORMS = ("file", "link", "dangling link", "folder", "link to a folder", "case variant")


class Fixture(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def made(self, name: str) -> Path:
        path = self.root / name
        path.write_text(ORIGINAL)
        return path

    def run_main(self, main: object, argv: list[str]) -> tuple[int, str]:
        err = io.StringIO()
        with contextlib.redirect_stderr(err), contextlib.redirect_stdout(io.StringIO()):
            code = main(argv)  # type: ignore[operator]
        return code, err.getvalue()

    def existing(self, form: str) -> tuple[Path, Callable[[], None]]:
        """An output path that something already holds, in the given form, and a check that the holder is as it was."""
        folder = self.root / "forms"
        folder.mkdir(exist_ok=True)
        target = folder / "target.txt"
        target.write_text(ORIGINAL)
        place = folder / "holder"
        if form == "file":
            place.write_text(ORIGINAL)
            return place, lambda: self.assertEqual(ORIGINAL, place.read_text())
        if form == "link":
            place.symlink_to(target)

            def link_kept() -> None:
                self.assertTrue(place.is_symlink())
                self.assertEqual(ORIGINAL, target.read_text())

            return place, link_kept
        if form == "dangling link":
            place.symlink_to(folder / "nowhere")

            def dangling_kept() -> None:
                self.assertTrue(place.is_symlink())
                self.assertFalse((folder / "nowhere").exists())

            return place, dangling_kept
        if form == "folder":
            place.mkdir()

            def folder_kept() -> None:
                self.assertTrue(place.is_dir())
                self.assertEqual([], list(place.iterdir()))

            return place, folder_kept
        if form == "link to a folder":
            (folder / "kept").mkdir()
            place.symlink_to(folder / "kept")

            def folder_link_kept() -> None:
                self.assertTrue(place.is_symlink())
                self.assertEqual([], list((folder / "kept").iterdir()))

            return place, folder_link_kept
        held = folder / "Holder"
        held.write_text(ORIGINAL)
        return place, lambda: self.assertEqual(ORIGINAL, held.read_text())

    def assert_nothing_left(self) -> None:
        left = [path for path in self.root.rglob("*") if path.name.endswith(".part")]
        self.assertEqual([], left)


class WriteNewFileTests(Fixture):
    def test_a_new_path_is_written_for_its_owner_only_and_leaves_no_temporary_file(self) -> None:
        path = self.root / "new.json"
        write_new_file(path, "text\n")
        self.assertEqual("text\n", path.read_text())
        self.assertEqual(0o600, stat.S_IMODE(path.stat().st_mode))
        self.assert_nothing_left()
        other = self.root / "bytes.bin"
        write_new_file(other, b"\x00\x01", mode=0o640)
        self.assertEqual(b"\x00\x01", other.read_bytes())

    def test_a_path_that_something_holds_in_any_form_is_refused_and_the_holder_is_left_as_it_was(self) -> None:
        for form in FORMS:
            with self.subTest(form=form):
                path, unchanged = self.existing(form)
                with self.assertRaises(ClosedInputError) as caught:
                    write_new_file(path, "new\n")
                self.assertEqual(EXISTS, str(caught.exception))
                unchanged()
                self.assert_nothing_left()
                shutil_rmtree(self.root / "forms")

    def test_a_case_variant_is_refused_on_a_folder_that_tells_the_two_names_apart(self) -> None:
        (self.root / "Result.json").write_text(ORIGINAL)
        with mock.patch("scripts.ingestion.output_file.os.lstat", side_effect=FileNotFoundError), self.assertRaises(ClosedInputError) as caught:
            check_new_output(self.root / "result.json")
        self.assertEqual(EXISTS, str(caught.exception))

    def test_a_folder_that_does_not_exist_is_refused_and_not_made(self) -> None:
        for path in (self.root / "absent" / "new.json", self.root / "new" / ".." / "alias.json"):
            with self.subTest(path=str(path)), self.assertRaises(ClosedInputError) as caught:
                write_new_file(path, "new\n")
            self.assertEqual(NO_FOLDER, str(caught.exception))
        self.assertFalse((self.root / "absent").exists() or (self.root / "new").exists())

    def test_a_file_in_place_of_the_folder_is_refused(self) -> None:
        self.made("file")
        with self.assertRaises(ClosedInputError) as caught:
            write_new_file(self.root / "file" / "new.json", "new\n")
        self.assertEqual(NO_FOLDER, str(caught.exception))

    def test_a_path_that_appears_after_the_check_is_not_replaced(self) -> None:
        path = self.root / "late.json"
        path.write_text(ORIGINAL)
        with mock.patch("scripts.ingestion.output_file.check_new_output"), self.assertRaises(ClosedInputError) as caught:
            write_new_file(path, "new\n")
        self.assertEqual(EXISTS, str(caught.exception))
        self.assertEqual(ORIGINAL, path.read_text())
        self.assert_nothing_left()

    def test_the_check_alone_refuses_a_path_that_something_holds_in_any_form(self) -> None:
        for form in FORMS:
            with self.subTest(form=form):
                path, unchanged = self.existing(form)
                with self.assertRaises(ClosedInputError) as caught:
                    check_new_output(path)
                self.assertEqual(EXISTS, str(caught.exception))
                unchanged()
                shutil_rmtree(self.root / "forms")

    def test_the_mode_that_is_asked_for_is_the_mode_of_the_file_whatever_the_mask_of_the_process(self) -> None:
        path = self.root / "readable.json"
        previous = os.umask(0o077)
        try:
            write_new_file(path, "text\n", mode=0o644)
        finally:
            os.umask(previous)
        self.assertEqual(0o644, stat.S_IMODE(path.stat().st_mode))

    def test_the_check_alone_creates_nothing(self) -> None:
        check_new_output(self.root / "new.json")
        self.assertEqual([], list(self.root.iterdir()))


def shutil_rmtree(path: Path) -> None:
    import shutil

    shutil.rmtree(path)


class CommandTests(Fixture):
    def test_every_command_refuses_an_output_that_something_holds_in_any_form(self) -> None:
        inputs = {name: self.made(f"in_{name}") for name in ("selection", "checksum_list", "pairings", "result", "reference", "samples", "release")}
        commands: dict[str, Callable[[Path], tuple[int, str]]] = {
            "check": lambda out: self.run_main(
                cli.main,
                [
                    "check",
                    "--selection",
                    str(inputs["selection"]),
                    "--checksum-list",
                    str(inputs["checksum_list"]),
                    "--pairings",
                    str(inputs["pairings"]),
                    "--result",
                    str(out),
                ],
            ),
            "pairings": lambda out: self.run_main(cli.main, ["pairings", "--pairings", str(out)]),
            "publish": lambda out: self.run_main(
                cli.main, ["publish", "--selection", str(inputs["selection"]), "--result", str(inputs["result"]), "--reference", str(out)]
            ),
            "copy cost": lambda out: self.run_main(
                copy_cost.main, ["--result", str(inputs["result"]), "--region", "us-west-2", "--since", "2026-01-01T00:00:00Z", "--output", str(out)]
            ),
            "copy probe sample": lambda out: self.run_main(copy_probe.main, ["sample", "--reference", str(inputs["reference"]), "--output", str(out)]),
            "staging peak": lambda out: self.run_main(staging_peak.main, ["peak", "--samples", str(inputs["samples"]), "--output", str(out)]),
            "candidate build": lambda out: self.run_main(
                lambda argv: candidates.main(argv), ["build", "--release", str(inputs["release"]), "--output", str(out)]
            ),
            "features batch": lambda out: self.run_main(features_batch.main, [str(inputs["samples"]), str(out)]),
        }
        for name, command in commands.items():
            for form in FORMS:
                with self.subTest(command=name, form=form):
                    path, unchanged = self.existing(form)
                    code, err = command(path)
                    self.assertNotEqual(0, code)
                    self.assertIn(EXISTS, err)
                    unchanged()
                    shutil_rmtree(self.root / "forms")
            with self.subTest(command=name, form="missing folder"):
                code, err = command(self.root / "absent" / "out.json")
                self.assertNotEqual(0, code)
                self.assertIn(NO_FOLDER, err)
                self.assertFalse((self.root / "absent").exists())
        self.assert_nothing_left()

    def test_the_staging_sampler_refuses_an_output_that_something_holds_in_any_form(self) -> None:
        for form in FORMS:
            with self.subTest(form=form):
                path, unchanged = self.existing(form)
                code, err = self.run_main(staging_peak.main, ["sample", "--output", str(path), "--node", "n1", "--", "sh"])
                self.assertEqual(1, code)
                unchanged()
                shutil_rmtree(self.root / "forms")

    def test_the_replay_commands_refuse_an_output_that_something_holds_in_any_form(self) -> None:
        for command in ("manifest", "observe"):
            for form in FORMS:
                with self.subTest(command=command, form=form):
                    path, unchanged = self.existing(form)
                    self.replay_files(command)
                    code, err = self.run_replay([command, "--run-record", str(self.root / "run.json"), "--out", str(path)])
                    self.assertEqual(2, code)
                    self.assertIn(EXISTS, err)
                    unchanged()
                    shutil_rmtree(self.root / "forms")

    def test_the_replay_commands_refuse_an_output_in_the_object_store_in_the_three_forms_that_replaced_a_stored_version(self) -> None:
        store = self.root / "store" / "cases"
        store.mkdir(parents=True)
        stored = store / "v0001"
        stored.write_text(ORIGINAL)
        (store / "link_to_v0001").symlink_to(stored)
        cases = {"the stored file": stored, "a case variant of it": store / "V0001", "a link to it": store / "link_to_v0001"}
        for command in ("manifest", "observe"):
            self.replay_files(command)
            for label, out in cases.items():
                with self.subTest(command=command, out=label):
                    code, err = self.run_replay([command, "--run-record", str(self.root / "run.json"), "--out", str(out)])
                    self.assertEqual(2, code)
                    self.assertIn(EXISTS, err)
                    self.assertEqual(ORIGINAL, stored.read_text())
        self.assertEqual(["link_to_v0001", "v0001"], sorted(path.name for path in store.iterdir()))

    def replay_files(self, command: str) -> None:
        """Write a run record that names the folder store of this test and the files that a replay run keeps."""
        (self.root / "state").mkdir(exist_ok=True)
        document = {
            "run_id": f"r_{command}",
            "rooms": 1,
            "speed": 1,
            "stagger_seconds": 0,
            "turnover_seconds": 0,
            "window_seconds": 60,
            "manifest_reference": str(self.root / "reference.json"),
            "object_store": {"kind": "folder", "root": str(self.root / "store")},
            "kafka_bootstrap": "kafka:9092",
            "registry_url": "http://registry:8081",
            "state_dir": str(self.root / "state"),
        }
        (self.root / "run.json").write_text(json.dumps(document))

    def run_replay(self, argv: list[str]) -> tuple[int, str]:
        """Run the replay program with the packages that only its image holds replaced by stand-ins, so its own command line and guard run."""
        stubs: dict[str, object] = {}
        while True:
            with mock.patch.dict(sys.modules, stubs):
                for name in [name for name in sys.modules if name.startswith("replay.service")]:
                    del sys.modules[name]
                try:
                    program = importlib.import_module("replay.service.__main__")
                except ModuleNotFoundError as error:
                    if error.name is None or error.name in stubs:
                        raise
                    stubs[error.name] = mock.MagicMock()
                    continue
                with mock.patch.object(sys, "stdin", types.SimpleNamespace(buffer=io.BytesIO(b""))):
                    return self.run_main(program.main, argv)

    def test_a_replay_command_that_cannot_read_its_run_record_names_the_argument_and_not_the_path(self) -> None:
        missing = self.root / "secret_folder_marker" / "run.json"
        code, err = self.run_replay(["observe", "--run-record", str(missing), "--out", str(self.root / "out.json")])
        self.assertNotEqual(0, code)
        self.assertNotIn("secret_folder_marker", err)
        self.assertNotIn("run.json", err)

    def test_the_replay_compare_shows_no_path_when_it_cannot_read_a_file(self) -> None:
        self.replay_files("compare")
        missing = self.root / "secret_manifest_marker" / "manifest.json"
        code, err = self.run_replay(
            ["compare", "--run-record", str(self.root / "run.json"), "--manifest", str(missing), "--observed", str(self.root / "o.json")]
        )
        self.assertNotEqual(0, code)
        self.assertNotIn("secret_manifest_marker", err)
        self.assertIn("FileNotFoundError", err)

    def test_features_batch_names_no_path_when_it_cannot_read_a_file(self) -> None:
        code, err = self.run_main(features_batch.main, [str(self.root / "secret_name_marker.json"), str(self.root / "rows.json")])
        self.assertEqual(2, code)
        self.assertNotIn("secret_name_marker", err)


if __name__ == "__main__":
    unittest.main()
