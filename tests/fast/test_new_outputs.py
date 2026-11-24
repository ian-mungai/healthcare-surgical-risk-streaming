"""A command never writes an output over a path that exists, in any form.

``write_new_file`` refuses an output path that is a file, a link (one that leads nowhere too), a folder or a name that differs from an existing one only in
case, and a path whose folder does not exist. It writes a private temporary file in the folder of the output, then puts it in place with ``os.link``, which
fails when the path has appeared since: nothing is replaced, and no temporary file stays. Every command that writes an output named on its command line
goes through it, or through ``check_new_output`` for an output that it appends to as it runs.

``publish_reserved`` is the same for a file that the caller reserved first: the reserved file is the only staging file. It receives the text, is linked to the
output (the link fails when the output exists, so nothing is replaced even by a late writer) and its name is removed on every path, so releasing the reserved
name removes every staged byte.
"""

from __future__ import annotations

import contextlib
import errno
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
from typing import Any
from unittest import mock

from features import batch as features_batch
from scripts.ingestion import cli, copy_cost, copy_probe, staging_peak
from scripts.ingestion.closed import ClosedInputError
from scripts.ingestion.notice import Code, Notice, Subject, render
from scripts.ingestion.output_file import check_new_output, publish_reserved, write_new_file
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

    def test_a_temporary_name_that_cannot_be_removed_after_the_link_is_refused_with_fixed_text(self) -> None:
        path = self.root / "new.json"
        real = os.unlink

        def unlink(target: Any, *arguments: Any, **keywords: Any) -> None:
            if str(target).endswith(".part"):
                raise OSError(errno.EIO, "MARKER_91 input output error")
            real(target, *arguments, **keywords)

        with mock.patch("os.unlink", unlink), self.assertRaises(ClosedInputError) as caught:
            write_new_file(path, "text\n")
        self.assertEqual(render(Notice(Code.CANNOT_WRITE, subject=Subject.OUTPUT)), str(caught.exception))
        self.assertNotIn("MARKER_91", str(caught.exception))
        self.assertIsNone(caught.exception.__cause__)
        self.assertEqual("text\n", path.read_text())

    def test_a_temporary_name_that_cannot_be_removed_after_a_refusal_keeps_the_first_refusal(self) -> None:
        path = self.root / "late.json"
        path.write_text(ORIGINAL)

        def unlink(target: Any, *arguments: Any, **keywords: Any) -> None:
            raise OSError(errno.EIO, "MARKER_91 input output error")

        with mock.patch("scripts.ingestion.output_file.check_new_output"), mock.patch("os.unlink", unlink), self.assertRaises(ClosedInputError) as caught:
            write_new_file(path, "new\n")
        self.assertEqual(EXISTS, str(caught.exception))
        self.assertEqual(ORIGINAL, path.read_text())

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


class PublishReservedTests(Fixture):
    """``publish_reserved``: the reserved file is the only staging file, so that releasing its name removes every staged byte."""

    def setUp(self) -> None:
        super().setUp()
        self.path = self.root / "result.json"
        self.reserved = self.root / "result.json.part"
        self.reserved.write_text("")

    def names(self) -> list[str]:
        return sorted(entry.name for entry in self.root.iterdir())

    def test_the_text_is_written_into_the_reserved_file_and_linked_and_no_other_file_is_made(self) -> None:
        seen: dict[str, Any] = {}
        real = os.link

        def link(source: str, target: str) -> None:
            seen["names"] = self.names()
            seen["source"] = Path(source).name
            seen["whole"] = Path(source).read_text()
            seen["target_before"] = os.path.lexists(target)
            real(source, target)

        with mock.patch("os.link", link):
            publish_reserved(self.reserved, self.path, "text\n", mode=0o644)
        self.assertEqual((["result.json.part"], "result.json.part", "text\n", False), (seen["names"], seen["source"], seen["whole"], seen["target_before"]))
        self.assertEqual("text\n", self.path.read_text())
        self.assertEqual(0o644, stat.S_IMODE(self.path.stat().st_mode))
        self.assertEqual(["result.json"], self.names())

    def test_bytes_are_written_as_they_are(self) -> None:
        publish_reserved(self.reserved, self.path, b"\x00\x01")
        self.assertEqual(b"\x00\x01", self.path.read_bytes())
        self.assertEqual(0o600, stat.S_IMODE(self.path.stat().st_mode))

    def test_the_text_is_flushed_to_disk_before_the_link(self) -> None:
        order: list[str] = []
        real_fsync, real_link = os.fsync, os.link

        def fsync(descriptor: int) -> None:
            order.append("fsync")
            real_fsync(descriptor)

        def link(source: str, target: str) -> None:
            order.append("link")
            real_link(source, target)

        with mock.patch("os.fsync", fsync), mock.patch("os.link", link):
            publish_reserved(self.reserved, self.path, "text\n")
        self.assertEqual(["fsync", "link"], order)

    def test_a_path_that_something_holds_in_any_form_is_refused_and_the_holder_is_left_as_it_was(self) -> None:
        self.reserved.unlink()
        for form in FORMS:
            with self.subTest(form=form):
                path, unchanged = self.existing(form)
                reserved = path.with_name(path.name + ".part")
                reserved.write_text("")
                with mock.patch("os.fsync") as flushed, self.assertRaises(ClosedInputError) as caught:
                    publish_reserved(reserved, path, "new\n")
                self.assertEqual(EXISTS, str(caught.exception))
                flushed.assert_not_called()
                unchanged()
                self.assertFalse(os.path.lexists(reserved))
                self.assert_nothing_left()
                shutil_rmtree(self.root / "forms")

    def test_a_path_that_appears_before_the_link_is_never_written_over(self) -> None:
        real = os.link

        def late(source: str, target: str) -> None:
            self.path.write_text(ORIGINAL)
            real(source, target)

        with mock.patch("os.link", late), self.assertRaises(ClosedInputError) as caught:
            publish_reserved(self.reserved, self.path, "new\n")
        self.assertEqual(EXISTS, str(caught.exception))
        self.assertEqual(ORIGINAL, self.path.read_text())
        self.assertEqual(["result.json"], self.names())

    def test_a_link_that_fails_leaves_nothing_and_shows_no_text_of_the_error(self) -> None:
        def refuse(source: str, target: str) -> None:
            raise OSError("MARKER_91 disk full")

        with mock.patch("os.link", refuse), self.assertRaises(ClosedInputError) as caught:
            publish_reserved(self.reserved, self.path, "new\n")
        self.assertEqual(render(Notice(Code.CANNOT_WRITE, subject=Subject.OUTPUT)), str(caught.exception))
        self.assertNotIn("MARKER_91", str(caught.exception))
        self.assertEqual([], self.names())

    def test_a_reserved_file_that_is_gone_or_is_not_a_plain_file_is_refused_and_nothing_else_is_touched(self) -> None:
        other = self.root / "other.txt"
        other.write_text(ORIGINAL)
        self.reserved.unlink()
        for form in ("missing", "link", "folder"):
            with self.subTest(form=form):
                if form == "link":
                    self.reserved.symlink_to(other)
                elif form == "folder":
                    self.reserved.mkdir()
                with self.assertRaises(ClosedInputError) as caught:
                    publish_reserved(self.reserved, self.path, "new\n")
                self.assertEqual(render(Notice(Code.CANNOT_WRITE, subject=Subject.OUTPUT)), str(caught.exception))
                self.assertEqual(ORIGINAL, other.read_text())
                self.assertFalse(self.path.exists())
                if form == "folder":
                    self.reserved.rmdir()
                else:
                    self.reserved.unlink(missing_ok=True)
        self.assertEqual(["other.txt"], self.names())

    def test_a_reserved_file_that_holds_old_bytes_is_cut_to_the_new_text(self) -> None:
        self.reserved.write_text("a much longer text than the new one\n")
        publish_reserved(self.reserved, self.path, "new\n")
        self.assertEqual("new\n", self.path.read_text())

    def test_a_reserved_file_that_has_another_name_is_refused_and_the_other_name_keeps_its_bytes(self) -> None:
        other = self.root / "other.txt"
        other.write_text(ORIGINAL)
        self.reserved.unlink()
        os.link(other, self.reserved)
        with self.assertRaises(ClosedInputError) as caught:
            publish_reserved(self.reserved, self.path, "new\n")
        self.assertEqual(render(Notice(Code.CANNOT_WRITE, subject=Subject.OUTPUT)), str(caught.exception))
        self.assertEqual(ORIGINAL, other.read_text())
        self.assertEqual(["other.txt"], self.names())

    def refuse_removal(self, error: OSError) -> Any:
        """A patch of ``os.unlink`` that fails for the reserved name only and leaves every other call as it is."""
        real = os.unlink

        def unlink(target: Any, *arguments: Any, **keywords: Any) -> None:
            if Path(target) == self.reserved:
                raise error
            real(target, *arguments, **keywords)

        return mock.patch("os.unlink", unlink)

    def test_a_reserved_name_that_cannot_be_removed_after_the_link_is_refused_with_fixed_text_and_both_names_stay(self) -> None:
        for error in (OSError(errno.EIO, "MARKER_91 input output error"), PermissionError(errno.EPERM, "MARKER_91"), OSError(errno.EBUSY, "MARKER_91")):
            with self.subTest(error=type(error).__name__, number=error.errno):
                with self.refuse_removal(error), self.assertRaises(ClosedInputError) as caught:
                    publish_reserved(self.reserved, self.path, "text\n")
                self.assertEqual(render(Notice(Code.CANNOT_WRITE, subject=Subject.OUTPUT)), str(caught.exception))
                self.assertNotIn("MARKER_91", str(caught.exception))
                self.assertIsNone(caught.exception.__cause__)
                self.assertTrue(caught.exception.__suppress_context__)
                self.assertEqual(["result.json", "result.json.part"], self.names())
                self.assertEqual("text\n", self.path.read_text())
                self.path.unlink()
                self.reserved.write_text("")

    def test_a_reserved_name_that_is_gone_after_the_link_is_a_clean_end(self) -> None:
        real = os.link

        def link_and_release(source: str, target: str) -> None:
            real(source, target)
            os.unlink(source)

        with mock.patch("os.link", link_and_release):
            publish_reserved(self.reserved, self.path, "text\n")
        self.assertEqual(["result.json"], self.names())
        self.assertEqual("text\n", self.path.read_text())

    def test_a_removal_that_fails_after_a_refusal_keeps_the_first_refusal(self) -> None:
        self.path.write_text(ORIGINAL)
        with self.refuse_removal(OSError(errno.EIO, "MARKER_91")), self.assertRaises(ClosedInputError) as caught:
            publish_reserved(self.reserved, self.path, "new\n")
        self.assertEqual(EXISTS, str(caught.exception))
        self.assertEqual(ORIGINAL, self.path.read_text())

    def test_a_path_in_another_folder_than_the_reserved_file_is_refused_and_the_reserved_name_is_removed(self) -> None:
        (self.root / "other").mkdir()
        with self.assertRaises(ClosedInputError):
            publish_reserved(self.reserved, self.root / "other" / "result.json", "new\n")
        self.assertEqual(["other"], self.names())


def shutil_rmtree(path: Path) -> None:
    import shutil

    shutil.rmtree(path)


class ReplayHarness(Fixture):
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

    def run_replay(self, argv: list[str], patch: Callable[[Any], contextlib.AbstractContextManager[object]] | None = None) -> tuple[int, str]:
        """Run the replay program with the packages that only its image holds replaced by stand-ins, so its own command line and guard run. ``patch`` gets
        the program module and returns a context to run it in, for a test that replaces one of its functions."""
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
                with mock.patch.object(sys, "stdin", types.SimpleNamespace(buffer=io.BytesIO(b""))), patch(program) if patch else contextlib.nullcontext():
                    return self.run_main(program.main, argv)


class CommandTests(ReplayHarness):
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
