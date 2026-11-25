"""The replay service writes through the two helpers, shows errors through the one formatter and keeps its report outputs out of the store folder.

- The heartbeat, the start counts and the saved position are replaced through ``replace_owned_file`` in the service's state folder: a link or a folder in
  their place is refused and what the link names stays as it was.
- The queue entry, its reference, the reservations, the result and the frozen schedule are created through ``write_new_file`` and never replace a path.
- An error that a request ends in is stored in the result and health files as the one formatter shows it: the type name, never a path or a value.
- ``manifest --out`` and ``observe --out`` refuse an output inside the folder store, by path and by the identity of the store folder.
"""

from __future__ import annotations

import contextlib
import json
import os
import signal
import stat
import sys
import threading
import time
import types
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from replay.service import serve as serve_module
from replay.service.output_guard import refuse_inside_store
from replay.service.serve import Health, Outcome, SubmitError, serve, submit
from replay.service.state_file import ScheduleFile, StateFile
from scripts.ingestion.closed import ClosedInputError
from scripts.ingestion.manifest import ManifestRef
from scripts.ingestion.notice import Code, Notice, Subject, render
from scripts.ingestion.output_file import replace_owned_file
from tests.fast.test_new_outputs import ORIGINAL, Fixture, ReplayHarness
from tests.fast.test_replay_serve import RECORD, REFERENCE, Clock

REFUSED = render(Notice(Code.OWNED_REFUSED))
EXISTS = render(Notice(Code.OUTPUT_EXISTS))
IN_STORE = render(Notice(Code.OUTPUT_IN_STORE))
CANNOT_WRITE = render(Notice(Code.CANNOT_WRITE, subject=Subject.OUTPUT))
RUN_REFUSED = "the run's state folder is not the service's state folder or a folder directly inside it; the run was refused and nothing was written"
NAME = str(RECORD["run_id"])


class HeartbeatTests(Fixture):
    def test_the_heartbeat_replaces_the_health_file_in_its_own_folder_and_nowhere_else(self) -> None:
        health = Health(self.root, Clock())
        with mock.patch.object(serve_module, "replace_owned_file", wraps=replace_owned_file) as helper:
            health.beat("waiting", force=True)
        helper.assert_called_once()
        self.assertEqual(self.root, helper.call_args.kwargs["state_root"])
        self.assertEqual(self.root / "health.json", helper.call_args.args[0])
        self.assertEqual("waiting", json.loads((self.root / "health.json").read_text())["status"])

    def test_a_link_or_a_folder_in_place_of_the_health_file_is_refused_and_what_the_link_names_stays(self) -> None:
        target = self.root / "target.txt"
        target.write_text(ORIGINAL)
        link = self.root / "health.json"
        link.symlink_to(target)
        with self.assertRaises(ClosedInputError) as caught:
            Health(self.root, Clock()).beat("waiting", force=True)
        self.assertEqual(REFUSED, str(caught.exception))
        self.assertEqual(ORIGINAL, target.read_text())
        link.unlink()
        link.mkdir()
        with self.assertRaises(ClosedInputError):
            Health(self.root, Clock()).beat("waiting", force=True)
        self.assertEqual([], list(link.iterdir()))

    def test_the_start_count_and_the_saved_position_go_through_the_same_helper(self) -> None:
        inbox = self.root / "inbox"
        inbox.mkdir()
        request = inbox / "a.json"
        request.write_text(json.dumps({**RECORD, "state_dir": str(self.root / "state" / "replay_0a1b2c3d")}))  # a record the loop's check admits
        count = request.with_suffix(".attempts")
        count.symlink_to(self.root / "elsewhere")
        stop = [False]

        def run(path: Path, beat: object) -> Outcome:
            return Outcome("finished", 0)

        (inbox / "a.reference").write_text(REFERENCE)
        with self.assertRaises(ClosedInputError):
            serve(inbox, self.root, run, lambda: stop[0], Clock(), lambda seconds: stop.__setitem__(0, True), 1.0)
        self.assertFalse((self.root / "elsewhere").exists())

    def test_the_saved_position_is_replaced_in_its_folder_and_a_link_is_refused(self) -> None:
        saved = StateFile(self.root)
        with mock.patch("replay.service.state_file.replace_owned_file", wraps=replace_owned_file) as helper:
            saved.save(mock.Mock(to_json=lambda: "{}"))
        self.assertEqual(self.root, helper.call_args.kwargs["state_root"])
        self.assertEqual(self.root / "state.json", saved.path)
        saved.path.unlink()
        saved.path.symlink_to(self.root / "elsewhere")
        with self.assertRaises(ClosedInputError):
            saved.save(mock.Mock(to_json=lambda: "{}"))
        self.assertFalse((self.root / "elsewhere").exists())

    def test_the_frozen_schedule_is_created_once_and_never_replaced(self) -> None:
        schedule = ScheduleFile(self.root / "schedule.json")
        frozen = mock.Mock(to_json=lambda: "first")
        schedule.freeze(frozen)
        with self.assertRaises(FileExistsError):
            schedule.freeze(mock.Mock(to_json=lambda: "second"))
        self.assertEqual("first", schedule.path.read_text())
        late = ScheduleFile(self.root / "late.json")
        late.path.symlink_to(self.root / "elsewhere")
        with self.assertRaises(FileExistsError):
            late.freeze(frozen)
        self.assertFalse((self.root / "elsewhere").exists())


def serve_until_stopped(harness: ReplayHarness, argv: list[str]) -> tuple[int, str]:
    """Run the service command line and stop it with SIGTERM from a helper thread that repeats until the command returns, with a deadline."""
    previous = signal.signal(signal.SIGTERM, lambda signum, frame: None)
    finished = threading.Event()

    def stopper() -> None:
        deadline = 30.0
        while not finished.wait(0.2) and (deadline := deadline - 0.2) > 0:
            os.kill(os.getpid(), signal.SIGTERM)

    thread = threading.Thread(target=stopper)
    thread.start()
    try:
        return harness.run_replay(argv)
    finally:
        finished.set()
        thread.join()
        signal.signal(signal.SIGTERM, previous)


class StateFolderCommandTests(ReplayHarness):
    """``serve`` and ``health`` take the state folder, never a file path: the health file is the fixed name ``health.json`` inside it."""

    def serve_until_stopped(self, argv: list[str]) -> tuple[int, str]:
        return serve_until_stopped(self, argv)

    def test_serve_with_a_state_folder_writes_health_json_in_it_and_leaves_an_unrelated_file_alone(self) -> None:
        state = self.root / "state"
        unrelated = self.made("unrelated.json")
        code, _ = self.serve_until_stopped(["serve", "--state", str(state), "--inbox", str(state / "inbox")])
        self.assertEqual(0, code)
        self.assertEqual("stopped", json.loads((state / "health.json").read_text())["status"])
        self.assertEqual(ORIGINAL, unrelated.read_text())
        self.assertEqual(["unrelated.json"], sorted(path.name for path in self.root.iterdir() if path.is_file()))

    def test_the_old_health_argument_is_refused_by_both_commands_and_the_file_it_names_stays(self) -> None:
        existing = self.made("existing.json")
        for command in ("serve", "health"):
            with self.subTest(command=command), self.assertRaises(SystemExit) as caught:
                self.run_replay([command, "--health", str(existing)])
            self.assertEqual(2, caught.exception.code)
        self.assertEqual(ORIGINAL, existing.read_text())

    def test_a_state_argument_that_names_a_file_is_refused_with_fixed_text_and_the_file_stays(self) -> None:
        existing = self.made("existing.json")
        code, error = self.run_replay(["serve", "--state", str(existing), "--inbox", str(self.root / "inbox")])
        self.assertEqual(2, code)
        self.assertIn(REFUSED + "\n", error)
        self.assertEqual(ORIGINAL, existing.read_text())

    def test_health_reads_the_fixed_file_name_in_the_state_folder(self) -> None:
        state = self.root / "state"
        state.mkdir()
        code, _ = self.run_replay(["health", "--state", str(state)])
        self.assertEqual(1, code)
        (state / "health.json").write_text(json.dumps({"status": "waiting", "updated": time.time()}))
        code, _ = self.run_replay(["health", "--state", str(state)])
        self.assertEqual(0, code)


class QueuedStateFolderTests(ReplayHarness):
    """A queued run writes its saved position only in the service's state folder or a folder directly inside it. ``replay_record`` is replaced by a run that
    saves its position through ``StateFile`` in the record's ``state_dir``, as the replayer does."""

    def setUp(self) -> None:
        super().setUp()
        self.state = self.root / "state"
        self.state.mkdir()
        # The inbox is beside the state folder, not in it, so the state folder itself is a place a run may use: the serve loop refuses a state folder
        # that holds the inbox before this rule is reached (``serve.path_refusal``), and that refusal has its own tests in test_replay_serve.
        self.inbox = self.root / "inbox"
        self.saved: list[Path] = []
        self.opens_store = False

    def checkpoint_run(self, program: Any) -> Any:
        opens_store = self.opens_store

        def replay(record: Any, flag: object, beat: object) -> int:
            folder = Path(record.state_dir)
            self.saved.append(folder)
            if opens_store:
                sys.modules[program.verify_inputs.__module__].open_store(record, ManifestRef.from_json(REFERENCE))
            StateFile(folder).save(mock.Mock(to_json=lambda: "saved by the run"))
            return 0

        return mock.patch.object(program, "replay_record", replay)

    def queue(self, state_dir: Path | str, read_log: Path | str | None = None, *, opens_store: bool = False) -> dict[str, Any]:
        """Queue one run that names ``state_dir`` (and, when given, the ``read_log`` of its folder store), serve until the service is stopped and return the
        run's result. The run opens its folder store as the replay does when ``opens_store`` is true."""
        self.opens_store = opens_store
        self.inbox.mkdir(exist_ok=True)
        store = dict(RECORD["object_store"]) | ({"read_log": str(read_log)} if read_log is not None else {})  # type: ignore[call-overload]
        (self.inbox / "a.json").write_text(json.dumps({**RECORD, "state_dir": str(state_dir), "object_store": store}))
        (self.inbox / "a.reference").write_text(REFERENCE)
        code, _ = serve_until_stopped(self, ["serve", "--state", str(self.state), "--inbox", str(self.inbox)])
        self.assertEqual(0, code)
        result: dict[str, Any] = json.loads((self.inbox / "results" / "a.json").read_text())
        return result

    def run_replay(self, argv: list[str], patch: Any = None) -> tuple[int, str]:
        return super().run_replay(argv, patch or self.checkpoint_run)

    def standalone_freeze(self, read_log: Path) -> tuple[int, str]:
        """Run the standalone ``freeze`` command for a record that names ``read_log``, with the inputs' checks and the freezing replaced by a run that opens
        the folder store as the command does."""
        self.replay_files("freeze")
        document = json.loads((self.root / "run.json").read_text())
        document["object_store"]["read_log"] = str(read_log)
        (self.root / "run.json").write_text(json.dumps(document))
        (self.root / "reference.json").write_text(REFERENCE)

        def opened(program: Any) -> Any:
            inputs_module = sys.modules[program.verify_inputs.__module__]

            def verify(record: Any) -> None:
                inputs_module.open_store(record, ManifestRef.from_json(REFERENCE))

            frozen = types.SimpleNamespace(schedule=types.SimpleNamespace(sha256="s", first_due_us=1, window_end_us=2))
            stack = contextlib.ExitStack()
            stack.enter_context(mock.patch.object(program, "verify_inputs", verify))
            stack.enter_context(mock.patch.object(program, "freeze_run", lambda record, inputs: frozen))
            return stack

        return super().run_replay(["freeze", "--run-record", str(self.root / "run.json")], opened)

    def held_elsewhere(self, folder: Path) -> Path:
        folder.mkdir(parents=True, exist_ok=True)
        held = folder / "state.json"
        held.write_text(ORIGINAL)
        return held

    def test_a_run_that_names_a_folder_outside_the_state_folder_is_refused_and_the_state_file_there_is_unchanged(self) -> None:
        held = self.held_elsewhere(self.root / "other")
        result = self.queue(self.root / "other")
        self.assertEqual(("failed", 2, RUN_REFUSED), (result["outcome"], result["exit_code"], result["error"]))
        self.assertEqual(ORIGINAL, held.read_text())
        self.assertEqual([], self.saved)
        self.assertEqual(["state.json"], [entry.name for entry in held.parent.iterdir()])

    def test_every_other_form_of_a_folder_outside_is_refused_and_leaves_what_it_names_alone(self) -> None:
        labels = (
            "two levels inside",
            "a sibling that starts with the state folder's name",
            "the folder above",
            "a link to a folder outside",
            "dot dot",
            "relative",
        )
        for label in labels:
            with self.subTest(named=label):
                self.setUp()
                (self.state / "deeper" / "run").mkdir(parents=True)
                (self.root / "state_sibling").mkdir()
                (self.state / "linked").symlink_to(self.root / "other")
                held = self.held_elsewhere(self.root / "other")
                forms: dict[str, Path | str] = {
                    "two levels inside": self.state / "deeper" / "run",
                    "a sibling that starts with the state folder's name": self.root / "state_sibling",
                    "the folder above": self.root,
                    "a link to a folder outside": self.state / "linked",
                    "dot dot": self.state / ".." / "other",
                    "relative": "other",
                }
                named = forms[label]
                previous = Path.cwd()
                os.chdir(self.root)
                try:
                    result = self.queue(named)
                finally:
                    os.chdir(previous)
                # The folder above holds the inbox, so the serve loop's own check refuses it first, with its fixed text (``serve.path_refusal``).
                self.assertEqual(serve_module.STATE_DIR_MESSAGE if label == "the folder above" else RUN_REFUSED, result["error"])
                self.assertEqual(ORIGINAL, held.read_text())
                self.assertEqual([], self.saved)

    def test_the_state_folder_itself_and_a_per_run_subfolder_still_work(self) -> None:
        for name in ("", "replay_0a1b2c3d", "not_made_yet"):
            with self.subTest(named=name or "the state folder"):
                self.setUp()
                named = self.state / name
                result = self.queue(named)
                self.assertEqual(("finished", 0, ""), (result["outcome"], result["exit_code"], result["error"]))
                self.assertEqual([named.resolve()], self.saved)
                self.assertEqual("saved by the run", (named / "state.json").read_text())

    def test_a_read_log_outside_the_run_folder_is_refused_and_the_file_it_names_is_unchanged(self) -> None:
        for label in ("outside the state folder", "in the state folder above the run"):
            with self.subTest(read_log=label):
                self.setUp()
                named = self.state / "replay_0a1b2c3d"
                log = self.root / "other" / "reads.log" if label == "outside the state folder" else self.state / "reads.log"
                log.parent.mkdir(parents=True, exist_ok=True)
                log.write_text(ORIGINAL)
                result = self.queue(named, read_log=log)
                self.assertEqual(RUN_REFUSED, result["error"])
                self.assertEqual(ORIGINAL, log.read_text())
                self.assertEqual([], self.saved)

    def test_a_read_log_directly_inside_the_run_folder_still_works(self) -> None:
        named = self.state / "replay_0a1b2c3d"
        result = self.queue(named, read_log=named / "reads.log")
        self.assertEqual(("finished", 0, ""), (result["outcome"], result["exit_code"], result["error"]))

    def test_a_read_log_that_exists_in_any_form_is_refused_by_the_run_that_opens_its_store_and_every_byte_is_unchanged(self) -> None:
        kinds = ("a file with text", "an empty file", "a hard link", "a link", "a folder", "a named pipe")
        for kind in kinds:
            for folder_label in ("a per-run folder", "the state folder itself"):
                with self.subTest(read_log=kind, folder=folder_label):
                    self.setUp()
                    named = self.state / "replay_0a1b2c3d" if folder_label == "a per-run folder" else self.state
                    named.mkdir(exist_ok=True)
                    other = named / "other.txt"
                    other.write_text(ORIGINAL)
                    log = named / "reads.log"
                    if kind == "a file with text":
                        log.write_text(ORIGINAL)
                    elif kind == "an empty file":
                        log.write_bytes(b"")
                    elif kind == "a hard link":
                        os.link(other, log)
                    elif kind == "a link":
                        log.symlink_to(other)
                    elif kind == "a folder":
                        log.mkdir()
                    else:
                        os.mkfifo(log)
                    mode = log.lstat().st_mode
                    held = log.read_bytes() if stat.S_ISREG(mode) else None
                    result = self.queue(named, read_log=log, opens_store=True)
                    self.assertEqual(("failed", 2, CANNOT_WRITE), (result["outcome"], result["exit_code"], result["error"]))
                    self.assertEqual(mode, log.lstat().st_mode)
                    self.assertEqual(held, log.read_bytes() if stat.S_ISREG(mode) else None)
                    self.assertEqual(ORIGINAL, other.read_text())
                    self.assertEqual([], [entry.name for entry in named.iterdir() if entry.name not in {"other.txt", log.name, "inbox", "health.json"}])

    def test_a_read_log_named_like_a_file_the_replay_writes_is_refused_before_the_run_starts_whether_the_file_exists_or_not(self) -> None:
        names = ("state.json", "schedule.json", "health.json", "STATE.JSON", "Schedule.Json", "HEALTH.json")
        for name in names:
            for folder_label in ("a per-run folder", "the state folder itself"):
                for exists in (False, True):
                    with self.subTest(read_log=name, folder=folder_label, exists=exists):
                        self.setUp()
                        named = self.state / "replay_0a1b2c3d" if folder_label == "a per-run folder" else self.state
                        named.mkdir(exist_ok=True)
                        log = named / name
                        if exists:
                            log.write_text(ORIGINAL)
                        before = sorted((entry.name, entry.read_bytes()) for entry in named.iterdir() if entry.is_file())
                        result = self.queue(named, read_log=log, opens_store=True)
                        self.assertEqual(("failed", 2, RUN_REFUSED), (result["outcome"], result["exit_code"], result["error"]))
                        self.assertEqual([], self.saved)
                        after = sorted((entry.name, entry.read_bytes()) for entry in named.iterdir() if entry.is_file())
                        own = named == self.state
                        self.assertEqual(
                            [item for item in before if not (own and item[0].casefold() == "health.json")],
                            [item for item in after if not (own and item[0].casefold() == "health.json")],
                        )

    def test_a_read_log_that_is_a_link_to_a_vacant_name_is_refused_and_the_name_stays_vacant(self) -> None:
        for target in ("state.json", "schedule.json", "reads_target.log"):
            for folder_label in ("a per-run folder", "the state folder itself"):
                with self.subTest(target=target, folder=folder_label):
                    self.setUp()
                    named = self.state / "replay_0a1b2c3d" if folder_label == "a per-run folder" else self.state
                    named.mkdir(exist_ok=True)
                    log = named / "reads.log"
                    log.symlink_to(named / target)
                    result = self.queue(named, read_log=log, opens_store=True)
                    self.assertEqual(("failed", 2, CANNOT_WRITE), (result["outcome"], result["exit_code"], result["error"]))
                    self.assertTrue(log.is_symlink())
                    self.assertFalse((named / target).exists())

    def test_a_queued_run_whose_state_folder_is_the_inbox_and_whose_read_log_is_its_own_queued_record_leaves_every_byte_unchanged(self) -> None:
        self.inbox.mkdir()
        record = self.inbox / "a.json"
        text = json.dumps({**RECORD, "state_dir": str(self.inbox), "object_store": dict(RECORD["object_store"]) | {"read_log": str(record)}})  # type: ignore[call-overload]
        record.write_text(text)
        (self.inbox / "a.reference").write_text(REFERENCE)
        self.opens_store = True
        code, _ = serve_until_stopped(self, ["serve", "--state", str(self.state), "--inbox", str(self.inbox)])
        self.assertEqual(0, code)
        result: dict[str, Any] = json.loads((self.inbox / "results" / "a.json").read_text())
        self.assertEqual(("failed", 2, serve_module.STATE_DIR_MESSAGE), (result["outcome"], result["exit_code"], result["error"]))
        kept = [entry for entry in self.inbox.rglob("a.json") if entry.read_text() == text]
        self.assertTrue(kept, "the queued record's bytes were changed")

    def test_a_new_read_log_directly_inside_the_run_folder_is_made_by_the_run_that_opens_its_store_and_a_second_run_that_names_it_is_refused(self) -> None:
        named = self.state / "replay_0a1b2c3d"
        named.mkdir()
        log = named / "reads.log"
        first = self.queue(named, read_log=log, opens_store=True)
        self.assertEqual(("finished", 0, ""), (first["outcome"], first["exit_code"], first["error"]))
        self.assertEqual(b"", log.read_bytes())
        log.write_text("a line that a read added\n")
        (self.inbox / "results" / "a.json").unlink()
        for entry in [*self.inbox.glob("a.*"), *(self.inbox / "archive").glob("a.*")]:
            entry.unlink()
        second = self.queue(named, read_log=log, opens_store=True)
        self.assertEqual(("failed", 2, CANNOT_WRITE), (second["outcome"], second["exit_code"], second["error"]))
        self.assertEqual("a line that a read added\n", log.read_text())

    def test_the_standalone_freeze_whose_read_log_names_a_valid_schedule_leaves_the_schedule_unchanged(self) -> None:
        schedule = self.root / "state" / "schedule.json"
        (self.root / "state").mkdir(exist_ok=True)
        schedule.write_text(ORIGINAL)
        code, error = self.standalone_freeze(schedule)
        self.assertEqual(2, code)
        self.assertIn(CANNOT_WRITE + "\n", error)
        self.assertEqual(ORIGINAL, schedule.read_text())

    def test_the_standalone_freeze_makes_a_new_read_log_once_and_refuses_a_second_run_that_names_it(self) -> None:
        log = self.root / "state" / "reads.log"
        (self.root / "state").mkdir(exist_ok=True)
        code, error = self.standalone_freeze(log)
        self.assertEqual((0, b""), (code, log.read_bytes()), error)
        log.write_text("kept\n")
        code, error = self.standalone_freeze(log)
        self.assertEqual(2, code)
        self.assertIn(CANNOT_WRITE + "\n", error)
        self.assertEqual("kept\n", log.read_text())

    def standalone(self, command: str, read_log: Path, created: list[Path] | None = None) -> tuple[int, str, list[str]]:
        """Run the real standalone ``freeze``, ``run`` or ``manifest`` command for a record that names ``read_log``. Only what lies beyond the opening of the
        folder store is replaced: the Kafka lookup of ``run``, the frozen schedule that ``manifest`` needs, and the manifest read, which records that it was
        reached and stops the command. Return the exit code, the error text and the commands' marks of having read the manifest."""
        self.replay_files(command)
        document = json.loads((self.root / "run.json").read_text())
        document["object_store"]["read_log"] = str(read_log)
        (self.root / "run.json").write_text(json.dumps(document))
        (self.root / "reference.json").write_text(REFERENCE)
        reached: list[str] = []

        def patched(program: Any) -> Any:
            inputs_module = sys.modules[program.verify_inputs.__module__]

            def read_manifest(store: object, reference: object) -> None:
                reached.append(command)
                raise inputs_module.InputError("the manifest read was reached")

            stack = contextlib.ExitStack()
            stack.enter_context(mock.patch.object(inputs_module, "read_manifest", read_manifest))
            stack.enter_context(mock.patch.object(program, "partition_count", lambda bootstrap, topic: 1))
            if created is not None:
                original = inputs_module.ReadLog.create

                def create(path: Path) -> Any:
                    created.append(path)
                    return original(path)

                stack.enter_context(mock.patch.object(inputs_module.ReadLog, "create", staticmethod(create)))
            stack.enter_context(mock.patch.object(program, "ScheduleFile", lambda path: types.SimpleNamespace(load=lambda: object())))
            return stack

        argv = [command, "--run-record", str(self.root / "run.json")] + (["--out", str(self.root / "manifest.json")] if command == "manifest" else [])
        code, error = super().run_replay(argv, patched)
        return code, error, reached

    def test_a_standalone_command_refuses_a_name_the_replay_writes_in_its_state_folder_as_the_read_log_before_anything_is_made(self) -> None:
        names = ("state.json", "schedule.json", "health.json", "STATE.JSON", "Schedule.Json", "HEALTH.json")
        for command in ("freeze", "run", "manifest"):
            for name in names:
                for folder_label in ("the state folder", "a folder directly inside it"):
                    with self.subTest(command=command, read_log=name, folder=folder_label):
                        self.setUp()
                        named = self.root / "state" if folder_label == "the state folder" else self.root / "state" / "replay_0a1b2c3d"
                        named.mkdir(parents=True, exist_ok=True)
                        before = sorted(path.relative_to(self.root) for path in self.root.rglob("*"))
                        code, error, reached = self.standalone(command, named / name)
                        self.assertEqual(2, code)
                        self.assertIn(CANNOT_WRITE + "\n", error)
                        self.assertEqual([], reached, "the refusal comes before the first read")
                        after = sorted(path.relative_to(self.root) for path in self.root.rglob("*"))
                        self.assertEqual(sorted({*before, Path("run.json"), Path("reference.json")}), after, "nothing is created")
                        self.assertFalse((named / name).exists())

    def test_a_standalone_command_creates_a_read_log_with_any_other_name_and_reaches_the_manifest(self) -> None:
        for command in ("freeze", "run", "manifest"):
            with self.subTest(command=command):
                self.setUp()
                log = self.root / "state" / "reads.log"
                (self.root / "state").mkdir(exist_ok=True)
                code, _, reached = self.standalone(command, log)
                self.assertEqual(2, code)
                self.assertEqual([command], reached)
                self.assertEqual(b"", log.read_bytes())

    def test_a_standalone_command_whose_read_log_is_a_link_to_a_vacant_reserved_name_is_refused_and_the_name_stays_vacant(self) -> None:
        for command in ("freeze", "run", "manifest"):
            for target in ("state.json", "schedule.json", "reads_target.log"):
                with self.subTest(command=command, target=target):
                    self.setUp()
                    (self.root / "state").mkdir(exist_ok=True)
                    log = self.root / "state" / "reads.log"
                    log.symlink_to(self.root / "state" / target)
                    code, error, reached = self.standalone(command, log)
                    self.assertEqual((2, []), (code, reached))
                    self.assertIn(CANNOT_WRITE + "\n", error)
                    self.assertTrue(log.is_symlink())
                    self.assertFalse((self.root / "state" / target).exists())

    def test_the_read_log_is_created_in_its_resolved_folder_under_its_own_last_name(self) -> None:
        (self.root / "state" / "sub").mkdir(parents=True)
        (self.root / "link").symlink_to(self.root / "state")
        for text, expected in (
            (self.root / "state" / "sub" / ".." / "reads.log", (self.root / "state").resolve() / "reads.log"),
            (self.root / "link" / "reads.log", (self.root / "state").resolve() / "reads.log"),
        ):
            with self.subTest(read_log=str(text)):
                created: list[Path] = []
                code, _, reached = self.standalone("freeze", text, created)
                self.assertEqual((2, ["freeze"], [expected]), (code, reached, created))
                expected.unlink()

    def test_an_empty_read_log_means_no_log_and_a_queued_run_with_it_is_not_refused(self) -> None:
        for label in ("a per-run folder", "the state folder itself"):
            with self.subTest(named=label):
                self.setUp()
                named = self.state / "replay_0a1b2c3d" if label == "a per-run folder" else self.state
                result = self.queue(named, read_log="")
                self.assertEqual(("finished", 0, ""), (result["outcome"], result["exit_code"], result["error"]))
                self.assertEqual([named.resolve()], self.saved)

    def test_the_standalone_run_command_keeps_its_folder_whatever_it_is(self) -> None:
        self.replay_files("run")
        elsewhere = self.held_elsewhere(self.root / "other")
        document = json.loads((self.root / "run.json").read_text()) | {"state_dir": str(elsewhere.parent)}
        (self.root / "run.json").write_text(json.dumps(document))
        code, _ = self.run_replay(["run", "--run-record", str(self.root / "run.json")])
        self.assertEqual(0, code)
        self.assertEqual([elsewhere.parent], self.saved)
        self.assertEqual("saved by the run", elsewhere.read_text())


class QueueWriteTests(Fixture):
    def setUp(self) -> None:
        super().setUp()
        self.inbox = self.root / "inbox"
        self.text = json.dumps(RECORD)

    def test_a_link_in_place_of_the_queue_entry_or_its_reference_is_refused_and_not_followed(self) -> None:
        for suffix in (".json", ".reference"):
            with self.subTest(suffix=suffix):
                inbox = self.root / f"inbox{suffix}"
                inbox.mkdir()
                (inbox / f"{NAME}{suffix}").symlink_to(self.root / "elsewhere")
                with self.assertRaises((SubmitError, ClosedInputError)):
                    submit(inbox, self.text, REFERENCE)
                self.assertFalse((self.root / "elsewhere").exists())
                self.assertTrue((inbox / f"{NAME}{suffix}").is_symlink())

    def test_a_reference_that_a_stop_left_behind_with_the_same_bytes_is_kept_and_the_entry_is_completed(self) -> None:
        submit(self.inbox, self.text, REFERENCE)
        (self.inbox / f"{NAME}.json").unlink()
        before = (self.inbox / f"{NAME}.reference").stat().st_ino
        self.assertEqual("submitted", submit(self.inbox, self.text, REFERENCE))
        self.assertEqual(self.text, (self.inbox / f"{NAME}.json").read_text())
        self.assertEqual(before, (self.inbox / f"{NAME}.reference").stat().st_ino)

    def test_a_reference_beside_a_missing_entry_with_other_bytes_is_refused_and_kept(self) -> None:
        submit(self.inbox, self.text, REFERENCE)
        (self.inbox / f"{NAME}.json").unlink()
        (self.inbox / f"{NAME}.reference").write_text("other\n")
        with self.assertRaises((SubmitError, ClosedInputError)):
            submit(self.inbox, self.text, REFERENCE)
        self.assertEqual("other\n", (self.inbox / f"{NAME}.reference").read_text())
        self.assertFalse((self.inbox / f"{NAME}.json").exists())

    def test_a_reservation_is_created_with_the_helper_and_a_link_in_its_place_is_never_followed(self) -> None:
        (self.inbox / "reserved").mkdir(parents=True)
        (self.inbox / "reserved" / NAME).symlink_to(self.root / "elsewhere")
        with self.assertRaises(SubmitError):
            submit(self.inbox, self.text, REFERENCE)
        self.assertFalse((self.root / "elsewhere").exists())

    def test_a_result_file_that_is_already_held_is_refused_and_never_replaced(self) -> None:
        inbox = self.root / "serve_inbox"
        (inbox / "results").mkdir(parents=True)
        request = inbox / "a.json"
        request.write_text(self.text)
        (inbox / "a.reference").write_text(REFERENCE)
        held = inbox / "results" / "a.json"
        held.write_text(ORIGINAL)
        with self.assertRaises(ClosedInputError) as caught:
            serve_module.keep(inbox, request, Outcome("failed", 2, "x"))
        self.assertEqual(EXISTS, str(caught.exception))
        self.assertEqual(ORIGINAL, held.read_text())
        self.assertTrue(request.exists(), "the request is not archived when its result was not written")

    def test_a_result_is_created_with_the_helper_and_never_replaces_a_path(self) -> None:
        inbox = self.root / "serve_inbox"
        (inbox / "results").mkdir(parents=True)
        request = inbox / "a.json"
        request.write_text(self.text)
        (inbox / "a.reference").write_text(REFERENCE)
        (inbox / "results" / "a.json").symlink_to(self.root / "elsewhere")
        with self.assertRaises(ClosedInputError):
            serve_module.keep(inbox, request, Outcome("failed", 2, "x"))
        self.assertFalse((self.root / "elsewhere").exists())
        self.assertTrue(request.exists())


class ErrorTextTests(Fixture):
    def serve_one(self, run: object) -> dict[str, object]:
        inbox = self.root / "inbox"
        inbox.mkdir()
        (inbox / "a.json").write_text(json.dumps(RECORD))
        (inbox / "a.reference").write_text(REFERENCE)
        stop = [False]
        health = self.root / "health.json"
        serve(inbox, self.root, run, lambda: stop[0], Clock(), lambda seconds: stop.__setitem__(0, True), 1.0)  # type: ignore[arg-type]
        stored: dict[str, object] = json.loads((inbox / "results" / "a.json").read_text())
        return stored | {"health": json.loads(health.read_text())}

    def test_an_error_that_ends_a_replay_is_stored_as_its_type_name_and_names_no_path_or_value(self) -> None:
        for error in (ValueError("/secret/path marker_one"), OverflowError("marker_two"), ZeroDivisionError("marker_three")):
            with self.subTest(error=type(error).__name__):
                self.setUp()

                def run(path: Path, beat: object, error: Exception = error) -> Outcome:
                    raise error

                stored = self.serve_one(run)
                self.assertEqual(type(error).__name__, stored["error"])
                self.assertNotIn("marker", json.dumps(stored))
                self.assertNotIn("/secret", json.dumps(stored))

    def test_a_record_that_cannot_be_read_is_stored_as_its_type_name(self) -> None:
        inbox = self.root / "inbox"
        inbox.mkdir()
        (inbox / "a.json").write_text("not json marker_four")
        (inbox / "a.reference").write_text(REFERENCE)
        stop = [False]
        health = self.root / "health.json"
        serve(inbox, self.root, lambda path, beat: Outcome("finished", 0), lambda: stop[0], Clock(), lambda seconds: stop.__setitem__(0, True), 1.0)
        stored = json.loads((inbox / "results" / "a.json").read_text())
        self.assertEqual("RunRecordError", stored["error"])
        self.assertNotIn("marker_four", json.dumps(stored) + health.read_text())

    def test_the_reason_that_reaches_the_health_file_is_the_stored_one(self) -> None:
        def run(path: Path, beat: object) -> Outcome:
            raise ValueError("/secret/health marker_five")

        stored = self.serve_one(run)
        self.assertNotIn("marker_five", json.dumps(stored["health"]))
        self.assertEqual("ValueError", stored["health"]["last"]["error"])  # type: ignore[index]


class OutputGuardTests(Fixture):
    def setUp(self) -> None:
        super().setUp()
        self.store = self.root / "store"
        (self.store / "cases").mkdir(parents=True)

    def refused(self, out: Path, root: Path | None = None) -> None:
        with self.assertRaises(ClosedInputError) as caught:
            refuse_inside_store(root or self.store, out)
        self.assertEqual(IN_STORE, str(caught.exception))

    def test_an_output_anywhere_inside_the_store_folder_is_refused_in_every_form(self) -> None:
        (self.root / "link_to_store").symlink_to(self.store)
        (self.root / "link_to_cases").symlink_to(self.store / "cases")
        forms = {
            "in the root": self.store / "new.json",
            "in a folder of the store": self.store / "cases" / "v0002",
            "a version of an object": self.store / "cases" / "v0001",
            "through a dot dot part": self.store / "cases" / ".." / "new.json",
            "through a link to the store": self.root / "link_to_store" / "new.json",
            "through a link to a folder of the store": self.root / "link_to_cases" / "v0002",
            "a link and a dot dot part": self.root / "link_to_store" / "cases" / ".." / "new.json",
            "a path that leaves and returns": self.root / "other" / ".." / "store" / "new.json",
        }
        (self.root / "other").mkdir()
        for label, out in forms.items():
            with self.subTest(out=label):
                self.refused(out)

    def test_an_empty_store_folder_is_guarded_the_same_way(self) -> None:
        empty = self.root / "empty_store"
        empty.mkdir()
        self.refused(empty / "cases" / "v0001", empty)
        self.refused(empty / "new.json", empty)

    def test_a_store_folder_that_is_a_link_and_a_relative_path_are_guarded_by_identity(self) -> None:
        (self.root / "alias").symlink_to(self.store)
        self.refused(self.store / "new.json", self.root / "alias")
        previous = Path.cwd()
        os.chdir(self.root)
        try:
            self.refused(Path("store") / "new.json", Path("store"))
            self.refused(Path("alias") / "new.json", self.store)
        finally:
            os.chdir(previous)

    def test_a_case_variant_of_the_store_folder_is_refused_where_the_folder_tells_the_names_apart_by_identity(self) -> None:
        variant = Path(str(self.store).replace("store", "STORE"))
        if not variant.exists():
            self.skipTest("this folder tells letter case apart")
        self.refused(variant / "new.json")

    def test_an_output_outside_the_store_folder_is_not_refused(self) -> None:
        (self.root / "reports").mkdir()
        (self.root / "store_reports").mkdir()
        for out in (self.root / "reports" / "manifest.json", self.root / "store_reports" / "x.json", self.root / "manifest.json", self.root / "store.json"):
            with self.subTest(out=out.name):
                refuse_inside_store(self.store, out)

    def test_a_store_folder_that_does_not_exist_still_guards_a_path_under_its_name(self) -> None:
        self.refused(self.root / "absent_store" / "cases" / "new.json", self.root / "absent_store")
        refuse_inside_store(self.root / "absent_store", self.root / "report.json")

    def test_the_refusal_names_no_path(self) -> None:
        with self.assertRaises(ClosedInputError) as caught:
            refuse_inside_store(self.store, self.store / "secret_marker.json")
        self.assertNotIn("secret_marker", str(caught.exception))


class CommandGuardTests(ReplayHarness):
    def test_manifest_and_observe_refuse_an_output_in_the_store_folder_and_create_nothing(self) -> None:
        (self.root / "store" / "cases").mkdir(parents=True)
        (self.root / "link").symlink_to(self.root / "store")
        for command in ("manifest", "observe"):
            self.replay_files(command)
            for label, out in {
                "new object": self.root / "store" / "cases" / "v0002",
                "root file": self.root / "store" / "n.json",
                "link": self.root / "link" / "n.json",
            }.items():
                with self.subTest(command=command, out=label):
                    code, err = self.run_replay([command, "--run-record", str(self.root / "run.json"), "--out", str(out)])
                    self.assertEqual(2, code)
                    self.assertIn(IN_STORE, err)
        self.assertEqual(["cases"], [path.name for path in (self.root / "store").iterdir()])
        self.assertEqual([], list((self.root / "store" / "cases").iterdir()))


if __name__ == "__main__":
    unittest.main()
