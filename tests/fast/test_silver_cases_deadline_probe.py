"""The Silver cases runner's deadline probe passes only on a flag that the probe program wrote inside the container, and records only Booleans.

The server that stalls the catalog and the program that waits for it run in the slow group (``tests/slow/test_silver_cases_stalled_catalog.py``, project
standard Q39), which also plants the job outputs that the program must refuse. Here the probe case runs on a stack that answers what the test says, and the
stack's own reading of the flag file is checked with a stand-in for the command that it starts. Nothing binds a port. The runner is untracked, so each test
skips when it is not on this machine.

The probe program starts the job's command line as a child with its standard error piped, compares the whole of that stream in memory with exactly the stop
line, and writes one small JSON flag to a folder that is mounted for it. The runner reads that flag and the exit code, never a stream of Docker, Compose or the
job, so no line of text leaves the container.

Failure modes covered here:

- The probe passed on a job that exited 124 with any other diagnostic, or with none, because it never looked at the line that the job writes.
- The probe judged only the lines that start with the job's prefix and tracebacks, so a warning in the catalog call, a line with no prefix, a Python
  exception-group traceback and another job's stop line beside the stop line all passed. The whole stream of the job is now compared, in the container.
- The runner trusted the stream of ``docker compose run``: a stop line there with no flag file, or a flag file that is malformed, missing, has another key,
  or has a value of another type, passed. Each of them now fails the case (it fails closed).
- A text of a stream reached the log, the commands or the record.
- The probe passed on a job that exited 124 and left a result file or a container, or on a job that exited with another code.
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from tests.e2e.run_record import Record
from tests.fast.test_runner_gold_sealed import runner

GOOD = {"stderr_is_stop_line": True, "exit": 124}
SEALED = ("cases", "verify", "stalled_catalog")


class Probe(unittest.TestCase):
    """The case of ``bounded_failure`` that holds the probe, on a stack that answers what the test says."""

    def stack_of(self, answer: tuple[int, dict[str, Any], Any], left: list[str], calls: list[dict[str, Any]]) -> Any:
        def job_with_flag(
            module_arguments: list[str],
            mounts: Any,
            reset: Any,
            env: dict[str, str] | None,
            limit: float,
            sealed: tuple[str, str, str] | None,
            flag_folder: bool,
        ) -> tuple[int, dict[str, Any], Any, float]:
            calls.append({"module_arguments": module_arguments, "env": env, "sealed": sealed, "flag_folder": flag_folder})
            return answer[0], answer[1], answer[2], 2.5

        return SimpleNamespace(
            silver=lambda *_, **__: (1, {"diagnostics": [{"code": "branch_missing"}]}, "", 0.1),
            job=lambda *_, **__: (2, {}, "", 0.1),
            job_with_flag=job_with_flag,
            spark_containers=lambda: left,
        )

    def run_probe(self, answer: tuple[int, dict[str, Any], Any], left: list[str]) -> tuple[bool, list[dict[str, Any]], Record]:
        module = runner("silver_cases")
        calls: list[dict[str, Any]] = []
        run = module.Run.__new__(module.Run)
        run.record, run.labels, run.suffix, run.made, run.trunk = Record(feature="t", slug="t", rerun="none"), {}, "x1", [], "trunk_x1"
        run.stack = self.stack_of(answer, left, calls)
        run.bounded_failure()
        return run.record.observations[-1].passed, calls, run.record

    def test_the_probe_runs_the_probe_program_with_a_deadline_of_2_seconds_and_a_nessie_timeout_of_100_seconds(self) -> None:
        module = runner("silver_cases")
        _, calls, _ = self.run_probe((124, {}, GOOD), [])
        self.assertEqual(1, len(calls))
        self.assertEqual(["-c", module.STALL], calls[0]["module_arguments"])
        self.assertEqual({"JOB_DEADLINE_SECONDS": "2", "NESSIE_TIMEOUT_SECONDS": "100"}, module.STALL_ENV)
        self.assertEqual(module.STALL_ENV, calls[0]["env"])
        self.assertEqual(SEALED, calls[0]["sealed"])

    def test_the_probe_asks_the_stack_for_the_flag_folder_and_for_no_line_of_standard_error(self) -> None:
        _, calls, _ = self.run_probe((124, {}, GOOD), [])
        self.assertEqual([True], [call["flag_folder"] for call in calls])

    def test_the_probe_passes_only_on_exit_124_with_the_one_true_flag_no_result_file_and_no_container(self) -> None:
        for label, answer, left, wanted in (
            ("a stop at the deadline", (124, {}, GOOD), [], True),
            ("another exit code of the container", (1, {}, GOOD), [], False),
            ("a result file", (124, {"diagnostics": []}, GOOD), [], False),
            ("a container left", (124, {}, GOOD), ["spark"], False),
            ("no flag file", (124, {}, None), [], False),
            ("the stream is not the stop line", (124, {}, {"stderr_is_stop_line": False, "exit": 124}), [], False),
            ("the child exited with another code", (124, {}, {"stderr_is_stop_line": True, "exit": 1}), [], False),
            ("a flag that is the number 1", (124, {}, {"stderr_is_stop_line": 1, "exit": 124}), [], False),
            ("a flag that is a text", (124, {}, {"stderr_is_stop_line": "true", "exit": 124}), [], False),
            ("an exit that is True", (124, {}, {"stderr_is_stop_line": True, "exit": True}), [], False),
            ("another key in the flag", (124, {}, {**GOOD, "line": "x"}), [], False),
            ("a key missing from the flag", (124, {}, {"stderr_is_stop_line": True}), [], False),
            ("an empty flag", (124, {}, {}), [], False),
        ):
            with self.subTest(label):
                self.assertIs(wanted, self.run_probe(answer, left)[0])

    def test_the_record_holds_whether_the_flag_was_read_and_matched_and_never_a_text_of_a_stream(self) -> None:
        answers: list[tuple[tuple[int, dict[str, Any], Any], str]] = [
            ((124, {}, GOOD), "True"),
            ((124, {}, {"stderr_is_stop_line": False, "exit": 124}), "False"),
        ]
        for answer, matched in answers:
            with self.subTest(matched):
                _, _, record = self.run_probe(answer, [])
                observed = record.observations[-1].observed
                self.assertIn(f"the child's own standard error is exactly the stop line {matched}", observed)
                self.assertIn("the flag file was read True", observed)
        _, _, record = self.run_probe((124, {}, None), [])
        self.assertIn("the flag file was read False", record.observations[-1].observed)


def mounted(command: list[str], target: str) -> Path:
    """The host folder that the command mounts at ``target`` in the container."""
    (item,) = [command[index + 1] for index, word in enumerate(command[:-1]) if word == "--volume" and command[index + 1].endswith(f":{target}")]
    return Path(item.removesuffix(f":{target}"))


class StackFlag(unittest.TestCase):
    """``Stack.job_with_flag`` gives back the flag file of its own mounted folder when it is exactly the closed document, and nothing otherwise. The standard
    error of ``docker compose run`` is never read."""

    def job(
        self, flag_text: str | bytes | None, *, extra: dict[str, str] | None = None, error: str = "", flag_folder: bool = True, code: int = 124
    ) -> tuple[tuple[int, dict[str, Any], Any, float], list[str], list[list[str]], list[str]]:
        module = runner("silver_cases")
        folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="stack_flag_")))
        (folder / "data").mkdir()
        log: list[str] = []
        commands: list[list[str]] = []
        record = Record(feature="t", slug="t", rerun="none")
        stack = module.Stack(folder / "compose", "project", folder / "data", record, log)

        def run(command: list[str], limit: float, **_: Any) -> tuple[int, str, str]:
            commands.append(command)
            if flag_folder:
                target = mounted(command, "/probe") / "result.json"
                if isinstance(flag_text, bytes):
                    target.write_bytes(flag_text)
                elif flag_text is not None:
                    target.write_text(flag_text)
                for name, text in (extra or {}).items():
                    (target.parent / name).write_text(text)
            return code, "", error

        stack.run = run
        stack.watch = lambda done, stalled: None
        answer = stack.job_with_flag(["-c", "pass"], None, None, {}, 300, SEALED, flag_folder)
        return answer, [*log, *record.commands], commands, sorted(os.listdir(folder))

    def test_a_flag_file_that_is_exactly_the_closed_document_gives_the_flag(self) -> None:
        for text in (json.dumps(GOOD), json.dumps({"exit": 124, "stderr_is_stop_line": True}), json.dumps({"stderr_is_stop_line": False, "exit": 1}) + "\n"):
            with self.subTest(text=text):
                answer, _, _, _ = self.job(text)
                self.assertEqual((124, {}, json.loads(text)), answer[:3])

    def test_a_missing_or_malformed_or_odd_flag_file_gives_none(self) -> None:
        for label, text in (
            ("no file", None),
            ("empty", ""),
            ("not json", "stderr_is_stop_line"),
            ("cut short", '{"stderr_is_stop_line": true, "exit"'),
            ("a list", "[true, 124]"),
            ("a number", "124"),
            ("an extra key", json.dumps({**GOOD, "line": "x"})),
            ("a key missing", json.dumps({"stderr_is_stop_line": True})),
            ("another key", json.dumps({"stderr_is_stop_line": True, "code": 124})),
            ("the flag as 1", '{"stderr_is_stop_line": 1, "exit": 124}'),
            ("the flag as a text", '{"stderr_is_stop_line": "true", "exit": 124}'),
            ("the flag as null", '{"stderr_is_stop_line": null, "exit": 124}'),
            ("the exit as a Boolean", '{"stderr_is_stop_line": true, "exit": true}'),
            ("the exit as a text", '{"stderr_is_stop_line": true, "exit": "124"}'),
            ("the exit as a float", '{"stderr_is_stop_line": true, "exit": 124.0}'),
            ("a key twice", '{"stderr_is_stop_line": false, "stderr_is_stop_line": true, "exit": 124}'),
            ("not text", b"\xff\xfe\x00"),
            ("too large", json.dumps({**GOOD, "pad": "x" * 100_000})),
        ):
            with self.subTest(label):
                answer, _, _, _ = self.job(text)
                self.assertEqual(124, answer[0])
                self.assertIsNone(answer[2])

    def test_a_second_file_in_the_flag_folder_gives_none(self) -> None:
        answer, _, _, _ = self.job(json.dumps(GOOD), extra={"other.json": "{}"})
        self.assertIsNone(answer[2])

    def test_the_standard_error_of_the_run_is_never_read_so_a_stop_line_there_without_a_flag_file_gives_none(self) -> None:
        for error in ("silver_cases: timeout\n", "silver_cases: timeout\nsilver_cases: timeout\n", ""):
            with self.subTest(error=repr(error)):
                answer, _, _, _ = self.job(None, error=error)
                self.assertIsNone(answer[2])

    def test_a_flag_file_decides_whatever_the_standard_error_of_the_run_holds(self) -> None:
        for error in ("WARN[0000] a variable is not set\nsilver_cases: boom\n", "Traceback (most recent call last):\n", "silver_cases: timeout\n"):
            with self.subTest(error=repr(error)):
                answer, _, _, _ = self.job(json.dumps(GOOD), error=error)
                self.assertEqual(GOOD, answer[2])

    def test_the_log_and_the_commands_never_hold_a_text_of_standard_error(self) -> None:
        planted = "planted diagnostic text that names another place"
        answer, kept, _, _ = self.job(json.dumps(GOOD), error=planted + "\n")
        self.assertNotIn(planted, json.dumps([answer, kept]))

    def test_the_flag_folder_is_a_second_folder_mounted_at_probe_and_is_removed_after_the_job(self) -> None:
        _, _, (command,), left = self.job(json.dumps(GOOD))
        self.assertNotEqual(mounted(command, "/probe"), mounted(command, "/result"))
        self.assertEqual(["data"], left)

    def test_a_call_without_the_flag_folder_mounts_none_and_gives_no_flag(self) -> None:
        answer, _, (command,), _ = self.job(json.dumps(GOOD), flag_folder=False)
        self.assertIsNone(answer[2])
        self.assertFalse([word for word in command if word.endswith(":/probe")])

    def test_job_gives_the_text_empty_and_mounts_no_flag_folder(self) -> None:
        module = runner("silver_cases")
        folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="stack_flag_")))
        (folder / "data").mkdir()
        commands: list[list[str]] = []
        stack = module.Stack(folder / "compose", "project", folder / "data", Record(feature="t", slug="t", rerun="none"), [])

        def run(command: list[str], limit: float, **_: Any) -> tuple[int, str, str]:
            commands.append(command)
            return 124, "", "silver_cases: timeout\n"

        stack.run = run
        stack.watch = lambda done, stalled: None
        answer = stack.job(["-c", "pass"], None, None, {}, 300, SEALED)
        self.assertEqual((124, {}, ""), answer[:3])
        self.assertFalse([word for word in commands[0] if word.endswith(":/probe")])

    def test_a_probe_that_exits_1_or_0_with_a_valid_flag_and_no_result_file_gives_the_flag_so_the_case_can_record_the_failure(self) -> None:
        """The probe program ends with the child's exit code, and 1 when the child outlasted its limit. The job under test wrote no result file in that case."""
        for code, flag in ((1, {"stderr_is_stop_line": False, "exit": 1}), (0, {"stderr_is_stop_line": True, "exit": 0})):
            with self.subTest(f"exit {code}"):
                answer, _, commands, _ = self.job(json.dumps(flag), code=code)
                self.assertEqual((code, {}, flag), answer[:3])
                self.assertEqual(1, len(commands))

    def test_a_probe_that_exits_1_with_no_flag_file_gives_no_flag_and_does_not_stop_the_run(self) -> None:
        answer, _, _, _ = self.job(None, code=1)
        self.assertEqual((1, {}, None), answer[:3])

    def test_a_job_with_no_flag_folder_that_exits_1_and_leaves_no_result_file_still_fails_the_run(self) -> None:
        with self.assertRaises(RuntimeError) as caught:
            self.job(None, code=1, flag_folder=False)
        self.assertIn("the job left no result file", str(caught.exception))

    def test_a_flag_that_is_refused_is_not_retried_and_does_not_fail_the_run(self) -> None:
        answer, _, commands, _ = self.job("not json")
        self.assertIsNone(answer[2])
        self.assertEqual(1, len(commands))


class ComposeProgress(unittest.TestCase):
    """Compose writes its own container progress to the standard error of ``docker compose run``. The probe asks for none, which keeps that stream short; the
    flag does not depend on it, because the stream is not read."""

    def stack(self) -> tuple[Any, list[list[str]]]:
        module = runner("silver_cases")
        folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="compose_progress_")))
        (folder / "data").mkdir()
        commands: list[list[str]] = []
        stack = module.Stack(folder / "compose", "project", folder / "data", Record(feature="t", slug="t", rerun="none"), [])

        def run(command: list[str], limit: float, **_: Any) -> tuple[int, str, str]:
            commands.append(command)
            return 124, "", " Container run-postgres-1  Running\n"

        stack.run = run
        stack.watch = lambda done, stalled: None
        return stack, commands

    def test_the_run_command_asks_for_no_progress_before_its_subcommand(self) -> None:
        stack, commands = self.stack()
        stack.job_with_flag(["-c", "pass"], None, None, {}, 300, SEALED, True)
        (command,) = commands
        self.assertLess(command.index("--progress"), command.index("run"))
        self.assertEqual("quiet", command[command.index("--progress") + 1])

    def test_every_job_of_the_stack_runs_without_progress(self) -> None:
        stack, commands = self.stack()
        stack.job(["-c", "pass"], None, None, {}, 300, SEALED)
        stack.job(["/data/stand_in.py", "x", "y"], None, None, None, 300, None)
        self.assertEqual(2, len(commands))
        for command in commands:
            self.assertIn(["--progress", "quiet"], [command[index : index + 2] for index in range(len(command) - 1)])


if __name__ == "__main__":
    unittest.main()
