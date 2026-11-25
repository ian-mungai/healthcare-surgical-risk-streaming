"""The Silver cases runner's deadline probe passes only on the job's own stop line, and only a flag of the match is recorded.

The server that stalls the catalog and the program that waits for it run in the slow group (``tests/slow/test_silver_cases_stalled_catalog.py``, project
standard Q39). Here the probe case runs on a stack that answers what the test says, and the stack's call is checked with a stand-in for the command that it
starts. Nothing binds a port. The runner is untracked, so each test skips when it is not on this machine.

Failure modes covered here:

- The probe passed on a job that exited 124 with any other diagnostic, or with none, because it never looked at the line that the job writes.
- The text of the diagnostic was kept: the stack gives back the one fixed line when the job's own lines of standard error are exactly that line and nothing
  otherwise, and the run records only whether it matched.
- The probe failed a job that stopped correctly because Docker, Compose or the Python runtime wrote a line of their own beside the stop line (the job's code
  writes nothing else on this path): the check was on the whole stream, not on the job's own lines. It must still fail a stop line that is twice there, a
  second line with the job's prefix (a failed-check code, a usage or settings code) and a traceback.
- The probe passed on a job that exited 124 and left a result file or a container, or on a job that exited with another code.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from tests.e2e.run_record import Record
from tests.fast.test_runner_gold_sealed import runner

LINE = "silver_cases: timeout"


class Probe(unittest.TestCase):
    """The case of ``bounded_failure`` that holds the probe, on a stack that answers what the test says."""

    def stack_of(self, answer: tuple[int, dict[str, Any], str], left: list[str], calls: list[dict[str, Any]]) -> Any:
        def job_with_line(
            module_arguments: list[str],
            mounts: Any,
            reset: Any,
            env: dict[str, str] | None,
            limit: float,
            sealed: tuple[str, str, str] | None,
            stderr_is: str | None,
        ) -> tuple[int, dict[str, Any], str, float]:
            calls.append({"module_arguments": module_arguments, "env": env, "sealed": sealed, "stderr_is": stderr_is})
            return answer[0], answer[1], answer[2], 2.5

        return SimpleNamespace(
            silver=lambda *_, **__: (1, {"diagnostics": [{"code": "branch_missing"}]}, "", 0.1),
            job=lambda *_, **__: (2, {}, "", 0.1),
            job_with_line=job_with_line,
            spark_containers=lambda: left,
        )

    def run_probe(self, answer: tuple[int, dict[str, Any], str], left: list[str]) -> tuple[bool, list[dict[str, Any]], Record]:
        module = runner("silver_cases")
        calls: list[dict[str, Any]] = []
        run = module.Run.__new__(module.Run)
        run.record, run.labels, run.suffix, run.made, run.trunk = Record(feature="t", slug="t", rerun="none"), {}, "x1", [], "trunk_x1"
        run.stack = self.stack_of(answer, left, calls)
        run.bounded_failure()
        return run.record.observations[-1].passed, calls, run.record

    def test_the_probe_runs_the_job_command_line_with_a_deadline_of_2_seconds_and_a_nessie_timeout_of_100_seconds(self) -> None:
        module = runner("silver_cases")
        _, calls, _ = self.run_probe((124, {}, LINE), [])
        self.assertEqual(1, len(calls))
        self.assertEqual(["-c", module.STALL], calls[0]["module_arguments"])
        self.assertEqual({"JOB_DEADLINE_SECONDS": "2", "NESSIE_TIMEOUT_SECONDS": "100"}, module.STALL_ENV)
        self.assertEqual(module.STALL_ENV, calls[0]["env"])
        self.assertIsNotNone(calls[0]["sealed"])

    def test_the_probe_asks_the_stack_to_compare_standard_error_with_the_one_stop_line(self) -> None:
        module = runner("silver_cases")
        self.assertEqual(LINE, module.DEADLINE_LINE)
        _, calls, _ = self.run_probe((124, {}, LINE), [])
        self.assertEqual([LINE], [call["stderr_is"] for call in calls])

    def test_the_probe_passes_only_on_exit_124_with_the_stop_line_no_result_file_and_no_container(self) -> None:
        for label, answer, left, wanted in (
            ("a stop at the deadline", (124, {}, LINE), [], True),
            ("another exit code", (1, {}, LINE), [], False),
            ("a result file", (124, {"diagnostics": []}, LINE), [], False),
            ("a container left", (124, {}, LINE), ["spark"], False),
            ("an unrelated diagnostic", (124, {}, ""), [], False),
            ("a diagnostic that only starts with the line", (124, {}, LINE + " and more"), [], False),
            ("another line", (124, {}, "silver_cases: failed"), [], False),
        ):
            with self.subTest(label):
                self.assertIs(wanted, self.run_probe(answer, left)[0])

    def test_the_record_holds_whether_the_line_matched_and_never_a_text_of_standard_error(self) -> None:
        planted = "planted diagnostic text that names another place"
        answers: list[tuple[tuple[int, dict[str, Any], str], str]] = [((124, {}, LINE), "True"), ((124, {}, planted), "False")]
        for answer, matched in answers:
            with self.subTest(matched):
                _, _, record = self.run_probe(answer, [])
                self.assertNotIn(planted, json.dumps([item.observed for item in record.observations]))
                self.assertIn(f"the diagnostic is the stop line {matched}", record.observations[-1].observed)


class StackDiagnostic(unittest.TestCase):
    """``Stack.job_with_line`` gives back that line only when the job's own lines of standard error are exactly it, and keeps no other text of the stream.

    A line is the job's own when it starts with the job's prefix (``silver_cases: ``), the form of every line that the job writes, or is a traceback. The other
    lines come from Docker, Compose or the Python runtime and are not kept."""

    def job(self, error: str, **keywords: Any) -> tuple[tuple[int, dict[str, Any], str, float], list[str]]:
        module = runner("silver_cases")
        folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="stack_diagnostic_")))
        (folder / "data").mkdir()
        log: list[str] = []
        record = Record(feature="t", slug="t", rerun="none")
        stack = module.Stack(folder / "compose", "project", folder / "data", record, log)
        stack.run = lambda command, limit, **_: (124, "", error)
        stack.watch = lambda done, stalled: None
        sealed = ("cases", "verify", "stalled_catalog")
        answer: tuple[int, dict[str, Any], str, float]
        if keywords:
            answer = stack.job_with_line(["-c", "pass"], None, None, {}, 300, sealed, keywords["stderr_is"])
        else:
            answer = stack.job(["-c", "pass"], None, None, {}, 300, sealed)
        return answer, [*log, *record.commands]

    def test_standard_error_that_is_exactly_the_line_gives_the_line(self) -> None:
        for error in (LINE + "\n", LINE, LINE + "\r\n"):
            with self.subTest(error=repr(error)):
                answer, _ = self.job(error, stderr_is=LINE)
                self.assertEqual((124, {}, LINE), answer[:3])

    def test_the_line_with_lines_of_docker_compose_or_the_runtime_around_it_gives_the_line(self) -> None:
        for error in (
            " Container run-postgres-1  Running\n" + LINE + "\n",
            LINE + "\n Container run-spark-1  Stopped\n",
            "WARN[0000] a variable is not set\n" + LINE + "\nsomething from the runtime\n",
            "silver_labels: timeout\n" + LINE + "\n",
        ):
            with self.subTest(error=repr(error)):
                answer, _ = self.job(error, stderr_is=LINE)
                self.assertEqual((124, {}, LINE), answer[:3])

    def test_any_other_lines_of_the_job_give_nothing(self) -> None:
        for error in (
            "",
            "\n",
            LINE + "\n" + LINE + "\n",
            LINE + "\nsilver_cases: failed\n",
            "silver_cases: internal_error\n" + LINE + "\n",
            "silver_cases: usage\n",
            "Traceback (most recent call last):\n" + LINE + "\n",
            LINE + "\nTraceback (most recent call last):\n",
            "x" + LINE,
            LINE + " x",
            "silver_labels: timeout\n",
            "Traceback: boom\n",
            " Container run-postgres-1  Running\n",
        ):
            with self.subTest(error=repr(error)):
                answer, _ = self.job(error, stderr_is=LINE)
                self.assertEqual((124, {}, ""), answer[:3])

    def test_a_call_without_the_line_gives_no_text_even_when_standard_error_holds_the_line(self) -> None:
        answer, _ = self.job(LINE + "\n")
        self.assertEqual("", answer[2])

    def test_the_log_and_the_commands_never_hold_a_text_of_standard_error(self) -> None:
        planted = "planted diagnostic text that names another place"
        for keywords in ({"stderr_is": LINE}, {}):
            with self.subTest(keywords):
                answer, kept = self.job(planted + "\n", **keywords)
                self.assertEqual("", answer[2])
                self.assertNotIn(planted, json.dumps(kept))


class ComposeProgress(unittest.TestCase):
    """Compose writes its own container progress (``Container x Creating``) to the standard error of ``docker compose run``, before the job's line. The probe
    asks for no progress to keep that stream short; the check on the job's own lines does not depend on it."""

    PROGRESS = " Container run-postgres-1  Running\n Container run-nessie-1  Running\n"

    def compose_like(self, command: list[str], limit: float, **_: Any) -> tuple[int, str, str]:
        """A stand-in for ``docker compose``: its global ``--progress quiet`` (before the subcommand ``run``) silences the progress lines."""
        quiet = "run" in command and any(command[index : index + 2] == ["--progress", "quiet"] for index in range(command.index("run")))
        return 124, "", ("" if quiet else self.PROGRESS) + LINE + "\n"

    def stack(self) -> tuple[Any, list[list[str]]]:
        module = runner("silver_cases")
        folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="compose_progress_")))
        (folder / "data").mkdir()
        commands: list[list[str]] = []
        stack = module.Stack(folder / "compose", "project", folder / "data", Record(feature="t", slug="t", rerun="none"), [])

        def run(command: list[str], limit: float, **keywords: Any) -> tuple[int, str, str]:
            commands.append(command)
            return self.compose_like(command, limit, **keywords)

        stack.run = run
        stack.watch = lambda done, stalled: None
        return stack, commands

    def test_a_timeout_with_compose_progress_lines_still_gives_the_stop_line(self) -> None:
        stack, _ = self.stack()
        answer = stack.job_with_line(["-c", "pass"], None, None, {}, 300, ("cases", "verify", "stalled_catalog"), LINE)
        self.assertEqual((124, {}, LINE), answer[:3])

    def test_the_run_command_asks_for_no_progress_before_its_subcommand_and_the_exact_line_check_stays(self) -> None:
        stack, commands = self.stack()
        stack.job_with_line(["-c", "pass"], None, None, {}, 300, ("cases", "verify", "stalled_catalog"), LINE)
        (command,) = commands
        self.assertLess(command.index("--progress"), command.index("run"))
        self.assertEqual("quiet", command[command.index("--progress") + 1])
        # The check on the job's own lines still decides: a stop line with another line of the job beside it gives nothing.
        stack.run = lambda command, limit, **_: (124, "", LINE + "\nsilver_cases: failed\n")
        self.assertEqual("", stack.job_with_line(["-c", "pass"], None, None, {}, 300, ("cases", "verify", "stalled_catalog"), LINE)[2])

    def test_every_job_of_the_stack_runs_without_progress(self) -> None:
        stack, commands = self.stack()
        stack.job(["-c", "pass"], None, None, {}, 300, ("cases", "verify", "stalled_catalog"))
        stack.job(["/data/stand_in.py", "x", "y"], None, None, None, 300, None)
        self.assertEqual(2, len(commands))
        for command in commands:
            self.assertIn(["--progress", "quiet"], [command[index : index + 2] for index in range(len(command) - 1)])


if __name__ == "__main__":
    unittest.main()
