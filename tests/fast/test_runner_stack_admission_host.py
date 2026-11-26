"""The host mode of the admission runner runs on the build and test host, from the runner folder that the deploy made, as the other native host runners do.

The runner stays untracked, so each test skips when it is not on this machine. The admission command, the checkout records and the memory reading are
stand-ins. No SSH, Docker or host runs here.

Failure modes covered here:

- Run on the owner's computer, the runner only says that it needs a Linux host, and not how to run it on the host.
- On the host the revision is asked of Git, which exits 128 in the runner folder (it holds no Git history).
- A runner folder whose files differ from its records runs anyway.
- On the host the admission command lists the running services with Docker as the SSH user, who may not reach the Docker daemon, or root writes bytecode
  into the SSH user's runner folder.
- The mode with stated figures starts to need root or the host's records.
- The placement refuses the runner because a module that it imports is not named with it, or a module that it executes is not checked against its record.
"""

from __future__ import annotations

import ast
import contextlib
import io
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock

from scripts.infra import runner_place
from scripts.infra.host import FixedHostError
from tests.e2e.run_record import Record
from tests.fast.runner_import import import_runner

ROOT = Path(__file__).resolve().parents[2]
PLACED = ("tests/e2e/stack_admission.py", "tests/e2e/host_checkout.py")


class HostMode(unittest.TestCase):
    def setUp(self) -> None:
        self.module: ModuleType = import_runner("stack_admission")
        self.records: list[Record] = []
        self.blocked: list[str] = []
        self.commands: list[tuple[str, list[str]]] = []

    def run_main(self, *arguments: str, on_host: bool, checkout: Any = None) -> None:
        def close(record: Record, log: list[str], *, blocked: str = "", recheck: Any = None) -> int:
            self.records.append(record)
            self.blocked.append(blocked)
            return 0

        def command(program: str, args: list[str], **options: Any) -> SimpleNamespace:
            self.commands.append((program, list(args)))
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        git = mock.Mock(return_value=("a" * 40, "b" * 64))
        self.git = git
        self.checkout = mock.Mock(side_effect=checkout) if checkout is not None else mock.Mock(return_value=("c" * 40, "d" * 64))
        with (
            mock.patch.object(Record, "close", close),
            mock.patch.object(self.module, "run_command", command),
            mock.patch.object(self.module, "on_a_host", return_value=on_host),
            mock.patch.object(self.module, "read_meminfo_available_mib", return_value=20000),
            mock.patch.object(self.module, "recorded_state", git),
            mock.patch.object(self.module, "checkout_state", self.checkout),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            self.module.main(list(arguments))

    def test_on_another_computer_the_blocked_reason_names_how_to_run_it_on_the_host(self) -> None:
        self.run_main("--host", on_host=False)
        reason = self.blocked[-1]
        self.assertIn(f'make host-runners-place RUNNERS="{" ".join(PLACED)}"', reason)
        self.assertIn("cd /opt/platform/runner/<commit> && .venv/bin/python -m tests.e2e.stack_admission --host", reason)
        self.assertEqual(self.commands, [])
        self.checkout.assert_not_called()

    def test_on_the_host_the_revision_comes_from_the_checked_records_and_not_from_git(self) -> None:
        self.run_main("--host", on_host=True)
        self.git.assert_not_called()
        self.checkout.assert_called_once_with(self.module.ROOT, True, self.module.RUNNER_FILES)
        self.assertEqual((self.records[-1].revision, self.records[-1].tree_sha256), ("c" * 40, "d" * 64))
        self.assertEqual(self.blocked[-1], "")

    def test_a_checkout_that_differs_from_its_records_is_blocked_and_runs_nothing(self) -> None:
        self.run_main("--host", on_host=True, checkout=self.module.HostCheckoutError("the checkout does not equal its records: x"))
        self.assertIn("does not equal its records", self.blocked[-1])
        self.assertEqual(self.commands, [])

    def test_on_the_host_the_admission_command_runs_as_root_and_writes_no_bytecode(self) -> None:
        self.run_main("--host", on_host=True)
        self.assertEqual(len(self.commands), 2)
        for program, arguments in self.commands:
            self.assertEqual(program, "sudo")
            self.assertEqual(arguments[:5], ["-n", str(self.module.ROOT / ".venv" / "bin" / "python"), "-B", "-m", "scripts.stack.admission"])
            self.assertNotIn("--running", arguments)
            self.assertNotIn("--available-mib", arguments)

    def test_the_stated_figures_mode_needs_no_root_and_no_host_record(self) -> None:
        self.run_main(on_host=False)
        self.git.assert_called_once()
        self.checkout.assert_not_called()
        self.assertEqual({program for program, _ in self.commands}, {".venv/bin/python"})

    def test_the_placement_takes_the_two_named_files_and_refuses_the_runner_alone(self) -> None:
        texts = {name: (ROOT / name).read_text() for name in PLACED}
        with contextlib.redirect_stdout(io.StringIO()):
            runner_place.check_imports(texts, "HEAD", ROOT)
            with self.assertRaises(FixedHostError):
                runner_place.check_imports({PLACED[0]: texts[PLACED[0]]}, "HEAD", ROOT)

    def test_every_module_of_tests_that_the_runner_executes_is_checked_against_its_record(self) -> None:
        found: set[str] = set()
        waiting = [PLACED[0]]
        while waiting:
            name = waiting.pop()
            if name in found:
                continue
            found.add(name)
            parts = Path(name).parts[:-1]
            found.update("/".join((*parts[:end], "__init__.py")) for end in range(1, len(parts) + 1))
            for node in ast.walk(ast.parse((ROOT / name).read_text())):
                modules: list[str] = []
                if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("tests"):
                    modules = [node.module, *(f"{node.module}.{alias.name}" for alias in node.names)]
                for module in modules:
                    candidate = f"{module.replace('.', '/')}.py"
                    if (ROOT / candidate).is_file():
                        waiting.append(candidate)
        self.assertLessEqual(found, set(self.module.RUNNER_FILES))


if __name__ == "__main__":
    unittest.main()
