"""The host mode of the admission runner runs on the build and test host, from the runner folder that the deploy made, as the other native host runners do.

The runner stays untracked, so each test skips when it is not on this machine. The admission command and the memory reading are stand-ins. The runner
folder is a copy of every tracked file and of the two placed files with no Git history, with the records that the deploy and the placement write, and the
runner's own record checks read it. No SSH, Docker or host runs here.

Failure modes covered here:

- Run on the owner's computer, the runner only says that it needs a Linux host, and not how to run it on the host.
- On the host the revision is asked of Git, which exits 128 in the runner folder (it holds no Git history).
- A runner folder whose files differ from its records runs anyway: a placed runner, or any tracked file, such as the admission command that runs as root,
  is changed, missing or replaced by a link, and the record names the clean commit.
- A blocked run saves its record before its clean-up, so the record says that the clean-up is pending.
- On the host the admission command lists the running services with Docker as the SSH user, who may not reach the Docker daemon, or root writes bytecode
  into the SSH user's runner folder.
- The mode with stated figures starts to need root or the host's records.
- The placement refuses the runner because a module that it imports is not named with it, or a module that it executes is not checked against its record.
"""

from __future__ import annotations

import ast
import contextlib
import hashlib
import io
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock

from scripts.infra import runner_place
from scripts.infra.host import FixedHostError
from scripts.process import run_command
from tests.e2e.run_record import Record
from tests.fast.runner_import import import_runner

ROOT = Path(__file__).resolve().parents[2]
PLACED = ("tests/e2e/stack_admission.py", "tests/e2e/host_checkout.py")
COMMIT = "c" * 40
TRACKED_CHANGES = ("scripts/stack/admission.py", "scripts/stack/limits.py", "scripts/process.py", "tests/e2e/run_record.py")


def digests(folder: Path, names: list[str]) -> dict[str, str]:
    return {name: hashlib.sha256((folder / name).read_bytes()).hexdigest() for name in names}


class HostMode(unittest.TestCase):
    folder: Path
    tracked: dict[str, str]

    @classmethod
    def setUpClass(cls) -> None:
        import_runner("stack_admission")
        scratch = tempfile.TemporaryDirectory(prefix="admission_runner_folder_")
        cls.addClassCleanup(scratch.cleanup)
        cls.folder = Path(scratch.name).resolve()
        listed = run_command("git", ["ls-files", "-z"], cwd=ROOT, timeout=60, check=True).stdout
        names = [name for name in listed.split("\0") if name]
        for name in (*names, *PLACED):
            (cls.folder / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, cls.folder / name)
        cls.tracked = digests(cls.folder, names)
        (cls.folder / "runner_manifest.json").write_text(json.dumps({"commit": COMMIT, "files": cls.tracked}))
        (cls.folder / "placed_runners.json").write_text(json.dumps({"commit": COMMIT, "files": digests(cls.folder, list(PLACED))}))

    def setUp(self) -> None:
        self.module: ModuleType = import_runner("stack_admission")
        self.saved: list[SimpleNamespace] = []
        self.commands: list[tuple[str, list[str]]] = []
        self.git = mock.Mock(return_value=("a" * 40, "b" * 64))

    def keep_original(self, name: str) -> Path:
        """The file ``name`` of the runner folder, put back as it was after the test."""
        target = self.folder / name
        original = target.read_bytes()

        def restore() -> None:
            target.unlink(missing_ok=True)
            target.write_bytes(original)

        self.addCleanup(restore)
        return target

    def run_main(self, *arguments: str, on_host: bool) -> SimpleNamespace:
        """``main`` in the runner folder, and what the record held at the moment that it was saved."""

        def close(record: Record, log: list[str], *, blocked: str = "", recheck: Any = None) -> int:
            self.saved.append(
                SimpleNamespace(
                    blocked=blocked,
                    revision=record.revision,
                    digest=record.tree_sha256,
                    cleanup=record.cleanup,
                    cases={item.case: item.passed for item in record.observations},
                )
            )
            return 0

        def command(program: str, args: list[str], **options: Any) -> SimpleNamespace:
            self.commands.append((program, list(args)))
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        with (
            mock.patch.object(Record, "close", close),
            mock.patch.object(self.module, "ROOT", self.folder if arguments else ROOT),
            mock.patch.object(self.module, "run_command", command),
            mock.patch.object(self.module, "on_a_host", return_value=on_host),
            mock.patch.object(self.module, "read_meminfo_available_mib", return_value=20000),
            mock.patch.object(self.module, "recorded_state", self.git),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            self.module.main(list(arguments))
        self.assertEqual(len(self.saved), 1)
        return self.saved.pop()

    def assert_blocked_and_cleaned(self, saved: SimpleNamespace, *words: str) -> None:
        for word in words:
            self.assertIn(word, saved.blocked)
        self.assertEqual(self.commands, [])
        self.assertEqual((saved.cleanup, saved.cases.get("cleanup")), ("scratch folder removed", True))

    def test_on_another_computer_the_blocked_reason_names_how_to_run_it_on_the_host(self) -> None:
        saved = self.run_main("--host", on_host=False)
        self.assert_blocked_and_cleaned(
            saved,
            f'make host-runners-place RUNNERS="{" ".join(PLACED)}"',
            "cd /opt/platform/runner/<commit> && .venv/bin/python -m tests.e2e.stack_admission --host",
        )

    def test_on_the_host_the_revision_comes_from_the_checked_records_and_not_from_git(self) -> None:
        saved = self.run_main("--host", on_host=True)
        self.git.assert_not_called()
        self.assertEqual(saved.blocked, "")
        self.assertEqual((saved.revision, saved.digest), (COMMIT, hashlib.sha256(json.dumps(self.tracked, sort_keys=True).encode()).hexdigest()))
        self.assertEqual(len(self.commands), 2)

    def test_a_changed_tracked_file_is_blocked_before_any_command_and_no_record_names_the_commit(self) -> None:
        for name in TRACKED_CHANGES:
            with self.subTest(name=name):
                target = self.keep_original(name)
                original = target.read_bytes()
                target.write_bytes(original + b"\n# changed on the host\n")
                saved = self.run_main("--host", on_host=True)
                self.assert_blocked_and_cleaned(saved, "does not equal its records", name)
                self.assertEqual((saved.revision, saved.digest), ("no commit", ""))
                target.write_bytes(original)

    def test_a_tracked_file_that_no_module_of_the_runner_imports_is_checked_too(self) -> None:
        self.keep_original("compose/limits.yml").write_text("host_reserve_mib: 1\n")
        self.assert_blocked_and_cleaned(self.run_main("--host", on_host=True), "compose/limits.yml")

    def test_a_tracked_file_that_is_missing_or_a_link_is_blocked(self) -> None:
        target = self.keep_original("scripts/stack/admission.py")
        elsewhere = self.keep_original("scripts/stack/limits.py")
        target.unlink()
        self.assert_blocked_and_cleaned(self.run_main("--host", on_host=True), "scripts/stack/admission.py", "missing")
        target.symlink_to(elsewhere)
        self.assert_blocked_and_cleaned(self.run_main("--host", on_host=True), "scripts/stack/admission.py", "link")

    def test_a_changed_placed_runner_is_blocked(self) -> None:
        for name in PLACED:
            with self.subTest(name=name):
                target = self.keep_original(name)
                original = target.read_bytes()
                target.write_bytes(original + b"\n# changed on the host\n")
                self.assert_blocked_and_cleaned(self.run_main("--host", on_host=True), name, "placed_runners.json")
                target.write_bytes(original)

    def test_a_runner_folder_with_no_source_manifest_is_blocked(self) -> None:
        self.keep_original("runner_manifest.json").unlink()
        self.assert_blocked_and_cleaned(self.run_main("--host", on_host=True), "runner_manifest.json")

    def test_on_the_host_the_admission_command_runs_as_root_and_writes_no_bytecode(self) -> None:
        self.run_main("--host", on_host=True)
        self.assertEqual(len(self.commands), 2)
        for program, arguments in self.commands:
            self.assertEqual(program, "sudo")
            self.assertEqual(arguments[:5], ["-n", str(self.folder / ".venv" / "bin" / "python"), "-B", "-m", "scripts.stack.admission"])
            self.assertNotIn("--running", arguments)
            self.assertNotIn("--available-mib", arguments)

    def test_the_stated_figures_mode_needs_no_root_and_no_host_record(self) -> None:
        saved = self.run_main(on_host=False)
        self.git.assert_called_once()
        self.assertEqual((saved.revision, saved.cleanup), ("a" * 40, "scratch folder removed"))
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
