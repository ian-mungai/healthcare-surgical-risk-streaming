"""Each host runner checks each of its files against the record of the file's kind, in a checkout that is laid out as the host's is.

The deploy writes ``runner_manifest.json`` for the files of ``git archive``: tracked files only. ``make host-runners-place`` sends the Git-ignored runner
files, each directly under ``tests/e2e/``, and writes their hashes in ``placed_runners.json`` with the commit. Each probe here runs a runner's own ``main`` in a
new process, in a scratch checkout with both records in those forms, and stops it at the first step that needs a download or Docker. Only the probe's own
marker lines are read: the output of a runner is never shown. The runners are untracked, so the tests run only where they are on disk. Nothing starts.

Failure modes covered here:

- A placed runner file (the shared helper first of all) was looked up in the manifest, which cannot hold it, so a host run stopped before any setup.
- A placement record of another commit, a file that differs from its hash, a link, or a missing record was accepted.
- A tracked file was accepted from the placement record, or a placed file from the manifest.
- A native Gold or Silver host run started with a runner file that no record vouches for.
- A runner imports a module of ``tests/`` that its own list does not hold, so that module is never checked; or the list of tracked runner modules differs
  from what Git tracks, so a file is looked up in the wrong record.
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.process import run_command
from tests.fast.runner_import import import_runner

ROOT = Path(__file__).resolve().parents[2]
E2E = ROOT / "tests" / "e2e"
MANIFEST = "runner_manifest.json"
PLACED = "placed_runners.json"
COMMIT = "a" * 40
NATIVE = ("gold_dimensions", "silver_signals", "silver_hypotension")
RUNNERS = ("host_checkout", "stream_host", "stream_features", "stack_replay", "pinned_images", "silver_cases", *NATIVE)
PRESENT = all((E2E / f"{name}.py").is_file() for name in RUNNERS) and (E2E / "stream_stack").is_dir()
PROBE_SECONDS = 120

# The probe of the streaming runner: its real main, stopped where the download would start.
STREAM_PROBE = """
import json, sys
try:
    from tests.e2e import stream_host as runner
    original = runner.source_problems

    def seen(*arguments, **named):
        found = original(*arguments, **named)
        print("PROBE problems " + json.dumps(found))
        return found

    def stop(*arguments, **named):
        print("PROBE gate passed")
        raise runner.Blocked("the probe stops at the download")

    runner.source_problems = seen
    runner.fill_landed_cache = stop
    print(f"PROBE exit {runner.main(['--landed-cache', sys.argv[1]])}")
except Exception as error:
    print(f"PROBE crash {type(error).__name__}")
"""

# The probe of a native runner: its real main in host mode, stopped where the run would start its services.
NATIVE_PROBE = """
import sys
try:
    import importlib

    runner = importlib.import_module("tests.e2e." + sys.argv[1])

    def stop(*arguments, **named):
        print("PROBE gate passed")
        raise SystemExit(42)

    runner.run_local = stop
    print(f"PROBE exit {runner.main()}")
except SystemExit as error:
    print(f"PROBE stopped {error.code}")
except Exception as error:
    print(f"PROBE crash {type(error).__name__}")
"""


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def tracked_files() -> list[str]:
    own = {name: value for name, value in os.environ.items() if not name.startswith("GIT_")}
    listed = run_command("git", ["-C", str(ROOT), "ls-files"], timeout=60, env=own, check=True)
    return [name for name in listed.stdout.splitlines() if (ROOT / name).is_file()]


class HostLayout:
    """A scratch checkout as the host holds it: the tracked files with their manifest, and the ignored runner files with the record of the direct ones."""

    def __init__(self, folder: Path) -> None:
        self.root = folder / "checkout"
        self.tracked = tracked_files()
        self.direct = sorted(f"tests/e2e/{path.name}" for path in E2E.glob("*.py") if f"tests/e2e/{path.name}" not in self.tracked)
        probes = sorted(f"tests/e2e/stream_stack/{path.name}" for path in (E2E / "stream_stack").iterdir() if path.is_file())
        for name in (*self.tracked, *self.direct, *probes):
            (self.root / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, self.root / name)
        self.manifest = {name: sha256(self.root / name) for name in self.tracked}
        self.placed = {name: sha256(self.root / name) for name in self.direct}

    def write(self, manifest: dict[str, str] | None = None, placed: dict[str, str] | None = None, placed_commit: str = COMMIT) -> None:
        for name, commit, files in ((MANIFEST, COMMIT, manifest or self.manifest), (PLACED, placed_commit, placed or self.placed)):
            (self.root / name).unlink(missing_ok=True)
            (self.root / name).write_text(json.dumps({"commit": commit, "files": files}, indent=2) + "\n")

    def run(self, script: str, argument: str, extra: dict[str, str]) -> tuple[list[str], str]:
        """The probe's marker lines and the standard error of one probe process, with no cloud access and no setting of the caller's that the runners read."""
        environment = {"PATH": os.environ["PATH"], "HOME": str(self.root.parent), "PYTHONDONTWRITEBYTECODE": "1"} | extra
        environment |= {"AWS_PROFILE": "no-such-profile", "AWS_CONFIG_FILE": os.devnull, "AWS_SHARED_CREDENTIALS_FILE": os.devnull}
        done = run_command(sys.executable, ["-c", script, argument], cwd=self.root, env=environment, timeout=PROBE_SECONDS)
        return [line.removeprefix("PROBE ") for line in done.stdout.splitlines() if line.startswith("PROBE ")], done.stderr


@unittest.skipUnless(PRESENT, "the temporary runners are not in this checkout")
class LayoutCase(unittest.TestCase):
    layout: HostLayout

    @classmethod
    def setUpClass(cls) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="host_layout_")
        cls.addClassCleanup(scratch.cleanup)
        cls.layout = HostLayout(Path(scratch.name))

    def setUp(self) -> None:
        self.layout.write()

    def change(self, name: str) -> None:
        """Add a comment line to one file of the checkout, and put the file back after the test."""
        path = self.layout.root / name
        before = path.read_bytes()
        self.addCleanup(path.write_bytes, before)
        path.write_bytes(before + b"# edited on the host\n")

    def link(self, name: str) -> None:
        """Replace one file of the checkout with a link to a file of the same bytes outside it, and put the file back after the test."""
        path = self.layout.root / name
        outside = self.layout.root.parent / f"outside_{path.name}"
        path.replace(outside)
        path.symlink_to(outside)

        def restore() -> None:
            path.unlink(missing_ok=True)
            outside.replace(path)

        self.addCleanup(restore)

    def moved(self, name: str, source: str) -> tuple[dict[str, str], dict[str, str]]:
        """The two maps with the entry of ``name`` moved from the record ``source`` to the other one."""
        manifest, placed = dict(self.layout.manifest), dict(self.layout.placed)
        giver, taker = (manifest, placed) if source == MANIFEST else (placed, manifest)
        taker[name] = giver.pop(name)
        return manifest, placed


class StreamStartup(LayoutCase):
    def probe(self) -> tuple[list[str] | None, bool, str]:
        """The problems that the startup gate listed (None when it did not reach the list), whether the gate passed, and the outcome in the run's record."""
        runs = self.layout.root / "runs"
        shutil.rmtree(runs, ignore_errors=True)
        lines, _ = self.layout.run(STREAM_PROBE, str(self.layout.root.parent / "landed"), {})
        self.assertFalse([line for line in lines if line.startswith("crash ")], "the probe of the streaming runner crashed")
        problems = next((json.loads(line.removeprefix("problems ")) for line in lines if line.startswith("problems ")), None)
        records = list(runs.glob("stream_host_*/record.json"))
        self.assertEqual(1, len(records), "the run wrote one record")
        return problems, "gate passed" in lines, str(json.loads(records[0].read_text())["outcome"])

    def unplaceable(self) -> list[str]:
        """The files that no record can hold today: the placement command sends only ``.py`` files directly under ``tests/e2e/``."""
        stream = import_runner("stream_host")
        return [f"{name}: {PLACED} has no entry for it" for name in stream.runner_files(self.layout.root) if name.startswith("tests/e2e/stream_stack/")]

    def test_the_placed_runners_pass_and_only_the_files_that_the_placement_command_cannot_send_are_left(self) -> None:
        problems, passed, outcome = self.probe()
        self.assertEqual(sorted(self.unplaceable()), sorted(problems or []), outcome)
        self.assertEqual(8, len(problems or []))
        self.assertFalse(passed)

    def assert_one_more_problem(self, wanted: str) -> None:
        problems, passed, outcome = self.probe()
        self.assertEqual(sorted([*self.unplaceable(), wanted]), sorted(problems or []), outcome)
        self.assertFalse(passed)

    def test_a_placed_runner_that_differs_from_the_placement_record_is_named(self) -> None:
        self.change("tests/e2e/stream_features.py")
        self.assert_one_more_problem(f"tests/e2e/stream_features.py: it differs from {PLACED}")

    def test_a_tracked_file_that_differs_from_the_manifest_is_named(self) -> None:
        self.change("tests/fast/host_mode.py")
        self.assert_one_more_problem(f"tests/fast/host_mode.py: it differs from {MANIFEST}")

    def test_a_tracked_file_that_only_the_placement_record_holds_is_refused(self) -> None:
        self.layout.write(*self.moved("tests/e2e/run_record.py", MANIFEST))
        self.assert_one_more_problem(f"tests/e2e/run_record.py: {MANIFEST} has no entry for it")

    def test_a_placed_file_that_only_the_manifest_holds_is_refused(self) -> None:
        self.layout.write(*self.moved("tests/e2e/host_checkout.py", PLACED))
        self.assert_one_more_problem(f"tests/e2e/host_checkout.py: {PLACED} has no entry for it")

    def test_a_placed_file_in_neither_record_is_refused(self) -> None:
        self.layout.write(placed={name: value for name, value in self.layout.placed.items() if name != "tests/e2e/stack_replay.py"})
        self.assert_one_more_problem(f"tests/e2e/stack_replay.py: {PLACED} has no entry for it")

    def test_a_placed_runner_that_is_a_link_is_refused(self) -> None:
        self.link("tests/e2e/stack_replay.py")
        self.assert_one_more_problem("tests/e2e/stack_replay.py: it is a link, is reached through a link, is not a plain file or cannot be read")

    def assert_blocked_before_the_files(self, words: str) -> None:
        problems, passed, outcome = self.probe()
        self.assertEqual((None, False), (problems, passed), outcome)
        self.assertEqual(f"blocked: {words}", outcome)

    def test_a_placement_record_of_another_commit_blocks_the_run(self) -> None:
        self.layout.write(placed_commit="b" * 40)
        self.assert_blocked_before_the_files(f"the placement record {PLACED} names another commit than {MANIFEST}: place the runners again")

    def test_a_missing_placement_record_blocks_the_run(self) -> None:
        (self.layout.root / PLACED).unlink()
        self.assert_blocked_before_the_files(f"the placement record {PLACED} is missing in the checkout root: place the runners with make host-runners-place")

    def test_a_record_that_is_a_link_blocks_the_run(self) -> None:
        for name, what in ((PLACED, "placement record"), (MANIFEST, "source manifest")):
            with self.subTest(record=name):
                self.layout.write()
                outside = self.layout.root.parent / f"outside_{name}"
                (self.layout.root / name).replace(outside)
                (self.layout.root / name).symlink_to(outside)
                self.assert_blocked_before_the_files(f"the {what} {name} is a link, is not a plain file or cannot be read")


class NativeStartup(LayoutCase):
    def probe(self, runner: str) -> tuple[list[str], str]:
        data = str(self.layout.root.parent / "data")
        settings = {f"{runner.upper()}_HOST": "1", "LANDED_CACHE": data, "SIGNAL_CASE_CACHE": data, "CLINICAL_CSV": data, "LAB_CSV": data}
        return self.layout.run(NATIVE_PROBE, runner, settings)

    def test_each_runner_reaches_its_services_in_a_checkout_laid_out_as_the_hosts(self) -> None:
        for runner in NATIVE:
            with self.subTest(runner=runner):
                lines, _ = self.probe(runner)
                self.assertEqual(["gate passed", "stopped 42"], lines)

    def assert_refused(self, runner: str, words: str) -> None:
        lines, errors = self.probe(runner)
        self.assertEqual(["exit 2"], lines, "the runner went on past a file or a record that it must refuse")
        self.assertEqual(f"{runner}: {words}\n", errors)

    def test_a_placed_file_that_differs_from_the_placement_record_stops_each_runner_before_any_work(self) -> None:
        self.change("tests/e2e/pinned_images.py")
        for runner in NATIVE:
            with self.subTest(runner=runner):
                self.assert_refused(runner, f"the checkout does not equal its records: tests/e2e/pinned_images.py: it differs from {PLACED}")

    def test_a_tracked_file_that_differs_from_the_manifest_stops_each_runner_before_any_work(self) -> None:
        self.change("tests/fast/silver_accept.py")
        for runner in NATIVE:
            with self.subTest(runner=runner):
                self.assert_refused(runner, f"the checkout does not equal its records: tests/fast/silver_accept.py: it differs from {MANIFEST}")

    def test_a_placement_record_of_another_commit_stops_each_runner_before_any_work(self) -> None:
        self.layout.write(placed_commit="b" * 40)
        for runner in NATIVE:
            with self.subTest(runner=runner):
                self.assert_refused(runner, f"the placement record {PLACED} names another commit than {MANIFEST}: place the runners again")


def tests_closure(entry: str) -> set[str]:
    """The files under ``tests/`` that importing ``tests.e2e.<entry>`` executes, read from the import statements."""

    def file_of(module: str) -> Path | None:
        for path in (ROOT / f"{module.replace('.', '/')}.py", ROOT / module.replace(".", "/") / "__init__.py"):
            if path.is_file():
                return path
        return None

    found: dict[str, Path] = {}
    waiting = [f"tests.e2e.{entry}"]
    while waiting:
        parts = waiting.pop().split(".")
        for end in range(1, len(parts) + 1):
            module = ".".join(parts[:end])
            path = file_of(module) if module.split(".")[0] == "tests" else None
            if path is None or module in found:
                continue
            found[module] = path
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.Import):
                    waiting += [item.name for item in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                    waiting += [node.module, *(f"{node.module}.{item.name}" for item in node.names)]
    return {path.relative_to(ROOT).as_posix() for path in found.values()}


@unittest.skipUnless(PRESENT, "the temporary runners are not in this checkout")
class FileLists(unittest.TestCase):
    def test_each_native_runner_lists_every_module_of_tests_that_it_imports(self) -> None:
        for name in NATIVE:
            with self.subTest(runner=name):
                self.assertEqual(sorted(tests_closure(name)), sorted(import_runner(name).RUNNER_FILES))

    def test_the_streaming_runner_lists_every_module_of_tests_that_it_imports(self) -> None:
        self.assertEqual(sorted(tests_closure("stream_host")), sorted(import_runner("stream_host").RUNNER_MODULES))

    def test_the_runner_modules_that_the_helper_takes_as_tracked_are_the_ones_that_git_tracks(self) -> None:
        tracked = {name for name in tracked_files() if name.startswith("tests/e2e/")}
        self.assertEqual(tracked, set(import_runner("host_checkout").TRACKED_E2E))


if __name__ == "__main__":
    unittest.main()
