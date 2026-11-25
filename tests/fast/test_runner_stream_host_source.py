"""The host batch names its revision and checks its own files from the source manifest that the deploy writes in the checkout root, and uses no Git.

The approved host checkout has no Git history. The deploy writes ``runner_manifest.json`` there: ``{"commit": "<40 hex>", "files": {"<path>": "<sha256>"}}``.
The runners are untracked, so the tests run only where they are on disk. Nothing starts.

Failure modes covered here:

- The runner asks Git for its revision or for the tracked files of ``compose/``, which fails (or names another tree) in a checkout with no history.
- The manifest is missing, is not the fixed shape, names a commit that is not 40 hex digits or a digest that is not 64, and the run goes on.
- A file of the runner (its modules, its probes, its overlay) differs from the manifest, has no entry, or is missing, and the run goes on with code that no
  commit holds.
- A probe file that is not in the manifest is copied to the probe folder and run in a container.
- A service file of ``compose/`` differs from the manifest (the runner reads some of them) or its deployed copy does, and the run goes on.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest import mock

from scripts.process import CompletedProcess
from tests.e2e.run_record import ROOT, Record

RUNNERS = (ROOT / "tests" / "e2e" / "stream_host.py", ROOT / "tests" / "e2e" / "stream_features.py")
COMMIT = "0123456789abcdef0123456789abcdef01234567"


def digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def done(code: int = 0, text: str = "", error: str = "") -> CompletedProcess[str]:
    return CompletedProcess(["docker"], code, text, error)


@unittest.skipUnless(all(path.is_file() for path in RUNNERS), "the temporary runners are not in this checkout")
class SourceCase(unittest.TestCase):
    def setUp(self) -> None:
        self.host: ModuleType = importlib.import_module("tests.e2e.stream_host")
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)

    def checkout(self, *, probes: tuple[str, ...] = ("plant_late.py", "read_bounds.py")) -> dict[str, str]:
        """A checkout of a few files whose names are the runner's own, and the manifest's digest of each. Returns the manifest's files."""
        files: dict[str, str] = {}
        names = [name for name in self.host.runner_files(ROOT) if not name.startswith("tests/e2e/stream_stack/")]
        names += [f"tests/e2e/stream_stack/{probe}" for probe in probes] + ["tests/e2e/stream_stack/compose.host.yml"]
        names += ["compose/services/kafka.yml", "compose/kafka/topics.txt"]
        for name in dict.fromkeys(names):
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"content of {name}\n")
            files[name] = digest(f"content of {name}\n")
        return files

    def write_manifest(self, document: Any) -> None:
        (self.root / "runner_manifest.json").write_text(json.dumps(document))

    def good(self) -> dict[str, Any]:
        document = {"commit": COMMIT, "files": self.checkout()}
        self.write_manifest(document)
        return document


class ReadManifest(SourceCase):
    def test_a_good_manifest_gives_the_commit_and_the_digest_of_each_file(self) -> None:
        document = self.good()
        manifest = self.host.read_source_manifest(self.root)
        self.assertEqual(COMMIT, manifest.commit)
        self.assertEqual(document["files"], manifest.files)

    def test_a_missing_manifest_refuses_the_run_and_names_the_file(self) -> None:
        with self.assertRaisesRegex(self.host.Blocked, "runner_manifest.json"):
            self.host.read_source_manifest(self.root)

    def test_a_manifest_of_another_shape_is_refused(self) -> None:
        files = self.checkout()
        documents: tuple[Any, ...] = (
            "not json",
            [],
            {"commit": COMMIT},
            {"files": files},
            {"commit": COMMIT[:39], "files": files},
            {"commit": COMMIT.upper(), "files": files},
            {"commit": 7, "files": files},
            {"commit": COMMIT, "files": []},
            {"commit": COMMIT, "files": {"a.py": "short"}},
            {"commit": COMMIT, "files": {"a.py": 5}},
            {"commit": COMMIT, "files": {}},
            {"commit": COMMIT, "files": files, "extra": 1},
            {"commit": COMMIT, "files": {"../outside.py": "0" * 64}},
            {"commit": COMMIT, "files": {"/etc/passwd": "0" * 64}},
        )
        for document in documents:
            with self.subTest(document=str(document)[:60]):
                (self.root / "runner_manifest.json").write_text(document if isinstance(document, str) else json.dumps(document))
                with self.assertRaises(self.host.Blocked):
                    self.host.read_source_manifest(self.root)

    def test_the_run_records_the_manifests_commit_and_a_digest_of_its_files(self) -> None:
        self.good()
        manifest = self.host.read_source_manifest(self.root)
        commit, tree = self.host.source_state(manifest)
        self.assertEqual(COMMIT, commit)
        self.assertEqual(64, len(tree))
        again = self.host.SourceManifest(COMMIT, dict(reversed(list(manifest.files.items()))))
        self.assertEqual(tree, self.host.source_state(again)[1])
        other = self.host.SourceManifest(COMMIT, {**manifest.files, "tests/e2e/stream_host.py": "f" * 64})
        self.assertNotEqual(tree, self.host.source_state(other)[1])


class RunnerFiles(SourceCase):
    def problems(self) -> list[str]:
        return list(self.host.source_problems(self.host.read_source_manifest(self.root), self.root))

    def test_a_checkout_that_equals_its_manifest_has_no_problem(self) -> None:
        self.good()
        self.assertEqual([], self.problems())

    def test_a_runner_file_that_differs_from_the_manifest_is_named(self) -> None:
        self.good()
        (self.root / "tests/e2e/stream_host.py").write_text("changed\n")
        found = self.problems()
        self.assertEqual(1, len(found))
        self.assertIn("tests/e2e/stream_host.py", found[0])

    def test_a_runner_file_with_no_entry_or_no_file_is_named(self) -> None:
        document = self.good()
        del document["files"]["tests/e2e/stream_features.py"]
        self.write_manifest(document)
        self.assertTrue(any("stream_features.py" in item and "no entry" in item for item in self.problems()))
        document = self.good()
        (self.root / "tests/e2e/stack_replay.py").unlink()
        self.assertTrue(any("stack_replay.py" in item and "missing" in item for item in self.problems()))

    def test_a_probe_in_the_folder_that_the_manifest_does_not_hold_is_named(self) -> None:
        self.good()
        (self.root / "tests/e2e/stream_stack/extra_probe.py").write_text("print('x')\n")
        self.assertTrue(any("extra_probe.py" in item for item in self.problems()))

    def test_the_overlay_and_every_probe_are_runner_files(self) -> None:
        names = self.host.runner_files(ROOT)
        self.assertIn("tests/e2e/stream_stack/compose.host.yml", names)
        self.assertIn("tests/e2e/stream_stack/plant_late.py", names)
        self.assertIn("tests/e2e/stream_features.py", names)
        self.assertIn("tests/e2e/stream_host.py", names)
        self.assertIn("tests/e2e/stack_replay.py", names)

    def test_a_service_file_that_the_manifest_holds_and_the_checkout_changed_is_named(self) -> None:
        self.good()
        (self.root / "compose/kafka/topics.txt").write_text("changed\n")
        self.assertTrue(any("compose/kafka/topics.txt" in item for item in self.problems()))


class DeployedFiles(SourceCase):
    def stack(self) -> Any:
        record = Record(feature="stand-in", slug="stand_in", rerun="none")
        work = self.root / "work"
        (work / "run").mkdir(parents=True)
        return self.host.HostStack(record, [], "replay:test", "spark:test", work / "landed", work / "run", work)

    def test_the_deployed_copies_are_compared_with_the_manifest_and_git_is_never_asked(self) -> None:
        self.good()
        manifest = self.host.read_source_manifest(self.root)
        stack = self.stack()
        deployed = {
            f"{self.host.DEPLOYED_DIRECTORY}/{name.removeprefix('compose/')}": digest for name, digest in manifest.files.items() if name.startswith("compose/")
        }
        asked: list[str] = []

        def sha(*arguments: str, timeout: float = 60) -> CompletedProcess[str]:
            asked.append(arguments[-1])
            return done(0, f"{deployed[arguments[-1]]}  {arguments[-1]}\n")

        stack.docker_host = sha
        with mock.patch.object(self.host, "run_command", side_effect=AssertionError("no command is run here, git least of all")):
            self.assertEqual([], self.host.deployed_files_problems(stack, manifest))
        self.assertEqual(sorted(deployed), sorted(asked))

    def test_a_deployed_copy_that_differs_from_the_manifest_is_named(self) -> None:
        self.good()
        manifest = self.host.read_source_manifest(self.root)
        stack = self.stack()
        stack.docker_host = lambda *arguments, timeout=60: done(0, f"{'0' * 64}  {arguments[-1]}\n")
        self.assertEqual(["compose/kafka/topics.txt", "compose/services/kafka.yml"], sorted(self.host.deployed_files_problems(stack, manifest)))

    def test_a_deployed_file_that_cannot_be_read_is_a_difference(self) -> None:
        self.good()
        manifest = self.host.read_source_manifest(self.root)
        stack = self.stack()
        stack.docker_host = lambda *arguments, timeout=60: done(1, "", "no such file")
        self.assertEqual(2, len(self.host.deployed_files_problems(stack, manifest)))

    def test_a_manifest_with_no_service_file_refuses_the_run(self) -> None:
        stack = self.stack()
        manifest = self.host.SourceManifest(COMMIT, {"tests/e2e/stream_host.py": "0" * 64})
        with self.assertRaises(self.host.Blocked):
            self.host.deployed_files_problems(stack, manifest)

    def test_the_runner_no_longer_reads_its_revision_from_git(self) -> None:
        self.assertFalse(hasattr(self.host, "recorded_state"))
        source = Path(str(self.host.__file__)).read_text()
        self.assertNotIn('"git"', source)
        self.assertNotIn("ls-files", source)


if __name__ == "__main__":
    unittest.main()
