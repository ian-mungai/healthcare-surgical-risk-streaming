"""The run binding path that ``create`` takes: a relative path is read from the repository root, and a path outside the runs folder is refused with fixed text.

Failure modes, each with its case: a relative path inside the runs folder (it names the same file as its absolute form, so ``create`` finds the binding
that is there and writes no second one), a relative path outside it, an absolute path outside it, a path that leaves the runs folder through ``..`` and a
path in a subfolder of it (each refused with the fixed message and exit code 1, before any AWS call and with nothing written), and the same refusal
through the command line's own handler (no uncaught ``ClosedInputError``).
"""

from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from dataclasses import asdict, replace
from pathlib import Path
from unittest import mock

from scripts.infra import state_bootstrap
from scripts.infra.state_bootstrap import RunBinding

KEYS = ["state/dev/terraform.tfstate", "state/dev/terraform.tfstate.tflock"]
BOUND = RunBinding("20260101t0000-abcdef", "c0ffee", "s3", "us-west-2", "example-project", "default", KEYS, "verified", bucket_creation="created")
CONFIGURATION = {
    "S3_BUCKET": "example-project",
    "AWS_REGION": "us-west-2",
    "AWS_PROFILE": "profile_name",
    "AWS_ACCOUNT_ID": "account_id",
    "ENVIRONMENT": "dev",
    "PROJECT_NAME": "project",
}
REFUSED = "the run binding file must lie directly inside the runs folder of the repository; nothing was written"


class RepositoryFixture(unittest.TestCase):
    """A stand-in repository root with its runs folder, so no test touches the real one."""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name).resolve()
        self.runs = self.root / "runs"
        self.runs.mkdir()
        self.outside = self.root / "elsewhere"
        self.outside.mkdir()
        for name, value in (("ROOT", self.root), ("RUNS", self.runs)):
            patcher = mock.patch.object(state_bootstrap, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.aws = mock.Mock()

    def bind(self, folder: Path) -> Path:
        path = folder / "run_binding.json"
        path.write_text(json.dumps(asdict(BOUND), indent=2) + "\n")
        return path

    def create(self, path: Path) -> tuple[int, str]:
        error = io.StringIO()
        with (
            contextlib.redirect_stderr(error),
            contextlib.redirect_stdout(io.StringIO()),
            mock.patch.object(state_bootstrap, "new_binding", side_effect=lambda *_: replace(BOUND, status="planned")),
            mock.patch.object(state_bootstrap, "bucket_exists", return_value=True),
            mock.patch.object(state_bootstrap, "owned", return_value=True),
            mock.patch.object(state_bootstrap, "read_settings", return_value={}),
            mock.patch.object(state_bootstrap, "problems", return_value={}),
        ):
            code = state_bootstrap.create(self.aws, CONFIGURATION, path)
        return code, error.getvalue()


class BindingPathTest(RepositoryFixture):
    def test_a_relative_path_is_read_from_the_repository_root_and_not_from_the_working_folder(self) -> None:
        expected = self.runs / "run_binding.json"
        self.assertEqual(expected, state_bootstrap.binding_path(Path("runs/run_binding.json")))
        self.assertEqual(expected, state_bootstrap.binding_path(expected))
        self.assertEqual(expected, state_bootstrap.binding_path(Path("runs/../runs/run_binding.json")))

    def test_every_path_that_is_not_directly_inside_the_runs_folder_has_no_binding_path(self) -> None:
        for label, path in {
            "relative, outside": Path("elsewhere/run_binding.json"),
            "relative, in the root": Path("run_binding.json"),
            "absolute, outside": self.outside / "run_binding.json",
            "through a dot dot part": Path("runs/../elsewhere/run_binding.json"),
            "in a subfolder": Path("runs/sub/run_binding.json"),
            "the runs folder itself": Path("runs"),
        }.items():
            with self.subTest(path=label):
                self.assertIsNone(state_bootstrap.binding_path(path))


class CreateBindingTest(RepositoryFixture):
    def test_create_finds_the_binding_that_a_relative_path_inside_the_runs_folder_names(self) -> None:
        path = self.bind(self.runs)
        before = path.read_text()
        code, error = self.create(Path("runs/run_binding.json"))
        self.assertEqual((0, ""), (code, error))
        self.assertEqual(before, path.read_text())
        self.assertEqual(["run_binding.json"], [entry.name for entry in self.runs.iterdir()])

    def test_a_path_outside_the_runs_folder_is_refused_with_fixed_text_before_any_call_and_nothing_is_written(self) -> None:
        held = self.bind(self.outside)
        before = held.read_text()
        for label, path in {
            "relative, outside": Path("elsewhere/run_binding.json"),
            "absolute, outside": held,
            "through a dot dot part": Path("runs/../elsewhere/run_binding.json"),
            "in a subfolder": Path("runs/sub/run_binding.json"),
            "a new file outside": self.outside / "new_binding.json",
        }.items():
            with self.subTest(path=label):
                code, error = self.create(path)
                self.assertEqual((1, f"{REFUSED}\n"), (code, error))
                self.assertEqual([], self.aws.mock_calls)
        self.assertEqual(before, held.read_text())
        self.assertEqual(["run_binding.json"], [entry.name for entry in self.outside.iterdir()])
        self.assertEqual([], list(self.runs.iterdir()))

    def test_the_command_line_shows_the_refusal_through_its_own_handler(self) -> None:
        for argument in ("elsewhere/run_binding.json", str(self.outside / "run_binding.json")):
            with self.subTest(binding=argument):
                error = io.StringIO()
                with (
                    contextlib.redirect_stderr(error),
                    mock.patch("sys.argv", ["state_bootstrap", "create", "--binding", argument]),
                    mock.patch.object(state_bootstrap, "load_configuration", return_value=CONFIGURATION),
                    mock.patch.object(state_bootstrap, "Aws", return_value=self.aws),
                    mock.patch.object(state_bootstrap, "new_binding", side_effect=lambda *_: replace(BOUND, status="planned")),
                ):
                    code = state_bootstrap.main()
                self.assertEqual((1, f"{REFUSED}\n"), (code, error.getvalue()))
        self.assertEqual([], list(self.runs.iterdir()))
        self.assertEqual([], list(self.outside.iterdir()))


if __name__ == "__main__":
    unittest.main()
