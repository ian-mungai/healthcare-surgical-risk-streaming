"""A command never writes an output over a file that it reads.

``write_private`` and ``write_text`` open with truncation, so ``check --result X`` where X is also ``--selection`` emptied the selection before anything
was read. Each command that has both an input and an output path refuses an output that is the same file as an input, before it reads or writes anything:
the same path, a relative and an absolute spelling, a symbolic link and a hard link.
"""

from __future__ import annotations

import contextlib
import importlib
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from features import batch as features_batch
from scripts.ingestion import cli, copy_cost, copy_probe, staging_peak
from scripts.ingestion.closed import ClosedInputError
from scripts.ingestion.notice import Code, Notice, render
from scripts.ingestion.output_path import refuse_output_over_input, same_file
from scripts.sample import candidates

ORIGINAL = "original text\n"
REFUSAL = render(Notice(Code.OUTPUT_IS_INPUT))


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


class SameFileTests(Fixture):
    def test_each_spelling_of_one_file_is_the_same_file(self) -> None:
        target = self.made("target")
        link = self.root / "link"
        link.symlink_to(target)
        hard = self.root / "hard"
        os.link(target, hard)
        for other in (target, self.root / "." / "target", link, hard, self.root / "sub" / ".." / "target"):
            with self.subTest(other=other.name):
                self.assertTrue(same_file(target, other))

    def test_two_files_and_a_missing_output_are_not_the_same(self) -> None:
        self.assertFalse(same_file(self.made("one"), self.made("two")))
        self.assertFalse(same_file(self.root / "absent", self.made("two")))

    def test_the_refusal_is_a_closed_error_with_a_fixed_text(self) -> None:
        target = self.made("target")
        with self.assertRaises(ClosedInputError) as caught:
            refuse_output_over_input([target], [self.made("other"), target])
        self.assertEqual(REFUSAL, str(caught.exception))
        refuse_output_over_input([target, None], [self.made("other"), None])


class CommandTests(Fixture):
    def test_check_refuses_its_result_when_it_is_an_input(self) -> None:
        for shared in ("selection", "checksum_list", "pairings"):
            with self.subTest(shared=shared):
                files = {name: self.made(f"{shared}_{name}") for name in ("selection", "checksum_list", "pairings")}
                files["result"] = files[shared]
                argv = ["check", "--selection", str(files["selection"]), "--checksum-list", str(files["checksum_list"])]
                argv += ["--pairings", str(files["pairings"]), "--result", str(files["result"])]
                code, err = self.run_main(cli.main, argv)
                self.assertEqual(1, code)
                self.assertIn(REFUSAL, err)
                self.assertEqual(ORIGINAL, files[shared].read_text())

    def test_publish_refuses_its_reference_when_it_is_an_input(self) -> None:
        for shared in ("selection", "result"):
            with self.subTest(shared=shared):
                files = {name: self.made(f"{shared}_{name}") for name in ("selection", "result")}
                code, err = self.run_main(
                    cli.main, ["publish", "--selection", str(files["selection"]), "--result", str(files["result"]), "--reference", str(files[shared])]
                )
                self.assertEqual(1, code)
                self.assertIn(REFUSAL, err)
                self.assertEqual(ORIGINAL, files[shared].read_text())

    def test_copy_cost_refuses_its_output_when_it_is_the_result(self) -> None:
        result = self.made("result")
        code, err = self.run_main(
            copy_cost.main, ["--result", str(result), "--region", "us-west-2", "--since", "2026-01-01T00:00:00Z", "--output", str(result)]
        )
        self.assertEqual(1, code)
        self.assertIn(REFUSAL, err)
        self.assertEqual(ORIGINAL, result.read_text())

    def test_the_copy_probe_sample_refuses_its_output_when_it_is_the_reference(self) -> None:
        reference = self.made("reference")
        code, err = self.run_main(copy_probe.main, ["sample", "--reference", str(reference), "--output", str(reference)])
        self.assertEqual(1, code)
        self.assertIn(REFUSAL, err)
        self.assertEqual(ORIGINAL, reference.read_text())

    def test_the_staging_peak_refuses_its_output_when_it_is_the_samples(self) -> None:
        samples = self.made("samples")
        code, err = self.run_main(staging_peak.main, ["peak", "--samples", str(samples), "--output", str(samples)])
        self.assertEqual(1, code)
        self.assertIn(REFUSAL, err)
        self.assertEqual(ORIGINAL, samples.read_text())

    def test_the_candidate_build_refuses_its_output_when_it_is_the_release_pin(self) -> None:
        release = self.made("release.json")
        err = io.StringIO()
        with contextlib.redirect_stderr(err), contextlib.redirect_stdout(io.StringIO()):
            code = candidates.main(["build", "--release", str(release), "--output", str(release)])
        self.assertEqual(1, code)
        self.assertIn(REFUSAL, err.getvalue())
        self.assertEqual(ORIGINAL, release.read_text())

    def test_an_output_that_reaches_an_input_through_a_missing_folder_and_a_hard_link_is_refused(self) -> None:
        samples = self.made("samples.jsonl")
        os.link(samples, self.root / "alias.jsonl")
        code, err = self.run_main(staging_peak.main, ["peak", "--samples", str(samples), "--output", str(self.root / "new" / ".." / "alias.jsonl")])
        self.assertEqual(1, code)
        self.assertIn(REFUSAL, err)
        self.assertEqual(ORIGINAL, samples.read_text())
        self.assertFalse((self.root / "new").exists())

    def test_every_replay_command_refuses_an_output_that_any_input_of_the_run_names(self) -> None:
        for command in ("manifest", "observe"):
            for shared in ("run.json", "state/schedule.json", "state/state.json", "reference.json"):
                with self.subTest(command=command, shared=shared):
                    record = self.replay_record(command, shared)
                    code, err = self.run_replay([command, "--run-record", str(self.root / "run.json"), "--out", str(self.root / shared)])
                    self.assertEqual(2, code)
                    self.assertIn(REFUSAL, err)
                    self.assertEqual(record, (self.root / shared).read_text())
                    self.assertEqual([], sorted(path.name for path in self.root.rglob(".*")))

    def replay_record(self, command: str, shared: str) -> str:
        """Write a run record, the files that it names and the file ``shared``; return the text of that file."""
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
        for name in ("run.json", "state/schedule.json", "state/state.json", "reference.json"):
            (self.root / name).write_text(ORIGINAL)
        (self.root / "run.json").write_text(json.dumps(document))
        return (self.root / shared).read_text()

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
                return self.run_main(program.main, argv)

    def test_a_replay_command_that_cannot_read_its_run_record_names_the_argument_and_not_the_path(self) -> None:
        missing = self.root / "secret_folder_marker" / "run.json"
        code, err = self.run_replay(["observe", "--run-record", str(missing), "--out", str(self.root / "out.json")])
        self.assertNotEqual(0, code)
        self.assertNotIn("secret_folder_marker", err)
        self.assertNotIn("run.json", err)

    def test_features_batch_refuses_a_rows_file_that_is_its_windows_file(self) -> None:
        windows = self.made("windows.json")
        for target in (windows, self.root / "sub" / ".." / "windows.json"):
            with self.subTest(target=str(target)):
                code, err = self.run_main(features_batch.main, [str(windows), str(target)])
                self.assertEqual(2, code)
                self.assertIn(REFUSAL, err)
                self.assertEqual(ORIGINAL, windows.read_text())

    def test_features_batch_names_no_path_when_it_cannot_read_a_file(self) -> None:
        code, err = self.run_main(features_batch.main, [str(self.root / "secret_name_marker.json"), str(self.root / "rows.json")])
        self.assertEqual(2, code)
        self.assertNotIn("secret_name_marker", err)


if __name__ == "__main__":
    unittest.main()
