"""A command never writes an output over a file that it reads.

``write_private`` and ``write_text`` open with truncation, so ``check --result X`` where X is also ``--selection`` emptied the selection before anything
was read. Each command that has both an input and an output path refuses an output that is the same file as an input, before it reads or writes anything:
the same path, a relative and an absolute spelling, a symbolic link and a hard link.
"""

from __future__ import annotations

import ast
import contextlib
import io
import os
import tempfile
import unittest
from pathlib import Path

from scripts.ingestion import cli, copy_cost, copy_probe, staging_peak
from scripts.ingestion.closed import ClosedInputError
from scripts.ingestion.notice import Code, Notice, render
from scripts.ingestion.output_path import refuse_output_over_input, same_file
from scripts.sample import candidates
from tests.fast.git_fixture import ROOT

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

    def test_the_replay_commands_check_their_output_against_the_run_record(self) -> None:
        """The replay program imports ``vitaldb``, which only its image holds, so this test reads its source; the image run takes the behavior."""
        source = (ROOT / "replay" / "service" / "__main__.py").read_text()
        calls = [node for node in ast.walk(ast.parse(source)) if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "refuse_output_over_input"]
        self.assertEqual(1, len(calls))
        self.assertEqual(
            ["getattr(arguments, 'out', None)", "arguments.run_record"],
            [ast.unparse(argument) for argument in (calls[0].args[0].elts if isinstance(calls[0].args[0], ast.List) else [])]
            + [ast.unparse(argument) for argument in (calls[0].args[1].elts if isinstance(calls[0].args[1], ast.List) else [])],
        )
        index = source.index("refuse_output_over_input(")
        self.assertLess(index, source.index("arguments.run_record.read_text()"))


if __name__ == "__main__":
    unittest.main()
