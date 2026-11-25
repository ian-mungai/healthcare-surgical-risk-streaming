"""The fixed sample runner records no case of a test patient, no count that mixes test and development and no output of a child, passing or failing.

The runner stays untracked, so each test skips when it is not on this machine. The publisher's bucket, the checksum list, the child commands and the run folder
are stand-ins: the runner's own ``main`` runs on them and the folder that it wrote is read back.

Failure modes covered here:

- A candidate of a test patient is missing from the publisher's checksum list, and the record named its case ID and the count of all candidates, which mixes
  test and development candidates. The record holds only a flag for the match of the checksums and one for the match of the sizes.
- A child command failed and the log copied its output, which can hold a case or a checksum. The log keeps the words of the command, its exit code and nothing
  of its streams.
- A step raised, and the blocked reason held the text of the error. It names the class of the error only.
"""

from __future__ import annotations

import contextlib
import io
import json
import re
import tempfile
import unittest
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from subprocess import CompletedProcess
from typing import Any
from unittest import mock

from scripts.sample.candidates import CANDIDATES_FILE, RELEASE_FILE, Candidate, read_candidates
from scripts.sample.publisher import RELEASE_PREFIX
from scripts.sample.split import patient_part
from tests.e2e import run_record
from tests.fast.test_runner_gold_sealed import runner

PATIENTS = 6090
SENTINEL = "CHILD-OUTPUT-SENTINEL"
HEX_RUN = re.compile(r"[0-9a-f]{40,}")
PLANTED_ERROR_CASE = "424242"


def whole(number: int | str) -> re.Pattern[str]:
    """A number as a whole number: not part of a longer run of digits."""
    return re.compile(rf"(?<![0-9]){number}(?![0-9])")


@dataclass
class World:
    """What the stand-in bucket and the stand-in child commands do. A case in ``unlisted`` is left out of the publisher's checksum list, a case in ``resized``
    is listed with another size, and ``raises`` is an error that the bucket's listing raises."""

    unlisted: set[int] = field(default_factory=set)
    resized: set[int] = field(default_factory=set)
    raises: Exception | None = None
    check_exit: int = 0
    tracked: list[Candidate] = field(default_factory=lambda: read_candidates(CANDIDATES_FILE))
    commands: list[list[str]] = field(default_factory=list)

    def run_command(self, program: str, arguments: Sequence[str], **_: Any) -> CompletedProcess[str]:
        words = list(arguments)
        self.commands.append([program, *words])
        text = f"{SENTINEL} case {self.tracked[0].case} {self.tracked[0].sha256}"
        if "build" in words:
            if "--release" in words:
                return CompletedProcess(words, 1, "", f"the checksum list does not match the pinned SHA-256 {text}")
            Path(words[words.index("--output") + 1]).write_text(CANDIDATES_FILE.read_text())
            return CompletedProcess(words, 0, text, "")
        if "--candidates" in words:
            return CompletedProcess(words, 1, f"case {self.tracked[0].case}: the checksum is not the one the publisher's checksum list gives {text}", "")
        if "--release" in words:
            return CompletedProcess(words, 1, f"the checksum list does not match the pinned SHA-256 {text}", "")
        return CompletedProcess(words, self.check_exit, f"the tracked files equal the rebuild {text}", "")

    def download_key(self, key: str, target: Path) -> None:
        if self.raises is not None:
            raise self.raises
        if key.endswith("SHA256SUMS.txt"):
            lines = [f"{item.sha256}  {item.key.removeprefix(RELEASE_PREFIX)}" for item in self.tracked if item.case not in self.unlisted]
            target.write_text("\n".join(lines) + "\n")
        else:
            target.write_text("subjectid\n" + "\n".join(str(patient) for patient in range(1, PATIENTS + 1)) + "\n")

    def list_release(self) -> dict[str, int]:
        return {item.key: item.size_bytes + (1 if item.case in self.resized else 0) for item in self.tracked}


@dataclass
class Saved:
    code: int
    record: dict[str, Any]
    text: str
    printed: str

    def observation(self, case: str) -> dict[str, Any]:
        found = [item for item in self.record["observations"] if item["case"] == case]
        if len(found) != 1:
            raise AssertionError(f"{len(found)} observations for {case}")
        item: dict[str, Any] = found[0]
        return item


class FixedSampleRecords(unittest.TestCase):
    def setUp(self) -> None:
        self.module = runner("fixed_sample")
        self.release = json.loads(RELEASE_FILE.read_text())
        self.tracked = read_candidates(CANDIDATES_FILE)
        self.test_cases = [item.case for item in self.tracked if patient_part(item.patient) == "test"]
        self.assertGreater(len(self.test_cases), 0)

    @contextlib.contextmanager
    def stood_in(self, world: World) -> Iterator[Path]:
        with tempfile.TemporaryDirectory(prefix="fixed_sample_test_") as folder:
            patches: list[Any] = [
                mock.patch.object(run_record, "ROOT", Path(folder)),
                mock.patch.object(self.module, "ROOT", Path(folder)),
                mock.patch.object(self.module, "run_command", world.run_command),
                mock.patch.object(self.module, "download_key", world.download_key),
                mock.patch.object(self.module, "list_release", world.list_release),
                mock.patch.object(self.module, "file_sha256", lambda path: self.release["checksum_list"]["sha256"]),
                mock.patch.object(self.module, "recorded_state", lambda root: ("a-revision", "a-tree")),
                mock.patch.object(self.module, "tool_version", lambda program: f"{program} 1.0"),
            ]
            with contextlib.ExitStack() as stack:
                for patch in patches:
                    stack.enter_context(patch)
                yield Path(folder)

    def saved(self, world: World | None = None) -> Saved:
        world = world or World()
        out = io.StringIO()
        with self.stood_in(world) as folder, contextlib.redirect_stdout(out):
            code = self.module.main()
            runs = sorted((folder / "runs").iterdir())
            self.assertEqual(1, len(runs))
            record = json.loads((runs[0] / "record.json").read_text())
            text = (runs[0] / "record.json").read_text() + (runs[0] / "log.txt").read_text()
        printed = "\n".join(line for line in out.getvalue().splitlines() if line.startswith(("PASS", "FAIL")))
        return Saved(code, record, text + "\n" + printed, printed)

    def assert_no_leak(self, saved: Saved, world: World) -> None:
        for case in self.test_cases:
            self.assertIsNone(whole(case).search(saved.text), "a case of a test patient is in the record, its log or its printed lines")
        for item in self.tracked:
            self.assertNotIn(item.sha256, saved.text, "a candidate's checksum is in the record")
        self.assertIsNone(HEX_RUN.search(saved.text), "a checksum is in the record")
        self.assertNotIn(SENTINEL, saved.text, "the output of a child command is in the record")
        self.assertNotIn(self.release["checksum_list"]["sha256"], saved.text, "the pinned checksum is in the record")

    def test_a_passing_run_passes_and_records_no_case_checksum_or_output(self) -> None:
        world = World()
        saved = self.saved(world)
        self.assertEqual(0, saved.code, saved.printed)
        self.assertEqual("pass", saved.record["outcome"])
        self.assertGreaterEqual(len(saved.record["observations"]), 8)
        self.assert_no_leak(saved, world)

    def test_a_test_candidate_missing_from_the_list_fails_the_run_and_the_record_names_no_case_and_no_count(self) -> None:
        for case in self.test_cases:
            with self.subTest(missing="a test candidate"):
                world = World(unlisted={case})
                saved = self.saved(world)
                self.assertEqual(1, saved.code)
                observed = saved.observation("candidates against the checksum list")
                self.assertFalse(observed["passed"])
                self.assertIsNone(re.search(r"[0-9]", observed["observed"]), "the observation holds a number: a case or a count that mixes the sets")
                self.assert_no_leak(saved, world)

    def test_a_test_candidate_of_another_size_fails_the_run_and_the_record_names_no_case_and_no_count(self) -> None:
        for case in self.test_cases:
            with self.subTest(resized="a test candidate"):
                world = World(resized={case})
                saved = self.saved(world)
                self.assertEqual(1, saved.code)
                self.assertFalse(saved.observation("candidates against the checksum list")["passed"])
                self.assertIsNone(re.search(r"[0-9]", saved.observation("candidates against the checksum list")["observed"]))
                self.assert_no_leak(saved, world)

    def flags_of(self, saved: Saved) -> dict[str, str]:
        """The flags of the observation of the candidates against the list, by phrase."""
        observed = saved.observation("candidates against the checksum list")["observed"]
        return {part.rsplit(" ", 1)[0]: part.rsplit(" ", 1)[1] for part in observed.split(", ")}

    def test_the_observation_holds_a_flag_for_the_checksums_and_one_for_the_sizes(self) -> None:
        checksums, sizes = "every candidate's key and checksum is in the list", "every size is the listed one"
        self.assertEqual({checksums: "False", sizes: "True"}, self.flags_of(self.saved(World(unlisted={self.test_cases[0]}))))
        self.assertEqual({checksums: "True", sizes: "False"}, self.flags_of(self.saved(World(resized={self.test_cases[0]}))))
        self.assertEqual({checksums: "True", sizes: "True"}, self.flags_of(self.saved(World())))

    def test_a_failed_child_command_leaves_no_output_in_the_record_and_its_log(self) -> None:
        world = World(check_exit=1)
        saved = self.saved(world)
        self.assertEqual(1, saved.code)
        self.assertFalse(saved.observation("check of the tracked files")["passed"])
        self.assertIn("exit 1", saved.observation("check of the tracked files")["observed"])
        self.assert_no_leak(saved, world)

    def test_no_expectation_names_a_case_a_checksum_or_a_number_of_candidates(self) -> None:
        saved = self.saved(World())
        for item in saved.record["observations"]:
            for candidate in self.tracked:
                self.assertIsNone(whole(candidate.case).search(item["expected"]), "an expectation names a case")
            self.assertIsNone(HEX_RUN.search(item["expected"]), "an expectation holds a checksum")
            self.assertIsNone(whole(len(self.tracked)).search(item["expected"]), "an expectation holds the number of candidates")

    def test_a_step_that_raises_blocks_the_run_and_the_reason_names_only_the_class_of_the_error(self) -> None:
        for error in (ValueError(f"case {PLANTED_ERROR_CASE}"), KeyError(PLANTED_ERROR_CASE), OSError(f"cannot read {PLANTED_ERROR_CASE}")):
            with self.subTest(error=type(error).__name__):
                world = World(raises=error)
                saved = self.saved(world)
                self.assertEqual(1, saved.code)
                self.assertEqual(f"blocked: {type(error).__name__}", saved.record["outcome"])
                self.assertNotIn(PLANTED_ERROR_CASE, saved.text)


if __name__ == "__main__":
    unittest.main()
