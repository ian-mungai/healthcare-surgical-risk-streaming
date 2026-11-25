"""The placement of named Git-ignored runner files in the runner folder on the host: what it refuses, the one command it sends for each file, the hash it
records and the make target."""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import shlex
import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path

from scripts.infra import runner_place
from scripts.infra.host import HostError
from scripts.process import CompletedProcess, TimeoutExpired
from tests.fast.ansible_paths import group_variables
from tests.fast.git_fixture import TemporaryRepository

ROOT = Path(__file__).resolve().parents[2]
OPTIONS = ["-o", "BatchMode=yes"]
RUNNER_TEXT = "from __future__ import annotations\n\nprint('runner')\n"


def completed(code: int, out: str = "") -> CompletedProcess[str]:
    return CompletedProcess(["ssh"], code, out, "")


class Host:
    """A stand-in for the host: it records each call, keeps the record of placed runners between calls and answers a command as the remote command does."""

    def __init__(
        self,
        *,
        folder_exists: bool = True,
        wrong_hash: bool = False,
        code: int | None = None,
        expire: bool = False,
        record: str = "",
        wrong_record_hash: bool = False,
    ) -> None:
        self.calls: list[tuple[str, list[str], str | None, float]] = []
        self.folder_exists, self.wrong_hash, self.code, self.expire = folder_exists, wrong_hash, code, expire
        self.record, self.wrong_record_hash = record, wrong_record_hash

    def files(self) -> list[tuple[str, list[str], str | None, float]]:
        """The calls that each receive one runner file."""
        return [call for call in self.calls if f"{runner_place.PLACED_RECORD}" not in call[1][-1]]

    def commands(self, word: str) -> list[str]:
        return [call[1][-1] for call in self.calls if word in call[1][-1]]

    def __call__(self, program: str, args: Sequence[str], *, timeout: float, input_text: str | None = None) -> CompletedProcess[str]:
        self.calls.append((program, list(args), input_text, timeout))
        if self.expire:
            raise TimeoutExpired("ssh", timeout)
        if self.code is not None:
            return completed(self.code)
        if not self.folder_exists:
            return completed(3, "no_runner_folder\n")
        command = args[-1]
        data = (input_text or "").encode()
        if f"{runner_place.PLACED_RECORD}.part" in command:
            self.record = input_text or ""
            digest = "0" * 64 if self.wrong_record_hash else hashlib.sha256(data).hexdigest()
            return completed(0, f"{digest}  {runner_place.PLACED_RECORD}\n")
        if runner_place.PLACED_RECORD in command:
            return completed(0, self.record)
        digest = "0" * 64 if self.wrong_hash else hashlib.sha256(data).hexdigest()
        relative = shlex.split(command)[-1]
        return completed(0, f"{digest}  {relative}\n")


class RepositoryCase(TemporaryRepository):
    def setUp(self) -> None:
        super().setUp()
        self.enterContext(contextlib.redirect_stdout(io.StringIO()))
        (self.repo / ".gitignore").write_text("tests/e2e/silver_signals.py\ntests/e2e/stream_host.py\n")
        (self.repo / "tests" / "e2e").mkdir(parents=True)
        (self.repo / "tests" / "e2e" / "tracked_runner.py").write_text("print('tracked')\n")
        self.write("silver_signals", RUNNER_TEXT)
        self.write("stream_host", RUNNER_TEXT + "# second\n")
        self.git("add", ".gitignore", "tests/e2e/tracked_runner.py")
        self.git("commit", "--quiet", "-m", "chore: fixture")
        self.commit = self.git("rev-parse", "HEAD").stdout.strip()

    def write(self, name: str, text: str) -> Path:
        path = self.repo / "tests" / "e2e" / f"{name}.py"
        path.write_text(text)
        return path

    def read(self, relative: str) -> str:
        return runner_place.runner_text(relative, self.commit, self.repo, self.env)


class RefusalTest(RepositoryCase):
    def test_an_ignored_runner_under_tests_e2e_is_read(self) -> None:
        self.assertEqual(RUNNER_TEXT, self.read("tests/e2e/silver_signals.py"))

    def test_a_path_that_is_not_one_python_file_directly_under_tests_e2e_is_refused(self) -> None:
        for relative in (
            "../silver_signals.py",
            "/etc/passwd",
            "tests/e2e/../../README.md",
            "tests/e2e/sub/runner.py",
            "tests/fast/runner.py",
            "tests/e2e/runner.sh",
            "tests/e2e/Runner.py",
            "tests/e2e/run ner.py",
            "tests/e2e/.hidden.py",
            "tests/e2e/runner.py ",
            "tests/e2e/",
            "",
            ".env",
            "requirements.txt",
        ):
            with self.subTest(relative=relative), contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(HostError, "not one Python runner"):
                self.read(relative)

    def test_a_missing_file_a_folder_and_a_link_are_refused(self) -> None:
        (self.repo / "tests" / "e2e" / "folder.py").mkdir()
        outside = self.repo / "outside.txt"
        outside.write_text("secret\n")
        (self.repo / "tests" / "e2e" / "link.py").symlink_to(outside)
        for name in ("absent", "folder", "link"):
            with self.subTest(name=name), self.assertRaisesRegex(HostError, "not a plain file"):
                self.read(f"tests/e2e/{name}.py")

    def test_a_file_that_the_commit_holds_is_refused_because_the_archive_already_placed_it(self) -> None:
        with self.assertRaisesRegex(HostError, "already in the commit"):
            self.read("tests/e2e/tracked_runner.py")

    def test_a_file_that_is_too_large_has_a_nul_byte_or_is_not_text_is_refused(self) -> None:
        for name, data in (("big", b"x" * (runner_place.MAX_BYTES + 1)), ("nul", b"a\0b\n"), ("latin", b"caf\xe9\n")):
            (self.repo / "tests" / "e2e" / f"{name}.py").write_bytes(data)
            with self.subTest(name=name), self.assertRaisesRegex(HostError, "too large|not text"):
                self.read(f"tests/e2e/{name}.py")

    def test_a_file_that_holds_personal_or_environment_specific_data_is_refused_with_its_kind_and_line_and_never_shows_the_value(self) -> None:
        self.write("leaky", "OWNER = 'someone" + "@personal-domain.net'\n")
        shown = io.StringIO()
        with contextlib.redirect_stdout(shown), self.assertRaises(HostError) as caught:
            self.read("tests/e2e/leaky.py")
        self.assertIn("tests/e2e/leaky.py:1: email address", shown.getvalue())
        self.assertNotIn("someone@", shown.getvalue() + str(caught.exception))
        self.assertNotIn("leaky", str(caught.exception), "the refusal's own text is fixed and names no file")

    def test_a_commit_that_is_not_forty_hexadecimal_digits_is_refused(self) -> None:
        for commit in ("HEAD", "main", "abc", "A" * 40, self.commit[:-1], f"{self.commit}\n"):
            with self.subTest(commit=commit), self.assertRaisesRegex(HostError, "commit"):
                runner_place.runner_text("tests/e2e/silver_signals.py", commit, self.repo, self.env)


class PlacementTest(RepositoryCase):
    def place(self, host: Host, names: list[str] | None = None) -> dict[str, str]:
        return runner_place.place(
            "run", "host.example.invalid", self.commit, names or ["tests/e2e/silver_signals.py"], root=self.repo, env=self.env, run=host, ssh_options=OPTIONS
        )

    def test_each_file_is_one_ssh_command_that_receives_the_file_and_nothing_else_and_its_hash_is_returned(self) -> None:
        host = Host()
        hashes = self.place(host, ["tests/e2e/silver_signals.py", "tests/e2e/stream_host.py"])
        self.assertEqual(2, len(host.files()))
        self.assertEqual(
            {
                "tests/e2e/silver_signals.py": hashlib.sha256(RUNNER_TEXT.encode()).hexdigest(),
                "tests/e2e/stream_host.py": hashlib.sha256((RUNNER_TEXT + "# second\n").encode()).hexdigest(),
            },
            hashes,
        )
        self.assertEqual([RUNNER_TEXT, RUNNER_TEXT + "# second\n"], [call[2] for call in host.files()])

    def test_the_remote_command_works_in_the_commits_folder_after_its_marker_and_keeps_the_file_from_others(self) -> None:
        host = Host()
        self.place(host)
        program, args, _, timeout = host.files()[0]
        self.assertEqual("ssh", program)
        self.assertIn("ubuntu@host.example.invalid", args)
        self.assertLessEqual(timeout, runner_place.SSH_SECONDS)
        command = args[-1]
        self.assertIn(f"cd {runner_place.RUNNER_DIRECTORY}/{self.commit}", command)
        self.assertIn(runner_place.MARKER, command)
        self.assertIn("umask 077", command)
        self.assertIn(".part", command)
        for refused in ("sudo", "chmod", "rm ", "chown"):
            self.assertNotIn(refused, command)

    def test_a_missing_runner_folder_says_to_deploy_this_commit_first_and_a_wrong_remote_hash_is_refused(self) -> None:
        with self.assertRaisesRegex(HostError, "no runner folder for this commit.*make host-up"):
            self.place(Host(folder_exists=False))
        with self.assertRaisesRegex(HostError, "differs from the one sent"):
            self.place(Host(wrong_hash=True))

    def test_an_ssh_failure_and_an_expired_deadline_are_refused_with_fixed_text(self) -> None:
        with self.assertRaisesRegex(HostError, "exit status 255"):
            self.place(Host(code=255))
        with self.assertRaisesRegex(HostError, "did not finish within its deadline"):
            self.place(Host(expire=True))

    def test_a_refused_file_sends_nothing_even_when_it_comes_after_a_good_one(self) -> None:
        host = Host()
        with self.assertRaises(HostError):
            self.place(host, ["tests/e2e/silver_signals.py", "tests/e2e/tracked_runner.py"])
        self.assertEqual([], host.calls)

    def test_a_file_named_twice_is_refused_and_no_name_means_nothing_to_place(self) -> None:
        with self.assertRaisesRegex(HostError, "twice"):
            self.place(Host(), ["tests/e2e/silver_signals.py", "tests/e2e/silver_signals.py"])
        with self.assertRaisesRegex(HostError, "names no runner"):
            runner_place.place("run", "host.example.invalid", self.commit, [], root=self.repo, env=self.env, run=Host(), ssh_options=OPTIONS)


class PlacedRecordTest(RepositoryCase):
    """``placed_runners.json`` in the checkout's root, on the host: the hashes of the placed runners, for the runners to check their own files."""

    def place(self, host: Host, names: list[str]) -> dict[str, str]:
        return runner_place.place("run", "host.example.invalid", self.commit, names, root=self.repo, env=self.env, run=host, ssh_options=OPTIONS)

    def digest(self, text: str) -> str:
        return hashlib.sha256(text.encode()).hexdigest()

    def test_the_host_gets_the_commit_and_the_hash_of_each_placed_runner_in_the_one_form_the_manifest_has(self) -> None:
        host = Host()
        self.place(host, ["tests/e2e/silver_signals.py", "tests/e2e/stream_host.py"])
        document = json.loads(host.record)
        self.assertEqual(["commit", "files"], sorted(document))
        self.assertEqual(self.commit, document["commit"])
        self.assertEqual(
            {"tests/e2e/silver_signals.py": self.digest(RUNNER_TEXT), "tests/e2e/stream_host.py": self.digest(RUNNER_TEXT + "# second\n")}, document["files"]
        )
        self.assertTrue(host.record.endswith("}\n"))

    def test_a_later_call_keeps_the_hash_of_a_runner_that_an_earlier_call_placed(self) -> None:
        host = Host()
        self.place(host, ["tests/e2e/silver_signals.py"])
        self.place(host, ["tests/e2e/stream_host.py"])
        self.assertEqual(
            {"tests/e2e/silver_signals.py": self.digest(RUNNER_TEXT), "tests/e2e/stream_host.py": self.digest(RUNNER_TEXT + "# second\n")},
            json.loads(host.record)["files"],
        )

    def test_a_runner_that_is_placed_again_gets_its_new_hash_and_the_others_keep_theirs(self) -> None:
        host = Host()
        self.place(host, ["tests/e2e/silver_signals.py", "tests/e2e/stream_host.py"])
        self.write("silver_signals", RUNNER_TEXT + "# edited\n")
        self.place(host, ["tests/e2e/silver_signals.py"])
        self.assertEqual(
            {"tests/e2e/silver_signals.py": self.digest(RUNNER_TEXT + "# edited\n"), "tests/e2e/stream_host.py": self.digest(RUNNER_TEXT + "# second\n")},
            json.loads(host.record)["files"],
        )

    def test_the_record_is_written_after_every_file_is_placed_and_checked_and_is_made_read_only_to_its_owner_by_the_umask(self) -> None:
        host = Host()
        self.place(host, ["tests/e2e/silver_signals.py"])
        written = host.commands(f"{runner_place.PLACED_RECORD}.part")
        self.assertEqual(1, len(written))
        self.assertIs(host.calls[-1][1][-1], written[0])
        command = written[0]
        self.assertIn(f"cd {runner_place.RUNNER_DIRECTORY}/{self.commit}", command)
        self.assertIn(runner_place.MARKER, command)
        self.assertIn("umask 277", command)
        for refused in ("sudo", "chmod", "rm ", "chown", "tests/e2e"):
            self.assertNotIn(refused, command)

    def test_the_record_of_another_commit_is_started_again_and_one_that_is_not_valid_is_refused_before_a_file_is_sent(self) -> None:
        stale = json.dumps({"commit": "b" * 40, "files": {"tests/e2e/old.py": "1" * 64}})
        host = Host(record=stale)
        self.place(host, ["tests/e2e/silver_signals.py"])
        self.assertEqual({"tests/e2e/silver_signals.py": self.digest(RUNNER_TEXT)}, json.loads(host.record)["files"])
        bad = {
            "not json": "{",
            "not an object": "[]",
            "extra key": json.dumps({"commit": self.commit, "files": {}, "more": 1}),
            "files is a list": json.dumps({"commit": self.commit, "files": []}),
            "path outside tests/e2e": json.dumps({"commit": self.commit, "files": {"../x.py": "1" * 64}}),
            "the manifest": json.dumps({"commit": self.commit, "files": {"runner_manifest.json": "1" * 64}}),
            "short hash": json.dumps({"commit": self.commit, "files": {"tests/e2e/a.py": "1"}}),
        }
        for name, record in bad.items():
            with self.subTest(record=name):
                host = Host(record=record)
                with self.assertRaisesRegex(HostError, "placed_runners.json.*not valid"):
                    self.place(host, ["tests/e2e/silver_signals.py"])
                self.assertEqual(1, len(host.calls), "only the read of the record was sent")

    def test_a_wrong_hash_of_the_written_record_is_refused(self) -> None:
        with self.assertRaisesRegex(HostError, "record.*differs"):
            self.place(Host(wrong_record_hash=True), ["tests/e2e/silver_signals.py"])

    def test_no_command_names_the_manifest_and_the_launcher_cannot_place_a_file_over_it(self) -> None:
        host = Host()
        self.place(host, ["tests/e2e/silver_signals.py", "tests/e2e/stream_host.py"])
        self.assertEqual([], host.commands(runner_place.MANIFEST_NAME))
        for relative in (
            "runner_manifest.json",
            "tests/e2e/runner_manifest.json",
            "placed_runners.json",
            "tests/e2e/placed_runners.py/../../runner_manifest.json",
        ):
            with self.subTest(relative=relative), contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(HostError, "not one Python runner"):
                self.read(relative)

    def test_the_names_are_the_ones_the_role_writes(self) -> None:
        variables = group_variables()
        self.assertTrue(str(variables["runner_checkout_manifest"]).endswith(f"/{runner_place.MANIFEST_NAME}"))
        self.assertTrue(str(variables["runner_checkout_placed_runners"]).endswith(f"/{runner_place.PLACED_RECORD}"))


class RecordTest(unittest.TestCase):
    def test_the_record_holds_the_commit_and_each_hash_and_a_second_call_adds_to_it_for_the_same_commit_only(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            runner_place.record_files(folder, "a" * 40, {"tests/e2e/one.py": "1" * 64})
            runner_place.record_files(folder, "a" * 40, {"tests/e2e/two.py": "2" * 64})
            same = json.loads((folder / runner_place.RECORD_FILE).read_text())
            runner_place.record_files(folder, "b" * 40, {"tests/e2e/three.py": "3" * 64})
            newer = json.loads((folder / runner_place.RECORD_FILE).read_text())
            mode = (folder / runner_place.RECORD_FILE).stat().st_mode & 0o777
        self.assertEqual({"commit": "a" * 40, "files": {"tests/e2e/one.py": "1" * 64, "tests/e2e/two.py": "2" * 64}}, same)
        self.assertEqual({"commit": "b" * 40, "files": {"tests/e2e/three.py": "3" * 64}}, newer)
        self.assertEqual(0o644, mode)


class MakeTargetTest(unittest.TestCase):
    def test_the_make_target_needs_the_runner_names_and_runs_the_module(self) -> None:
        makefile = (ROOT / "Makefile").read_text()
        self.assertIn("host-runners-place", makefile.split(".PHONY:")[1].split("\n")[0])
        target = makefile.split("\nhost-runners-place:\n")[1].split("\n\n")[0]
        self.assertIn("$(error", target)
        self.assertIn("RUNNERS", target)
        self.assertIn("-m scripts.infra.runner_place", target)


if __name__ == "__main__":
    unittest.main()
