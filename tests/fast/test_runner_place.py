"""The placement of named Git-ignored runner files in the runner folder on the host: what it refuses, the commands it sends for each file, the hash it
records and the make target.

The remote commands are also run for real, through ``sh`` in a temporary folder that stands in for the runner folder, so that a link or a planted file in the
folder is proved to stop them or to be left alone."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
import re
import shlex
import shutil
import stat
import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path
from unittest import mock

from scripts.infra import runner_imports, runner_place
from scripts.infra.host import HostError
from scripts.process import CompletedProcess, TimeoutExpired, run_command
from tests.fast.ansible_paths import group_variables
from tests.fast.git_fixture import TemporaryRepository

ROOT = Path(__file__).resolve().parents[2]
OPTIONS = ["-o", "BatchMode=yes"]
RUNNER_TEXT = "from __future__ import annotations\n\nprint('runner')\n"
LOCK_TEXT = "pyyaml==6.0.3 \\\n    --hash=sha256:" + "0" * 64 + "\n"
GROUP_TEXT = "runner_checkout_requirements:\n  - requirements.txt\n"
FIRST = "tests/e2e/silver_signals.py"
SECOND = "tests/e2e/silver_hypotension.py"
PROBE_FOLDER = "tests/e2e/stream_stack"
# The Git-ignored files of the probe folder that the streaming host runner needs on the host.
STACK_FILES = tuple(
    f"{PROBE_FOLDER}/{name}"
    for name in (
        "batch_features.py",
        "batch_oracle.py",
        "plant_late.py",
        "read_bounds.py",
        "read_lake.py",
        "read_ready.py",
        "replay_fault.py",
        "compose.host.yml",
    )
)
OVERLAY_TEXT = "services:\n  replay:\n    image: ${REPLAY_IMAGE:?name the replay image}\n"


def completed(code: int, out: str = "") -> CompletedProcess[str]:
    return CompletedProcess(["ssh"], code, out, "")


def digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def token() -> str:
    """A credential-shaped value that is made at run time, so that no scan of this file's own text finds it."""
    return "ghp" + "_" + "aB3dE5gH7jK9mN1pQ3sT5vW7yZ9bC1dE3fG5"


class Host:
    """A stand-in for the host: it records each call, keeps the files and the record of placed runners between calls and answers a command as the remote
    command does."""

    def __init__(
        self,
        *,
        folder_exists: bool = True,
        wrong_hash: bool = False,
        code: int | None = None,
        expire: bool = False,
        record: str = "",
        wrong_record_hash: bool = False,
        disk: dict[str, str] | None = None,
    ) -> None:
        self.calls: list[tuple[str, list[str], str | None, float]] = []
        self.folder_exists, self.wrong_hash, self.code, self.expire = folder_exists, wrong_hash, code, expire
        self.record, self.wrong_record_hash = record, wrong_record_hash
        self.disk: dict[str, str] = dict(disk or {})

    def files(self) -> list[tuple[str, list[str], str | None, float]]:
        """The calls that each receive one runner file."""
        return [call for call in self.calls if "mktemp" in call[1][-1] and runner_place.PLACED_RECORD not in call[1][-1]]

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
        if "chmod 0400" in command:
            self.record = input_text or ""
            sent = "0" * 64 if self.wrong_record_hash else hashlib.sha256(data).hexdigest()
            return completed(0, f"{sent}  {runner_place.PLACED_RECORD}\n")
        if f"cat -- {runner_place.PLACED_RECORD}" in command:
            return completed(0, self.record)
        retained = re.search(r"for f in (.*?); do", command)
        if retained is not None and "sha256sum" in command:
            names = shlex.split(retained.group(1))
            if any(name not in self.disk for name in names):
                return completed(4, "retained_runner_is_not_a_plain_file\n")
            return completed(0, "".join(f"{digest(self.disk[name])}  {name}\n" for name in names))
        relative = shlex.split(command)[-1]
        self.disk[relative] = input_text or ""
        sent = "0" * 64 if self.wrong_hash else hashlib.sha256(data).hexdigest()
        return completed(0, f"{sent}  {relative}\n")


class BoundedSource(runner_imports.DiskSource):
    """The files on disk, with a limit on the reads: a read of a cycle that does not end fails the test and does not hang it."""

    reads = 0

    def read(self, relative: str) -> str | None:
        self.reads += 1
        if self.reads > 100:
            raise AssertionError("the probes that take from each other were read again and again")
        return super().read(relative)


class RepositoryCase(TemporaryRepository):
    def setUp(self) -> None:
        super().setUp()
        self.shown = io.StringIO()
        self.enterContext(contextlib.redirect_stdout(self.shown))
        (self.repo / ".gitignore").write_text(
            "tests/e2e/*.py\n!tests/e2e/__init__.py\n!tests/e2e/tracked_runner.py\n!tests/e2e/helper.py\n!tests/e2e/visible.py\n"
        )
        (self.repo / "tests" / "e2e").mkdir(parents=True)
        (self.repo / "ansible" / "group_vars").mkdir(parents=True)
        (self.repo / "ansible" / "group_vars" / "all.yml").write_text(GROUP_TEXT)
        (self.repo / "requirements.txt").write_text(LOCK_TEXT)
        for package in ("tests", "tests/e2e", "scripts"):
            (self.repo / package).mkdir(exist_ok=True)
            (self.repo / package / "__init__.py").write_text("")
        (self.repo / "tests" / "e2e" / "tracked_runner.py").write_text("print('tracked')\n")
        (self.repo / "tests" / "e2e" / "helper.py").write_text("import json\n\nVALUE = json.dumps(1)\n")
        self.write("silver_signals", RUNNER_TEXT)
        self.write("silver_hypotension", RUNNER_TEXT + "# second\n")
        self.git("add", "--all", ".")
        self.git("commit", "--quiet", "-m", "chore: fixture")
        self.commit = self.git("rev-parse", "HEAD").stdout.strip()
        (self.repo / "tests" / "e2e" / "visible.py").write_text("print('visible')\n")

    def write(self, name: str, text: str) -> Path:
        path = self.repo / "tests" / "e2e" / f"{name}.py"
        path.write_text(text)
        return path

    def read(self, relative: str) -> str:
        return runner_place.runner_text(relative, self.commit, self.repo, self.env)


class RefusalTest(RepositoryCase):
    def test_an_ignored_runner_under_tests_e2e_is_read_and_the_read_itself_prints_no_path(self) -> None:
        self.assertEqual(RUNNER_TEXT, self.read(FIRST))
        self.assertEqual("", self.shown.getvalue(), "the path is printed by the placement, after the imports passed too")

    def test_a_path_that_is_not_one_python_file_directly_under_tests_e2e_is_refused_and_is_never_printed(self) -> None:
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
            "tests/e2e/runner\x1b[2J.py",
            "tests/e2e/",
            "",
            ".env",
            "requirements.txt",
            "tests/e2e/*.py",
        ):
            with self.subTest(relative=relative), self.assertRaisesRegex(HostError, "not one runner file"):
                self.read(relative)
        self.assertEqual("", self.shown.getvalue())

    def test_a_missing_file_a_folder_a_link_and_a_pipe_are_refused_and_nothing_is_printed(self) -> None:
        (self.repo / "tests" / "e2e" / "folder.py").mkdir()
        outside = self.repo / "outside.txt"
        outside.write_text("secret\n")
        (self.repo / "tests" / "e2e" / "link.py").symlink_to(outside)
        os.mkfifo(self.repo / "tests" / "e2e" / "pipe.py")
        for name in ("absent", "folder", "link", "pipe"):
            with self.subTest(name=name), self.assertRaisesRegex(HostError, "not a plain file"):
                self.read(f"tests/e2e/{name}.py")
        self.assertEqual("", self.shown.getvalue())

    def test_a_file_with_a_second_name_is_refused_before_it_is_read(self) -> None:
        os.link(self.repo / FIRST, self.repo / "tests" / "e2e" / "second_name.py")
        for name in ("silver_signals", "second_name"):
            with self.subTest(name=name), self.assertRaisesRegex(HostError, "more than one name"):
                self.read(f"tests/e2e/{name}.py")
        self.assertEqual("", self.shown.getvalue())

    def test_a_file_that_was_swapped_for_a_link_or_another_file_after_it_was_looked_at_is_not_read(self) -> None:
        path = self.repo / FIRST
        before = path.lstat()
        other = self.repo / "other.txt"
        other.write_text("other\n")
        path.unlink()
        path.symlink_to(other)
        with self.assertRaisesRegex(HostError, "not a plain file"):
            runner_place.read_plain_file(path, before)
        path.unlink()
        path.write_text("another file\n")
        with self.assertRaisesRegex(HostError, "not a plain file"):
            runner_place.read_plain_file(path, before)

    def test_a_file_that_git_does_not_ignore_is_refused_and_so_is_one_that_the_commit_holds(self) -> None:
        with self.assertRaisesRegex(HostError, "not ignored by Git"):
            self.read("tests/e2e/visible.py")
        with self.assertRaisesRegex(HostError, "already in the commit"):
            self.read("tests/e2e/tracked_runner.py")
        self.assertEqual("", self.shown.getvalue())

    def test_a_file_that_is_too_large_has_a_nul_byte_or_is_not_text_is_refused(self) -> None:
        for name, data in (("big", b"x" * (runner_place.MAX_BYTES + 1)), ("nul", b"a\0b\n"), ("latin", b"caf\xe9\n")):
            (self.repo / "tests" / "e2e" / f"{name}.py").write_bytes(data)
            with self.subTest(name=name), self.assertRaisesRegex(HostError, "too large|not text"):
                self.read(f"tests/e2e/{name}.py")

    def test_a_file_that_holds_personal_or_environment_specific_data_is_refused_with_its_kind_and_line_and_never_shows_the_value(self) -> None:
        self.write("leaky", "OWNER = 'someone" + "@personal-domain.net'\n")
        with self.assertRaises(HostError) as caught:
            self.read("tests/e2e/leaky.py")
        self.assertIn("tests/e2e/leaky.py:1: email address", self.shown.getvalue())
        self.assertNotIn("someone@", self.shown.getvalue() + str(caught.exception))
        self.assertNotIn("leaky", str(caught.exception), "the refusal's own text is fixed and names no file")
        self.assertNotIn("checked", self.shown.getvalue())

    def test_a_commit_that_is_not_forty_hexadecimal_digits_is_refused(self) -> None:
        for commit in ("HEAD", "main", "abc", "A" * 40, self.commit[:-1], f"{self.commit}\n"):
            with self.subTest(commit=commit), self.assertRaisesRegex(HostError, "commit"):
                runner_place.runner_text(FIRST, commit, self.repo, self.env)


class CredentialScanTest(RepositoryCase):
    """The pinned credential scanner (gitleaks, as the commit hook runs it) over the exact bytes that go to the host, besides the privacy scan."""

    def test_a_file_with_a_credential_the_privacy_scan_does_not_know_is_refused_with_fixed_text_and_nothing_is_printed(self) -> None:
        self.write("keyed", f"TOKEN = '{token()}'\n")
        with self.assertRaisesRegex(HostError, "credential scanner found a possible secret") as caught:
            self.read("tests/e2e/keyed.py")
        self.assertEqual("", self.shown.getvalue(), "no path is printed for a file that failed")
        self.assertNotIn(token(), str(caught.exception))
        self.assertNotIn("keyed", str(caught.exception))

    def test_the_scanner_reads_the_exact_bytes_that_go_and_a_clean_file_passes(self) -> None:
        scratch = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="scanner_")))
        seen = scratch / "seen.bin"
        scanner = scratch / "scanner.sh"
        scanner.write_text(f"#!/bin/sh\ncat > {shlex.quote(str(seen))}\nexit 0\n")
        scanner.chmod(0o700)
        data = b"print('a\\r')\r\n# caf\xc3\xa9\n"
        (self.repo / FIRST).write_bytes(data)
        with mock.patch.object(runner_place, "GITLEAKS", scanner):
            text = self.read(FIRST)
        self.assertEqual(data, seen.read_bytes())
        self.assertEqual(data, text.encode())

    def test_a_missing_scanner_and_a_scanner_that_fails_or_does_not_finish_are_refused(self) -> None:
        scratch = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="scanner_")))
        broken = scratch / "broken.sh"
        broken.write_text("#!/bin/sh\nexit 2\n")
        broken.chmod(0o700)
        with mock.patch.object(runner_place, "GITLEAKS", scratch / "absent"), self.assertRaisesRegex(HostError, "not installed"):
            self.read(FIRST)
        with mock.patch.object(runner_place, "GITLEAKS", broken), self.assertRaisesRegex(HostError, "scan failed: exit status 2"):
            self.read(FIRST)
        slow = scratch / "slow.sh"
        slow.write_text("#!/bin/sh\nsleep 5\n")
        slow.chmod(0o700)
        with (
            mock.patch.object(runner_place, "GITLEAKS", slow),
            mock.patch.object(runner_place, "SCAN_SECONDS", 0.3),
            self.assertRaisesRegex(HostError, "did not finish within its deadline"),
        ):
            self.read(FIRST)
        self.assertEqual("", self.shown.getvalue())

    def test_the_real_scanner_still_finds_the_credential_when_configuration_comes_from_the_environment_the_folder_or_the_file_itself(self) -> None:
        scratch = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="scanner_cwd_")))
        quiet = 'title = "quiet"\n[allowlist]\nregexes = [".*"]\n'
        (scratch / "quiet.toml").write_text(quiet)
        (scratch / ".gitleaks.toml").write_text(quiet)
        self.write("keyed", f"TOKEN = '{token()}'\n")
        marked = self.write("marked", f"TOKEN = '{token()}'  # gitleaks:allow\n")
        self.assertTrue(marked.is_file())
        previous = Path.cwd()
        self.addCleanup(os.chdir, previous)
        os.chdir(scratch)
        for name, variables in (
            ("config path", {"GITLEAKS_CONFIG": str(scratch / "quiet.toml")}),
            ("config text", {"GITLEAKS_CONFIG_TOML": quiet}),
            ("config in the working folder", {}),
        ):
            with (
                self.subTest(case=name),
                mock.patch.dict(os.environ, variables),
                self.assertRaisesRegex(HostError, "credential scanner found a possible secret"),
            ):
                self.read("tests/e2e/keyed.py")
        with self.subTest(case="allow comment"), self.assertRaisesRegex(HostError, "credential scanner found a possible secret"):
            self.read("tests/e2e/marked.py")

    def test_a_scanner_that_cannot_be_started_is_refused_with_the_fixed_text_and_the_path_is_not_shown(self) -> None:
        scratch = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="scanner_")))
        closed = scratch / "closed_scanner"
        closed.write_text("#!/bin/sh\nexit 0\n")
        closed.chmod(0o600)
        garbage = scratch / "garbage_scanner"
        garbage.write_bytes(b"\x00\x01 not a program\n")
        garbage.chmod(0o700)
        for scanner in (closed, garbage):
            with (
                self.subTest(scanner=scanner.name),
                mock.patch.object(runner_place, "GITLEAKS", scanner),
                self.assertRaisesRegex(HostError, "the credential scanner cannot be started") as caught,
            ):
                self.read(FIRST)
            self.assertNotIn(str(scratch), str(caught.exception))
        self.assertEqual("", self.shown.getvalue())

    def test_the_deadline_that_the_refusal_names_is_the_deadline_of_the_scan(self) -> None:
        self.assertEqual(60, runner_place.SCAN_SECONDS)

    def test_the_scanner_is_the_pinned_one_of_the_commit_hook(self) -> None:
        hook = (ROOT / ".pre-commit-config.yaml").read_text()
        self.assertIn(".tools/bin/gitleaks", hook)
        self.assertEqual(ROOT / ".tools" / "bin" / "gitleaks", runner_place.GITLEAKS)
        self.assertTrue(runner_place.GITLEAKS.is_file(), "run make setup: the credential scanner is installed in .tools/bin")


class ImportCheckTest(RepositoryCase):
    """The imports of the files that are placed, checked against the commit's lock files before anything is sent."""

    def place(self, host: Host, names: list[str]) -> runner_place.Placement:
        return runner_place.place("run", "host.example.invalid", self.commit, names, root=self.repo, env=self.env, run=host, ssh_options=OPTIONS)

    def refused(self, names: list[str]) -> str:
        host = Host()
        with self.assertRaisesRegex(HostError, "the runner checkout would not hold"):
            self.place(host, names)
        self.assertEqual([], host.calls, "nothing is sent")
        return self.shown.getvalue()

    def test_a_package_that_the_commits_lock_files_do_not_pin_refuses_the_placement_and_names_the_package_and_the_file(self) -> None:
        self.write("takes_boto", "import boto3\n")
        shown = self.refused(["tests/e2e/takes_boto.py"])
        self.assertRegex(shown, r"boto3: .*imported by tests/e2e/takes_boto\.py\)")

    def test_a_package_taken_through_a_relative_import_of_a_tracked_module_is_found(self) -> None:
        (self.repo / "tests" / "e2e" / "helper.py").write_text("import fastavro\n")
        self.git("add", "tests/e2e/helper.py")
        self.git("commit", "--quiet", "-m", "chore: helper")
        self.commit = self.git("rev-parse", "HEAD").stdout.strip()
        self.write("takes_helper", "from . import helper\nfrom .helper import VALUE\n")
        self.assertRegex(self.refused(["tests/e2e/takes_helper.py"]), r"fastavro: .*imported by tests/e2e/helper\.py")

    def test_a_pinned_package_a_standard_library_module_and_a_tracked_module_are_accepted_and_the_files_are_read_as_the_commit_holds_them(self) -> None:
        self.write("fine", "import json\nimport yaml\nfrom . import helper\nfrom tests.e2e import helper as again\n")
        (self.repo / "tests" / "e2e" / "helper.py").write_text("import boto3\n")
        host = Host()
        self.place(host, ["tests/e2e/fine.py"])
        self.assertEqual(1, len(host.files()), "the host gets the commit's helper, which the working tree's edit does not change")

    def test_the_runners_of_one_call_are_read_as_sent_and_one_may_import_another(self) -> None:
        self.write("first_one", "VALUE = 1\n")
        self.write("second_one", "from . import first_one\n")
        self.assertEqual(2, len(self.place(Host(), ["tests/e2e/first_one.py", "tests/e2e/second_one.py"]).placed))

    def test_a_module_of_the_repository_that_the_commit_does_not_hold_is_found(self) -> None:
        self.write("takes_gone", "from scripts.gone import thing\nfrom .gone_sibling import other\n")
        shown = self.refused(["tests/e2e/takes_gone.py"])
        self.assertIn("tests/e2e/takes_gone.py: scripts.gone has no file", shown)
        self.assertIn("tests/e2e/takes_gone.py: tests.e2e.gone_sibling has no file", shown)

    def test_a_file_under_tests_e2e_that_loads_a_module_at_run_time_is_refused_unless_it_is_listed_with_its_module(self) -> None:
        self.write("loads", "import importlib\nimportlib.import_module('x')\n")
        self.assertIn("loads a module at run time", self.refused(["tests/e2e/loads.py"]))
        with mock.patch.dict(runner_imports.LOADED_BY_PATH, {"tests/e2e/loads.py": ("tests/e2e/helper.py",)}):
            self.place(Host(), ["tests/e2e/loads.py"])

    def test_a_rejected_import_prints_no_checked_path_and_a_placement_that_passes_prints_each_path_after_every_check(self) -> None:
        self.write("takes_boto", "import boto3\n")
        shown = self.refused([FIRST, "tests/e2e/takes_boto.py"])
        self.assertNotIn("checked", shown)
        self.shown.truncate(0)
        self.shown.seek(0)
        self.place(Host(), [FIRST, SECOND])
        self.assertEqual(f"checked {FIRST}\nchecked {SECOND}\n", self.shown.getvalue())

    def test_a_runner_or_a_module_that_is_not_valid_python_is_refused_with_fixed_text_and_the_line_is_never_shown(self) -> None:
        self.write("broken", "def (:\n    private_value = 1\n")
        (self.repo / "tests" / "e2e" / "helper.py").write_text("class :\n")
        self.git("add", "tests/e2e/helper.py")
        self.git("commit", "--quiet", "-m", "chore: broken helper")
        self.commit = self.git("rev-parse", "HEAD").stdout.strip()
        self.write("takes_helper", "from . import helper\n")
        for name in ("broken", "takes_helper"):
            with self.subTest(name=name):
                host = Host()
                with self.assertRaisesRegex(HostError, "the runner checkout would not hold") as caught:
                    self.place(host, [f"tests/e2e/{name}.py"])
                self.assertEqual([], host.calls)
                shown = self.shown.getvalue()
                self.assertIn("the file is not valid Python, so its imports cannot be read", shown)
                self.assertNotIn("private_value", shown + str(caught.exception))
                self.assertNotIn("checked", shown)
                self.shown.truncate(0)
                self.shown.seek(0)

    def test_a_commit_that_does_not_name_its_lock_files_or_does_not_hold_one_is_refused(self) -> None:
        for name, text in (("ansible/group_vars/all.yml", "other: 1\n"), ("ansible/group_vars/all.yml", "runner_checkout_requirements: ['../x']\n")):
            (self.repo / name).write_text(text)
            self.git("add", name)
            self.git("commit", "--quiet", "-m", "chore: no lock")
            self.commit = self.git("rev-parse", "HEAD").stdout.strip()
            with self.subTest(text=text), self.assertRaisesRegex(HostError, "does not name the lock files"):
                self.place(Host(), [FIRST])
        (self.repo / "ansible" / "group_vars" / "all.yml").write_text("runner_checkout_requirements: [missing.txt]\n")
        self.git("add", "ansible/group_vars/all.yml")
        self.git("commit", "--quiet", "-m", "chore: missing lock")
        self.commit = self.git("rev-parse", "HEAD").stdout.strip()
        with self.assertRaisesRegex(HostError, "does not hold a lock file"):
            self.place(Host(), [FIRST])


class ProbeFolderCase(RepositoryCase):
    def setUp(self) -> None:
        super().setUp()
        with (self.repo / ".gitignore").open("a") as handle:
            handle.write(f"{PROBE_FOLDER}/*\n!{PROBE_FOLDER}/tracked_probe.py\n!{PROBE_FOLDER}/visible.yml\n")
        (self.repo / PROBE_FOLDER).mkdir()
        (self.repo / PROBE_FOLDER / "tracked_probe.py").write_text("print('tracked')\n")
        (self.repo / "scripts" / "takes_image_package.py").write_text("import fastavro\n\nVALUE = 1\n")
        self.commit_files(".gitignore", f"{PROBE_FOLDER}/tracked_probe.py", "scripts/takes_image_package.py")
        (self.repo / PROBE_FOLDER / "visible.yml").write_text(OVERLAY_TEXT)
        for relative in STACK_FILES:
            self.probe(relative.rsplit("/", 1)[1], self.stack_text(relative))

    @staticmethod
    def stack_text(relative: str) -> str:
        return OVERLAY_TEXT if relative.endswith(".yml") else f"{RUNNER_TEXT}# {relative.rsplit('/', 1)[1]}\n"

    def commit_files(self, *names: str) -> None:
        self.git("add", *names)
        self.git("commit", "--quiet", "-m", "chore: more of the fixture")
        self.commit = self.git("rev-parse", "HEAD").stdout.strip()

    def probe(self, name: str, text: str) -> str:
        (self.repo / PROBE_FOLDER / name).write_text(text)
        return f"{PROBE_FOLDER}/{name}"

    def place(self, host: Host, names: Sequence[str]) -> runner_place.Placement:
        return runner_place.place("run", "host.example.invalid", self.commit, names, root=self.repo, env=self.env, run=host, ssh_options=OPTIONS)


class ProbeFolderTest(ProbeFolderCase):
    """The Git-ignored files directly under ``tests/e2e/stream_stack/``: the probes (``.py``) and the Compose overlay (``.yml``)."""

    def test_each_file_that_the_streaming_runner_needs_from_the_probe_folder_is_placed_with_its_hash(self) -> None:
        for relative in STACK_FILES:
            with self.subTest(relative=relative):
                host = Host()
                placement = self.place(host, [relative])
                self.assertEqual({relative: digest(self.stack_text(relative))}, placement.placed)
                self.assertEqual([self.stack_text(relative)], [call[2] for call in host.files()])

    def test_the_eight_go_in_one_call_beside_a_runner_and_the_record_holds_the_nested_paths_which_a_later_call_keeps(self) -> None:
        host = Host()
        self.place(host, list(STACK_FILES))
        placement = self.place(host, [FIRST])
        files = {relative: digest(self.stack_text(relative)) for relative in STACK_FILES} | {FIRST: digest(RUNNER_TEXT)}
        self.assertEqual({"commit": self.commit, "files": dict(sorted(files.items()))}, json.loads(host.record))
        self.assertEqual(files, placement.record)
        checked = shlex.split(re.findall(r"for f in (.*?); do", host.commands("for f in")[0])[0])
        self.assertEqual(sorted(STACK_FILES), checked, "each kept file of the probe folder is hashed on the host")

    def test_a_deeper_folder_another_folder_another_suffix_and_an_odd_name_are_refused_even_when_the_file_is_there_and_ignored(self) -> None:
        for relative in (
            f"{PROBE_FOLDER}/deeper/probe.py",
            f"{PROBE_FOLDER}/deeper/compose.host.yml",
            "tests/e2e/other_stack/probe.py",
            "tests/e2e/other_stack/compose.host.yml",
            "tests/e2e/compose.host.yml",
            "tests/e2e/overlay.yml",
            "tests/fast/stream_stack/probe.py",
            "stream_stack/probe.py",
            f"{PROBE_FOLDER}/probe.sh",
            f"{PROBE_FOLDER}/probe.json",
            f"{PROBE_FOLDER}/compose.host.yaml",
            f"{PROBE_FOLDER}/probe.py.bak",
            f"{PROBE_FOLDER}/dotted.probe.py",
            f"{PROBE_FOLDER}/compose..yml",
            f"{PROBE_FOLDER}/compose.host..yml",
            f"{PROBE_FOLDER}/.hidden.yml",
            f"{PROBE_FOLDER}/Compose.yml",
            f"{PROBE_FOLDER}/compose.host.yml ",
            f"{PROBE_FOLDER}/compose host.yml",
            f"{PROBE_FOLDER}/../stream_stack/read_lake.py",
            f"{PROBE_FOLDER}/",
            f"{PROBE_FOLDER}/*.py",
        ):
            if ".." not in relative.split("/") and not relative.endswith("/") and "*" not in relative:
                (self.repo / relative).parent.mkdir(parents=True, exist_ok=True)
                (self.repo / relative).write_text(RUNNER_TEXT)
            with self.subTest(relative=relative), self.assertRaisesRegex(HostError, "not one runner file"):
                self.read(relative)

    def test_a_tracked_file_and_a_file_that_git_does_not_ignore_in_the_probe_folder_are_refused(self) -> None:
        with self.assertRaisesRegex(HostError, "already in the commit"):
            self.read(f"{PROBE_FOLDER}/tracked_probe.py")
        with self.assertRaisesRegex(HostError, "not ignored by Git"):
            self.read(f"{PROBE_FOLDER}/visible.yml")

    def test_a_link_a_hard_link_and_a_linked_probe_folder_are_refused(self) -> None:
        folder = self.repo / PROBE_FOLDER
        (folder / "link.yml").symlink_to(folder / "compose.host.yml")
        with self.assertRaisesRegex(HostError, "not a plain file"):
            self.read(f"{PROBE_FOLDER}/link.yml")
        os.link(folder / "compose.host.yml", folder / "second.yml")
        with self.assertRaisesRegex(HostError, "more than one name"):
            self.read(f"{PROBE_FOLDER}/second.yml")
        moved = self.repo / "moved_stack"
        folder.rename(moved)
        folder.symlink_to(moved)
        with self.assertRaisesRegex(HostError, "not a plain file"):
            self.read(f"{PROBE_FOLDER}/read_lake.py")

    def test_an_overlay_gets_the_privacy_scan_and_the_credential_scan_over_its_bytes(self) -> None:
        private = self.probe("private.yml", "owner: someone" + "@personal-domain.net\n")
        with self.assertRaisesRegex(HostError, "personal or environment-specific data"):
            self.read(private)
        self.assertIn(f"{private}:1: email address", self.shown.getvalue())
        keyed = self.probe("keyed.yml", f"token: '{token()}'\n")
        with self.assertRaisesRegex(HostError, "credential scanner found a possible secret"):
            self.read(keyed)


class ProbeImportTest(ProbeFolderCase):
    """The probes run in the replay and Spark images. Their imports are resolved against the commit, but what the repository's own modules take comes from the
    image, so the host's lock files are not asked for it."""

    def refused(self, names: Sequence[str]) -> str:
        host = Host()
        with self.assertRaisesRegex(HostError, "the runner checkout would not hold"):
            self.place(host, names)
        self.assertEqual([], host.calls, "nothing is sent")
        return self.shown.getvalue()

    def test_a_package_that_a_probe_reaches_through_a_repository_module_comes_from_the_image_and_is_not_asked_of_the_hosts_lock(self) -> None:
        name = self.probe("reaches.py", "from scripts.takes_image_package import VALUE\n")
        self.assertEqual([name], list(self.place(Host(), [name]).placed))

    def test_a_host_runner_that_reaches_the_same_module_is_still_refused_even_in_one_call_with_the_probe(self) -> None:
        name = self.probe("reaches.py", "from scripts.takes_image_package import VALUE\n")
        self.write("host_reaches", "from scripts.takes_image_package import VALUE\n")
        for names in ([name, "tests/e2e/host_reaches.py"], ["tests/e2e/host_reaches.py", name]):
            with self.subTest(names=names):
                self.assertRegex(self.refused(names), r"fastavro: .*imported by scripts/takes_image_package\.py")

    def test_a_package_that_a_probe_names_itself_must_be_pinned_as_for_a_runner(self) -> None:
        name = self.probe("takes_boto.py", "import json\nimport yaml\n\ndef later():\n    import boto3\n")
        shown = self.refused([name])
        self.assertRegex(shown, rf"boto3: .*imported by {name}\)")
        self.assertNotIn("yaml:", shown)

    def test_a_probe_imports_another_probe_of_the_same_call_and_one_that_the_call_does_not_hold_is_refused(self) -> None:
        user = self.probe("user_probe.py", "from batch_oracle import CaseBatch\n")
        oracle = self.probe("batch_oracle.py", "class CaseBatch:\n    pass\n")
        self.assertEqual(2, len(self.place(Host(), [user, oracle]).placed))
        self.assertRegex(self.refused([user]), rf"batch_oracle: .*imported by {user}\)")

    def test_each_kind_of_name_at_the_top_level_of_a_sibling_probe_can_be_taken_from_it(self) -> None:
        oracle = self.probe(
            "batch_oracle.py",
            "from json import dumps\nimport yaml as loader\nfrom scripts.takes_image_package import *\nLIMIT: int = 3\nfirst, second = 1, 2\n"
            "try:\n    GUARDED = 1\nexcept ValueError:\n    pass\n\ndef build():\n    pass\n\nclass CaseBatch:\n    pass\n",
        )
        user = self.probe(
            "user_probe.py",
            "import batch_oracle\nimport batch_oracle as oracle\n"
            "from batch_oracle import CaseBatch, build as make, dumps, loader, LIMIT, first, second, GUARDED, VALUE\n",
        )
        self.assertEqual(2, len(self.place(Host(), [user, oracle]).placed))

    def test_a_name_that_the_sibling_probe_does_not_hold_at_its_top_level_is_refused(self) -> None:
        oracle = self.probe("batch_oracle.py", "VALUE = 1\n\ndef build():\n    in_a_function = 1\n\nclass Holder:\n    in_a_class = 1\n")
        for name, text in (
            ("CaseBatch", "from batch_oracle import CaseBatch\n"),
            ("CaseBatch", "from batch_oracle import VALUE, CaseBatch\n"),
            ("in_a_function", "from batch_oracle import in_a_function\n"),
            ("in_a_class", "from batch_oracle import in_a_class\n"),
            ("CaseBatch", "def later():\n    from batch_oracle import CaseBatch\n"),
        ):
            with self.subTest(text=text):
                user = self.probe("user_probe.py", text)
                self.assertIn(f"{user}: the probe batch_oracle has no name {name} at its top level\n", self.refused([user, oracle]))
                self.shown.truncate(0)
                self.shown.seek(0)

    def assert_each_name_is_refused(self, cases: Sequence[tuple[str, str]]) -> None:
        """Each case is the text of ``batch_oracle`` and the name that another probe cannot take from it."""
        for text, name in cases:
            with self.subTest(text=text):
                oracle = self.probe("batch_oracle.py", text)
                user = self.probe("user_probe.py", f"from batch_oracle import {name}\n")
                self.assertIn(f"{user}: the probe batch_oracle has no name {name} at its top level\n", self.refused([user, oracle]))
                self.shown.truncate(0)
                self.shown.seek(0)

    def test_an_annotation_without_a_value_defines_no_name_that_another_probe_can_take(self) -> None:
        self.assert_each_name_is_refused([("VALUE: int\n", "VALUE"), ("if True:\n    VALUE: int\n", "VALUE")])

    def test_a_name_that_a_direct_del_removes_at_the_top_level_cannot_be_taken_from_the_probe(self) -> None:
        self.assert_each_name_is_refused(
            [
                ("VALUE = 1\ndel VALUE\n", "VALUE"),
                ("first, second = 1, 2\ndel (first, second)\n", "second"),
                ("first, second = 1, 2\ndel second, first\n", "first"),
                ("import json\ndel json\n", "json"),
                ("def build():\n    pass\n\ndel build\n", "build"),
                ("if True:\n    VALUE = 1\ndel VALUE\n", "VALUE"),
                ("VALUE = 1\ndel VALUE\nVALUE: int\n", "VALUE"),
            ]
        )

    def test_a_name_that_stays_bound_after_an_annotation_or_a_del_can_be_taken_from_the_probe(self) -> None:
        oracle = self.probe(
            "batch_oracle.py",
            "again = 1\ndel again\nagain = 2\nkept = 1\nkept: int\nholder = {'a': 1}\ndel holder['a']\nowner = type('Owner', (), {'a': 1})\ndel owner.a\n"
            "unmoved = 1\nif not unmoved:\n    del unmoved\n\ndef later():\n    global unmoved\n    del unmoved\n",
        )
        user = self.probe("user_probe.py", "from batch_oracle import again, kept, holder, owner, unmoved\n")
        self.assertEqual(2, len(self.place(Host(), [user, oracle]).placed))

    def test_a_probe_passes_on_the_names_that_it_takes_from_another_probe_by_a_star_or_by_name(self) -> None:
        for oracle_text, helper_text, taken in (
            ("__all__ = ['VALUE']\nVALUE = 1\n", "from batch_oracle import *\n", "VALUE"),
            ("VALUE = 1\n", "from batch_oracle import VALUE\n", "VALUE"),
            ("VALUE = 1\n", "from batch_oracle import VALUE as renamed\n", "renamed"),
            ("__all__ = ['listed', '_named']\nlisted = 1\n_named = 2\n", "from batch_oracle import *\n", "listed, _named"),
            ("__all__ = ('listed',)\nlisted = 1\n", "from batch_oracle import *\n", "listed"),
            ("from scripts.takes_image_package import *\n__all__ = ['VALUE']\n", "from batch_oracle import *\n", "VALUE"),
        ):
            with self.subTest(oracle=oracle_text, helper=helper_text):
                oracle = self.probe("batch_oracle.py", oracle_text)
                helper = self.probe("batch_helper.py", helper_text)
                user = self.probe("user_probe.py", f"from batch_helper import {taken}\n")
                self.assertEqual(3, len(self.place(Host(), [user, helper, oracle]).placed))

    def test_a_star_from_another_probe_passes_on_only_what_python_would_take(self) -> None:
        for oracle_text, name in (
            ("VALUE = 1\n", "MISSING"),
            ("_private = 1\n", "_private"),
            ("__all__ = ['listed']\nlisted = 1\nunlisted = 2\n", "unlisted"),
            ("__all__ = ['never_bound']\n", "never_bound"),
            ("VALUE: int\n", "VALUE"),
            ("VALUE = 1\ndel VALUE\n", "VALUE"),
            ("__all__ = ['listed']\n__all__ += ['added']\nlisted = 1\nadded = 2\n", "listed"),
            ("__all__ = ['listed']\n__all__.remove('listed')\nlisted = 1\n", "listed"),
            ("names = ['listed']\n__all__ = names\nlisted = 1\n", "listed"),
            ("if True:\n    __all__ = ['listed']\nlisted = 1\n", "listed"),
        ):
            with self.subTest(oracle=oracle_text, name=name):
                oracle = self.probe("batch_oracle.py", oracle_text)
                helper = self.probe("batch_helper.py", "from batch_oracle import *\n")
                user = self.probe("user_probe.py", f"from batch_helper import {name}\n")
                self.assertIn(f"{user}: the probe batch_helper has no name {name} at its top level\n", self.refused([user, helper, oracle]))
                self.shown.truncate(0)
                self.shown.seek(0)

    def test_the_all_of_a_repository_module_does_not_limit_what_a_probe_that_takes_its_star_passes_on(self) -> None:
        (self.repo / "scripts" / "listing.py").write_text("__all__ = []\nLISTED = 1\n")
        self.commit_files("scripts/listing.py")
        oracle = self.probe("batch_oracle.py", "from scripts.listing import *\n__all__ = ['LISTED', 'VALUE']\nVALUE = 1\n")
        helper = self.probe("batch_helper.py", "from batch_oracle import *\n")
        user = self.probe("user_probe.py", "from batch_helper import LISTED, VALUE\n")
        self.assertEqual(3, len(self.place(Host(), [user, helper, oracle]).placed))

    def test_probes_that_take_a_star_from_each_other_are_read_once_each_and_the_read_ends(self) -> None:
        oracle = self.probe("batch_oracle.py", "from batch_helper import *\n__all__ = ['VALUE']\nVALUE = 1\n")
        helper = self.probe("batch_helper.py", "from batch_oracle import *\nfrom batch_helper import *\n__all__ = ['VALUE']\n")
        user = self.probe("user_probe.py", "from batch_helper import VALUE, MISSING\n")
        siblings = {Path(name).stem: name for name in (user, helper, oracle)}
        problems = runner_imports.sibling_problems(BoundedSource(self.repo), siblings)
        self.assertEqual([f"{user}: the probe batch_helper has no name MISSING at its top level"], problems)
        user = self.probe("user_probe.py", "from batch_helper import VALUE\n")
        self.assertEqual(3, len(self.place(Host(), [user, helper, oracle]).placed))

    def test_a_relative_import_in_a_probe_is_refused_because_a_probe_runs_on_its_own_in_the_image(self) -> None:
        support = self.probe("support_probe.py", "VALUE = 1\n")
        for text in (
            "from .support_probe import VALUE\n",
            "from .support_probe import *\n",
            "from . import support_probe\n",
            "def later():\n    from .support_probe import VALUE\n",
            "from ..runner_checks import anything\n",
        ):
            with self.subTest(text=text):
                user = self.probe("user_probe.py", text)
                self.assertIn(f"{user}: a probe runs on its own, so its relative import fails in the image\n", self.refused([user, support]))
                self.shown.truncate(0)
                self.shown.seek(0)

    def assert_the_star_is_refused(self, oracle_text: str, user_text: str, reason: str) -> None:
        support = self.probe("support_probe.py", oracle_text)
        user = self.probe("user_probe.py", user_text)
        shown = self.refused([user, support])
        self.assertEqual(f"{user}: a star import of the probe support_probe is refused: {reason}\n", shown)
        self.shown.truncate(0)
        self.shown.seek(0)

    def test_a_star_import_of_a_probe_that_lists_a_name_it_does_not_bind_unconditionally_is_refused_by_the_position_of_the_entry(self) -> None:
        for oracle_text, position in (
            ("__all__ = ['never_bound']\n", 1),
            ("__all__ = ('listed', 'gone')\nlisted = 1\ngone = 2\ndel gone\n", 2),
            ("__all__ = ['typed']\ntyped: int\n", 1),
            ("__all__ = ['typed']\ntyped: int = 1\n", 1),
            ("__all__ = ['inside']\n\ndef build():\n    inside = 1\n", 1),
            ("from typing import TYPE_CHECKING\n__all__ = ['hidden']\nif TYPE_CHECKING:\n    hidden = 1\n", 1),
            ("__all__ = ['hidden']\nif TYPE_CHECKING:\n    from json import dumps as hidden\n", 1),
            ("__all__ = ['hidden']\ntry:\n    hidden = 1\nexcept ValueError:\n    pass\n", 1),
            ("__all__ = ['hidden']\nmatch 1:\n    case 2:\n        hidden = 1\n", 1),
            ("__all__ = ['hidden']\nwith open('x'):\n    hidden = 1\n", 1),
            ("__all__ = ['hidden']\nfor hidden in ():\n    pass\n", 1),
            ("__all__ = ['hidden']\nwhile False:\n    def hidden():\n        pass\n", 1),
            ("__all__ = ['hidden']\n(hidden := 1)\n", 1),
            ("__all__ = ['hidden']\nholder = (hidden := 1)\n", 1),
            ("__all__ = ['hidden']\nhidden += 1\n", 1),
            ("__all__ = ['hidden']\nholder = {}\nholder[hidden] = 1\nholder.hidden = 2\n", 1),
        ):
            for user_text in ("from support_probe import *\n", "try:\n    from support_probe import *\nexcept ImportError:\n    pass\n"):
                with self.subTest(oracle=oracle_text, user=user_text):
                    reason = f"entry {position} of its __all__ is not bound by an unconditional top-level def, class, plain assignment or import"
                    self.assert_the_star_is_refused(oracle_text, user_text, reason)

    def test_a_star_import_of_a_probe_without_exactly_one_plain_top_level_all_of_string_literals_is_refused(self) -> None:
        reason = "it needs one __all__, a plain top-level list or tuple of string literals, and no other mention of that name"
        for oracle_text in (
            "VALUE = 1\n",
            "names = ['listed']\n__all__ = names\nlisted = 1\n",
            "__all__ = ['listed']\n__all__ += ['added']\nlisted = 1\nadded = 2\n",
            "__all__ = ['listed']\n__all__.remove('listed')\nlisted = 1\n",
            "__all__ = ['listed']\n__all__ = ['listed']\nlisted = 1\n",
            "__all__: tuple[str, ...] = ('listed',)\nlisted = 1\n",
            "__all__ = other = ['listed']\nlisted = 1\n",
            "__all__ = ['listed'] + []\nlisted = 1\n",
            "__all__ = {'listed'}\nlisted = 1\n",
            "__all__ = ['listed', 3]\nlisted = 1\n",
            "__all__ = [*()]\n",
            "if True:\n    __all__ = ['listed']\nlisted = 1\n",
            "match 1:\n    case 1:\n        __all__ = ['hidden']\n",
            "names = ['hidden']\n(__all__ := names)\n",
            "__all__ = ['listed']\nlisted = 1\n(__all__ := ['hidden'])\n",
            "__all__ = ['listed']\nlisted = 1\nmatch ['hidden']:\n    case __all__:\n        pass\n",
            "__all__ = ['listed']\nlisted = 1\nmatch {}:\n    case {**__all__}:\n        pass\n",
            "__all__ = ['listed']\nlisted = 1\ndel __all__\n",
            "__all__ = ['listed']\nlisted = 1\nfrom json import __all__\n",
            "__all__ = ['listed']\nlisted = 1\nimport json as __all__\n",
            "__all__ = ['listed']\nlisted = 1\ndef change():\n    global __all__\n",
            "__all__ = ['listed']\nlisted = 1\nglobals()['__all__'] = ['hidden']\n",
            "import sys\n__all__ = ['listed']\nlisted = 1\nsys.modules[__name__].__all__ = ['hidden']\n",
            "__all__ = ['__all__']\n",
        ):
            with self.subTest(oracle=oracle_text):
                self.assert_the_star_is_refused(oracle_text, "from support_probe import *\n", reason)

    def test_the_refusal_of_a_listed_entry_shows_no_text_of_the_source(self) -> None:
        for entry in ("a private sentence of the source", "unbound_marker_name", "x\\n::error::"):
            with self.subTest(entry=entry):
                support = self.probe("support_probe.py", f"__all__ = ['listed', '{entry}']\nlisted = 1\n")
                user = self.probe("user_probe.py", "from support_probe import *\n")
                host = Host()
                with self.assertRaisesRegex(HostError, "the runner checkout would not hold") as caught:
                    self.place(host, [user, support])
                shown = self.shown.getvalue()
                self.assertIn("entry 2 of its __all__", shown)
                for part in ("private sentence", "unbound_marker_name", "::error::"):
                    self.assertNotIn(part, shown + str(caught.exception))
                self.shown.truncate(0)
                self.shown.seek(0)

    def test_a_star_import_of_a_probe_whose_all_names_only_what_it_binds_unconditionally_is_accepted(self) -> None:
        for oracle_text in (
            "__all__ = ['listed', '_named', 'dumps', 'VALUE']\nlisted = 1\n_named = 2\nfrom json import dumps\nfrom scripts.takes_image_package import *\n",
            "__all__ = ('first', 'second', 'rest', 'build', 'later', 'Case', 'json', 'loader')\nfirst, (second, *rest) = 1, (2, 3)\n\n"
            "def build():\n    pass\n\nasync def later():\n    pass\n\nclass Case:\n    pass\n\nimport json.decoder\nimport yaml as loader\n",
            "__all__ = ['again']\nagain = 1\ndel again\nagain = 2\n",
            "__all__ = []\n",
            "VALUE = 1\n__all__ = ['VALUE']\n",
        ):
            with self.subTest(oracle=oracle_text):
                support = self.probe("support_probe.py", oracle_text)
                user = self.probe("user_probe.py", "from support_probe import *\n")
                self.assertEqual(2, len(self.place(Host(), [user, support]).placed))

    def test_a_star_import_from_outside_the_repository_lists_no_name_that_another_probe_can_take(self) -> None:
        oracle = self.probe("batch_oracle.py", "from json import *\n\ndef __getattr__(name):\n    return name\n")
        user = self.probe("user_probe.py", "from batch_oracle import dumps\n")
        self.assertIn(f"{user}: the probe batch_oracle has no name dumps at its top level\n", self.refused([user, oracle]))

    def test_a_submodule_of_a_sibling_probe_is_refused_because_a_probe_is_a_module_and_not_a_package(self) -> None:
        oracle = self.probe("batch_oracle.py", "VALUE = 1\n")
        for module, text in (
            ("batch_oracle.missing", "import batch_oracle.missing\n"),
            ("batch_oracle.VALUE", "import batch_oracle.VALUE as value\n"),
            ("batch_oracle.missing", "from batch_oracle.missing import VALUE\n"),
            ("batch_oracle.missing", "def later():\n    import batch_oracle.missing\n"),
        ):
            with self.subTest(text=text):
                user = self.probe("user_probe.py", text)
                self.assertIn(f"{user}: {module} cannot be imported: the probe batch_oracle is a module, not a package\n", self.refused([user, oracle]))
                self.shown.truncate(0)
                self.shown.seek(0)

    def test_the_sibling_probes_own_imports_are_checked_too(self) -> None:
        self.probe("batch_oracle.py", "import boto3\n")
        user = self.probe("user_probe.py", "import batch_oracle\n")
        shown = self.refused([user, f"{PROBE_FOLDER}/batch_oracle.py"])
        self.assertRegex(shown, rf"boto3: .*imported by {PROBE_FOLDER}/batch_oracle\.py\)")
        self.assertNotIn("batch_oracle: ", shown)

    def test_a_host_runner_cannot_take_a_probe_by_the_name_that_only_the_containers_have(self) -> None:
        self.write("takes_sibling", "import batch_oracle\n")
        shown = self.refused(["tests/e2e/takes_sibling.py", f"{PROBE_FOLDER}/batch_oracle.py"])
        self.assertRegex(shown, r"batch_oracle: .*imported by tests/e2e/takes_sibling\.py\)")

    def test_a_probe_with_a_missing_repository_module_a_broken_text_or_a_load_at_run_time_is_refused(self) -> None:
        for name, text, line in (
            ("gone.py", "from scripts.gone import thing\n", "scripts.gone has no file"),
            ("broken.py", "def (:\n", "the file is not valid Python"),
            ("loads.py", "import importlib\nimportlib.import_module('x')\n", "loads a module at run time"),
        ):
            with self.subTest(name=name):
                self.assertIn(line, self.refused([self.probe(name, text)]))
                self.shown.truncate(0)
                self.shown.seek(0)

    def test_an_overlay_is_not_read_as_python(self) -> None:
        overlay = f"{PROBE_FOLDER}/compose.host.yml"
        self.probe("compose.host.yml", "services:\n  - not: [python\n")
        self.assertEqual([overlay], list(self.place(Host(), [overlay]).placed))


class PlacementTest(RepositoryCase):
    def place(self, host: Host, names: list[str] | None = None) -> dict[str, str]:
        return runner_place.place(
            "run", "host.example.invalid", self.commit, names or [FIRST], root=self.repo, env=self.env, run=host, ssh_options=OPTIONS
        ).placed

    def test_each_file_is_one_ssh_command_that_receives_the_file_and_nothing_else_and_its_hash_is_returned(self) -> None:
        host = Host()
        hashes = self.place(host, [FIRST, SECOND])
        self.assertEqual(2, len(host.files()))
        self.assertEqual({FIRST: digest(RUNNER_TEXT), SECOND: digest(RUNNER_TEXT + "# second\n")}, hashes)
        self.assertEqual([RUNNER_TEXT, RUNNER_TEXT + "# second\n"], [call[2] for call in host.files()])

    def test_the_remote_command_works_in_the_commits_folder_after_its_marker_and_keeps_the_file_from_others(self) -> None:
        host = Host()
        self.place(host)
        program, args, _, timeout = host.files()[0]
        self.assertEqual("ssh", program)
        self.assertIn("ubuntu@host.example.invalid", args)
        self.assertLessEqual(timeout, runner_place.SSH_SECONDS)
        command = args[-1]
        self.assertIn(f"cd -- {runner_place.RUNNER_DIRECTORY}/{self.commit}", command)
        self.assertIn(runner_place.MARKER, command)
        self.assertIn("umask 077", command)
        self.assertIn("mktemp", command)
        self.assertNotIn(".part", command, "no temporary name that others can predict")
        for refused in ("sudo", "chmod", "chown", "rm -r", "rm -rf"):
            self.assertNotIn(refused, command)

    def test_a_missing_runner_folder_says_to_deploy_this_commit_first_and_a_wrong_remote_hash_is_refused(self) -> None:
        with self.assertRaisesRegex(HostError, "no runner folder for this commit.*make host-up"):
            self.place(Host(folder_exists=False))
        with self.assertRaisesRegex(HostError, "differs from the one sent"):
            self.place(Host(wrong_hash=True))

    def test_a_linked_folder_on_the_host_is_refused_with_its_own_text(self) -> None:
        class Linked(Host):
            def __call__(self, program: str, args: Sequence[str], *, timeout: float, input_text: str | None = None) -> CompletedProcess[str]:
                self.calls.append((program, list(args), input_text, timeout))
                return completed(runner_place.LINKED_CODE, runner_place.LINKED_WORD + "\n")

        with self.assertRaisesRegex(HostError, "is a link or is no folder"):
            self.place(Linked())

    def test_an_ssh_failure_and_an_expired_deadline_are_refused_with_fixed_text(self) -> None:
        with self.assertRaisesRegex(HostError, "exit status 255"):
            self.place(Host(code=255))
        with self.assertRaisesRegex(HostError, "did not finish within its deadline"):
            self.place(Host(expire=True))

    def test_a_refused_file_sends_nothing_even_when_it_comes_after_a_good_one(self) -> None:
        host = Host()
        with self.assertRaises(HostError):
            self.place(host, [FIRST, "tests/e2e/tracked_runner.py"])
        self.assertEqual([], host.calls)

    def test_a_file_named_twice_is_refused_and_no_name_means_nothing_to_place(self) -> None:
        with self.assertRaisesRegex(HostError, "twice"):
            self.place(Host(), [FIRST, FIRST])
        with self.assertRaisesRegex(HostError, "names no runner"):
            runner_place.place("run", "host.example.invalid", self.commit, [], root=self.repo, env=self.env, run=Host(), ssh_options=OPTIONS)


class PlacedRecordTest(RepositoryCase):
    """``placed_runners.json`` in the checkout's root, on the host: the hashes of the placed runners, for the runners to check their own files."""

    def place(self, host: Host, names: list[str]) -> runner_place.Placement:
        return runner_place.place("run", "host.example.invalid", self.commit, names, root=self.repo, env=self.env, run=host, ssh_options=OPTIONS)

    def test_the_host_gets_the_commit_and_the_hash_of_each_placed_runner_in_the_one_form_the_manifest_has(self) -> None:
        host = Host()
        self.place(host, [FIRST, SECOND])
        document = json.loads(host.record)
        self.assertEqual(["commit", "files"], sorted(document))
        self.assertEqual(self.commit, document["commit"])
        self.assertEqual({FIRST: digest(RUNNER_TEXT), SECOND: digest(RUNNER_TEXT + "# second\n")}, document["files"])
        self.assertTrue(host.record.endswith("}\n"))

    def test_a_later_call_keeps_the_hash_of_a_runner_that_an_earlier_call_placed_after_the_host_proved_it(self) -> None:
        host = Host()
        self.place(host, [FIRST])
        placement = self.place(host, [SECOND])
        files = {FIRST: digest(RUNNER_TEXT), SECOND: digest(RUNNER_TEXT + "# second\n")}
        self.assertEqual(files, json.loads(host.record)["files"])
        self.assertEqual(files, placement.record)
        self.assertEqual({SECOND: files[SECOND]}, placement.placed)
        self.assertEqual(1, len(host.commands("for f in")), "the kept runner was hashed on the host before the record was written")

    def test_a_runner_that_is_placed_again_gets_its_new_hash_and_the_others_keep_theirs(self) -> None:
        host = Host()
        self.place(host, [FIRST, SECOND])
        self.write("silver_signals", RUNNER_TEXT + "# edited\n")
        self.place(host, [FIRST])
        self.assertEqual({FIRST: digest(RUNNER_TEXT + "# edited\n"), SECOND: digest(RUNNER_TEXT + "# second\n")}, json.loads(host.record)["files"])

    def test_a_kept_runner_that_differs_on_the_host_from_the_record_is_refused_before_a_file_is_sent_and_naming_it_replaces_it(self) -> None:
        record = json.dumps({"commit": self.commit, "files": {SECOND: digest("what the record says")}})
        host = Host(record=record, disk={SECOND: "what another process wrote"})
        with self.assertRaisesRegex(HostError, "differs on the host from the host's record"):
            self.place(host, [FIRST])
        self.assertEqual([], host.files())
        self.assertEqual(record, host.record, "the record is not touched")
        self.place(host, [FIRST, SECOND])
        self.assertEqual({FIRST: digest(RUNNER_TEXT), SECOND: digest(RUNNER_TEXT + "# second\n")}, json.loads(host.record)["files"])

    def test_a_kept_runner_that_is_missing_on_the_host_is_refused_with_its_own_text(self) -> None:
        record = json.dumps({"commit": self.commit, "files": {SECOND: digest(RUNNER_TEXT)}})
        host = Host(record=record)
        with self.assertRaisesRegex(HostError, "missing on the host or is not a plain file"):
            self.place(host, [FIRST])
        self.assertEqual([], host.files())

    def test_the_record_is_written_after_every_file_is_placed_and_checked_and_is_closed_to_its_owners_read_only(self) -> None:
        host = Host()
        self.place(host, [FIRST])
        written = host.commands("chmod 0400")
        self.assertEqual(1, len(written))
        self.assertIs(host.calls[-1][1][-1], written[0])
        command = written[0]
        self.assertIn(f"cd -- {runner_place.RUNNER_DIRECTORY}/{self.commit}", command)
        self.assertIn(runner_place.MARKER, command)
        self.assertIn("mktemp", command)
        for refused in ("sudo", "chown", "rm -r", "rm -rf", "tests/e2e"):
            self.assertNotIn(refused, command)

    def test_the_record_of_another_commit_is_started_again_and_one_that_is_not_valid_is_refused_before_a_file_is_sent(self) -> None:
        stale = json.dumps({"commit": "b" * 40, "files": {"tests/e2e/old.py": "1" * 64}})
        host = Host(record=stale)
        self.place(host, [FIRST])
        self.assertEqual({FIRST: digest(RUNNER_TEXT)}, json.loads(host.record)["files"])
        bad = {
            "not json": "{",
            "not an object": "[]",
            "extra key": json.dumps({"commit": self.commit, "files": {}, "more": 1}),
            "files is a list": json.dumps({"commit": self.commit, "files": []}),
            "path outside tests/e2e": json.dumps({"commit": self.commit, "files": {"../x.py": "1" * 64}}),
            "the manifest": json.dumps({"commit": self.commit, "files": {"runner_manifest.json": "1" * 64}}),
            "short hash": json.dumps({"commit": self.commit, "files": {"tests/e2e/a.py": "1"}}),
            "a deeper path": json.dumps({"commit": self.commit, "files": {"tests/e2e/stream_stack/deeper/a.py": "1" * 64}}),
            "an overlay outside the probe folder": json.dumps({"commit": self.commit, "files": {"tests/e2e/a.yml": "1" * 64}}),
        }
        for name, record in bad.items():
            with self.subTest(record=name):
                host = Host(record=record)
                with self.assertRaisesRegex(HostError, "placed_runners.json.*not valid"):
                    self.place(host, [FIRST])
                self.assertEqual(1, len(host.calls), "only the read of the record was sent")

    def test_a_wrong_hash_of_the_written_record_is_refused(self) -> None:
        with self.assertRaisesRegex(HostError, "record.*differs"):
            self.place(Host(wrong_record_hash=True), [FIRST])

    def test_no_command_names_the_manifest_and_the_launcher_cannot_place_a_file_over_it(self) -> None:
        host = Host()
        self.place(host, [FIRST, SECOND])
        self.assertEqual([], host.commands(runner_place.MANIFEST_NAME))
        for relative in (
            "runner_manifest.json",
            "tests/e2e/runner_manifest.json",
            "placed_runners.json",
            "tests/e2e/placed_runners.py/../../runner_manifest.json",
        ):
            with self.subTest(relative=relative), self.assertRaisesRegex(HostError, "not one runner file"):
                self.read(relative)

    def test_the_names_are_the_ones_the_role_writes(self) -> None:
        variables = group_variables()
        self.assertTrue(str(variables["runner_checkout_manifest"]).endswith(f"/{runner_place.MANIFEST_NAME}"))
        self.assertTrue(str(variables["runner_checkout_placed_runners"]).endswith(f"/{runner_place.PLACED_RECORD}"))
        expected = variables["runner_checkout_expected"]
        self.assertIsInstance(expected, dict)
        self.assertEqual(sorted(expected), sorted(runner_place.MARKER_FIELDS), "the guard reads exactly the fields that the role writes in the marker")


class RecordTest(unittest.TestCase):
    def test_the_local_record_is_the_hosts_whole_record_and_a_second_call_replaces_it_and_never_adds_an_unverified_entry(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            runner_place.record_files(folder, "a" * 40, {"tests/e2e/one.py": "1" * 64, "tests/e2e/two.py": "2" * 64})
            first = json.loads((folder / runner_place.RECORD_FILE).read_text())
            runner_place.record_files(folder, "a" * 40, {"tests/e2e/two.py": "9" * 64})
            same = json.loads((folder / runner_place.RECORD_FILE).read_text())
            runner_place.record_files(folder, "b" * 40, {"tests/e2e/three.py": "3" * 64})
            newer = json.loads((folder / runner_place.RECORD_FILE).read_text())
            mode = (folder / runner_place.RECORD_FILE).stat().st_mode & 0o777
        self.assertEqual({"commit": "a" * 40, "files": {"tests/e2e/one.py": "1" * 64, "tests/e2e/two.py": "2" * 64}}, first)
        self.assertEqual({"commit": "a" * 40, "files": {"tests/e2e/two.py": "9" * 64}}, same)
        self.assertEqual({"commit": "b" * 40, "files": {"tests/e2e/three.py": "3" * 64}}, newer)
        self.assertEqual(0o644, mode)


class RemoteCommandTest(unittest.TestCase):
    """The remote commands, run through ``sh`` in a temporary folder that stands for the runner folder: a link, a planted name and a missing marker."""

    COMMIT = "c" * 40

    def setUp(self) -> None:
        self.base = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="remote_")))
        self.directory = self.base / "runner"
        self.folder = self.directory / self.COMMIT
        self.marker = self.folder / runner_place.MARKER
        self.outside = self.base / "outside"
        self.build()
        shims = self.base / "shims"
        shims.mkdir()
        if shutil.which("sha256sum") is None:
            shim = shims / "sha256sum"
            shim.write_text('#!/bin/sh\nexec shasum -a 256 "$@"\n')
            shim.chmod(0o755)
        self.env = {"PATH": f"{shims}:{os.environ['PATH']}"}

    def build(self) -> None:
        """A finished folder of the commit with its marker, and a folder outside it that nothing may write to."""
        (self.folder / "tests" / "e2e").mkdir(parents=True)
        self.marker.write_text(self.marker_text())
        self.outside.mkdir()

    def marker_text(self, **changes: object) -> str:
        """The marker as the role writes it (``runner_checkout_marker_text``): sorted JSON with one line end."""
        fields: dict[str, object] = {
            "commit": self.COMMIT,
            "python": "3.14.10",
            "python_archive_sha256": "a" * 64,
            "requirements": ["requirements.txt"],
        }
        return json.dumps(fields | changes, sort_keys=True) + "\n"

    def reset(self) -> None:
        for path in (self.directory, self.base / "moved", self.outside):
            if path.is_symlink():
                path.unlink()
            elif path.exists():
                shutil.rmtree(path)
        self.build()

    def written_outside(self) -> list[str]:
        return sorted(str(path.relative_to(self.outside)) for path in self.outside.rglob("*") if path.is_file() and path.name != runner_place.MARKER)

    def run_remote(self, command: str, text: str | None = None) -> CompletedProcess[str]:
        return run_command("sh", ["-c", command], env=self.env, input_text=text, timeout=30)

    def send(self, relative: str = FIRST, text: str = RUNNER_TEXT) -> CompletedProcess[str]:
        return self.run_remote(runner_place.remote_command(self.COMMIT, relative, str(self.directory)), text)

    def leftovers(self) -> list[str]:
        return sorted(str(path.relative_to(self.folder)) for path in self.folder.rglob(".*") if path.is_file() or path.is_symlink())

    def test_a_file_arrives_with_its_hash_in_a_private_mode_and_no_temporary_file_is_left(self) -> None:
        result = self.send()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(f"{digest(RUNNER_TEXT)} {FIRST}", " ".join(result.stdout.split()))
        self.assertEqual(RUNNER_TEXT, (self.folder / FIRST).read_text())
        self.assertEqual(0o600, stat.S_IMODE((self.folder / FIRST).stat().st_mode))
        self.assertEqual([runner_place.MARKER], self.leftovers())

    def test_a_link_that_was_planted_at_the_old_temporary_name_is_not_written_through(self) -> None:
        victim = self.outside / "victim.txt"
        victim.write_text("untouched\n")
        (self.folder / f"{FIRST}.part").symlink_to(victim)
        result = self.send()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("untouched\n", victim.read_text())
        self.assertEqual(RUNNER_TEXT, (self.folder / FIRST).read_text())

    def test_a_link_at_the_final_name_is_replaced_and_never_written_through(self) -> None:
        victim = self.outside / "victim.txt"
        victim.write_text("untouched\n")
        (self.folder / FIRST).symlink_to(victim)
        result = self.send()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("untouched\n", victim.read_text())
        self.assertFalse((self.folder / FIRST).is_symlink())
        self.assertEqual(RUNNER_TEXT, (self.folder / FIRST).read_text())

    def test_a_folder_at_the_final_name_stops_the_command_and_leaves_no_temporary_file(self) -> None:
        (self.folder / FIRST).mkdir()
        result = self.send()
        self.assertEqual(runner_place.LINKED_CODE, result.returncode)
        self.assertEqual(runner_place.LINKED_WORD, result.stdout.strip())
        self.assertEqual([runner_place.MARKER], self.leftovers())

    def test_a_linked_runner_folder_commit_folder_or_destination_folder_stops_the_command_before_anything_is_written(self) -> None:
        moved = self.base / "moved"

        def link_runner_folder() -> None:
            self.directory.rename(moved)
            self.directory.symlink_to(moved)

        def link_commit_folder() -> None:
            self.folder.rename(self.outside / "elsewhere")
            self.folder.symlink_to(self.outside / "elsewhere")

        def link_tests_folder() -> None:
            shutil.rmtree(self.folder / "tests")
            (self.folder / "tests").symlink_to(self.outside)

        def link_destination_folder() -> None:
            (self.folder / "tests" / "e2e").rmdir()
            (self.folder / "tests" / "e2e").symlink_to(self.outside)

        cases = {
            "runner folder": link_runner_folder,
            "commit folder": link_commit_folder,
            "tests folder": link_tests_folder,
            "destination folder": link_destination_folder,
        }
        for name, make in cases.items():
            with self.subTest(case=name):
                self.reset()
                make()
                result = self.send()
                self.assertEqual(runner_place.LINKED_CODE, result.returncode, result.stderr)
                self.assertEqual(runner_place.LINKED_WORD, result.stdout.strip())
                self.assertEqual([], self.written_outside())

    def test_a_missing_marker_a_marker_of_another_commit_and_a_marker_that_is_a_link_stop_the_command(self) -> None:
        def make_link() -> None:
            (self.outside / "marker").write_text(self.marker_text())
            self.marker.unlink()
            self.marker.symlink_to(self.outside / "marker")

        def other_commit() -> None:
            self.marker.write_text(self.marker_text(commit="d" * 40))

        cases = {"missing": self.marker.unlink, "another commit": other_commit, "a link": make_link}
        for name, make in cases.items():
            with self.subTest(case=name):
                self.reset()
                make()
                result = self.send()
                self.assertEqual(runner_place.NO_FOLDER_CODE, result.returncode)
                self.assertEqual(runner_place.NO_FOLDER_WORD, result.stdout.strip())
                self.assertFalse((self.folder / FIRST).exists())

    def test_a_marker_that_is_not_the_roles_json_with_exactly_this_commit_stops_the_command_even_when_the_text_holds_the_commit(self) -> None:
        commit_field = f'"commit": "{self.COMMIT}"'
        texts = {
            "the commit text in other text": f"not json {commit_field} not json\n",
            "the commit text in a comment line": f"# {commit_field}\n",
            "the commit only": '{"commit": "' + self.COMMIT + '"}\n',
            "an extra field": self.marker_text(extra=1)[:-2] + "}\n",
            "a commit that has more text": self.marker_text(commit=self.COMMIT + " "),
            "a commit in a list": self.marker_text(commit=[self.COMMIT]),
            "a repeated commit field": '{"commit": "' + "d" * 40 + '", ' + self.marker_text()[1:],
            "a document that is no object": json.dumps([self.COMMIT]) + "\n",
            "an archive digest that is no digest": self.marker_text(python_archive_sha256="x"),
            "requirements that are no list": self.marker_text(requirements="requirements.txt"),
            "an empty file": "",
        }
        for name, text in texts.items():
            with self.subTest(case=name):
                self.reset()
                self.marker.write_text(text)
                result = self.send()
                self.assertEqual((runner_place.NO_FOLDER_CODE, runner_place.NO_FOLDER_WORD), (result.returncode, result.stdout.strip()))
                self.assertFalse((self.folder / FIRST).exists())
        self.reset()
        self.assertEqual(0, self.send().returncode, "the marker as the role writes it is accepted")

    def test_a_marker_with_a_non_standard_constant_or_a_value_outside_the_roles_schema_stops_the_command(self) -> None:
        """The role's schema is its refusal of a pin of the wrong form: an exact version 3.x.y and at least one plain relative requirements path."""
        requirements = {
            "no requirements file": [],
            "NaN among the requirements": ["requirements.txt", float("nan")],
            "an infinity among the requirements": ["requirements.txt", float("inf")],
            "a number among the requirements": ["requirements.txt", 1],
            "an empty requirements path": [""],
            "a requirements path that climbs out": ["../requirements.txt"],
            "an absolute requirements path": ["/etc/passwd"],
            "a requirements path with a space": ["a b.txt"],
        }
        texts = {name: self.marker_text(requirements=value) for name, value in requirements.items()}
        for name, version in {"an empty": "", "a short": "3.14", "another major": "2.7.18", "a suffixed": "3.14.10rc1", "a two-line": "3.14.10\n"}.items():
            texts[f"{name} Python version"] = self.marker_text(python=version)
        for name, text in texts.items():
            with self.subTest(case=name):
                self.reset()
                self.marker.write_text(text)
                result = self.send()
                self.assertEqual((runner_place.NO_FOLDER_CODE, runner_place.NO_FOLDER_WORD), (result.returncode, result.stdout.strip()))
                self.assertFalse((self.folder / FIRST).exists())
        self.reset()
        self.marker.write_text(self.marker_text(requirements=["requirements.txt", "spark/requirements.txt"], python="3.9.0"))
        self.assertEqual(0, self.send().returncode, "another valid version and a nested requirements path are accepted")

    def test_the_destination_folder_is_made_when_it_is_missing_and_a_file_in_its_place_stops_the_command(self) -> None:
        (self.folder / "tests" / "e2e").rmdir()
        self.assertEqual(0, self.send().returncode)
        self.assertTrue((self.folder / FIRST).is_file())
        (self.folder / "tests" / "e2e" / "silver_signals.py").unlink()
        (self.folder / "tests" / "e2e").rmdir()
        (self.folder / "tests" / "e2e").write_text("a file\n")
        self.assertEqual(runner_place.LINKED_CODE, self.send().returncode)

    def test_the_probe_folder_is_made_for_its_owner_only_and_its_files_are_hashed_again_there(self) -> None:
        overlay = f"{PROBE_FOLDER}/compose.host.yml"
        result = self.send(overlay, OVERLAY_TEXT)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(f"{digest(OVERLAY_TEXT)} {overlay}", " ".join(result.stdout.split()))
        self.assertEqual(0o700, stat.S_IMODE((self.folder / PROBE_FOLDER).stat().st_mode))
        self.assertEqual(0o600, stat.S_IMODE((self.folder / overlay).stat().st_mode))
        self.assertEqual([runner_place.MARKER], self.leftovers())
        check = runner_place.verify_retained_command(self.COMMIT, [overlay], str(self.directory))
        self.assertEqual(f"{digest(OVERLAY_TEXT)} {overlay}", " ".join(self.run_remote(check).stdout.split()))

    def test_a_linked_probe_folder_and_a_file_in_its_place_stop_the_command_before_anything_is_written(self) -> None:
        probe_folder = self.folder / PROBE_FOLDER
        self.outside.joinpath("kept").mkdir()

        def link_to_a_folder() -> None:
            probe_folder.symlink_to(self.outside / "kept")

        def link_to_nothing() -> None:
            probe_folder.symlink_to(self.outside / "absent")

        def file_in_its_place() -> None:
            probe_folder.write_text("a file\n")

        for name, make in (("a link", link_to_a_folder), ("a link to nothing", link_to_nothing), ("a file", file_in_its_place)):
            with self.subTest(case=name):
                self.reset()
                self.outside.joinpath("kept").mkdir(exist_ok=True)
                make()
                result = self.send(f"{PROBE_FOLDER}/read_lake.py")
                self.assertEqual((runner_place.LINKED_CODE, runner_place.LINKED_WORD), (result.returncode, result.stdout.strip()), result.stderr)
                self.assertEqual([], self.written_outside())

    def test_the_record_is_received_closed_to_read_only_and_a_link_at_its_name_is_replaced(self) -> None:
        victim = self.outside / "victim.txt"
        victim.write_text("untouched\n")
        (self.folder / runner_place.PLACED_RECORD).symlink_to(victim)
        record = json.dumps({"commit": self.COMMIT, "files": {}}) + "\n"
        result = self.run_remote(runner_place.write_record_command(self.COMMIT, str(self.directory)), record)
        self.assertEqual(0, result.returncode, result.stderr)
        written = self.folder / runner_place.PLACED_RECORD
        self.assertEqual("untouched\n", victim.read_text())
        self.assertFalse(written.is_symlink())
        self.assertEqual(record, written.read_text())
        self.assertEqual(0o400, stat.S_IMODE(written.stat().st_mode))
        self.assertEqual([runner_place.MARKER], self.leftovers())
        self.assertEqual(f"{digest(record)} {runner_place.PLACED_RECORD}", " ".join(result.stdout.split()))

    def test_the_record_is_read_only_when_it_is_a_plain_file_and_a_kept_runner_that_is_a_link_or_missing_stops_the_check(self) -> None:
        record = self.folder / runner_place.PLACED_RECORD
        read = runner_place.read_record_command(self.COMMIT, str(self.directory))
        self.assertEqual("", self.run_remote(read).stdout)
        record.symlink_to(self.outside / "elsewhere.json")
        (self.outside / "elsewhere.json").write_text("secret\n")
        self.assertEqual("", self.run_remote(read).stdout, "a link is not read")
        record.unlink()
        record.write_text("{}\n")
        self.assertEqual("{}\n", self.run_remote(read).stdout)
        self.assertEqual(0, self.send().returncode)
        check = runner_place.verify_retained_command(self.COMMIT, [FIRST], str(self.directory))
        self.assertEqual(f"{digest(RUNNER_TEXT)} {FIRST}", " ".join(self.run_remote(check).stdout.split()))
        (self.folder / FIRST).unlink()
        self.assertEqual(runner_place.RETAINED_CODE, self.run_remote(check).returncode)
        (self.folder / FIRST).symlink_to(self.outside / "elsewhere.json")
        result = self.run_remote(check)
        self.assertEqual((runner_place.RETAINED_CODE, runner_place.RETAINED_WORD), (result.returncode, result.stdout.strip()))

    def check_kept(self, *relatives: str) -> CompletedProcess[str]:
        return self.run_remote(runner_place.verify_retained_command(self.COMMIT, relatives, str(self.directory)))

    def test_a_kept_runner_below_a_linked_folder_or_a_file_in_a_folders_place_stops_the_check_before_a_hash_is_given(self) -> None:
        probe = f"{PROBE_FOLDER}/read_lake.py"
        elsewhere = self.outside / "elsewhere"

        def link_to_the_moved_folder(folder: Path) -> None:
            folder.rename(elsewhere)
            folder.symlink_to(elsewhere)

        def link_to_nothing(folder: Path) -> None:
            shutil.rmtree(folder)
            folder.symlink_to(elsewhere)

        def file_in_its_place(folder: Path) -> None:
            shutil.rmtree(folder)
            folder.write_text("a file\n")

        for relative in ("tests", "tests/e2e", PROBE_FOLDER):
            for name, make in (("moved", link_to_the_moved_folder), ("a link to nothing", link_to_nothing), ("a file", file_in_its_place)):
                with self.subTest(folder=relative, case=name):
                    self.reset()
                    self.assertEqual(0, self.send(probe).returncode)
                    make(self.folder / relative)
                    result = self.check_kept(probe)
                    self.assertEqual((runner_place.LINKED_CODE, runner_place.LINKED_WORD), (result.returncode, result.stdout.strip()), result.stderr)

    def test_a_linked_probe_folder_stops_the_check_when_a_runner_outside_it_is_kept_first(self) -> None:
        probe = f"{PROBE_FOLDER}/read_lake.py"
        self.assertEqual(0, self.send().returncode)
        self.assertEqual(0, self.send(probe).returncode)
        (self.folder / PROBE_FOLDER).rename(self.outside / "elsewhere")
        (self.folder / PROBE_FOLDER).symlink_to(self.outside / "elsewhere")
        result = self.check_kept(FIRST, probe)
        self.assertEqual((runner_place.LINKED_CODE, runner_place.LINKED_WORD), (result.returncode, result.stdout.strip()), result.stderr)

    def test_a_kept_probe_that_is_a_link_a_folder_or_missing_or_whose_folder_is_missing_stops_the_check(self) -> None:
        probe = f"{PROBE_FOLDER}/read_lake.py"
        same_bytes = self.outside / "same_bytes.py"

        def link_to_the_same_bytes() -> None:
            same_bytes.write_text(RUNNER_TEXT)
            (self.folder / probe).symlink_to(same_bytes)

        def link_to_nothing() -> None:
            (self.folder / probe).symlink_to(self.outside / "absent")

        def folder_in_its_place() -> None:
            (self.folder / probe).mkdir()

        def folder_gone() -> None:
            (self.folder / PROBE_FOLDER).rmdir()

        cases = {
            "a link to the same bytes": link_to_the_same_bytes,
            "a link to nothing": link_to_nothing,
            "a folder": folder_in_its_place,
            "missing": lambda: None,
            "no probe folder": folder_gone,
        }
        for name, make in cases.items():
            with self.subTest(case=name):
                self.reset()
                self.assertEqual(0, self.send().returncode)
                self.assertEqual(0, self.send(probe).returncode)
                (self.folder / probe).unlink()
                make()
                result = self.check_kept(FIRST, probe)
                self.assertEqual((runner_place.RETAINED_CODE, runner_place.RETAINED_WORD), (result.returncode, result.stdout.strip()), result.stderr)
                self.assertEqual(name != "no probe folder", (self.folder / PROBE_FOLDER).is_dir(), "the check makes no folder")


class MakeTargetTest(unittest.TestCase):
    def test_the_make_target_needs_the_runner_names_and_passes_them_as_data_to_the_module(self) -> None:
        makefile = (ROOT / "Makefile").read_text()
        self.assertIn("host-runners-place", makefile.split(".PHONY:")[1].split("\n")[0])
        target = makefile.split("\nhost-runners-place:\n")[1].split("\n\n")[0]
        self.assertIn("$(error", target)
        self.assertIn("-m scripts.infra.runner_place --names-from-environment", target)
        recipe = [line for line in target.splitlines() if "-m scripts.infra" in line]
        self.assertEqual(1, len(recipe))
        self.assertNotIn("RUNNERS", recipe[0], "the names are never put on a shell line")
        self.assertRegex(makefile, rf"(?m)^export {runner_place.NAMES_VARIABLE}$")
        self.assertRegex(makefile, rf"(?m)^{runner_place.NAMES_VARIABLE} := \$\(value RUNNERS\)$")
        self.assertNotRegex(makefile, r"(?m)^export RUNNERS$", "RUNNERS is never exported expanded")

    def make_names(self, names: str, *more: str) -> tuple[int, str, str]:
        """Run the real ``make`` target with ``printenv`` in place of the program: what it prints is the value that the program would read."""
        with tempfile.TemporaryDirectory(prefix="make_names_") as scratch:
            result = run_command(
                "make",
                ["-C", str(ROOT), "-s", "host-runners-place", f"PYTHON=printenv {runner_place.NAMES_VARIABLE} #", f"RUNNERS={names}", *more],
                cwd=Path(scratch),
                timeout=30,
            )
            self.assertEqual([], list(Path(scratch).iterdir()))
        return result.returncode, result.stdout, result.stderr

    def test_a_name_with_a_glob_a_variable_a_substitution_or_a_quote_reaches_the_program_exactly_as_it_was_typed(self) -> None:
        for names in (
            "tests/e2e/a.py *.py $HOME `id` \"q\" 'x' ; touch pwned",
            "tests/e2e/$(VAR).py",
            "tests/e2e/${VAR}.py $(shell echo expanded) $$ $$$$ %% #",
            "tests/e2e/a.py\\ tests/e2e/b.py",
        ):
            with self.subTest(names=names):
                code, out, err = self.make_names(names, "VAR=other")
                self.assertEqual(0, code, err)
                self.assertEqual(names, out.rstrip("\n").split("\n")[-1])
        self.assertFalse((ROOT / "pwned").exists())

    def test_no_name_stops_the_target_and_a_name_that_is_a_variable_alone_is_still_a_name(self) -> None:
        code, _, err = self.make_names("")
        self.assertNotEqual(0, code)
        self.assertIn("name the runner files", err)
        code, out, _ = self.make_names("$(VAR)", "VAR=")
        self.assertEqual((0, "$(VAR)"), (code, out.strip()), "the name is not empty: Make does not expand it")

    def test_the_names_are_split_on_white_space_in_python_and_every_name_must_pass_the_path_form(self) -> None:
        arguments = argparse.Namespace(names_from_environment=True, runners=[])
        variable = runner_place.NAMES_VARIABLE
        names = runner_place.runner_names(arguments, {variable: "tests/e2e/a.py  tests/e2e/*.py\t$(x)\n"})
        self.assertEqual(["tests/e2e/a.py", "tests/e2e/*.py", "$(x)"], names)
        self.assertEqual([], runner_place.runner_names(arguments, {}))
        for name in names[1:]:
            with self.subTest(name=name), self.assertRaisesRegex(HostError, "not one runner file"):
                runner_place.runner_text(name, "a" * 40)
        with self.assertRaisesRegex(HostError, "not both"):
            runner_place.runner_names(argparse.Namespace(names_from_environment=True, runners=["x"]), {variable: "y"})
        self.assertEqual(["x"], runner_place.runner_names(argparse.Namespace(names_from_environment=False, runners=["x"]), {variable: "y"}))


if __name__ == "__main__":
    unittest.main()
