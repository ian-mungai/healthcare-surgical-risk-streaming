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
    def test_an_ignored_runner_under_tests_e2e_is_read_and_its_path_is_printed_only_after_every_check_passed(self) -> None:
        self.assertEqual(RUNNER_TEXT, self.read(FIRST))
        self.assertEqual(f"checked {FIRST}\n", self.shown.getvalue())

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
            with self.subTest(relative=relative), self.assertRaisesRegex(HostError, "not one Python runner"):
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
            with self.subTest(relative=relative), self.assertRaisesRegex(HostError, "not one Python runner"):
                self.read(relative)

    def test_the_names_are_the_ones_the_role_writes(self) -> None:
        variables = group_variables()
        self.assertTrue(str(variables["runner_checkout_manifest"]).endswith(f"/{runner_place.MANIFEST_NAME}"))
        self.assertTrue(str(variables["runner_checkout_placed_runners"]).endswith(f"/{runner_place.PLACED_RECORD}"))


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
        self.marker.write_text(json.dumps({"commit": self.COMMIT, "python": "3.14.10"}, sort_keys=True))
        self.outside.mkdir()

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
            (self.outside / "marker").write_text(json.dumps({"commit": self.COMMIT}))
            self.marker.unlink()
            self.marker.symlink_to(self.outside / "marker")

        def other_commit() -> None:
            self.marker.write_text(json.dumps({"commit": "d" * 40}))

        cases = {"missing": self.marker.unlink, "another commit": other_commit, "a link": make_link}
        for name, make in cases.items():
            with self.subTest(case=name):
                self.reset()
                make()
                result = self.send()
                self.assertEqual(runner_place.NO_FOLDER_CODE, result.returncode)
                self.assertEqual(runner_place.NO_FOLDER_WORD, result.stdout.strip())
                self.assertFalse((self.folder / FIRST).exists())

    def test_the_destination_folder_is_made_when_it_is_missing_and_a_file_in_its_place_stops_the_command(self) -> None:
        (self.folder / "tests" / "e2e").rmdir()
        self.assertEqual(0, self.send().returncode)
        self.assertTrue((self.folder / FIRST).is_file())
        (self.folder / "tests" / "e2e" / "silver_signals.py").unlink()
        (self.folder / "tests" / "e2e").rmdir()
        (self.folder / "tests" / "e2e").write_text("a file\n")
        self.assertEqual(runner_place.LINKED_CODE, self.send().returncode)

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
        self.assertRegex(makefile, r"(?m)^export RUNNERS$")

    def test_a_name_with_a_glob_a_substitution_or_a_quote_reaches_the_program_unchanged(self) -> None:
        names = "tests/e2e/a.py *.py $HOME `id` \"q\" 'x' ; touch pwned"
        with tempfile.TemporaryDirectory(prefix="make_names_") as scratch:
            result = run_command(
                "make",
                ["-C", str(ROOT), "-s", "host-runners-place", "PYTHON=printenv RUNNERS #", f"RUNNERS={names.replace('$', '$$')}"],
                cwd=Path(scratch),
                timeout=30,
            )
            self.assertEqual([], list(Path(scratch).iterdir()))
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(names, result.stdout.rstrip("\n").split("\n")[-1])
        self.assertFalse((ROOT / "pwned").exists())

    def test_the_names_are_split_on_white_space_in_python_and_every_name_must_pass_the_path_form(self) -> None:
        arguments = argparse.Namespace(names_from_environment=True, runners=[])
        names = runner_place.runner_names(arguments, {"RUNNERS": "tests/e2e/a.py  tests/e2e/*.py\t$(x)\n"})
        self.assertEqual(["tests/e2e/a.py", "tests/e2e/*.py", "$(x)"], names)
        self.assertEqual([], runner_place.runner_names(arguments, {}))
        for name in names[1:]:
            with self.subTest(name=name), self.assertRaisesRegex(HostError, "not one Python runner"):
                runner_place.runner_text(name, "a" * 40)
        with self.assertRaisesRegex(HostError, "not both"):
            runner_place.runner_names(argparse.Namespace(names_from_environment=True, runners=["x"]), {"RUNNERS": "y"})
        self.assertEqual(["x"], runner_place.runner_names(argparse.Namespace(names_from_environment=False, runners=["x"]), {"RUNNERS": "y"}))


if __name__ == "__main__":
    unittest.main()
