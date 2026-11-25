"""The script that places AIStor's licence on the host: one fixed program, the licence on standard input and one code on standard output.

The launcher reads the licence on the owner's computer and pipes it to this script over SSH, so no Ansible task, variable or log holds it. The script writes one
root-only file (mode 0400) whole or not at all and prints a code and nothing else. The cases use a synthetic marker for the licence and look for it in every
output, every error and every file but the one the script is meant to write.
"""

from __future__ import annotations

import importlib.machinery
import importlib.util
import io
import os
import stat
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest import mock

from scripts.process import run_command
from tests.fast.ansible_paths import ROLES, SHARED_MODULE_UTILS

SCRIPT = ROLES / "compose_stack" / "files" / "aistor_licence_install.py"
SUDOERS = ROLES / "compose_stack" / "files" / "aistor_licence_install.sudoers"
OWNER = f"{os.getuid()}:{os.getgid()}"
LICENCE = "marker.licence.text-that-must-not-leak-0123456789"
OTHER = "marker.other.licence-text-that-must-not-leak-9876543210"


def install_form(folder: Path) -> Path:
    """The script and its helper side by side, as the role installs them, and the path of the script."""
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "secret_files.py").write_text((SHARED_MODULE_UTILS / "secret_files.py").read_text())
    script = folder / "aistor_licence_install"
    script.write_text(SCRIPT.read_text())
    script.chmod(0o700)
    return script


def load_script(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "aistor_licence_install", path, loader=importlib.machinery.SourceFileLoader("aistor_licence_install", str(path))
    )
    if spec is None or spec.loader is None:
        raise ImportError("the script cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules["aistor_licence_install"] = module
    spec.loader.exec_module(module)
    return module


class ScriptTest(unittest.TestCase):
    def setUp(self) -> None:
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.folder = Path(self.scratch.name)
        self.script = install_form(self.folder / "bin")
        self.module = load_script(self.script)
        self.directory = self.folder / "secrets"
        self.target = self.directory / "aistor" / "licence"

    def run_script(self, text: str, *extra: str, directory: Path | None = None, owner: str = OWNER) -> tuple[int, str, str]:
        done = run_command(
            sys.executable, ["-I", str(self.script), "--directory", str(directory or self.directory), "--owner", owner, *extra], input_text=text, timeout=30
        )
        return done.returncode, done.stdout, done.stderr

    def test_the_licence_is_written_as_a_root_only_file_of_mode_0400_and_the_only_output_is_one_code(self) -> None:
        code, out, err = self.run_script(LICENCE + "\n")
        self.assertEqual((0, "placed\n", ""), (code, out, err))
        status = self.target.stat()
        self.assertEqual((0o400, os.getuid(), os.getgid()), (stat.S_IMODE(status.st_mode), status.st_uid, status.st_gid))
        self.assertEqual(LICENCE, self.target.read_text().rstrip("\n"))

    def test_a_second_run_with_the_same_licence_changes_nothing_and_another_licence_replaces_the_first_whole(self) -> None:
        self.run_script(LICENCE)
        before = self.target.stat().st_ino
        self.assertEqual((0, "unchanged\n", ""), self.run_script(LICENCE)[:3])
        self.assertEqual(before, self.target.stat().st_ino, "the file is not written again")
        self.assertEqual((0, "placed\n", ""), self.run_script(OTHER)[:3])
        self.assertEqual(OTHER, self.target.read_text().rstrip("\n"))
        self.assertEqual(0o400, stat.S_IMODE(self.target.stat().st_mode))

    def test_a_file_with_another_mode_is_given_mode_0400_and_an_empty_or_short_file_is_replaced(self) -> None:
        self.run_script(LICENCE)
        self.target.chmod(0o600)
        self.assertEqual((0, "placed\n", ""), self.run_script(LICENCE)[:3])
        self.assertEqual(0o400, stat.S_IMODE(self.target.stat().st_mode))
        self.target.chmod(0o600)
        self.target.write_text("")
        self.target.chmod(0o400)
        self.assertEqual((0, "placed\n", ""), self.run_script(LICENCE)[:3])
        self.assertEqual(LICENCE, self.target.read_text().rstrip("\n"))

    def test_input_that_cannot_be_a_licence_is_refused_by_a_fixed_code_and_nothing_is_written(self) -> None:
        cases = {
            "oversized": ("x" * 16385, "oversized"),
            "empty": ("", "empty"),
            "white space only": (" \n\t\n", "empty"),
            "a control character": (f"{LICENCE}\x00more", "bad_form"),
            "text that is not ASCII": (f"{LICENCE}é", "bad_form"),
        }
        for label, (text, expected) in cases.items():
            with self.subTest(label):
                code, out, err = self.run_script(text)
                self.assertEqual((3, f"{expected}\n"), (code, out))
                self.assertNotIn(LICENCE, out + err)
                self.assertFalse(self.target.exists())
        with self.assertRaises(self.module.Refusal) as caught:
            self.module.parse_licence(b"\xff\xfe" + LICENCE.encode())
        self.assertEqual("bad_form", caught.exception.code)

    def test_a_licence_of_more_than_one_line_is_kept_line_for_line(self) -> None:
        text = f"{LICENCE}\nsecond-line-of-the-licence"
        self.assertEqual((0, "placed\n", ""), self.run_script(text + "\n")[:3])
        self.assertEqual(text, self.target.read_text().rstrip("\n"))

    def test_an_owner_that_is_not_a_numeric_pair_is_refused_before_any_file_or_folder_is_made(self) -> None:
        for owner in ("root:root", "1", "-1:0", "0:99999999999", ""):
            with self.subTest(owner=owner):
                code, out, _ = self.run_script(LICENCE, owner=owner)
                self.assertEqual((3, "bad_owner\n"), (code, out))
                self.assertFalse(self.directory.exists())

    def test_a_folder_that_cannot_be_written_is_a_fixed_code_and_names_no_path_and_no_text(self) -> None:
        blocker = self.folder / "blocked"
        blocker.write_text("a file where the folder must be")
        code, out, err = self.run_script(LICENCE, directory=blocker)
        self.assertEqual((3, "cannot_write\n"), (code, out))
        self.assertNotIn(LICENCE, out + err)
        self.assertNotIn(str(blocker), out + err)

    def test_a_wrong_argument_is_the_code_usage_on_standard_error_and_exit_3(self) -> None:
        done = run_command(sys.executable, ["-I", str(self.script), "--nonsense"], input_text="", timeout=30)
        self.assertEqual((3, "usage\n"), (done.returncode, done.stderr))

    def test_the_licence_is_never_in_an_exception_text_a_print_or_the_file_names_in_process_memory_calls(self) -> None:
        out, err = io.StringIO(), io.StringIO()
        stdin = mock.Mock(buffer=io.BytesIO((LICENCE + "\n").encode()))
        with redirect_stdout(out), redirect_stderr(err), mock.patch.object(sys, "stdin", stdin):
            code = self.module.main(["--directory", str(self.directory), "--owner", OWNER])
        self.assertEqual((0, "placed\n", ""), (code, out.getvalue(), err.getvalue()))

    def test_the_script_names_the_licence_folder_and_file_of_the_compose_secret_and_no_other_file(self) -> None:
        self.assertEqual(("aistor", "licence"), (self.module.FOLDER, self.module.FILE))
        text = SCRIPT.read_text()
        for name in ("root_user", "root_password", "project_password", "MINIO_ROOT"):
            self.assertNotIn(name, text)

    def test_the_script_is_a_program_that_runs_in_isolated_mode_and_the_sudoers_entry_names_exactly_that_command_line(self) -> None:
        self.assertEqual("#!/usr/bin/python3 -I", SCRIPT.read_text().splitlines()[0])
        entry = SUDOERS.read_text()
        self.assertIn("Cmnd_Alias AISTOR_LICENCE_INSTALL = /usr/bin/python3 -I /opt/platform/bin/aistor_licence_install", entry)
        self.assertIn("Defaults!AISTOR_LICENCE_INSTALL !log_input, !log_output, !log_stdin, !log_stdout, !log_stderr, !log_ttyin, !log_ttyout", entry)
        privileges = [line for line in entry.splitlines() if line.strip() and not line.startswith("#") and not line.startswith(("Cmnd_Alias", "Defaults"))]
        self.assertEqual([], privileges, "the drop-in grants no privilege")


class FormTest(unittest.TestCase):
    def test_the_module_imports_only_the_standard_library_and_the_helper_beside_it(self) -> None:
        text = SCRIPT.read_text()
        imports: Any = [line for line in text.splitlines() if line.startswith(("import ", "from "))]
        self.assertTrue(
            all(name.split()[1].split(".")[0] in {"__future__", "argparse", "importlib", "re", "sys", "pathlib", "typing"} for name in imports), imports
        )


if __name__ == "__main__":
    unittest.main()
