"""Every file that Ansible runs on the host compiles for the host's Python, 3.12, and the formatter keeps it that way.

The tools environment runs Python 3.14, which accepts syntax that the host's 3.12 refuses (an exception list without parentheses is one). A module with
such a line loads in every test and fails on the host. This test parses each module, each shared module helper and each file that a role copies to the
host, as 3.12 does.
"""

from __future__ import annotations

import ast
import unittest
from pathlib import Path

from scripts.project_config import ROOT

HOST_VERSION = (3, 12)
PATTERNS = ("ansible/**/library/*.py", "ansible/**/module_utils/*.py", "ansible/**/files/*.py")


def host_files() -> list[Path]:
    return sorted({path for pattern in PATTERNS for path in ROOT.glob(pattern)})


class HostSyntaxTest(unittest.TestCase):
    def test_every_file_that_runs_on_the_host_parses_as_python_3_12(self) -> None:
        files = host_files()
        self.assertGreater(len(files), 8, "the test must find the roles' modules")
        failed = []
        for path in files:
            try:
                ast.parse(path.read_text(), filename=str(path), feature_version=HOST_VERSION)
            except SyntaxError as error:
                failed.append(f"{path.relative_to(ROOT)}:{error.lineno}: {error.msg}")
        self.assertEqual([], failed)

    def test_the_formatter_targets_the_host_python_for_every_such_file(self) -> None:
        text = (ROOT / "pyproject.toml").read_text()
        for pattern in ("ansible/roles/*/files/*.py", "ansible/roles/*/library/*.py", "ansible/roles/*/module_utils/*.py"):
            with self.subTest(pattern=pattern):
                self.assertIn(f'"{pattern}" = "py312"', text)

    def test_the_test_finds_the_airbyte_modules_and_the_shared_helper(self) -> None:
        names = {path.name for path in host_files()}
        self.assertLessEqual({"airbyte_connection.py", "airbyte_key_install.py", "secret_files.py", "workload_credentials.py"}, names)


if __name__ == "__main__":
    unittest.main()
