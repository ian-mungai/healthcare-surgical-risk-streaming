"""The check of a runner checkout's environment against its lock file: ``ansible/roles/runner_checkout/library/runner_environment.py``.

The role skips the build of a finished folder only when this check finds the environment as the lock file made it. The slow test of the role
(``tests/slow/test_runner_checkout_role.py``) runs the module through the role.

Failure modes covered here:

- An environment whose interpreter is missing, does not run or is another version passes, so a folder with a broken environment is kept.
- A package that the lock file does not pin is installed, or a pin is not installed or is another version, and the folder is kept.
- A pin with an environment marker is read as always required, so a build that was right on this platform is rebuilt for ever, or as never required, so that
  a package that the marker requires here (``cffi`` on CPython) is missing and the folder is kept. The marker is evaluated for the folder's own interpreter.
- A program of the check that never ends holds the deploy, or is left running when the check gives up.
- A lock file name that leaves the folder is read.
"""

from __future__ import annotations

import importlib.util
import os
import re
import shutil
import sys
import tempfile
import unittest
import venv
from pathlib import Path
from types import ModuleType
from typing import Any

from scripts.process import run_command
from tests.fast.ansible_paths import ROLES

LIBRARY = ROLES / "runner_checkout" / "library"
VERSION = ".".join(str(part) for part in sys.version_info[:3])
LOCK = "requirements.txt"


def load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("runner_environment", LIBRARY / "runner_environment.py")
    if spec is None or spec.loader is None:
        raise ImportError("runner_environment")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


environment_module: Any = load_module()


def add_distribution(folder: Path, name: str, version: str) -> None:
    """A package that ``importlib.metadata`` finds in the environment, with no code."""
    site = next((folder / ".venv" / "lib").glob("python*/site-packages"))
    info = site / f"{name.replace('-', '_')}-{version}.dist-info"
    info.mkdir()
    (info / "METADATA").write_text(f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n")


class Checkout(unittest.TestCase):
    template: Path

    @classmethod
    def setUpClass(cls) -> None:
        holder = tempfile.TemporaryDirectory(prefix="environment_template_")
        cls.addClassCleanup(holder.cleanup)
        cls.template = Path(holder.name) / "checkout"
        venv.create(cls.template / ".venv", with_pip=True, symlinks=True)

    def setUp(self) -> None:
        self.base = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="environment_")))
        self.folder = self.base / "checkout"
        shutil.copytree(self.template, self.folder, symlinks=True)

    def lock(self, text: str) -> None:
        (self.folder / LOCK).write_text(text)

    def check(self, version: str = VERSION, deadline: int = 60) -> list[str]:
        found: list[str] = environment_module.problems(self.folder, version, [LOCK], deadline)
        return found


class VerdictTest(Checkout):
    def test_an_environment_that_holds_exactly_the_pins_is_intact_and_pip_is_not_counted(self) -> None:
        add_distribution(self.folder, "alpha-pkg", "1.0")
        add_distribution(self.folder, "pip", "99.0")
        self.lock("alpha-pkg==1.0 \\\n    --hash=sha256:" + "0" * 64 + "\n    # via -r requirements.in\n")
        self.assertEqual([], self.check())

    def test_a_pin_that_is_not_installed_an_extra_package_and_another_version_are_each_found_by_name(self) -> None:
        add_distribution(self.folder, "alpha-pkg", "1.0")
        add_distribution(self.folder, "extra-pkg", "2.0")
        add_distribution(self.folder, "beta", "3.0")
        self.lock("alpha-pkg==1.5 \\\n    --hash=sha256:00\ngamma[extra]==1.0 \\\n    --hash=sha256:11\nbeta==3.0 \\\n    --hash=sha256:22\n")
        self.assertEqual(
            [
                "another version than the lock files: alpha-pkg",
                "installed but not in the lock files: extra-pkg",
                "in the lock files but not installed: gamma",
            ],
            self.check(),
        )

    def test_a_pin_whose_marker_is_true_for_the_interpreter_is_required_and_is_checked_by_version(self) -> None:
        self.lock("cffi==2.1.1 ; platform_python_implementation == 'CPython' \\\n    --hash=sha256:00\n")
        self.assertEqual(["in the lock files but not installed: cffi"], self.check(), "absent: the marker is true on CPython, so cffi is required")
        add_distribution(self.folder, "cffi", "2.0.0")
        self.assertEqual(["another version than the lock files: cffi"], self.check())
        shutil.rmtree(next((self.folder / ".venv" / "lib").glob("python*/site-packages")) / "cffi-2.0.0.dist-info")
        add_distribution(self.folder, "cffi", "2.1.1")
        self.assertEqual([], self.check())

    def test_a_pin_whose_marker_is_false_for_the_interpreter_may_be_absent_and_is_not_one_of_the_pins_when_it_is_installed(self) -> None:
        self.lock("cffi==2.1.1 ; platform_python_implementation != 'CPython' \\\n    --hash=sha256:00\n")
        self.assertEqual([], self.check(), "absent: the marker is false here, so pip would not install it")
        add_distribution(self.folder, "cffi", "2.1.1")
        self.assertEqual(["installed but not in the lock files: cffi"], self.check())

    def test_markers_are_evaluated_with_the_interpreters_own_values_and_each_of_two_pins_of_one_name_counts_only_where_its_marker_is_true(self) -> None:
        self.lock(
            "both==1.0 ; python_version < '3' \\\n    --hash=sha256:00\n"
            "both==2.0 ; python_version >= '3' and sys_platform != 'no-such-platform' \\\n    --hash=sha256:11\n"
            "other==1.0 ; implementation_name == 'cpython' and os_name != 'no-such-os' \\\n    --hash=sha256:22\n"
        )
        add_distribution(self.folder, "both", "2.0")
        self.assertEqual(["in the lock files but not installed: other"], self.check())
        shutil.rmtree(next((self.folder / ".venv" / "lib").glob("python*/site-packages")) / "both-2.0.dist-info")
        add_distribution(self.folder, "both", "1.0")
        add_distribution(self.folder, "other", "1.0")
        self.assertEqual(["another version than the lock files: both"], self.check())

    def test_a_marker_that_cannot_be_read_is_a_problem_of_the_environment_check_and_not_a_pass(self) -> None:
        self.lock("odd==1.0 ; not a marker at all \\\n    --hash=sha256:00\n")
        self.assertEqual(["the environment's installed packages or the lock files' markers cannot be read"], self.check())

    def test_names_are_compared_in_the_form_that_pip_compares(self) -> None:
        add_distribution(self.folder, "Typing_Extensions", "4.0")
        self.lock("typing.extensions==4.0 \\\n    --hash=sha256:00\n")
        self.assertEqual([], self.check())

    def test_the_problems_are_at_most_twenty_with_a_count_of_the_rest_and_never_a_file_content(self) -> None:
        self.lock("".join(f"pkg{number:02}==1.0 \\\n    --hash=sha256:00\n" for number in range(30)))
        found = self.check()
        self.assertEqual(21, len(found))
        self.assertEqual("and 10 more", found[-1])


class InterpreterTest(Checkout):
    def test_a_missing_interpreter_another_version_and_an_interpreter_that_fails_are_problems_and_not_errors(self) -> None:
        self.lock("alpha==1.0 \\\n    --hash=sha256:00\n")
        self.assertEqual(["the environment's interpreter does not run or is not the pinned version"], self.check("3.0.0"))
        python = self.folder / ".venv" / "bin" / "python"
        python.unlink()
        python.write_text("#!/bin/sh\nexit 3\n")
        python.chmod(0o755)
        self.assertEqual(["the environment's interpreter does not run or is not the pinned version"], self.check())
        python.unlink()
        self.assertEqual(["the environment's interpreter is not there"], self.check())

    def test_an_interpreter_that_never_ends_fails_the_check_at_its_deadline_and_no_process_of_it_is_left(self) -> None:
        python = self.folder / ".venv" / "bin" / "python"
        python.unlink()
        python.write_text("#!/bin/sh\nsleep 7001 &\nwait\n")
        python.chmod(0o755)
        self.lock("alpha==1.0 \\\n    --hash=sha256:00\n")
        with self.assertRaisesRegex(environment_module.EnvironmentCheckError, "did not finish within 1 seconds and was stopped"):
            self.check(deadline=1)
        self.assertEqual(1, run_command("pgrep", ["-f", "sleep 7001"], timeout=10).returncode, "the session was killed with the program")

    def test_a_lock_file_that_leaves_the_folder_or_pins_nothing_is_an_error(self) -> None:
        for names in (["../requirements.txt"], ["/etc/passwd"], ["a b"]):
            with self.subTest(names=names), self.assertRaisesRegex(environment_module.EnvironmentCheckError, "plain relative path"):
                environment_module.problems(self.folder, VERSION, names, 5)
        self.lock("# nothing is pinned\n")
        with self.assertRaisesRegex(environment_module.EnvironmentCheckError, "pin no requirement"):
            self.check()


class FormTest(unittest.TestCase):
    def test_the_module_is_plain_standard_library_for_the_hosts_python_and_has_the_one_state_check(self) -> None:
        text = (LIBRARY / "runner_environment.py").read_text()
        self.assertIn('"check"', text)
        imports = {name.split(".")[0] for name in re.findall(r"(?m)^(?:from|import) ([a-z_.]+)", text)}
        self.assertLessEqual(imports, {"__future__", "json", "os", "re", "signal", "subprocess", "pathlib", "typing", "ansible"})
        self.assertNotIn("packaging", imports, "the markers are evaluated by the folder's own interpreter: the host's Python needs no package")
        self.assertTrue(os.path.isfile(LIBRARY / "runner_environment.py"))


if __name__ == "__main__":
    unittest.main()
