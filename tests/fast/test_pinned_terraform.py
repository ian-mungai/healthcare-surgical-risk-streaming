"""The project runs its own pinned Terraform and never the computer's.

A system upgrade replaced the computer's Terraform with a newer one, and every local check was refused because the
configuration requires ``~> 1.16.4``. The project now installs one pinned copy into ``.tools/bin`` and calls only that.
Failure modes covered here:

- The installer's pinned version, the ``required_version`` of a ``versions.tf`` file and the hosted workflow's ``terraform_version`` drift apart.
- The installer's URL, archive names or target name one version and the pin names another, or a platform has no archive.
- The Makefile or a script calls the bare ``terraform`` command, so a newer or older copy on ``PATH`` is used.
- The Makefile and the wrapper name different executables, or one of them names one other than the installer's target.
- The pinned copy is missing and the wrapper falls back to the computer's Terraform, or fails without naming ``make setup``.
- An exported ``TERRAFORM`` or a command-line assignment names another program, so ``make check-terraform`` starts it.
- The pinned path is a directory, a dangling link or a file that is not executable, and the guard lets it through.
- A hook names the bare command as its ``entry`` and passes the subcommand as separate arguments, so the line scan sees no subcommand.
- The hosted job reaches ``make check-terraform`` before ``make setup`` has installed the pinned copy.
"""

from __future__ import annotations

import ast
import os
import re
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.infra import terraform_wrapper
from scripts.infra.terraform_wrapper import WrapperError, pinned_terraform
from scripts.process import CompletedProcess, run_command
from scripts.quality import install_tools
from tests.fast.git_fixture import ROOT, TIMEOUT_SECONDS

PINNED_PATH = ".tools/bin/terraform"
SUBCOMMANDS = "fmt|init|validate|plan|apply|destroy|test|workspace|show|state|output|version|providers|import|console|graph"
# The bare command in a shell-like file: the lowercase word with no path or variable before it, followed by a flag or a subcommand.
BARE_COMMAND = re.compile(rf"(?<![\w./$()-])terraform(?=\s+(?:-|(?:{SUBCOMMANDS})\b))")
# A hook's ``entry`` or a workflow's ``run`` whose first word is the bare command, whatever arguments follow on other lines.
BARE_ENTRY = re.compile(r"^\s*(?:-\s*)?(?:entry|run):\s*[\"']?terraform[\"']?(?:\s|$)")


def tracked_files() -> list[str]:
    output = run_command("git", ["ls-files", "-z"], cwd=ROOT, timeout=TIMEOUT_SECONDS, check=True).stdout
    return [name for name in output.split("\0") if name and (ROOT / name).is_file()]


def pinned_version() -> str:
    return install_tools.TERRAFORM.version


def bare_calls_in_python(source: str) -> list[int]:
    """The lines of each call that starts the program ``terraform`` by name or looks it up on ``PATH``."""
    lines = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call) and node.args:
            name = node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, "id", "")
            first = node.args[0]
            if (
                name in {"run_command", "which", "Popen", "run", "check_call", "check_output", "call"}
                and isinstance(first, ast.Constant)
                and first.value == "terraform"
            ):
                lines.append(node.lineno)
    return lines


def bare_calls_in_text(text: str) -> list[int]:
    """The lines of a Makefile, a hook configuration or a workflow that start the bare command, with its arguments on the line or apart from it."""
    return [number for number, line in enumerate(text.splitlines(), 1) if BARE_COMMAND.search(line) or BARE_ENTRY.search(line)]


def make_stub(path: Path, marker: Path) -> None:
    path.write_text(f"#!/bin/sh\necho ran >> {marker}\nexit 0\n")
    path.chmod(0o755)


def remove(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        path.rmdir()
    else:
        path.unlink(missing_ok=True)


def build_directory(path: Path) -> None:
    path.mkdir()


def build_dangling_link(path: Path) -> None:
    path.symlink_to(path.parent / "no_such_target")


def build_plain_file(path: Path) -> None:
    path.write_text("#!/bin/sh\n")
    path.chmod(stat.S_IRUSR | stat.S_IWUSR)


INVALID_COPIES = {"a directory": build_directory, "a dangling link": build_dangling_link, "a file that is not executable": build_plain_file}


class PinnedVersionTest(unittest.TestCase):
    def test_every_required_version_is_the_pinned_version_with_the_patch_range(self) -> None:
        files = [name for name in tracked_files() if name.endswith("versions.tf")]
        self.assertGreaterEqual(len(files), 4, files)
        for name in files:
            found = re.findall(r'required_version\s*=\s*"([^"]+)"', (ROOT / name).read_text())
            self.assertEqual([f"~> {pinned_version()}"], found, name)

    def test_the_hosted_workflow_installs_the_pinned_version(self) -> None:
        found = re.findall(r'terraform_version:\s*"?([^"\s]+)"?', (ROOT / ".github" / "workflows" / "ci.yml").read_text())
        self.assertEqual([pinned_version()], found)

    def test_the_installers_url_archives_and_member_name_the_pinned_version(self) -> None:
        tool = install_tools.TERRAFORM
        self.assertEqual(f"https://releases.hashicorp.com/terraform/{pinned_version()}", tool.url)
        self.assertEqual(("terraform", "bin/terraform"), (tool.member, tool.target))
        self.assertEqual({("Darwin", "arm64"), ("Linux", "x86_64")}, set(tool.assets))
        for (system, machine), (archive, checksum) in tool.assets.items():
            platform_name = {"Darwin": "darwin", "Linux": "linux"}[system] + "_" + {"arm64": "arm64", "x86_64": "amd64"}[machine]
            self.assertEqual(f"terraform_{pinned_version()}_{platform_name}.zip", archive)
            self.assertRegex(checksum, r"^[0-9a-f]{64}$")

    def test_the_installer_installs_the_pinned_terraform_with_the_other_tools(self) -> None:
        self.assertIn(install_tools.TERRAFORM, install_tools.TOOL_LIST)


class OneNamedExecutableTest(unittest.TestCase):
    def test_the_makefile_variable_the_wrapper_and_the_installer_name_the_same_file(self) -> None:
        makefile = (ROOT / "Makefile").read_text()
        self.assertEqual([PINNED_PATH], re.findall(r"^override TERRAFORM := (\S+)$", makefile, re.MULTILINE))
        self.assertEqual(PINNED_PATH, f".tools/{install_tools.TERRAFORM.target}")
        with mock.patch.object(Path, "is_file", return_value=True), mock.patch("os.access", return_value=True):
            self.assertEqual(ROOT / PINNED_PATH, pinned_terraform())

    def test_no_tracked_script_or_config_file_calls_the_bare_command(self) -> None:
        found = []
        for name in tracked_files():
            if name.startswith("tests/") or not (name.endswith((".py", ".yml", ".yaml", ".sh", ".toml")) or name in {"Makefile", ".pre-commit-config.yaml"}):
                continue
            text = (ROOT / name).read_text()
            if name.endswith(".py"):
                found += [f"{name}:{line}" for line in bare_calls_in_python(text)]
            else:
                found += [f"{name}:{number}" for number in bare_calls_in_text(text)]
        self.assertEqual([], found)

    def test_no_fast_test_starts_the_bare_command(self) -> None:
        found = []
        for name in tracked_files():
            if name.startswith("tests/") and name.endswith(".py"):
                found += [f"{name}:{line}" for line in bare_calls_in_python((ROOT / name).read_text())]
        self.assertEqual([], found)

    def test_the_makefile_calls_terraform_only_through_its_variable(self) -> None:
        recipe = (ROOT / "Makefile").read_text().split("check-terraform:", 1)[1].split("\ncheck-ansible:", 1)[0]
        runs = "\n".join(line for line in recipe.splitlines() if not line.startswith("\t@test -f"))
        calls = re.findall(r"\$\(TERRAFORM\) (\S+)", runs)
        self.assertEqual(["fmt", "-chdir=infra", "-chdir=infra", "-chdir=infra"], calls)

    def test_the_scanner_finds_a_bare_call_in_a_script_and_in_a_shell_line(self) -> None:
        self.assertEqual([3], bare_calls_in_python('import x\n\nrun_command("terraform", [])\n'))
        self.assertEqual([1], bare_calls_in_python('shutil.which("terraform")\n'))
        self.assertEqual([], bare_calls_in_python('run_command(str(pinned), [])\nprint("terraform")\n'))
        for line in ("\tterraform fmt -check", "TF_DATA_DIR=x terraform -chdir=infra init", "- run: terraform plan"):
            self.assertTrue(BARE_COMMAND.search(line), line)
        for line in (
            "\t$(TERRAFORM) fmt -check",
            "\t.tools/bin/terraform fmt",
            "required_version for terraform",
            'name: "Static checks of the terraform code"',
        ):
            self.assertIsNone(BARE_COMMAND.search(line), line)

    def test_a_hook_that_names_the_bare_command_as_its_entry_is_found_even_with_the_arguments_apart(self) -> None:
        planted = (
            "repos:\n  - repo: local\n    hooks:\n      - id: planted\n        name: planted\n"
            "        entry: terraform\n        args: [fmt, -check]\n        language: system\n"
        )
        with tempfile.TemporaryDirectory() as folder:
            copy = Path(folder) / ".pre-commit-config.yaml"
            copy.write_text((ROOT / ".pre-commit-config.yaml").read_text())
            self.assertEqual([], bare_calls_in_text(copy.read_text()))
            copy.write_text(copy.read_text() + "\n".join(planted.splitlines()[3:]) + "\n")
            self.assertEqual(1, len(bare_calls_in_text(copy.read_text())))
        for line in ('        entry: "terraform"', "      - run: terraform", "        entry: terraform"):
            self.assertIsNotNone(BARE_ENTRY.search(line), line)
        for line in ("        entry: .tools/bin/terraform fmt", "        entry: .venv/bin/python -m terraform_thing", "        id: terraform"):
            self.assertIsNone(BARE_ENTRY.search(line), line)


class MissingPinnedCopyTest(unittest.TestCase):
    def test_a_missing_pinned_copy_names_make_setup_and_starts_nothing(self) -> None:
        run = mock.MagicMock()
        with (
            mock.patch.object(terraform_wrapper, "ROOT", ROOT / "tests" / "no_such_folder"),
            mock.patch.object(terraform_wrapper, "run_command") as started,
            self.assertRaises(WrapperError) as refused,
        ):
            terraform_wrapper.terraform(run, ["plan"], 5)
        started.assert_not_called()
        self.assertIn("make setup", str(refused.exception))
        self.assertIn(PINNED_PATH, str(refused.exception))

    def test_a_present_pinned_copy_is_the_program_that_starts(self) -> None:
        run = mock.MagicMock()
        with (
            mock.patch.object(terraform_wrapper, "pinned_terraform", return_value=Path("/pinned/terraform")),
            mock.patch.object(terraform_wrapper, "configuration_folder", return_value=Path("configuration")),
            mock.patch.object(terraform_wrapper, "run_command", return_value=mock.Mock(returncode=0, stdout="out", stderr="")) as started,
        ):
            self.assertEqual("out", terraform_wrapper.terraform(run, ["version"], 5))
        self.assertEqual("/pinned/terraform", started.call_args.args[0])

    def test_a_pinned_copy_that_is_not_an_executable_file_is_refused_by_the_wrapper(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / ".tools" / "bin").mkdir(parents=True)
            target = root / PINNED_PATH
            for label, build in INVALID_COPIES.items():
                build(target)
                with mock.patch.object(terraform_wrapper, "ROOT", root), self.assertRaises(WrapperError, msg=label) as refused:
                    pinned_terraform()
                self.assertIn("make setup", str(refused.exception), label)
                remove(target)
            make_stub(target, root / "marker")
            with mock.patch.object(terraform_wrapper, "ROOT", root):
                self.assertEqual(target, pinned_terraform())


class MakeTargetTest(unittest.TestCase):
    """``make check-terraform`` in a temporary folder with the repository's Makefile, so the pinned path is the folder's own."""

    def setUp(self) -> None:
        self.work = Path(self.enterContext(tempfile.TemporaryDirectory()))
        (self.work / ".tools" / "bin").mkdir(parents=True)
        self.pinned_marker = self.work / "pinned_ran"
        self.other_marker = self.work / "other_ran"
        self.other = self.work / "other_terraform"
        make_stub(self.other, self.other_marker)

    def make(self, *, environment: dict[str, str] | None = None, arguments: list[str] | None = None) -> CompletedProcess[str]:
        command = ["--no-print-directory", "--file", str(ROOT / "Makefile"), *(arguments or []), "check-terraform"]
        return run_command("make", command, cwd=self.work, env={"PATH": os.environ["PATH"], **(environment or {})}, timeout=TIMEOUT_SECONDS)

    def overrides(self) -> dict[str, dict[str, object]]:
        return {
            "exported variable": {"environment": {"TERRAFORM": str(self.other)}},
            "exported variable with make -e": {"environment": {"TERRAFORM": str(self.other)}, "arguments": ["-e"]},
            "command-line assignment": {"arguments": [f"TERRAFORM={self.other}"]},
        }

    def test_with_the_pinned_copy_absent_no_override_starts_another_program(self) -> None:
        for label, options in self.overrides().items():
            result = self.make(**options)  # type: ignore[arg-type]
            self.assertNotEqual(0, result.returncode, label)
            self.assertIn("run: make setup", result.stderr, label)
            self.assertFalse(self.other_marker.exists(), label)

    def test_with_the_pinned_copy_present_it_is_the_program_that_starts_whatever_the_override(self) -> None:
        make_stub(self.work / PINNED_PATH, self.pinned_marker)
        for label, options in self.overrides().items():
            self.pinned_marker.unlink(missing_ok=True)
            self.make(**options)  # type: ignore[arg-type]
            self.assertTrue(self.pinned_marker.exists(), label)
            self.assertFalse(self.other_marker.exists(), label)

    def test_a_pinned_path_that_is_not_an_executable_file_gives_the_setup_message_and_starts_nothing(self) -> None:
        target = self.work / PINNED_PATH
        for label, build in INVALID_COPIES.items():
            build(target)
            result = self.make()
            self.assertNotEqual(0, result.returncode, label)
            self.assertIn("run: make setup", result.stderr, label)
            remove(target)


class HostedJobTest(unittest.TestCase):
    def test_the_hosted_job_installs_the_pinned_tools_before_it_runs_the_terraform_check(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
        job = workflow.split("  infrastructure:", 1)[1]
        self.assertLess(job.index("run: make setup"), job.index("run: make check-terraform"))


if __name__ == "__main__":
    unittest.main()
