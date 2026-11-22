"""The project runs its own pinned Terraform and never the computer's.

A system upgrade replaced the computer's Terraform with a newer one, and every local check was refused because the
configuration requires ``~> 1.16.4``. The project now installs one pinned copy into ``.tools/bin`` and calls only that.
Failure modes covered here:

- The installer's pinned version, the ``required_version`` of a ``versions.tf`` file and the hosted workflow's ``terraform_version`` drift apart.
- The installer's URL, archive names or target name one version and the pin names another, or a platform has no archive.
- The Makefile or a script calls the bare ``terraform`` command, so a newer or older copy on ``PATH`` is used.
- The Makefile and the wrapper name different executables, or one of them names one other than the installer's target.
- The pinned copy is missing and the wrapper falls back to the computer's Terraform, or fails without naming ``make setup``.
- The hosted job reaches ``make check-terraform`` before ``make setup`` has installed the pinned copy.
"""

from __future__ import annotations

import ast
import re
import unittest
from pathlib import Path
from unittest import mock

from scripts.infra import terraform_wrapper
from scripts.infra.terraform_wrapper import WrapperError, pinned_terraform
from scripts.process import run_command
from scripts.quality import install_tools
from tests.fast.git_fixture import ROOT, TIMEOUT_SECONDS

PINNED_PATH = ".tools/bin/terraform"
SUBCOMMANDS = "fmt|init|validate|plan|apply|destroy|test|workspace|show|state|output|version|providers|import|console|graph"
# The bare command in a shell-like file: the lowercase word with no path or variable before it, followed by a flag or a subcommand.
BARE_COMMAND = re.compile(rf"(?<![\w./$()-])terraform(?=\s+(?:-|(?:{SUBCOMMANDS})\b))")


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
        self.assertEqual([PINNED_PATH], re.findall(r"^TERRAFORM \?= (\S+)$", makefile, re.MULTILINE))
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
                found += [f"{name}:{number}" for number, line in enumerate(text.splitlines(), 1) if BARE_COMMAND.search(line)]
        self.assertEqual([], found)

    def test_no_fast_test_starts_the_bare_command(self) -> None:
        found = []
        for name in tracked_files():
            if name.startswith("tests/") and name.endswith(".py"):
                found += [f"{name}:{line}" for line in bare_calls_in_python((ROOT / name).read_text())]
        self.assertEqual([], found)

    def test_the_makefile_calls_terraform_only_through_its_variable(self) -> None:
        recipe = (ROOT / "Makefile").read_text().split("check-terraform:", 1)[1].split("\ncheck-ansible:", 1)[0]
        runs = "\n".join(line for line in recipe.splitlines() if not line.startswith("\t@test -x"))
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

    def test_the_make_target_with_a_missing_copy_names_make_setup(self) -> None:
        result = run_command("make", ["--no-print-directory", "check-terraform", "TERRAFORM=/no/such/terraform"], cwd=ROOT, timeout=TIMEOUT_SECONDS)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("run: make setup", result.stderr)


class HostedJobTest(unittest.TestCase):
    def test_the_hosted_job_installs_the_pinned_tools_before_it_runs_the_terraform_check(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
        job = workflow.split("  infrastructure:", 1)[1]
        self.assertLess(job.index("run: make setup"), job.index("run: make check-terraform"))


if __name__ == "__main__":
    unittest.main()
