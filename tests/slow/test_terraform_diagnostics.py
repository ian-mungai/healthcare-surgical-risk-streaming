"""What Terraform prints when it refuses a value that looks like a secret: the diagnostic must name the rule and never show the value.

A validation failure prints each variable that its condition reads, as ``var.name is "value"``, unless the variable is sensitive. The Tailscale client ID
is no secret, but the rule that refuses an auth key in its place reads it, so a key pasted there would reach the terminal, the saved plan text and every log
of the run. The case runs the pinned Terraform on a copy of ``infra/`` with a test file that gives a marker in the form of an auth key, and reads the output
raw. The providers come from the project's own install folder when it exists, so the run needs no network.

Failure modes covered here:

- A validation rule that reads a secret-prone variable shows its value in the diagnostic.
- A value that the rule accepts is refused by nothing else, so the sensitive mark costs no plan.
"""

from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

from scripts.infra.terraform_wrapper import pinned_terraform
from scripts.process import CompletedProcess, run_command
from scripts.project_config import ROOT
from tests.fast.secret_inputs import Marker, make_marker, scan_text

INIT_SECONDS = 300
TEST_SECONDS = 300
PROVIDERS = ROOT / ".tools" / "terraform" / "static" / "providers"
PLAN_TESTS = ROOT / "infra" / "tests" / "plan.tftest.hcl"
VARIABLES = """
variables {
  aws_region           = "us-west-2"
  aws_profile          = "example_project_dev"
  project_name         = "example_project"
  environment          = "dev"
  data_bucket_name     = "example-project"
  github_repository    = "example-owner/example-repository"
  github_owner_id      = "1234567"
  github_repository_id = "7654321"
  tailscale_audience   = "example-audience"
  host_reserve_mib     = 4096
  always_on_limits_mib = 13568
  spark_limit_mib      = 4096
}

run "probe" {
  command = plan
}
"""


class Terraform:
    """The pinned Terraform on a copy of ``infra/`` with no cloud access: an unreadable profile, empty credential files and a data folder of its own."""

    def __init__(self, scratch: Path) -> None:
        self.scratch = scratch
        self.config = scratch / "infra"
        shutil.copytree(ROOT / "infra", self.config, ignore=shutil.ignore_patterns(".terraform", "probe_tests"))
        (scratch / "empty").write_text("")
        self.env = {
            "PATH": "/usr/bin:/bin",
            "HOME": str(scratch),
            "TF_DATA_DIR": str(scratch / "data"),
            "TF_IN_AUTOMATION": "1",
            "AWS_PROFILE": "no_such_profile",
            "AWS_CONFIG_FILE": str(scratch / "empty"),
            "AWS_SHARED_CREDENTIALS_FILE": str(scratch / "empty"),
        }

    def run(self, *arguments: str, seconds: int) -> CompletedProcess[str]:
        return run_command(str(pinned_terraform()), [f"-chdir={self.config}", *arguments], cwd=self.scratch, env=self.env, timeout=seconds)

    def init(self) -> None:
        plugin = [f"-plugin-dir={PROVIDERS}"] if PROVIDERS.is_dir() else []
        result = self.run("init", "-backend=false", "-input=false", "-no-color", *plugin, seconds=INIT_SECONDS)
        if result.returncode != 0:
            raise AssertionError("terraform init failed:\n" + (result.stdout + result.stderr)[-2000:])

    def probe(self, client_id: str) -> CompletedProcess[str]:
        """``terraform test`` with the mock provider of the project's own plan tests, and the client ID on the command line as the wrapper gives it."""
        folder = self.config / "probe_tests"
        folder.mkdir()
        mock_provider = PLAN_TESTS.read_text().split("\nvariables {", 1)[0]
        (folder / "probe.tftest.hcl").write_text(mock_provider + VARIABLES)
        return self.run("test", "-no-color", "-test-directory=probe_tests", f"-var=tailscale_client_id={client_id}", seconds=TEST_SECONDS)


class DiagnosticTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.terraform = Terraform(Path(self.directory.name))
        self.terraform.init()

    def test_an_auth_key_given_as_the_client_id_is_refused_by_the_rule_and_its_value_is_never_printed(self) -> None:
        marker = Marker("auth key", "tskey-auth-" + make_marker("plain").value)
        result = self.terraform.probe(marker.value)
        output = result.stdout + result.stderr
        self.assertNotEqual(0, result.returncode, output)
        self.assertIn("Give the federated identity's client ID, never an auth key.", output, "control: the validation rule refused the value")
        self.assertEqual([], scan_text(output, [marker]), output)

    def test_a_client_id_that_the_rule_accepts_passes_the_plan(self) -> None:
        result = self.terraform.probe("example-client-id")
        output = result.stdout + result.stderr
        self.assertEqual(0, result.returncode, output)
        self.assertIn("1 passed", output)


if __name__ == "__main__":
    unittest.main()
