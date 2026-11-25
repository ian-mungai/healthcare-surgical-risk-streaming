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

import tempfile
import unittest
from pathlib import Path

from scripts.process import CompletedProcess
from scripts.project_config import ROOT
from tests.fast.secret_inputs import Marker, make_marker, scan_text
from tests.slow.terraform_support import Terraform

TEST_SECONDS = 300
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


def probe(terraform: Terraform, client_id: str) -> CompletedProcess[str]:
    """``terraform test`` with the mock provider of the project's own plan tests, and the client ID on the command line as the wrapper gives it."""
    folder = terraform.config / "probe_tests"
    folder.mkdir()
    mock_provider = PLAN_TESTS.read_text().split("\nvariables {", 1)[0]
    (folder / "probe.tftest.hcl").write_text(mock_provider + VARIABLES)
    return terraform.run("test", "-no-color", "-test-directory=probe_tests", f"-var=tailscale_client_id={client_id}", seconds=TEST_SECONDS)


class DiagnosticTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.terraform = Terraform(Path(self.directory.name))
        self.terraform.init()

    def test_an_auth_key_given_as_the_client_id_is_refused_by_the_rule_and_its_value_is_never_printed(self) -> None:
        marker = Marker("auth key", "tskey-auth-" + make_marker("plain").value)
        result = probe(self.terraform, marker.value)
        output = result.stdout + result.stderr
        self.assertNotEqual(0, result.returncode, output)
        self.assertIn("Give the federated identity's client ID, never an auth key.", output, "control: the validation rule refused the value")
        self.assertEqual([], scan_text(output, [marker]), output)

    def test_a_client_id_that_the_rule_accepts_passes_the_plan(self) -> None:
        result = probe(self.terraform, "example-client-id")
        output = result.stdout + result.stderr
        self.assertEqual(0, result.returncode, output)
        self.assertIn("1 passed", output)


if __name__ == "__main__":
    unittest.main()
