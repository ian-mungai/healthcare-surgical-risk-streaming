"""The OpenBao role's task files that install the intake script and make OpenBao match the lists, through the real ``ansible-playbook``.

``intake_script.yml`` places the script that the launcher runs as root on the host, with its helper and the sudoers drop-in that keeps sudo from recording its
input: the folder must be private to its owner, and the drop-in must pass ``visudo`` before it takes its place. ``bootstrap.yml`` hands the list of external
secrets to the module, and the real admin policy of ``compose/openbao/openbao.hcl`` must be enough for the module to write each policy. The server is the
stand-in of ``tests/slow/openbao_stand_in.py``, which judges every call by the policy texts.
"""

from __future__ import annotations

import grp
import os
import pwd
import re
import shutil
import stat
import tempfile
import unittest
from pathlib import Path
from typing import Any

from scripts.project_config import ROOT
from tests.slow.ansible_support import ROLES, run_role_tasks
from tests.slow.openbao_stand_in import ADMIN_ROLE, Role, StandIn

FILES = ROLES / "openbao" / "files"
OWNER = pwd.getpwuid(os.getuid()).pw_name
GROUP = grp.getgrgid(os.getgid()).gr_name
HCL = (ROOT / "compose" / "openbao" / "openbao.hcl").read_text()
EXTERNAL_LIST = ROOT / "compose" / "external_secrets.txt"
OK_VISUDO = '#!/bin/sh\ncp "$2" "$(dirname "$0")/checked.sudoers"\nexit 0\n'
BAD_VISUDO = '#!/bin/sh\ncp "$2" "$(dirname "$0")/checked.sudoers"\necho "visudo refuses the file" >&2\nexit 1\n'


def mode_of(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


class IntakeScriptTaskTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.folder = Path(self.directory.name)
        self.bin = self.folder / "opt" / "bin"
        self.sudoers = self.folder / "sudoers.d" / "external_secret_intake"

    def visudo(self, text: str) -> Path:
        path = self.folder / "tools" / "visudo"
        path.parent.mkdir(exist_ok=True)
        path.write_text(text)
        path.chmod(0o755)
        return path

    def play(self, visudo: str = OK_VISUDO) -> tuple[int, str]:
        variables: dict[str, Any] = {
            "openbao_bin_directory": str(self.bin),
            "openbao_intake_script": str(self.bin / "external_secret_intake"),
            "openbao_intake_sudoers": str(self.sudoers),
            "openbao_visudo": str(self.visudo(visudo)),
            "openbao_owner_user": OWNER,
            "openbao_owner_group": GROUP,
        }
        result = run_role_tasks("openbao", ["intake_script.yml"], variables)
        return result.returncode, result.stdout + result.stderr

    def test_the_script_its_helper_and_the_drop_in_are_placed_with_the_modes_that_keep_them_private_and_nothing_else_is_installed(self) -> None:
        code, output = self.play()
        self.assertEqual(0, code, output)
        self.assertEqual(0o700, mode_of(self.bin), "the folder is private to its owner")
        self.assertEqual(0o700, mode_of(self.bin / "external_secret_intake"))
        self.assertEqual(0o600, mode_of(self.bin / "external_secrets.py"))
        self.assertEqual(0o440, mode_of(self.sudoers))
        self.assertEqual(["external_secret_intake", "external_secrets.py"], sorted(path.name for path in self.bin.iterdir()), "no unused helper is installed")
        self.assertEqual((FILES / "external_secret_intake.py").read_text(), (self.bin / "external_secret_intake").read_text())
        self.assertEqual((FILES / "external_secret_intake.sudoers").read_text(), self.sudoers.read_text())

    def test_a_folder_that_exists_with_a_wider_mode_is_made_private(self) -> None:
        self.bin.mkdir(parents=True)
        self.bin.chmod(0o755)
        code, output = self.play()
        self.assertEqual(0, code, output)
        self.assertEqual(0o700, mode_of(self.bin))

    def test_the_drop_in_goes_through_visudo_before_it_takes_its_place_and_a_refusal_installs_nothing(self) -> None:
        code, output = self.play()
        self.assertEqual(0, code, output)
        self.assertEqual(self.sudoers.read_text(), (self.folder / "tools" / "checked.sudoers").read_text(), "visudo was given the repository's drop-in")
        shutil.rmtree(self.folder / "sudoers.d")
        code, output = self.play(BAD_VISUDO)
        self.assertNotEqual(0, code, output)
        self.assertFalse(self.sudoers.exists(), "a file that visudo refuses is never installed")

    def test_a_second_run_changes_nothing(self) -> None:
        self.assertEqual(0, self.play()[0])
        code, output = self.play()
        self.assertEqual(0, code, output)
        self.assertRegex(output, r"changed=0\s")


class BootstrapTaskTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.folder = Path(self.directory.name)
        self.secrets = self.folder / "secrets"
        (self.secrets / "openbao").mkdir(parents=True)
        (self.secrets / "openbao").chmod(0o700)
        (self.secrets / "postgres").mkdir()
        (self.secrets / "openbao" / "bootstrap.secret_id").write_text("admin-login-marker")
        (self.secrets / "openbao" / "bootstrap.secret_id").chmod(0o400)
        self.services = self.folder / "services.txt"
        self.services.write_text(
            "".join(
                f"{line.split('#', 1)[0].split()[0]}\n"
                for line in (ROOT / "compose" / "openbao_services.txt").read_text().splitlines()
                if line.split("#", 1)[0].split()
            )
        )
        self.tools = self.folder / "tools.txt"
        self.tools.write_text("nessie\n")
        (self.secrets / "postgres" / "nessie.password").write_text("nessie-database-value-marker")
        # The admin policy as the first start of the tracked configuration writes it.
        policy = HCL.split("write_admin_policy", 1)[1].split("EOT", 2)[1]
        self.server = StandIn({ADMIN_ROLE: Role("admin-login-marker", "admin-token", (), (), ("bootstrap_admin",))})
        self.server.policies["bootstrap_admin"] = policy
        self.server.__enter__()
        self.addCleanup(self.server.__exit__)

    def play(self, ready_deadline_seconds: int = 10) -> tuple[int, str]:
        owner = f"{os.getuid()}:{os.getgid()}"
        variables: dict[str, Any] = {
            "openbao_secrets_directory": str(self.secrets),
            "openbao_services_file": str(self.services),
            "openbao_tools_file": str(self.tools),
            "openbao_external_secrets_file": str(EXTERNAL_LIST),
            "openbao_client_service": "aistor",
            "openbao_address": self.server.address,
            "openbao_user": owner,
            "secret_files_root_owner": owner,
            "openbao_ready_deadline_seconds": ready_deadline_seconds,
            "openbao_token_ttl_seconds": 3600,
            "openbao_token_max_ttl_seconds": 86400,
        }
        result = run_role_tasks("openbao", ["bootstrap.yml"], variables)
        return result.returncode, result.stdout + result.stderr

    def test_the_tracked_admin_policy_lets_the_step_write_every_policy_and_the_list_gives_the_consumer_and_the_intake_role_their_paths(self) -> None:
        code, output = self.play()
        self.assertEqual(0, code, output)
        self.assertIn("secret/data/external/dockerhub_read", self.server.policies["service_deploy"])
        self.assertEqual(
            ["read"], re.findall(r'path "secret/data/external/dockerhub_read" \{ capabilities = \[(?:"(\w+)")\] \}', self.server.policies["service_deploy"])
        )
        self.assertRegex(
            self.server.policies["service_external_intake"],
            r'path "secret/data/external/dockerhub_read" \{ capabilities = \["create", "read", "update", "delete"\] \}',
        )
        self.assertNotIn("external", self.server.policies["service_nessie"], "a service that consumes nothing external gets no external path")
        self.assertTrue((self.secrets / "openbao" / "external_intake.secret_id").is_file())

    # The values of this setup that no output may carry. The runner (tests/e2e/stack_openbao.py) reads the actions and the refusals from the same output.
    SECRETS = ("admin-login-marker", "admin-token", "nessie-database-value-marker")

    def assert_no_secret(self, output: str) -> None:
        for value in (*self.SECRETS, *(path.read_text().strip() for path in (self.secrets / "openbao").glob("*.secret_id"))):
            self.assertNotIn(value, output)

    def test_a_visible_task_names_each_action_of_the_hidden_step_and_a_second_run_shows_an_empty_list(self) -> None:
        first_code, first = self.play()
        second_code, second = self.play()
        self.assertEqual((0, 0), (first_code, second_code), first + second)
        self.assertIn("write policy service_nessie", first)
        self.assertIn("issue a secret ID for nessie", first)
        self.assertNotIn('"actions": []', first)
        self.assertIn('"actions": []', second)
        self.assert_no_secret(first)
        self.assert_no_secret(second)

    def test_a_refusal_of_the_hidden_step_stops_the_run_with_its_fixed_text(self) -> None:
        self.tools.write_text("nessie\nghost_tool\n")
        code, output = self.play()
        self.assertNotEqual(0, code, output)
        self.assertIn("PostgreSQL tool 'ghost_tool' has no entry in the service list", output)
        self.assert_no_secret(output)

    # A value in the shape of a secret, as a server could put it in a reply field that the visible output reads.
    SHAPED = "hvs.synthetic-secret-shaped-marker-0123456789"

    def test_a_seal_status_that_holds_something_else_than_booleans_is_not_printed(self) -> None:
        self.server.replies[("GET", "/v1/sys/seal-status")] = (200, {"initialized": True, "sealed": {"client_token": self.SHAPED}})
        code, output = self.play(ready_deadline_seconds=1)
        self.assertNotEqual(0, code, output)
        self.assertIn("did not become ready within 1 seconds (last answer: HTTP 200, a malformed seal status)", output)
        self.assertNotIn(self.SHAPED, output)

    def test_the_roles_and_retired_policies_that_the_server_lists_are_counted_or_unnamed_in_the_report(self) -> None:
        self.server.policies[f"service_{self.SHAPED}"] = 'path "secret/data/external/dockerhub_read" { capabilities = ["read"] }\n'
        self.server.approle[self.SHAPED] = {}
        code, output = self.play()
        self.assertEqual(0, code, output)
        self.assertIn("drop the external grants of the policy of a service that left the list", output)
        self.assertRegex(output, r'"unlisted": 1\b')
        self.assertNotIn(self.SHAPED, output)
        self.assertEqual("", self.server.policies[f"service_{self.SHAPED}"], "the grant of the retired policy is still dropped")

    def test_a_list_reply_that_holds_something_else_than_names_is_refused_without_it(self) -> None:
        for path in ("/v1/auth/approle/role?list=true", "/v1/sys/policies/acl?list=true"):
            with self.subTest(path=path):
                self.server.replies = {("GET", path): (200, {"data": {"keys": [{"client_token": self.SHAPED}]}})}
                code, output = self.play()
                self.assertNotEqual(0, code, output)
                self.assertIn(f"GET {path.removeprefix('/v1/')}: a malformed reply", output)
                self.assertNotIn(self.SHAPED, output)

    def test_a_failure_that_the_step_did_not_word_is_not_printed(self) -> None:
        """A crash of the hidden module can carry any text, a secret too, so only the module's own refusals are shown."""
        self.tools.unlink()
        code, output = self.play()
        self.assertNotEqual(0, code, output)
        self.assertIn("the OpenBao step failed for a reason that is not printed", output)
        self.assertNotIn("tools.txt", output)
        self.assert_no_secret(output)


if __name__ == "__main__":
    unittest.main()
