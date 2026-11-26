"""A host whose OpenBao was initialised before external secrets existed completes the intake after one deploy, and its data stays as it was.

OpenBao's first start writes the admin policy once, so a host made before this change holds the old text and the volume is kept. The deploy's reconcile step
(run here against the stand-in of ``tests/slow/openbao_stand_in.py``, which judges every call by the real policy texts) writes the intake role's policy, AppRole
and secret ID from the declared paths with the bootstrap login, and the installed intake script then logs in with that role. The values are synthetic markers.
"""

from __future__ import annotations

import unittest
from typing import Any

from scripts.project_config import ROOT
from tests.fast.ansible_paths import shared_module
from tests.fast.external_intake_constants import NAME, VALUE
from tests.fast.test_openbao_bootstrap import MAX_TTL, MY_OWNER, TTL, bootstrap
from tests.slow.openbao_stand_in import ADMIN_ROLE, Role, StandIn
from tests.slow.test_external_intake_script import ADMIN_MARKER, INTAKE_FILE_NAME, LIST, Case

# The admin policy that the first start wrote before this change: no rule for an external path.
OLD_ADMIN_POLICY = """path "sys/policies/acl" { capabilities = ["list"] }
path "sys/policies/acl/service_*" { capabilities = ["create", "read", "update", "list"] }
path "auth/approle/role" { capabilities = ["list"] }
path "auth/approle/role/*" { capabilities = ["create", "read", "update", "list"] }
path "secret/data/services/*" { capabilities = ["create", "read", "update"] }
path "secret/metadata/services/*" { capabilities = ["read", "list"] }
"""
OLD_SERVICES = ("nessie", "airbyte", "dagster", "alloy", "apicurio")
OWNER_TEXT = f"{MY_OWNER[0]}:{MY_OWNER[1]}"
INTAKE_FILE = INTAKE_FILE_NAME


def services_text() -> str:
    """The tracked service list with its owner words dropped: a test user cannot give a file to another user."""
    lines = []
    for line in (ROOT / "compose" / "openbao_services.txt").read_text().splitlines():
        words = line.split("#", 1)[0].split()
        if words:
            lines.append(words[0])
    return "\n".join(lines) + "\n"


class ExistingHostCase(Case):
    """The script's case, with the server and the files of an old host in place of a fresh one."""

    def make_server(self) -> StandIn:
        self.secrets = self.folder / "secrets"
        (self.secrets / "postgres").mkdir(parents=True)
        self.services_file = self.folder / "services.txt"
        self.services_file.write_text(services_text())
        self.tools_file = ROOT / "compose" / "postgres_tools.txt"
        (self.secrets / "openbao" / INTAKE_FILE).unlink()
        (self.secrets / "openbao" / "bootstrap.secret_id").write_text(ADMIN_MARKER)
        server = StandIn({ADMIN_ROLE: Role(ADMIN_MARKER, "admin-token-marker", (), (), ("bootstrap_admin",))})
        server.policies["bootstrap_admin"] = OLD_ADMIN_POLICY
        for service in OLD_SERVICES:
            server.policies[f"service_{service}"] = bootstrap.policy_text(service)
            server.approle[service] = {**bootstrap.role_settings(service, TTL, MAX_TTL)}
            server.secret_ids[service] = {f"old-secret-id-of-{service}"}
            (self.secrets / "openbao" / f"{service}.secret_id").write_text(f"old-secret-id-of-{service}")
            (self.secrets / "openbao" / f"{service}.secret_id").chmod(0o400)
            server.versions[f"services/{service}/postgres"] = [f"password of {service}"]
        server.versions["services/grafana/admin_password"] = ["grafana-marker-password"]
        tools = shared_module("secret_files").parse_names(self.tools_file.read_text(), "tool")
        for tool in tools:
            (self.secrets / "postgres" / f"{tool}.password").write_text(f"password of {tool}")
        return server

    def deploy(self) -> dict[str, Any]:
        """The reconcile step of one deploy, with the bootstrap login."""
        parameters = {
            "secrets_directory": str(self.secrets),
            "services_file": str(self.services_file),
            "tools_file": str(self.tools_file),
            "external_secrets_file": str(LIST),
            "client_service": "aistor",
            "address": self.server.address,
            "file_owner": OWNER_TEXT,
            "root_owner": OWNER_TEXT,
            "ready_deadline_seconds": 5,
            "token_ttl_seconds": TTL,
            "token_max_ttl_seconds": MAX_TTL,
        }
        return dict(bootstrap.reconcile(parameters, check_only=False))

    def snapshot(self) -> dict[str, Any]:
        """What the old host held: the policies, roles, secret IDs, stored values and secret ID files of the services that were listed then."""
        return {
            "admin": self.server.policies["bootstrap_admin"],
            "policies": {name: self.server.policies[f"service_{name}"] for name in OLD_SERVICES},
            "roles": {name: dict(self.server.approle[name]) for name in OLD_SERVICES},
            "secret_ids": {name: set(self.server.secret_ids[name]) for name in OLD_SERVICES},
            "values": {name: list(versions) for name, versions in self.server.versions.items() if name.startswith("services/")},
            "files": {name: (self.secrets / "openbao" / f"{name}.secret_id").read_text() for name in OLD_SERVICES},
        }


class OneDeployTest(ExistingHostCase):
    def test_the_intake_is_refused_before_the_deploy_and_completes_after_one_deploy(self) -> None:
        before = self.run_script({"name": NAME, "value": VALUE})
        self.assertEqual((3, "no_login\n"), before[:2], "the old host's admin policy has no external rule, so there is no role for the script yet")
        self.assertIsNone(self.server.stored(f"external/{NAME}"))
        self.deploy()
        self.assertEqual((0, "written\n", ""), self.run_script({"name": NAME, "value": VALUE}))
        self.assertEqual(VALUE, self.server.stored(f"external/{NAME}"))
        self.assertEqual((0, "present\n", ""), self.run_script({"name": NAME, "value": None}))

    def test_the_deploy_leaves_the_volume_s_data_and_every_service_credential_as_they_were(self) -> None:
        before = self.snapshot()
        result = self.deploy()
        self.assertEqual(before, self.snapshot())
        self.assertIn("write role external_intake", result["actions"])
        new = ("external_intake", "deploy", "aistor")
        self.assertTrue(
            all(any(name in action for name in new) for action in result["actions"]),
            f"only the new intake role, the new deploy consumer and the new AIStor client are written: {result['actions']}",
        )
        self.assertEqual(OLD_ADMIN_POLICY, self.server.policies["bootstrap_admin"])

    def test_a_second_deploy_changes_nothing(self) -> None:
        self.deploy()
        again = self.deploy()
        self.assertFalse(again["changed"], again["actions"])

    def test_the_intake_secret_id_file_is_private_to_the_owner(self) -> None:
        self.deploy()
        path = self.secrets / "openbao" / INTAKE_FILE
        self.assertEqual(0o400, path.stat().st_mode & 0o777)
        self.assertEqual(MY_OWNER, (path.stat().st_uid, path.stat().st_gid))

    def test_the_intake_role_writes_the_declared_paths_and_reaches_nothing_else(self) -> None:
        self.deploy()
        api = bootstrap.Api(self.server.address)
        token = api.login("external_intake", (self.secrets / "openbao" / INTAKE_FILE).read_text())
        denied = [
            "secret/data/services/nessie/postgres",
            "secret/data/services/grafana/admin_password",
            "secret/data/external/another_secret",
            "sys/policies/acl/service_nessie",
            "auth/approle/role/nessie",
        ]
        for path in denied:
            with self.subTest(path=path):
                self.assertEqual(403, api.call("GET", path, token)[0])
        self.assertEqual(404, api.call("GET", f"secret/data/external/{NAME}", token)[0])


class RetiredGrantTest(ExistingHostCase):
    """A service that left the list keeps its role and its data, and the deploy takes its external grants away."""

    GONE_POLICY = (
        bootstrap.policy_text("gone", ["secret/data/external/dockerhub_read"])
        + 'path "secret/data/external/other" { capabilities = ["read"] }\n'
        + 'path "secret/metadata/external/other" { capabilities = ["read", "list"] }\n'
    )

    def make_server(self) -> StandIn:
        server = super().make_server()
        server.policies["service_gone"] = self.GONE_POLICY
        server.approle["gone"] = {**bootstrap.role_settings("gone", TTL, MAX_TTL)}
        server.secret_ids["gone"] = {"old-secret-id-of-gone"}
        server.versions["services/gone/postgres"] = ["password of gone"]
        return server

    def test_the_deploy_drops_every_external_grant_of_a_policy_whose_service_left_the_list_and_keeps_its_other_rules(self) -> None:
        result = self.deploy()
        self.assertIn("drop the external grants of the policy of a service that left the list", result["actions"])
        self.assertEqual(bootstrap.policy_text("gone"), self.server.policies["service_gone"])

    def test_the_role_the_secret_ids_and_the_data_of_that_service_stay_and_it_is_reported_unlisted(self) -> None:
        result = self.deploy()
        self.assertEqual(1, result["unlisted"])
        self.assertEqual({**bootstrap.role_settings("gone", TTL, MAX_TTL)}, self.server.approle["gone"])
        self.assertEqual({"old-secret-id-of-gone"}, self.server.secret_ids["gone"])
        self.assertEqual(["password of gone"], self.server.versions["services/gone/postgres"])

    def test_a_second_deploy_changes_nothing_and_a_retired_policy_without_an_external_grant_is_left_alone(self) -> None:
        self.deploy()
        self.assertFalse(self.deploy()["changed"])

    def test_a_policy_of_another_name_than_the_managed_ones_is_not_touched(self) -> None:
        self.server.policies["not_managed"] = 'path "secret/data/external/dockerhub_read" { capabilities = ["read"] }\n'
        self.deploy()
        self.assertEqual('path "secret/data/external/dockerhub_read" { capabilities = ["read"] }\n', self.server.policies["not_managed"])


if __name__ == "__main__":
    unittest.main()
