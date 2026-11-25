"""The OpenBao bootstrap's rules: the policy each service gets, the plan that makes the server match the list and the owner of every secret file.

Real runs against a server are the acceptance test's job. Isolated cases are kept for what a fresh server never shows: a server that drifted (a policy
or role that was widened, a stale password, a secret ID that no longer works) and a secret file with another mode or owner than the list asks for. A test
user cannot give a file to another user, so the owners here are the test user's own groups.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import stat
import sys
import tempfile
import unittest
from functools import partial
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest import mock

from tests.fast.ansible_paths import shared_module
from tests.fast.header_secret_support import raised_text, unsafe_values

ROOT = Path(__file__).resolve().parents[2]
MODULE = ROOT / "ansible" / "roles" / "openbao" / "library" / "openbao_bootstrap.py"
TTL, MAX_TTL = 3600, 86400
UID, GID = os.getuid(), os.getgid()
OTHER_GIDS = [group for group in os.getgroups() if group != GID]
MY_OWNER = (UID, GID)


def load_module() -> ModuleType:
    shared_module("secret_files")
    spec = importlib.util.spec_from_file_location("openbao_bootstrap", MODULE)
    if spec is None or spec.loader is None:
        raise ImportError(str(MODULE))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


bootstrap = load_module()


def good_state(services: list[str], tools: list[str]) -> Any:
    """A server that already matches the desired state of ``services``."""
    return bootstrap.State(
        policies={f"service_{name}": bootstrap.policy_text(name) for name in services},
        roles={name: {**bootstrap.role_settings(name, TTL, MAX_TTL), "role_id": name} for name in services},
        secret_id_valid={name: True for name in services},
        passwords={name: f"password of {name}" for name in tools},
    )


def planned(services: list[str], tools: list[str], state: Any) -> list[str]:
    actions = bootstrap.plan(services, tools, state, lambda name: f"password of {name}", TTL, MAX_TTL)
    return [action.description for action in actions]


class PolicyTest(unittest.TestCase):
    def test_a_policy_reaches_only_the_path_of_its_own_service(self) -> None:
        for name in ("nessie", "nessie_two"):
            paths = re.findall(r'path "([^"]+)"', bootstrap.policy_text(name))
            with self.subTest(service=name):
                self.assertTrue(paths)
                for path in paths:
                    self.assertRegex(path, rf"^secret/(data|metadata)/services/{name}/\*$")

    def test_a_policy_grants_reading_and_nothing_that_writes(self) -> None:
        capabilities = set(re.findall(r'"(\w+)"', bootstrap.policy_text("nessie").replace('path "', "")))
        self.assertEqual({"read", "list"}, capabilities)


class RoleTest(unittest.TestCase):
    def test_a_role_is_bound_to_its_own_policy_with_a_secret_id_and_a_numbered_role_id_of_the_service_name(self) -> None:
        settings = bootstrap.role_settings("nessie", TTL, MAX_TTL)
        self.assertEqual(["service_nessie"], settings["token_policies"])
        self.assertEqual((TTL, MAX_TTL), (settings["token_ttl"], settings["token_max_ttl"]))
        self.assertTrue(settings["bind_secret_id"])
        self.assertEqual("nessie", settings["role_id"])


class PlanTest(unittest.TestCase):
    services = ("nessie", "airbyte", "dagster")
    tools = ("nessie", "airbyte")

    def test_a_server_that_matches_needs_no_action(self) -> None:
        self.assertEqual([], planned(list(self.services), list(self.tools), good_state(list(self.services), list(self.tools))))

    def test_an_empty_server_gets_a_policy_a_role_and_a_secret_id_for_each_service_and_a_password_for_each_tool(self) -> None:
        empty = bootstrap.State(policies={}, roles={}, secret_id_valid={}, passwords={})
        actions = planned(["nessie", "dagster"], ["nessie"], empty)
        self.assertEqual(
            [
                "write policy service_nessie",
                "write role nessie",
                "issue a secret ID for nessie",
                "write the PostgreSQL password of nessie",
                "write policy service_dagster",
                "write role dagster",
                "issue a secret ID for dagster",
            ],
            actions,
        )

    def test_a_service_added_to_the_list_changes_nothing_for_the_others(self) -> None:
        state = good_state(["nessie", "airbyte"], ["nessie", "airbyte"])
        self.assertEqual(
            ["write policy service_dagster", "write role dagster", "issue a secret ID for dagster"],
            planned(["nessie", "airbyte", "dagster"], ["nessie", "airbyte"], state),
        )

    def test_a_widened_policy_is_written_back(self) -> None:
        state = good_state(list(self.services), list(self.tools))
        state.policies["service_nessie"] += 'path "secret/data/*" { capabilities = ["read"] }\n'
        self.assertEqual(["write policy service_nessie"], planned(list(self.services), list(self.tools), state))

    def test_a_role_that_drifted_in_any_setting_is_written_back(self) -> None:
        for key, value in (
            ("token_policies", ["service_nessie", "bootstrap_admin"]),
            ("token_ttl", 7200),
            ("token_max_ttl", 1),
            ("bind_secret_id", False),
            ("role_id", "other"),
        ):
            state = good_state(list(self.services), list(self.tools))
            state.roles["nessie"][key] = value
            with self.subTest(setting=key):
                self.assertEqual(["write role nessie"], planned(list(self.services), list(self.tools), state))

    def test_a_secret_id_that_no_longer_works_is_issued_again(self) -> None:
        state = good_state(list(self.services), list(self.tools))
        state.secret_id_valid["airbyte"] = False
        self.assertEqual(["issue a secret ID for airbyte"], planned(list(self.services), list(self.tools), state))

    def test_a_stale_or_missing_password_is_written_and_a_password_that_matches_is_not(self) -> None:
        state = good_state(list(self.services), list(self.tools))
        state.passwords["nessie"] = "an older password"
        state.passwords["airbyte"] = None
        self.assertEqual(
            ["write the PostgreSQL password of nessie", "write the PostgreSQL password of airbyte"],
            planned(list(self.services), list(self.tools), state),
        )

    def test_a_service_that_is_no_tool_gets_no_password(self) -> None:
        state = good_state(list(self.services), list(self.tools))
        self.assertNotIn("dagster", state.passwords)
        self.assertEqual([], planned(list(self.services), list(self.tools), state))


class ProjectUserPolicyTest(unittest.TestCase):
    """The AIStor project user's sign-in is kept at the service's own path, and the service's own role reads exactly that one path and nothing else."""

    def plan_with(self, state: Any, services: list[str] | None = None) -> list[str]:
        names = services or ["nessie", "aistor"]
        return [action.description for action in bootstrap.plan(names, [], state, lambda name: "", TTL, MAX_TTL, client_service="aistor")]

    def matching_state(self) -> Any:
        state = good_state(["nessie", "aistor"], [])
        state.policies["service_aistor"] = bootstrap.policy_text("aistor", single_path="aistor")
        return state

    def test_the_clients_policy_reads_exactly_the_one_path_and_nothing_that_lists_writes_or_reaches_another_path(self) -> None:
        text = bootstrap.policy_text("aistor", single_path="aistor")
        self.assertEqual(['path "secret/data/services/aistor/aistor" { capabilities = ["read"] }'], text.strip().splitlines())
        self.assertNotIn("*", text)
        self.assertNotIn("metadata", text)
        self.assertNotIn("list", text)

    def test_the_policy_of_a_service_that_is_not_the_client_is_what_it_was(self) -> None:
        self.assertEqual(bootstrap.policy_text("nessie"), bootstrap.policy_text("nessie", single_path=None))
        self.assertIn("secret/data/services/nessie/*", bootstrap.policy_text("nessie"))
        self.assertNotIn("aistor", bootstrap.policy_text("nessie"))

    def test_a_server_that_holds_the_exact_path_policy_needs_no_action(self) -> None:
        self.assertEqual([], self.plan_with(self.matching_state()))

    def test_the_wide_policy_of_the_earlier_deploy_is_written_back_as_the_exact_path_policy(self) -> None:
        state = good_state(["nessie", "aistor"], [])
        self.assertEqual(["write policy service_aistor"], self.plan_with(state))

    def test_the_policy_that_is_written_for_the_client_is_the_exact_path_policy(self) -> None:
        sent: list[tuple[str, str, dict[str, Any]]] = []

        class Recorder:
            def must(self, method: str, path: str, token: str | None = None, body: dict[str, Any] | None = None) -> dict[str, Any]:
                sent.append((method, path, body or {}))
                return {}

        action = bootstrap.Action("policy", "aistor", "write policy service_aistor")
        with tempfile.TemporaryDirectory() as scratch:
            bootstrap.apply(Recorder(), "token", action, Path(scratch), MY_OWNER, TTL, MAX_TTL, client_service="aistor")
        self.assertEqual([("PUT", "sys/policies/acl/service_aistor", {"policy": bootstrap.policy_text("aistor", single_path="aistor")})], sent)

    def test_no_client_means_the_wide_policy_for_every_service(self) -> None:
        state = good_state(["nessie", "aistor"], [])
        self.assertEqual([], [action.description for action in bootstrap.plan(["nessie", "aistor"], [], state, lambda name: "", TTL, MAX_TTL)])

    def test_the_openbao_step_no_longer_writes_the_project_users_sign_in_because_the_aistor_step_does_after_it_verified_it(self) -> None:
        for name in ("Client", "client_user", "client_password_file", "write the AIStor project user's sign-in"):
            self.assertNotIn(name, MODULE.read_text(), name)

    def test_the_client_must_be_a_listed_service_so_no_policy_would_name_a_path_of_a_service_that_has_no_role(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            (folder / "services.txt").write_text("nessie\n")
            (folder / "tools.txt").write_text("nessie\n")
            params = {
                "secrets_directory": str(folder),
                "services_file": str(folder / "services.txt"),
                "tools_file": str(folder / "tools.txt"),
                "root_owner": "0:0",
                "file_owner": "100:1000",
                "address": "http://127.0.0.1:8200",
                "client_service": "aistor",
            }
            with self.assertRaisesRegex(bootstrap.BootstrapError, "no entry in the service list"):
                bootstrap.reconcile(params, check_only=True)


class FixedDiagnosticsTest(unittest.TestCase):
    """A request of the step can carry a secret, and an answer can repeat what it was sent: a failure names the request and its status, never the answer."""

    def setUp(self) -> None:
        FakeConnection.requests = []
        patcher = mock.patch.object(bootstrap.http.client, "HTTPConnection", FakeConnection)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.api = bootstrap.Api("http://127.0.0.1:8200")

    def test_a_refused_request_is_a_fixed_message_with_the_method_the_path_and_the_status_and_no_text_of_the_answer(self) -> None:
        FakeConnection.answer = (400, json.dumps({"errors": [f"invalid value {PLANTED_MARKER}"]}).encode())
        error, text = raised_text(
            partial(self.api.must, "POST", "secret/data/services/aistor/x", "a-token", {"data": {"password": PLANTED_MARKER}}), bootstrap.BootstrapError, "must"
        )
        self.assertEqual("POST secret/data/services/aistor/x: HTTP 400", str(error))
        self.assertNotIn(PLANTED_MARKER, text)

    def test_a_transport_failure_names_the_exception_type_only(self) -> None:
        class Broken(FakeConnection):
            def request(self, method: str, path: str, body: str | None, headers: dict[str, str]) -> None:
                raise OSError(PLANTED_MARKER)

        with mock.patch.object(bootstrap.http.client, "HTTPConnection", Broken):
            error, text = raised_text(partial(self.api.call, "GET", "sys/health", "a-token"), bootstrap.BootstrapError, "call")
        self.assertNotIn(PLANTED_MARKER, text)
        self.assertIn("OSError", str(error))


def secret_file(directory: Path, name: str, owner: tuple[int, int] = MY_OWNER, mode: int = 0o400) -> Path:
    path = directory / "openbao" / f"{name}.secret_id"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("value")
    os.chown(path, *owner)
    path.chmod(mode)
    return path


class OwnerTest(unittest.TestCase):
    entries = (bootstrap.Entry("nessie", (10000, 10001)), bootstrap.Entry("airbyte", None))

    def test_the_admin_file_and_each_service_file_have_the_owner_of_their_line_and_a_service_with_none_gets_the_root_owner(self) -> None:
        files = bootstrap.secret_files(Path("/s"), list(self.entries), (100, 1000), (0, 0))
        self.assertEqual(
            {
                "/s/openbao/bootstrap.secret_id": (100, 1000),
                "/s/openbao/nessie.secret_id": (10000, 10001),
                "/s/openbao/airbyte.secret_id": (0, 0),
            },
            {str(path): owner for path, owner in files.items()},
        )

    def test_a_file_with_another_mode_or_another_owner_than_its_line_asks_for_is_listed_and_a_file_that_matches_is_not(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            entries = [bootstrap.Entry("nessie", MY_OWNER), bootstrap.Entry("airbyte", None), bootstrap.Entry("dagster", MY_OWNER)]
            secret_file(folder, "nessie")
            secret_file(folder, "airbyte")
            secret_file(folder, "dagster", mode=0o440)
            actions = bootstrap.access_actions(entries, folder, [], MY_OWNER)
            matched = bootstrap.access_actions(entries[:2], folder, [], MY_OWNER)
            elsewhere = bootstrap.access_actions(entries[:2], folder, [], (UID + 1, GID))
        self.assertEqual(["dagster"], [action.service for action in actions])
        self.assertEqual([], matched)
        self.assertEqual(["airbyte"], [action.service for action in elsewhere])

    def test_a_file_issued_in_this_run_is_not_listed_again(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            secret_file(Path(scratch), "nessie", mode=0o600)
            self.assertEqual([], bootstrap.access_actions([bootstrap.Entry("nessie", MY_OWNER)], Path(scratch), ["nessie"], MY_OWNER))


class PrepareTest(unittest.TestCase):
    def prepared(self, folder: Path, owner: tuple[int, int] = MY_OWNER, *, check_only: bool = False) -> bool:
        unlock = folder / "unlock.env"
        unlock.write_text("OPENBAO_UNLOCK_KEY=" + "ab" * 32 + "\n")
        return bool(bootstrap.prepare(folder / "secrets", unlock, owner, check_only=check_only))

    def test_the_admin_file_is_made_for_its_owner_only_and_a_second_run_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            first = self.prepared(Path(scratch))
            path = Path(scratch) / "secrets" / "openbao" / "bootstrap.secret_id"
            status = path.stat()
            content = path.read_text()
            second = self.prepared(Path(scratch))
            kept = path.read_text()
        self.assertEqual((True, False), (first, second))
        self.assertEqual((0o400, MY_OWNER), (stat.S_IMODE(status.st_mode), (status.st_uid, status.st_gid)))
        self.assertEqual(content, kept)

    def test_an_admin_file_that_others_can_read_is_made_private_with_its_content_kept(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            self.prepared(Path(scratch))
            path = Path(scratch) / "secrets" / "openbao" / "bootstrap.secret_id"
            content = path.read_text()
            for mode in (0o600, 0o440, 0o404, 0o644):
                path.chmod(mode)
                with self.subTest(mode=oct(mode)):
                    self.assertTrue(self.prepared(Path(scratch)))
                    self.assertEqual((0o400, content), (stat.S_IMODE(path.stat().st_mode), path.read_text()))

    def test_a_check_run_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            self.assertTrue(self.prepared(Path(scratch), check_only=True))
            self.assertFalse((Path(scratch) / "secrets").exists())
            self.prepared(Path(scratch))
            path = Path(scratch) / "secrets" / "openbao" / "bootstrap.secret_id"
            path.chmod(0o640)
            self.assertTrue(self.prepared(Path(scratch), check_only=True))
            self.assertEqual(0o640, stat.S_IMODE(path.stat().st_mode))

    def test_an_admin_file_with_a_trailing_newline_is_refused_by_name_and_left_as_it_was(self) -> None:
        for ending in ("\n", "\r\n", "\n\n"):
            with self.subTest(ending=repr(ending)), tempfile.TemporaryDirectory() as scratch:
                path = Path(scratch) / "secrets" / "openbao" / "bootstrap.secret_id"
                path.parent.mkdir(parents=True)
                path.write_text("an-owner-placed-value" + ending)
                before = path.read_bytes()
                for check_only in (False, True):
                    with self.assertRaisesRegex(bootstrap.BootstrapError, "admin secret ID file ends with a line break"):
                        self.prepared(Path(scratch), check_only=check_only)
                self.assertEqual(before, path.read_bytes())

    def test_a_made_admin_file_has_no_line_break_so_the_first_start_and_the_login_use_the_same_value(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            self.prepared(Path(scratch))
            content = (Path(scratch) / "secrets" / "openbao" / "bootstrap.secret_id").read_bytes()
        self.assertTrue(content)
        self.assertNotIn(b"\n", content)
        self.assertNotIn(b"\r", content)

    def test_a_missing_unlock_key_file_stops_the_step_before_any_file_is_made(self) -> None:
        with tempfile.TemporaryDirectory() as scratch, self.assertRaisesRegex(bootstrap.BootstrapError, "unlock key file is missing"):
            bootstrap.prepare(Path(scratch) / "secrets", Path(scratch) / "none.env", MY_OWNER, check_only=False)
        self.assertFalse((Path(scratch) / "secrets").exists())


PLANTED_MARKER = "planted-vault-token"
REFUSAL = "the token that OpenBao gave is not a valid token"


class FakeConnection:
    """An HTTP connection that sends nothing: it keeps each request and answers every one with the same status and body."""

    requests: list[tuple[str, str, dict[str, str]]] = []
    answer: tuple[int, bytes] = (200, b"{}")

    def __init__(self, host: str, port: int, timeout: float) -> None:
        pass

    def request(self, method: str, path: str, body: str | None, headers: dict[str, str]) -> None:
        self.requests.append((method, path, headers))

    def getresponse(self) -> Any:
        status, body = self.answer
        return mock.Mock(status=status, read=lambda: body)

    def close(self) -> None:
        pass


class TokenHeaderTest(unittest.TestCase):
    def setUp(self) -> None:
        FakeConnection.requests = []
        FakeConnection.answer = (200, b"{}")
        patcher = mock.patch.object(bootstrap.http.client, "HTTPConnection", FakeConnection)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.api = bootstrap.Api("http://127.0.0.1:8200")

    def test_a_token_of_the_header_safe_form_goes_into_the_header_as_it_is(self) -> None:
        for token in ("hvs.CAESIJ-_x~y+z/w==", "s.abc", "a"):
            with self.subTest(token=token):
                self.api.call("GET", "sys/health", token)
                self.assertEqual({"X-Vault-Token": token}, FakeConnection.requests[-1][2])
        self.api.call("GET", "sys/seal-status")
        self.assertEqual({}, FakeConnection.requests[-1][2])

    def test_a_token_a_header_cannot_hold_is_refused_before_any_request_with_the_fixed_message_and_no_error_holds_it(self) -> None:
        for name, token in unsafe_values(PLANTED_MARKER).items():
            with self.subTest(character=name):
                error, text = raised_text(partial(self.api.call, "GET", "sys/health", token), bootstrap.BootstrapError, "call")
                self.assertEqual(REFUSAL, str(error))
                self.assertNotIn(PLANTED_MARKER, text)
        self.assertEqual([], FakeConnection.requests)

    def test_a_login_reply_with_such_a_token_is_refused_at_its_first_use_and_no_error_holds_it(self) -> None:
        for name, token in unsafe_values(PLANTED_MARKER).items():
            with self.subTest(character=name):
                FakeConnection.answer = (200, json.dumps({"auth": {"client_token": token}}).encode())
                issued = self.api.login("admin", "an-admin-secret-id")
                FakeConnection.requests = []
                error, text = raised_text(partial(self.api.must, "GET", "sys/policies/acl/service_a", issued), bootstrap.BootstrapError, "call")
                self.assertEqual(REFUSAL, str(error))
                self.assertNotIn(PLANTED_MARKER, text)
                self.assertEqual([], FakeConnection.requests)

    def test_a_login_reply_whose_token_is_not_text_is_refused_and_no_token_is_issued(self) -> None:
        """``True`` would go into the header as ``True``: a token must be a string, and an empty one is none."""
        for what, token in {"true": True, "number": 5, "list": ["x"], "object": {"a": "b"}, "empty": "", "null": None}.items():
            with self.subTest(token=what):
                FakeConnection.answer = (200, json.dumps({"auth": {"client_token": token}}).encode())
                error, _ = raised_text(lambda: self.api.login("admin", "an-admin-secret-id"), bootstrap.BootstrapError, "login")
                self.assertIn("the login of role admin", str(error))
        FakeConnection.answer = (200, json.dumps({"auth": {"client_token": "hvs.CAESIJ-_x~y+z/w=="}}).encode())
        self.assertEqual("hvs.CAESIJ-_x~y+z/w==", self.api.login("admin", "an-admin-secret-id"))


if __name__ == "__main__":
    unittest.main()
