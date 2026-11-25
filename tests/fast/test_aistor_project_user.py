"""The one-time AIStor bootstrap makes the project's user and its policy with the root pair in the server's own container, and does nothing when they match.

This is the only use of the root pair: the module's calls run ``mc`` inside the AIStor container, which reads the root files from its own secrets mount, so the
root pair is never an argument, never in the module's environment and never on the host's command line. The cases hold what the bootstrap can get wrong: a
policy wider than the project's bucket, a root value or the project's password on the host's command line, a user that is created again when it already
matches, a drifted policy that stays, a write in a check run and a failure message that carries output. The stand-in server answers the module's calls
and keeps what the calls changed, so a second run shows that nothing changes. The real ``mc`` calls are the host run's job.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
MODULE = ROOT / "ansible" / "roles" / "compose_stack" / "library" / "aistor_project_user.py"
BUCKET = "example-project"
USER = "project_user"
POLICY = "project_bucket"
PROJECT_VALUE = "example-project-password-0123456789abcdef"
ROOT_FIRST = "example-root-user-0123456789abcdef"
ROOT_SECOND = "example-root-password-0123456789abcdef"
COMPOSE = ["docker", "compose", "--project-directory", "/opt/platform/compose"]


def load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("aistor_project_user", MODULE)
    if spec is None or spec.loader is None:
        raise ImportError(str(MODULE))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


bootstrap: Any = load_module()


@dataclass
class Server:
    """A stand-in for the calls of the module: it keeps the policies, the users and each user's policy, as AIStor would."""

    policies: dict[str, dict[str, Any]] = field(default_factory=dict)
    users: dict[str, dict[str, str]] = field(default_factory=dict)
    calls: list[Any] = field(default_factory=list)
    fail_on: str = ""

    def __call__(self, call: Any) -> Any:
        self.calls.append(call)
        words = list(call.arguments)
        kind = call.kind
        if self.fail_on and self.fail_on == kind:
            return bootstrap.Result(1, f"leaked {ROOT_SECOND} {PROJECT_VALUE}")
        if kind == "read" and words[:3] == ["admin", "policy", "info"]:
            document = self.policies.get(words[4])
            return bootstrap.Result(0, json.dumps(document)) if document is not None else bootstrap.Result(1, "")
        if kind == "read" and words[:3] == ["admin", "user", "info"]:
            user = self.users.get(words[4])
            if user is None:
                return bootstrap.Result(1, "")
            return bootstrap.Result(0, f"AccessKey: {words[4]}\nStatus: {user['status']}\nPolicyName: {user['policy']}\nMemberOf: []\n")
        if kind == "policy_create":
            self.policies[words[0]] = json.loads(call.stdin)
        elif kind == "user_add":
            self.users[words[0]] = {"status": "enabled", "policy": ""}
        elif kind == "read" and words[:3] == ["admin", "policy", "attach"]:
            held = [name for name in self.users[words[6]]["policy"].split(",") if name]
            self.users[words[6]]["policy"] = ",".join([*held, words[4]])
        elif kind == "read" and words[:3] == ["admin", "policy", "detach"]:
            held = [name for name in self.users[words[6]]["policy"].split(",") if name and name != words[4]]
            self.users[words[6]]["policy"] = ",".join(held)
        elif kind == "read" and words[:3] == ["admin", "user", "enable"]:
            self.users[words[4]]["status"] = "enabled"
        else:
            raise AssertionError(f"the module made a call that the stand-in does not know: {kind} {words}")
        return bootstrap.Result(0, "")

    def writes(self) -> list[str]:
        return [call.kind if call.kind != "read" else " ".join(call.arguments[:3]) for call in self.calls if self.is_write(call)]

    @staticmethod
    def is_write(call: Any) -> bool:
        return call.kind != "read" or tuple(call.arguments[:3]) in {("admin", "policy", "attach"), ("admin", "policy", "detach"), ("admin", "user", "enable")}


def run(server: Server, *, check_only: bool = False) -> list[str]:
    return list(bootstrap.reconcile(server, USER, POLICY, BUCKET, PROJECT_VALUE, check_only=check_only))


class PolicyTest(unittest.TestCase):
    def test_the_policy_reaches_only_the_projects_bucket_and_what_is_inside_it(self) -> None:
        document = bootstrap.policy_document(BUCKET)
        resources = {resource for statement in document["Statement"] for resource in statement["Resource"]}
        self.assertEqual({f"arn:aws:s3:::{BUCKET}", f"arn:aws:s3:::{BUCKET}/*"}, resources)

    def test_the_policy_holds_no_admin_action_no_wildcard_action_and_no_other_effect_than_allow(self) -> None:
        document = bootstrap.policy_document(BUCKET)
        for statement in document["Statement"]:
            self.assertEqual("Allow", statement["Effect"])
            self.assertNotIn("Principal", statement)
            for action in statement["Action"]:
                self.assertRegex(action, r"^s3:[A-Za-z]+$")
                self.assertNotIn("*", action)
        text = json.dumps(document)
        self.assertNotIn("admin:", text)
        self.assertNotIn("kms:", text)

    def test_bucket_actions_apply_to_the_bucket_and_object_actions_to_its_objects_only(self) -> None:
        by_resource = {statement["Resource"][0]: set(statement["Action"]) for statement in bootstrap.policy_document(BUCKET)["Statement"]}
        bucket_actions, object_actions = by_resource[f"arn:aws:s3:::{BUCKET}"], by_resource[f"arn:aws:s3:::{BUCKET}/*"]
        self.assertTrue({"s3:CreateBucket", "s3:PutBucketVersioning", "s3:GetBucketVersioning", "s3:ListBucket"} <= bucket_actions)
        self.assertTrue({"s3:GetObject", "s3:PutObject", "s3:DeleteObject"} <= object_actions)
        self.assertFalse(bucket_actions & object_actions)

    def test_a_bucket_name_that_is_not_a_bucket_name_is_refused_so_no_wildcard_reaches_a_resource(self) -> None:
        for name in ("*", "a*", "Bad_Name", "", "a/b", "ab", "-start-hyphen"):
            with self.subTest(name=name), self.assertRaises(bootstrap.BootstrapError):
                bootstrap.policy_document(name)

    def test_two_policies_are_equal_when_only_the_order_of_their_lists_differs(self) -> None:
        document = bootstrap.policy_document(BUCKET)
        shuffled = json.loads(json.dumps(document))
        for statement in shuffled["Statement"]:
            statement["Action"].reverse()
        shuffled["Statement"].reverse()
        self.assertEqual(bootstrap.normalised(document), bootstrap.normalised(shuffled))
        widened = json.loads(json.dumps(document))
        widened["Statement"][0]["Resource"].append("arn:aws:s3:::*")
        self.assertNotEqual(bootstrap.normalised(document), bootstrap.normalised(widened))


class CommandLineTest(unittest.TestCase):
    """The host's command line holds the container's service and the script's fixed text and arguments, and no secret value."""

    def calls(self) -> list[Any]:
        server = Server()
        run(server)
        return server.calls

    def test_root_enters_only_as_a_read_of_the_containers_own_secret_files_in_the_script_and_never_as_a_value(self) -> None:
        for call in self.calls():
            line = bootstrap.command_line(COMPOSE, "aistor", call)
            text = "\n".join(line)
            self.assertEqual([*COMPOSE, "exec", "-T", "aistor", "sh", "-c"], line[: len(COMPOSE) + 5])
            self.assertIn("/run/secrets/aistor_root_user", text)
            self.assertIn("/run/secrets/aistor_root_password", text)
            for value in (ROOT_FIRST, ROOT_SECOND):
                self.assertNotIn(value, text)

    def test_the_project_password_travels_on_standard_input_and_never_in_the_host_command_line(self) -> None:
        server = Server()
        run(server)
        add = next(call for call in server.calls if call.kind == "user_add")
        self.assertEqual(PROJECT_VALUE + "\n", add.stdin)
        for call in server.calls:
            self.assertNotIn(PROJECT_VALUE, "\n".join(bootstrap.command_line(COMPOSE, "aistor", call)))

    def test_the_policy_text_travels_on_standard_input_and_not_in_the_command_line(self) -> None:
        server = Server()
        run(server)
        create = next(call for call in server.calls if call.kind == "policy_create")
        self.assertEqual(bootstrap.policy_document(BUCKET), json.loads(create.stdin))
        self.assertNotIn("Statement", "\n".join(bootstrap.command_line(COMPOSE, "aistor", create)))


class ReconcileTest(unittest.TestCase):
    def test_a_fresh_server_gets_the_policy_the_user_and_the_attachment_in_that_order(self) -> None:
        server = Server()
        actions = run(server)
        self.assertEqual(["create the policy", "create the user", "attach the policy to the user"], actions)
        self.assertEqual(bootstrap.normalised(bootstrap.policy_document(BUCKET)), bootstrap.normalised(server.policies[POLICY]))
        self.assertEqual({USER: {"status": "enabled", "policy": POLICY}}, server.users)

    def test_a_second_run_changes_nothing_and_makes_no_write_call_so_an_existing_matching_user_is_never_created_again(self) -> None:
        server = Server()
        run(server)
        server.calls.clear()
        self.assertEqual([], run(server))
        self.assertEqual([], server.writes())

    def test_a_policy_that_drifted_is_written_again_and_the_user_stays(self) -> None:
        server = Server()
        run(server)
        server.policies[POLICY]["Statement"][0]["Resource"].append("arn:aws:s3:::*")
        server.calls.clear()
        self.assertEqual(["create the policy"], run(server))
        self.assertEqual(["policy_create"], server.writes())
        self.assertNotIn("arn:aws:s3:::*", json.dumps(server.policies[POLICY]))

    def test_a_user_without_the_policy_gets_it_attached_and_is_not_created_again(self) -> None:
        server = Server()
        run(server)
        server.users[USER]["policy"] = ""
        server.calls.clear()
        self.assertEqual(["attach the policy to the user"], run(server))
        self.assertNotIn("user_add", server.writes())

    def test_a_user_that_holds_another_policy_loses_it_so_the_user_is_never_wider_than_the_project_policy(self) -> None:
        server = Server()
        run(server)
        server.users[USER]["policy"] = f"consoleAdmin,{POLICY}"
        self.assertEqual(["detach the policy consoleAdmin from the user"], run(server))
        self.assertEqual(POLICY, server.users[USER]["policy"])
        server.users[USER]["policy"] = "consoleAdmin"
        self.assertEqual(["attach the policy to the user", "detach the policy consoleAdmin from the user"], run(server))
        self.assertEqual(POLICY, server.users[USER]["policy"])

    def test_a_disabled_user_is_enabled_and_not_created_again(self) -> None:
        server = Server()
        run(server)
        server.users[USER]["status"] = "disabled"
        server.calls.clear()
        self.assertEqual(["enable the user"], run(server))
        self.assertEqual({USER: {"status": "enabled", "policy": POLICY}}, server.users)

    def test_a_check_run_names_the_actions_and_writes_nothing(self) -> None:
        server = Server()
        self.assertEqual(["create the policy", "create the user", "attach the policy to the user"], run(server, check_only=True))
        self.assertEqual([], server.writes())
        self.assertEqual({}, server.users)

    def test_the_user_is_not_the_root_user_name(self) -> None:
        for name in ("minioadmin", "root", ""):
            with self.subTest(name=name), self.assertRaises(bootstrap.BootstrapError):
                bootstrap.reconcile(Server(), name, POLICY, BUCKET, PROJECT_VALUE, check_only=False)

    def test_the_policy_may_not_be_one_of_the_built_in_policies(self) -> None:
        for name in ("consoleAdmin", "readwrite", "readonly", "writeonly", "diagnostics"):
            with self.subTest(name=name), self.assertRaises(bootstrap.BootstrapError):
                bootstrap.reconcile(Server(), USER, name, BUCKET, PROJECT_VALUE, check_only=False)

    def test_a_failed_call_is_a_fixed_message_with_the_exit_status_and_no_output_text(self) -> None:
        for kind in ("policy_create", "user_add"):
            with self.subTest(kind=kind):
                server = Server(fail_on=kind)
                with self.assertRaises(bootstrap.BootstrapError) as caught:
                    run(server)
                for value in (ROOT_SECOND, PROJECT_VALUE, "leaked"):
                    self.assertNotIn(value, str(caught.exception))
                self.assertIn("exit status 1", str(caught.exception))

    def test_a_password_of_another_form_is_refused_before_any_call(self) -> None:
        for password in ("", "short", "has a space-0123456789", "line\nbreak-0123456789"):
            with self.subTest(password=password):
                server = Server()
                with self.assertRaises(bootstrap.BootstrapError):
                    bootstrap.reconcile(server, USER, POLICY, BUCKET, password, check_only=False)
                self.assertEqual([], server.calls)


class UserInfoTest(unittest.TestCase):
    def test_the_status_and_the_policy_are_read_from_the_documented_text_output(self) -> None:
        text = "AccessKey: project_user\nStatus: enabled\nPolicyName: project_bucket\nMemberOf: []\nUpdatedAt: 2026-11-25T00:00:00Z\n"
        self.assertEqual(("enabled", ["project_bucket"]), bootstrap.parse_user_info(text))
        self.assertEqual(("disabled", []), bootstrap.parse_user_info("AccessKey: u\nStatus: disabled\nPolicyName: \n"))
        self.assertEqual(("enabled", ["a", "b"]), bootstrap.parse_user_info("Status: enabled\nPolicyName: a,b\n"))

    def test_an_answer_that_is_not_user_information_is_refused(self) -> None:
        with self.assertRaises(bootstrap.BootstrapError):
            bootstrap.parse_user_info("something else\n")


if __name__ == "__main__":
    unittest.main()
