"""The one-time AIStor bootstrap makes the project's user and its policy with the root pair in the server's own container, and does nothing when they match.

This is the only use of the root pair: the module's calls run ``mc`` inside the AIStor container, which reads the root files from its own secrets mount, so the
root pair is never an argument, never in the module's environment and never on the host's command line. The cases hold what the bootstrap can get wrong: a
policy wider than what the pinned Terraform provider calls, a root value or the project's password on the host's command line, a user that is created again
when it already matches, a user or a policy that is made because a read failed and not because it is absent, a user that keeps a policy or a group that is not
its own, a disabled user that is enabled before its extra access is gone, a drifted policy that stays, a write in a check run and a failure message that
carries output. The stand-in server answers the module's calls and keeps what the calls changed, so a second run shows that nothing changes. The real ``mc``
calls are the host run's job.
"""

from __future__ import annotations

import importlib.util
import json
import re
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
NO_SUCH_USER = json.dumps(
    {"status": "error", "error": {"cause": {"error": {"Code": "XMinioAdminNoSuchUser", "Message": "The specified user does not exist."}}}}
)
NO_SUCH_POLICY = json.dumps(
    {"status": "error", "error": {"cause": {"error": {"Code": "XMinioAdminNoSuchPolicy", "Message": "The canned policy does not exist."}}}}
)
NOT_AN_ANSWER = "mc: <ERROR> Unable to initialize admin connection. Get http://127.0.0.1:9000: connection refused"

# Each action of the project's policy with the call of the pinned provider (aminueza/minio v3.46.2, files under minio/) that needs it, and nothing else. A call
# that is not in this table is not in the policy. Bucket: resource_minio_s3_bucket.go. Versioning: resource_minio_s3_bucket_versioning.go. The minio-go calls
# named are the library's own S3 requests for each client call.
PROVIDER_CALLS = {
    "s3:CreateBucket": "bucket:197 MakeBucket",
    "s3:ListBucket": "bucket:191 and bucket:361 BucketExists (HeadBucket), versioning:151 BucketExists, bucket:832 ListObjects without versions",
    "s3:ListBucketVersions": "bucket:812 and bucket:835 ListObjects with WithVersions, before RemoveBucket",
    "s3:GetBucketLocation": "bucket:788 waitForBucketReady (GetBucketLocation), called by bucket:217 and versioning:92",
    "s3:GetBucketTagging": "bucket:327 GetBucketTagging when the resource is read",
    "s3:GetBucketPolicy": "versioning:120 and versioning:123 GetBucketPolicy after SetBucketVersioning (an error is ignored there)",
    "s3:DeleteBucketPolicy": "bucket:658 SetBucketPolicy with an empty policy, which minio-go sends as a DELETE of the policy: acl = private",
    "s3:GetBucketVersioning": "versioning:164 and versioning:179 GetBucketVersioning",
    "s3:PutBucketVersioning": "versioning:98 SetBucketVersioning and versioning:231 SuspendVersioning",
    "s3:DeleteBucket": "bucket:478 RemoveBucket when the bucket is destroyed (force_destroy = false)",
}


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
    """A stand-in for the calls of the module: it keeps the policies, the users, each user's policies and groups and each user's status, as AIStor would."""

    policies: dict[str, dict[str, Any]] = field(default_factory=dict)
    users: dict[str, dict[str, Any]] = field(default_factory=dict)
    calls: list[Any] = field(default_factory=list)
    fail_on: str = ""
    fail_words: tuple[str, ...] = ()
    unreadable: tuple[str, ...] = ()
    order: list[str] = field(default_factory=list)

    def __call__(self, call: Any) -> Any:
        self.calls.append(call)
        words = list(call.arguments)
        kind = call.kind
        if self.fail_on and self.fail_on == kind:
            return bootstrap.Result(1, f"leaked {ROOT_SECOND} {PROJECT_VALUE}")
        if kind == "read" and self.fail_words and tuple(words[: len(self.fail_words)]) == self.fail_words:
            return bootstrap.Result(1, f"leaked {ROOT_SECOND} {PROJECT_VALUE}")
        if kind == "read" and self.unreadable and tuple(words[: len(self.unreadable)]) == self.unreadable:
            return bootstrap.Result(1, NOT_AN_ANSWER)
        if kind == "read" and words[:3] == ["admin", "policy", "info"]:
            document = self.policies.get(words[4])
            if document is None:
                return bootstrap.Result(1, NO_SUCH_POLICY)
            return bootstrap.Result(0, json.dumps({"status": "success", "policy": words[4], "policyInfo": {"PolicyName": words[4], "Policy": document}}))
        if kind == "read" and words[:3] == ["admin", "user", "info"]:
            user = self.users.get(words[4])
            if user is None:
                return bootstrap.Result(1, NO_SUCH_USER)
            groups = [{"name": name, "policies": ["readwrite"]} for name in user["groups"]]
            return bootstrap.Result(
                0,
                json.dumps(
                    {"status": "success", "accessKey": words[4], "policyName": ",".join(user["policies"]), "userStatus": user["status"], "memberOf": groups}
                ),
            )
        self.order.append(kind if kind != "read" else " ".join(words[:3]))
        if kind == "policy_create":
            self.policies[words[0]] = json.loads(call.stdin)
        elif kind == "user_add":
            self.users[words[0]] = {"status": "enabled", "policies": [], "groups": [], "password": call.stdin.rstrip("\n")}
        elif kind == "read" and words[:3] == ["admin", "policy", "attach"]:
            self.users[words[6]]["policies"].append(words[4])
        elif kind == "read" and words[:3] == ["admin", "policy", "detach"]:
            self.users[words[6]]["policies"].remove(words[4])
        elif kind == "read" and words[:3] == ["admin", "user", "enable"]:
            self.users[words[4]]["status"] = "enabled"
        elif kind == "read" and words[:3] == ["admin", "group", "remove"]:
            self.users[words[5]]["groups"].remove(words[4])
        else:
            raise AssertionError(f"the module made a call that the stand-in does not know: {kind} {words}")
        return bootstrap.Result(0, "")

    def writes(self) -> list[str]:
        return [call.kind if call.kind != "read" else " ".join(call.arguments[:3]) for call in self.calls if self.is_write(call)]

    @staticmethod
    def is_write(call: Any) -> bool:
        written = {("admin", "policy", "attach"), ("admin", "policy", "detach"), ("admin", "user", "enable"), ("admin", "group", "remove")}
        return call.kind != "read" or tuple(call.arguments[:3]) in written


def run(server: Server, *, check_only: bool = False) -> list[str]:
    return list(bootstrap.reconcile(server, USER, POLICY, BUCKET, PROJECT_VALUE, check_only=check_only))


def made(server: Server) -> Server:
    """A server that a first run has made."""
    run(server)
    server.calls.clear()
    server.order.clear()
    return server


class PolicyTest(unittest.TestCase):
    def actions(self) -> set[str]:
        return {action for statement in bootstrap.policy_document(BUCKET)["Statement"] for action in statement["Action"]}

    def test_the_policy_reaches_only_the_projects_bucket(self) -> None:
        document = bootstrap.policy_document(BUCKET)
        resources = {resource for statement in document["Statement"] for resource in statement["Resource"]}
        self.assertEqual({f"arn:aws:s3:::{BUCKET}"}, resources)

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

    def test_the_policy_holds_exactly_the_calls_that_the_pinned_provider_makes_for_the_two_resources_and_each_has_its_source(self) -> None:
        self.assertEqual(set(PROVIDER_CALLS), self.actions())

    def test_the_project_user_can_never_publish_an_anonymous_policy_or_change_a_policy_other_than_removing_it(self) -> None:
        self.assertNotIn("s3:PutBucketPolicy", self.actions())
        self.assertNotIn("s3:PutBucketAcl", self.actions())
        self.assertNotIn("s3:PutBucketPublicAccessBlock", self.actions())
        self.assertIn("s3:DeleteBucketPolicy", self.actions(), "acl = private removes the bucket policy: the provider makes that call")

    def test_the_project_user_can_never_delete_a_version_write_an_object_or_change_an_object_lock(self) -> None:
        for action in self.actions():
            self.assertNotRegex(action, r"Object|Multipart", action)
        self.assertNotIn("s3:DeleteObjectVersion", self.actions())
        self.assertNotIn("s3:PutObject", self.actions())
        self.assertNotIn("s3:BypassGovernanceRetention", self.actions())

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


class ProviderSourceTest(unittest.TestCase):
    """The table above is the policy's source. These cases tie it to the Terraform root that it serves, so a new resource or argument needs a new row."""

    def files(self) -> dict[str, str]:
        return {path.name: path.read_text() for path in (ROOT / "infra" / "storage").glob("*.tf")}

    def test_the_root_declares_only_the_bucket_and_its_versioning_with_a_private_acl_and_no_forced_destroy(self) -> None:
        text = "\n".join(self.files().values())
        self.assertEqual(["minio_s3_bucket", "minio_s3_bucket_versioning"], sorted(re.findall(r'^resource "(\w+)"', text, re.MULTILINE)))
        self.assertRegex(text, r'acl\s*=\s*"private"')
        self.assertRegex(text, r"force_destroy\s*=\s*false")

    def test_the_root_sets_no_tag_lifecycle_policy_quota_or_object_lock_that_the_policy_would_need_a_write_action_for(self) -> None:
        text = re.sub(r"(?m)^\s*#.*$", "", "\n".join(self.files().values()))
        for argument in ("tags", "quota", "object_locking", "policy", "lifecycle"):
            self.assertNotRegex(text, rf"(?m)^\s*{argument}\s*=", argument)

    def test_the_provider_is_pinned_to_the_version_that_the_table_cites(self) -> None:
        self.assertRegex(self.files()["versions.tf"], r'version\s*=\s*"= 3\.46\.2"')
        self.assertIn("v3.46.2", MODULE.read_text())


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

    def test_the_user_add_script_gives_mc_the_access_key_alone_so_the_password_is_not_an_argument_of_the_container_process(self) -> None:
        script = bootstrap.SCRIPTS["user_add"]
        self.assertIn('admin user add {ALIAS} "$1"', script.replace(bootstrap.ALIAS, "{ALIAS}"))
        self.assertNotRegex(script, r"secret_value|read -r|\"\$2\"|\"\$\{?2")
        words = re.findall(r"admin user add \S+((?: \S+)*)", script)
        self.assertEqual(
            ['"$1"'], [word.strip() for word in words], "mc takes the secret key on standard input when it has the access key and no more arguments"
        )

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
        self.assertEqual({USER: {"status": "enabled", "policies": [POLICY], "groups": [], "password": PROJECT_VALUE}}, server.users)

    def test_a_second_run_changes_nothing_and_makes_no_write_call_so_an_existing_matching_user_is_never_created_again(self) -> None:
        server = made(Server())
        self.assertEqual([], run(server))
        self.assertEqual([], server.writes())

    def test_a_policy_that_drifted_is_written_again_and_the_user_stays(self) -> None:
        server = made(Server())
        server.policies[POLICY]["Statement"][0]["Resource"].append("arn:aws:s3:::*")
        self.assertEqual(["create the policy"], run(server))
        self.assertEqual(["policy_create"], server.writes())
        self.assertNotIn("arn:aws:s3:::*", json.dumps(server.policies[POLICY]))

    def test_a_user_without_the_policy_gets_it_attached_and_is_not_created_again(self) -> None:
        server = made(Server())
        server.users[USER]["policies"] = []
        self.assertEqual(["attach the policy to the user"], run(server))
        self.assertNotIn("user_add", server.writes())

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


class AbsenceTest(unittest.TestCase):
    """A user or a policy is made only when the server says in its own words that it does not exist. Any other answer fails and makes nothing."""

    def test_a_user_read_that_fails_for_another_reason_makes_no_user_and_no_write(self) -> None:
        for words in (("admin", "user", "info"),):
            with self.subTest(words=words):
                server = made(Server())
                server.unreadable = words
                with self.assertRaises(bootstrap.BootstrapError) as caught:
                    run(server)
                self.assertEqual([], server.writes())
                self.assertNotIn("connection refused", str(caught.exception))

    def test_a_policy_read_that_fails_for_another_reason_makes_no_policy_and_no_write(self) -> None:
        server = made(Server())
        server.unreadable = ("admin", "policy", "info")
        with self.assertRaises(bootstrap.BootstrapError):
            run(server)
        self.assertEqual([], server.writes())

    def test_an_answer_that_is_not_the_documented_form_makes_nothing(self) -> None:
        for broken in ("", "not json", "[]", "{}", json.dumps({"status": "success"}), json.dumps({"status": "success", "userStatus": "enabled"})):
            with self.subTest(broken=broken):
                server = made(Server())

                def answer(call: Any, server: Server = server, broken: str = broken) -> Any:
                    if call.kind == "read" and call.arguments[:3] == ("admin", "user", "info"):
                        return bootstrap.Result(0, broken)
                    return Server.__call__(server, call)

                with self.assertRaises(bootstrap.BootstrapError):
                    bootstrap.reconcile(answer, USER, POLICY, BUCKET, PROJECT_VALUE, check_only=False)
                self.assertEqual([], server.writes())

    def test_a_failure_whose_text_only_mentions_the_words_is_not_evidence_of_absence(self) -> None:
        server = made(Server())

        def answer(call: Any) -> Any:
            if call.kind == "read" and call.arguments[:3] == ("admin", "user", "info"):
                return bootstrap.Result(1, "the specified user does not exist; XMinioAdminNoSuchUser is only in this sentence, not in an answer")
            return Server.__call__(server, call)

        with self.assertRaises(bootstrap.BootstrapError):
            bootstrap.reconcile(answer, USER, POLICY, BUCKET, PROJECT_VALUE, check_only=False)
        self.assertEqual([], server.writes())

    def test_an_exit_status_of_zero_with_the_absence_code_is_not_absence(self) -> None:
        server = made(Server())

        def answer(call: Any) -> Any:
            if call.kind == "read" and call.arguments[:3] == ("admin", "policy", "info"):
                return bootstrap.Result(0, NO_SUCH_POLICY)
            return Server.__call__(server, call)

        with self.assertRaises(bootstrap.BootstrapError):
            bootstrap.reconcile(answer, USER, POLICY, BUCKET, PROJECT_VALUE, check_only=False)
        self.assertEqual([], server.writes())


class GrantsTest(unittest.TestCase):
    """The user holds exactly the project's policy and no group: a policy or a group that someone added is removed, and a disabled user is enabled last."""

    def test_a_user_that_holds_another_policy_loses_it_so_the_user_is_never_wider_than_the_project_policy(self) -> None:
        server = made(Server())
        server.users[USER]["policies"] = ["consoleAdmin", POLICY]
        self.assertEqual(["detach the policy consoleAdmin from the user"], run(server))
        self.assertEqual([POLICY], server.users[USER]["policies"])
        server.users[USER]["policies"] = ["consoleAdmin"]
        self.assertEqual(["detach the policy consoleAdmin from the user", "attach the policy to the user"], run(server))
        self.assertEqual([POLICY], server.users[USER]["policies"])

    def test_a_user_in_a_group_is_removed_from_it_because_the_group_policies_reach_the_user(self) -> None:
        server = made(Server())
        server.users[USER]["groups"] = ["admins", "analysts"]
        self.assertEqual(["remove the user from the group admins", "remove the user from the group analysts"], run(server))
        self.assertEqual([], server.users[USER]["groups"])
        self.assertEqual([], run(server))

    def test_a_disabled_user_is_enabled_last_after_each_policy_and_group_that_is_not_the_projects_is_gone(self) -> None:
        server = made(Server())
        server.users[USER].update(status="disabled", policies=["consoleAdmin"], groups=["admins"])
        actions = run(server)
        self.assertEqual(
            [
                "detach the policy consoleAdmin from the user",
                "remove the user from the group admins",
                "attach the policy to the user",
                "enable the user",
            ],
            actions,
        )
        self.assertEqual("enable the user", actions[-1])
        self.assertEqual(["admin policy detach", "admin group remove", "admin policy attach", "admin user enable"], server.order)
        self.assertEqual({"status": "enabled", "policies": [POLICY], "groups": [], "password": PROJECT_VALUE}, server.users[USER])

    def test_a_failed_detach_leaves_a_disabled_user_disabled_and_nothing_after_it_runs(self) -> None:
        server = made(Server())
        server.users[USER].update(status="disabled", policies=["consoleAdmin"], groups=["admins"])
        server.fail_words = ("admin", "policy", "detach")
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            run(server)
        self.assertEqual("disabled", server.users[USER]["status"])
        self.assertNotIn("admin user enable", server.order)
        self.assertNotIn("admin policy attach", server.order)
        self.assertIn("exit status 1", str(caught.exception))
        self.assertNotIn(ROOT_SECOND, str(caught.exception))

    def test_a_failed_group_removal_leaves_a_disabled_user_disabled(self) -> None:
        server = made(Server())
        server.users[USER].update(status="disabled", groups=["admins"])
        server.fail_words = ("admin", "group", "remove")
        with self.assertRaises(bootstrap.BootstrapError):
            run(server)
        self.assertEqual("disabled", server.users[USER]["status"])
        self.assertNotIn("admin user enable", server.order)

    def test_a_check_run_names_each_removal_and_the_enabling_and_writes_nothing(self) -> None:
        server = made(Server())
        server.users[USER].update(status="disabled", policies=["consoleAdmin"], groups=["admins"])
        actions = run(server, check_only=True)
        self.assertEqual("enable the user", actions[-1])
        self.assertEqual([], server.writes())

    def test_the_group_names_that_the_server_gives_are_checked_before_they_reach_a_command(self) -> None:
        server = made(Server())
        server.users[USER]["groups"] = ["bad group; rm"]
        with self.assertRaises(bootstrap.BootstrapError):
            run(server)
        self.assertEqual([], server.writes())


class UserInfoTest(unittest.TestCase):
    def info(self, **fields: Any) -> str:
        return json.dumps({"status": "success", "accessKey": USER, **fields})

    def test_the_status_the_policies_and_the_groups_are_read_from_the_json_answer(self) -> None:
        text = self.info(policyName=POLICY, userStatus="enabled", memberOf=[{"name": "g", "policies": ["readwrite"]}])
        self.assertEqual(("enabled", [POLICY], ["g"]), bootstrap.parse_user_info(text))
        self.assertEqual(("disabled", [], []), bootstrap.parse_user_info(self.info(userStatus="disabled")))
        self.assertEqual(("enabled", ["a", "b"], []), bootstrap.parse_user_info(self.info(userStatus="enabled", policyName="a,b")))

    def test_an_answer_that_is_not_user_information_is_refused(self) -> None:
        for text in ("something else\n", "", "[]", json.dumps({"status": "success"}), json.dumps({"status": "error", "userStatus": "enabled"})):
            with self.subTest(text=text), self.assertRaises(bootstrap.BootstrapError):
                bootstrap.parse_user_info(text)


if __name__ == "__main__":
    unittest.main()
