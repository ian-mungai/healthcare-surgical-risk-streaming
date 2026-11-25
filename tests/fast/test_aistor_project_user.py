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

from tests.fast.ansible_paths import shared_module

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
    shared_module("openbao_client")
    spec = importlib.util.spec_from_file_location("aistor_project_user", MODULE)
    if spec is None or spec.loader is None:
        raise ImportError(str(MODULE))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


bootstrap: Any = load_module()


AUTH_ERROR = "InvalidAccessKeyId"
PUBLISH = "write the project user's sign-in to OpenBao"
OTHER_VALUE = "example-older-password-0123456789abcdef"
LOST_VALUE = "example-regenerated-password-0123456789abcdef"


def error_answer(code: str) -> Any:
    return bootstrap.Result(1, json.dumps({"status": "error", "error": {"cause": {"error": {"Code": code, "Message": "text"}}}}))


@dataclass
class Server:
    """A stand-in for the calls of the module: it keeps the policies, the users, each user's policies, groups, status and password, as AIStor would."""

    policies: dict[str, dict[str, Any]] = field(default_factory=dict)
    users: dict[str, dict[str, Any]] = field(default_factory=dict)
    calls: list[Any] = field(default_factory=list)
    fail_on: str = ""
    fail_words: tuple[str, ...] = ()
    unreadable: tuple[str, ...] = ()
    order: list[str] = field(default_factory=list)
    bucket_exists: bool = True
    sign_in_answer: Any = None

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
        if kind == "sign_in":
            return self.sign_in(words, call.stdin.rstrip("\n"))
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
            existing = self.users.get(words[0])
            if existing is not None:
                existing["password"] = call.stdin.rstrip("\n")
            else:
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

    def sign_in(self, words: list[str], password: str) -> Any:
        """The answer of a call made as the project user: the S3 error code that AIStor gives for each reason that a sign-in fails."""
        if self.sign_in_answer is not None:
            return self.sign_in_answer
        user = self.users.get(words[0])
        if user is None:
            return error_answer(AUTH_ERROR)
        if user["password"] != password:
            return error_answer("SignatureDoesNotMatch")
        if user["status"] != "enabled":
            return error_answer(AUTH_ERROR)
        if not user["policies"]:
            return error_answer("AccessDenied")
        return bootstrap.Result(0, "") if self.bucket_exists else error_answer("NoSuchBucket")

    def root_calls(self) -> list[Any]:
        return [call for call in self.calls if call.kind != "sign_in"]

    def writes(self) -> list[str]:
        return [call.kind if call.kind != "read" else " ".join(call.arguments[:3]) for call in self.calls if self.is_write(call)]

    @staticmethod
    def is_write(call: Any) -> bool:
        written = {("admin", "policy", "attach"), ("admin", "policy", "detach"), ("admin", "user", "enable"), ("admin", "group", "remove")}
        return call.kind not in ("read", "sign_in") or tuple(call.arguments[:3]) in written


@dataclass
class Bao:
    """A stand-in for the sign-in that OpenBao keeps at the service's own path: ``read`` gives None for a path that holds nothing."""

    stored: dict[str, Any] | None = None
    written: list[dict[str, Any]] = field(default_factory=list)
    unreadable: bool = False
    lose_writes: bool = False

    @classmethod
    def current(cls, password: str = PROJECT_VALUE) -> Bao:
        """OpenBao as a finished deploy leaves it: the project's sign-in and the digest of the policy that was applied."""
        return cls({"user": USER, "password": password, "policy_sha256": bootstrap.policy_digest(BUCKET)})

    def read(self) -> dict[str, Any] | None:
        if self.unreadable:
            raise bootstrap.BootstrapError("OpenBao refused the read of the project user's sign-in (HTTP 503)")
        return None if self.stored is None else dict(self.stored)

    def write(self, user: str, password: str, digest: str) -> None:
        self.written.append({"user": user, "password": password, "policy_sha256": digest})
        if not self.lose_writes:
            self.stored = {"user": user, "password": password, "policy_sha256": digest}


def run(server: Server, bao: Bao | None = None, *, check_only: bool = False, rotate: bool = False, password: str = PROJECT_VALUE) -> list[str]:
    """One run of the module. OpenBao holds the current sign-in unless a case gives another, so a case sees what a redeploy sees."""
    return list(bootstrap.reconcile(server, Bao.current() if bao is None else bao, USER, POLICY, BUCKET, password, check_only=check_only, rotate=rotate))


def made(server: Server) -> Server:
    """A server that a first run has made."""
    run(server)
    server.calls.clear()
    server.order.clear()
    return server


def stale() -> Bao:
    """OpenBao with a sign-in that works but with the digest of an older policy: the module must apply the policy again."""
    return Bao({"user": USER, "password": PROJECT_VALUE, "policy_sha256": "0" * 64})


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
        run(server, Bao())
        return server.calls

    def test_root_enters_only_as_a_read_of_the_containers_own_secret_files_in_the_script_and_never_as_a_value(self) -> None:
        for call in self.calls():
            line = bootstrap.command_line(COMPOSE, "aistor", call)
            text = "\n".join(line)
            self.assertEqual([*COMPOSE, "exec", "-T", "aistor", "sh", "-c"], line[: len(COMPOSE) + 5])
            if call.kind == "sign_in":
                self.assertNotIn("/run/secrets/aistor_root", text, "the sign-in as the project user never reads the root pair")
                continue
            self.assertIn("/run/secrets/aistor_root_user", text)
            self.assertIn("/run/secrets/aistor_root_password", text)
            for value in (ROOT_FIRST, ROOT_SECOND):
                self.assertNotIn(value, text)

    def test_the_project_password_travels_on_standard_input_and_never_in_the_host_command_line(self) -> None:
        server = Server()
        run(server, Bao())
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

    def test_the_sign_in_script_gives_the_project_users_pair_to_one_mc_process_as_its_environment_and_not_as_an_argument(self) -> None:
        script = bootstrap.SCRIPTS["sign_in"]
        self.assertIn("IFS= read -r secret_value", script)
        self.assertRegex(script, r'MC_HOST_aistor="http://\$1:\$secret_value@127\.0\.0\.1:9000" exec mc ')
        self.assertNotRegex(script, r'mc [^"]*\$secret_value')
        self.assertNotIn("aistor_root", script)
        self.assertIn('ls --json aistor/"$2"', script, "the call is a listing of the project's bucket, which the project's policy allows")

    def test_the_sign_in_call_holds_the_user_and_the_bucket_as_arguments_and_the_password_on_standard_input(self) -> None:
        server = Server()
        run(server)
        call = server.calls[0]
        self.assertEqual(("sign_in", (USER, BUCKET), PROJECT_VALUE + "\n"), (call.kind, call.arguments, call.stdin))
        self.assertNotIn(PROJECT_VALUE, "\n".join(bootstrap.command_line(COMPOSE, "aistor", call)))

    def test_the_policy_text_travels_on_standard_input_and_not_in_the_command_line(self) -> None:
        server = Server()
        run(server, Bao())
        create = next(call for call in server.calls if call.kind == "policy_create")
        self.assertEqual(bootstrap.policy_document(BUCKET), json.loads(create.stdin))
        self.assertNotIn("Statement", "\n".join(bootstrap.command_line(COMPOSE, "aistor", create)))


class ReconcileTest(unittest.TestCase):
    def test_a_fresh_server_gets_the_policy_the_user_and_the_attachment_in_that_order_and_then_keeps_the_sign_in_in_openbao(self) -> None:
        server, bao = Server(), Bao()
        actions = run(server, bao)
        self.assertEqual(["create the policy", "create the user", "attach the policy to the user", PUBLISH], actions)
        self.assertEqual(bootstrap.normalised(bootstrap.policy_document(BUCKET)), bootstrap.normalised(server.policies[POLICY]))
        self.assertEqual({USER: {"status": "enabled", "policies": [POLICY], "groups": [], "password": PROJECT_VALUE}}, server.users)
        self.assertEqual([{"user": USER, "password": PROJECT_VALUE, "policy_sha256": bootstrap.policy_digest(BUCKET)}], bao.written)

    def test_a_policy_that_drifted_is_written_again_and_the_user_stays_when_the_root_step_runs(self) -> None:
        server = made(Server())
        server.policies[POLICY]["Statement"][0]["Resource"].append("arn:aws:s3:::*")
        server.users[USER]["policies"] = []
        self.assertEqual(["create the policy", "attach the policy to the user"], run(server))
        self.assertEqual(["policy_create", "admin policy attach"], server.writes())
        self.assertNotIn("arn:aws:s3:::*", json.dumps(server.policies[POLICY]))

    def test_a_user_without_the_policy_gets_it_attached_and_is_not_created_again(self) -> None:
        server = made(Server())
        server.users[USER]["policies"] = []
        self.assertEqual(["attach the policy to the user"], run(server))
        self.assertNotIn("user_add", server.writes())

    def test_a_check_run_names_the_actions_and_writes_nothing(self) -> None:
        server, bao = Server(), Bao()
        self.assertEqual(["create the policy", "create the user", "attach the policy to the user", PUBLISH], run(server, bao, check_only=True))
        self.assertEqual([], server.writes())
        self.assertEqual({}, server.users)
        self.assertEqual([], bao.written)

    def test_the_user_is_not_the_root_user_name(self) -> None:
        for name in ("minioadmin", "root", ""):
            with self.subTest(name=name), self.assertRaises(bootstrap.BootstrapError):
                bootstrap.reconcile(Server(), Bao(), name, POLICY, BUCKET, PROJECT_VALUE, check_only=False)

    def test_the_policy_may_not_be_one_of_the_built_in_policies(self) -> None:
        for name in ("consoleAdmin", "readwrite", "readonly", "writeonly", "diagnostics"):
            with self.subTest(name=name), self.assertRaises(bootstrap.BootstrapError):
                bootstrap.reconcile(Server(), Bao(), USER, name, BUCKET, PROJECT_VALUE, check_only=False)

    def test_a_failed_call_is_a_fixed_message_with_the_exit_status_and_no_output_text(self) -> None:
        for kind in ("policy_create", "user_add"):
            with self.subTest(kind=kind):
                server = Server(fail_on=kind)
                with self.assertRaises(bootstrap.BootstrapError) as caught:
                    run(server, Bao())
                for value in (ROOT_SECOND, PROJECT_VALUE, "leaked"):
                    self.assertNotIn(value, str(caught.exception))
                self.assertIn("exit status 1", str(caught.exception))

    def test_a_password_of_another_form_is_refused_before_any_call(self) -> None:
        for password in ("", "short", "has a space-0123456789", "line\nbreak-0123456789"):
            with self.subTest(password=password):
                server = Server()
                with self.assertRaises(bootstrap.BootstrapError):
                    run(server, Bao(), password=password)
                self.assertEqual([], server.calls)


class AbsenceTest(unittest.TestCase):
    """A user or a policy is made only when the server says in its own words that it does not exist. Any other answer fails and makes nothing."""

    def refused(self, answer: Any, server: Server) -> None:
        with self.assertRaises(bootstrap.BootstrapError):
            bootstrap.reconcile(answer, stale(), USER, POLICY, BUCKET, PROJECT_VALUE, check_only=False)
        self.assertEqual([], server.writes())

    def test_a_user_read_that_fails_for_another_reason_makes_no_user_and_no_write(self) -> None:
        server = made(Server())
        server.unreadable = ("admin", "user", "info")
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            run(server, stale())
        self.assertEqual([], server.writes())
        self.assertNotIn("connection refused", str(caught.exception))

    def test_a_policy_read_that_fails_for_another_reason_makes_no_policy_and_no_write(self) -> None:
        server = made(Server())
        server.unreadable = ("admin", "policy", "info")
        with self.assertRaises(bootstrap.BootstrapError):
            run(server, stale())
        self.assertEqual([], server.writes())

    def test_an_answer_that_is_not_the_documented_form_makes_nothing(self) -> None:
        for broken in ("", "not json", "[]", "{}", json.dumps({"status": "success"}), json.dumps({"status": "success", "userStatus": "enabled"})):
            with self.subTest(broken=broken):
                server = made(Server())

                def answer(call: Any, server: Server = server, broken: str = broken) -> Any:
                    if call.kind == "read" and call.arguments[:3] == ("admin", "user", "info"):
                        return bootstrap.Result(0, broken)
                    return Server.__call__(server, call)

                self.refused(answer, server)

    def test_a_failure_whose_text_only_mentions_the_words_is_not_evidence_of_absence(self) -> None:
        server = made(Server())

        def answer(call: Any) -> Any:
            if call.kind == "read" and call.arguments[:3] == ("admin", "user", "info"):
                return bootstrap.Result(1, "the specified user does not exist; XMinioAdminNoSuchUser is only in this sentence, not in an answer")
            return Server.__call__(server, call)

        self.refused(answer, server)

    def test_an_exit_status_of_zero_with_the_absence_code_is_not_absence(self) -> None:
        server = made(Server())

        def answer(call: Any) -> Any:
            if call.kind == "read" and call.arguments[:3] == ("admin", "policy", "info"):
                return bootstrap.Result(0, NO_SUCH_POLICY)
            return Server.__call__(server, call)

        self.refused(answer, server)


class GrantsTest(unittest.TestCase):
    """The user holds exactly the project's policy and no group: a policy or a group that someone added is removed, and a disabled user is enabled last."""

    def test_a_user_that_holds_another_policy_loses_it_so_the_user_is_never_wider_than_the_project_policy(self) -> None:
        server = made(Server())
        server.users[USER]["policies"] = ["consoleAdmin", POLICY]
        self.assertEqual(["detach the policy consoleAdmin from the user", PUBLISH], run(server, stale()))
        self.assertEqual([POLICY], server.users[USER]["policies"])
        server.users[USER]["policies"] = ["consoleAdmin"]
        self.assertEqual(["detach the policy consoleAdmin from the user", "attach the policy to the user", PUBLISH], run(server, stale()))
        self.assertEqual([POLICY], server.users[USER]["policies"])

    def test_a_user_in_a_group_is_removed_from_it_because_the_group_policies_reach_the_user(self) -> None:
        server = made(Server())
        server.users[USER]["groups"] = ["admins", "analysts"]
        self.assertEqual(["remove the user from the group admins", "remove the user from the group analysts", PUBLISH], run(server, stale()))
        self.assertEqual([], server.users[USER]["groups"])
        self.assertEqual([PUBLISH], run(server, stale()))

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
            run(server, stale())
        self.assertEqual([], server.writes())


class MatchingRedeployTest(unittest.TestCase):
    """A redeploy that matches signs in as the project user with the pair that OpenBao keeps and makes no call as root and no write."""

    def test_a_matching_redeploy_makes_one_sign_in_and_no_root_authenticated_call_and_no_write(self) -> None:
        server, bao = made(Server()), Bao.current()
        self.assertEqual([], run(server, bao))
        self.assertEqual(["sign_in"], [call.kind for call in server.calls])
        self.assertEqual([], server.root_calls())
        self.assertEqual([], server.writes())
        self.assertEqual([], bao.written)

    def test_the_sign_in_uses_the_password_that_openbao_keeps_and_not_the_one_of_the_file(self) -> None:
        server = made(Server())
        server.users[USER]["password"] = OTHER_VALUE
        self.assertEqual([], run(server, Bao.current(OTHER_VALUE), password=LOST_VALUE))
        self.assertEqual([OTHER_VALUE + "\n"], [call.stdin for call in server.calls])
        self.assertEqual([], server.root_calls())

    def test_a_bucket_that_does_not_exist_yet_still_counts_as_a_sign_in_because_the_listing_was_allowed_and_found_no_bucket(self) -> None:
        server = made(Server())
        server.bucket_exists = False
        self.assertEqual([], run(server))
        self.assertEqual([], server.root_calls())

    def test_an_answer_that_is_not_a_refusal_fails_and_does_not_send_the_deploy_to_root(self) -> None:
        for answer in (
            bootstrap.Result(1, "mc: <ERROR> connection refused"),
            bootstrap.Result(1, ""),
            bootstrap.Result(1, "[]"),
            error_answer("InternalError"),
        ):
            with self.subTest(answer=answer):
                server = made(Server())
                server.sign_in_answer = answer
                with self.assertRaises(bootstrap.BootstrapError) as caught:
                    run(server)
                self.assertEqual([], server.root_calls())
                self.assertNotIn(PROJECT_VALUE, str(caught.exception))
                self.assertNotIn("connection refused", str(caught.exception))

    def test_each_answer_of_a_sign_in_is_a_success_a_refusal_or_a_failure_and_only_a_refusal_sends_the_deploy_to_the_root_step(self) -> None:
        def answered(result: Any) -> Any:
            return bootstrap.signed_in(lambda call: result, USER, BUCKET, PROJECT_VALUE)

        self.assertIs(True, answered(bootstrap.Result(0, "")))
        self.assertIs(True, answered(error_answer("NoSuchBucket")))
        for code in ("InvalidAccessKeyId", "SignatureDoesNotMatch", "AccessDenied"):
            with self.subTest(code=code):
                self.assertIs(False, answered(error_answer(code)))

    def test_a_disabled_user_a_user_without_the_policy_and_a_user_that_is_gone_each_go_to_the_root_step(self) -> None:
        server = made(Server())
        server.users[USER]["status"] = "disabled"
        self.assertEqual(["enable the user"], run(server))
        server = made(Server())
        server.users[USER]["policies"] = []
        self.assertEqual(["attach the policy to the user"], run(server))
        server = made(Server())
        del server.users[USER]
        self.assertEqual(["create the user", "attach the policy to the user"], run(server))

    def test_a_policy_that_changed_since_the_last_deploy_is_applied_again_even_though_the_sign_in_works(self) -> None:
        server = made(Server())
        bao = stale()
        self.assertEqual([PUBLISH], run(server, bao))
        self.assertIn(("admin", "policy", "info"), [tuple(call.arguments[:3]) for call in server.calls if call.kind == "read"], "the root step ran")
        self.assertEqual(bootstrap.policy_digest(BUCKET), bao.stored["policy_sha256"] if bao.stored else None)

    def test_the_digest_is_of_the_policy_for_the_bucket_so_another_bucket_gets_a_new_policy(self) -> None:
        self.assertEqual(bootstrap.policy_digest(BUCKET), bootstrap.policy_digest(BUCKET))
        self.assertNotEqual(bootstrap.policy_digest(BUCKET), bootstrap.policy_digest("another-project"))
        self.assertRegex(bootstrap.policy_digest(BUCKET), r"^[0-9a-f]{64}$")


class VerifyBeforePublishTest(unittest.TestCase):
    """OpenBao is written only with a password that has just signed in. A password that nothing has verified never replaces the one that works."""

    def test_a_new_password_file_never_replaces_the_working_password_in_openbao(self) -> None:
        server = made(Server())
        bao = Bao.current()
        self.assertEqual([], run(server, bao, password=LOST_VALUE))
        self.assertEqual([], bao.written)
        self.assertEqual(PROJECT_VALUE, bao.stored["password"] if bao.stored else None)
        self.assertEqual([], server.root_calls())
        self.assertNotIn("user_add", server.writes())

    def test_openbao_that_holds_nothing_is_given_the_files_password_after_it_signed_in_and_not_before(self) -> None:
        server, bao = made(Server()), Bao()
        self.assertEqual(["sign_in", "admin policy info", "admin user info"], self.kinds(server, bao))
        self.assertEqual([PUBLISH], run(made(Server()), Bao()))

    def kinds(self, server: Server, bao: Bao) -> list[str]:
        run(server, bao)
        return [call.kind if call.kind != "read" else " ".join(call.arguments[:3]) for call in server.calls]

    def test_a_sign_in_in_openbao_that_no_longer_works_is_replaced_by_the_files_password_when_that_one_signs_in(self) -> None:
        server = made(Server())
        bao = Bao.current(OTHER_VALUE)
        self.assertEqual([PUBLISH], run(server, bao))
        self.assertEqual(PROJECT_VALUE, bao.stored["password"] if bao.stored else None)

    def test_a_sign_in_of_another_user_in_openbao_is_not_used_and_is_replaced_after_a_verified_sign_in(self) -> None:
        server = made(Server())
        bao = Bao({"user": "someone_else", "password": OTHER_VALUE, "policy_sha256": bootstrap.policy_digest(BUCKET)})
        self.assertEqual([PUBLISH], run(server, bao))
        self.assertEqual(USER, bao.stored["user"] if bao.stored else None)
        self.assertNotIn(OTHER_VALUE, [call.stdin.rstrip("\n") for call in server.calls if call.stdin])

    def test_a_document_in_openbao_of_another_form_is_not_used_and_is_replaced_after_a_verified_sign_in(self) -> None:
        for stored in ({}, {"user": USER}, {"password": 5, "user": USER}, {"user": 7, "password": "x"}):
            with self.subTest(stored=stored):
                bao = Bao(stored)
                self.assertEqual([PUBLISH], run(made(Server()), bao))
                self.assertEqual(PROJECT_VALUE, bao.stored["password"] if bao.stored else None)

    def test_nothing_that_signs_in_with_a_user_that_exists_refuses_by_name_of_the_approval_and_changes_no_password_and_no_openbao_value(self) -> None:
        server = made(Server())
        server.users[USER]["password"] = OTHER_VALUE
        bao = Bao.current(LOST_VALUE)
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            run(server, bao)
        message = str(caught.exception)
        self.assertIn("compose_stack_aistor_rotate_password", message)
        for value in (PROJECT_VALUE, OTHER_VALUE, LOST_VALUE):
            self.assertNotIn(value, message)
        self.assertNotIn("user_add", server.writes())
        self.assertEqual([], bao.written)
        self.assertEqual(OTHER_VALUE, server.users[USER]["password"])

    def test_the_approved_rotation_sets_the_files_password_on_the_user_and_then_keeps_it_in_openbao_after_a_sign_in(self) -> None:
        server = made(Server())
        server.users[USER]["password"] = OTHER_VALUE
        bao = Bao.current(LOST_VALUE)
        actions = run(server, bao, rotate=True)
        self.assertEqual(["set the project user's password to the one of the password file", PUBLISH], actions)
        self.assertEqual(PROJECT_VALUE, server.users[USER]["password"])
        self.assertEqual(PROJECT_VALUE, bao.stored["password"] if bao.stored else None)
        add = next(call for call in server.calls if call.kind == "user_add")
        self.assertEqual(PROJECT_VALUE + "\n", add.stdin)

    def test_the_approval_changes_nothing_when_a_password_already_signs_in(self) -> None:
        server = made(Server())
        self.assertEqual([], run(server, rotate=True))
        self.assertEqual([], server.root_calls())

    def test_a_user_that_was_made_in_this_run_and_cannot_sign_in_fails_and_publishes_nothing(self) -> None:
        server = Server()
        bao = Bao()
        original = Server.sign_in

        def never(self: Server, words: list[str], password: str) -> Any:
            return error_answer("SignatureDoesNotMatch")

        Server.sign_in = never  # type: ignore[method-assign]
        try:
            with self.assertRaises(bootstrap.BootstrapError):
                run(server, bao)
        finally:
            Server.sign_in = original  # type: ignore[method-assign]
        self.assertEqual([], bao.written)

    def test_a_read_of_openbao_that_fails_stops_before_any_call_to_aistor(self) -> None:
        server = made(Server())
        with self.assertRaises(bootstrap.BootstrapError):
            run(server, Bao(unreadable=True))
        self.assertEqual([], server.calls)

    def test_a_write_that_openbao_did_not_keep_is_found_by_the_read_back_and_fails(self) -> None:
        server = made(Server())
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            run(server, Bao(lose_writes=True))
        self.assertNotIn(PROJECT_VALUE, str(caught.exception))

    def test_a_check_run_does_not_write_openbao_and_names_the_write(self) -> None:
        server, bao = made(Server()), Bao()
        self.assertEqual([PUBLISH], run(server, bao, check_only=True))
        self.assertEqual([], bao.written)
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
