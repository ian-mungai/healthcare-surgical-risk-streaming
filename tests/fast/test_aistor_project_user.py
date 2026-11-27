"""The AIStor bootstrap uses the root pair once per host, and every later deploy checks the live state as a read-only audit identity.

This is the only use of the root pair: the module's root calls run ``mc`` inside the AIStor container, which reads the root files from its own secrets mount,
so the root pair is never an argument, never in the module's environment and never on the host's command line. The root step makes the project user, its
policy and the read-only audit identity, and only on the first bootstrap of a host (the marker file is absent) or for one deploy that the owner approved
(``repair``). After that a redeploy reads the live state as the audit identity and the project user, makes no call as root and no write, and fails with a
fixed message that names each difference. The cases hold what the bootstrap can get wrong: a policy wider than what the pinned Terraform provider calls, a
root value or a password on the host's command line, a user that is created again when it already matches, a user or a policy that is made because a read
failed and not because it is absent, a user that keeps a policy or a group that is not its own, a disabled user that is enabled before its extra access is
gone, a drifted live state that a redeploy accepts because of a saved digest, a refused sign-in or ``AccessDenied`` that sends the deploy to root, a write in
a check run and a failure message that carries output. The stand-in server answers the module's calls and keeps what the calls changed, so a second run
shows that nothing changes. The real ``mc`` calls are the host run's job.
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
from collections.abc import Callable
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
AUDIT_USER = "project_audit"
AUDIT_POLICY = "project_audit_read"
PROJECT_VALUE = "example-project-password-0123456789abcdef"
AUDIT_VALUE = "example-audit-password-0123456789abcdef"
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
# The audit identity's actions, each with the handler that needs it. The raw GetUserInfo request (``mc admin user info`` is not used: it also needs
# admin:GetGroup) gives the groups too; ``mc admin policy info`` calls InfoCannedPolicy. Sources: github.com/minio/pkg policy/admin-action.go (commit
# e87292ba) and github.com/minio/minio cmd/admin-handlers-users.go (commit 7aac2a2c). No other admin action and no S3 action is in the policy.
ADMIN_CALLS = {
    "admin:GetUser": "admin-action.go:104-105 GetUserAdminAction, required by GetUserInfo (admin-handlers-users.go:204) for the raw user-info request",
    "admin:GetPolicy": "admin-action.go:175-176 GetPolicyAdminAction, required by InfoCannedPolicy (admin-handlers-users.go:1558) for mc admin policy info",
}


def load_module() -> ModuleType:
    shared_module("openbao_client")
    shared_module("secret_files")
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
PUBLISH_AUDIT = "write the audit identity's sign-in to OpenBao"
RECORD = "record the first bootstrap on the host"
SET_VALUE = "set the project user's password to the one of the password file"
SET_AUDIT_VALUE = "set the audit user's password to the one of the password file"
FIRST_ACTIONS = [
    "create the policy",
    "create the user",
    "attach the policy to the user",
    "create the audit policy",
    "create the audit user",
    "attach the audit policy to the audit user",
    PUBLISH,
    PUBLISH_AUDIT,
    RECORD,
]
OTHER_VALUE = "example-older-password-0123456789abcdef"
LOST_VALUE = "example-regenerated-password-0123456789abcdef"
APPROVAL = "--aistor-repair"


def raw_error(code: str, status: int) -> Any:
    """The answer of a raw admin request that the server refused: its JSON error (``writeErrorResponseJSON``), a new line and the HTTP status."""
    return bootstrap.Result(0, json.dumps({"Code": code, "Message": "text", "RequestID": "0"}) + f"\n{status}")


def error_answer(code: str) -> Any:
    return bootstrap.Result(1, json.dumps({"status": "error", "error": {"cause": {"error": {"Code": code, "Message": "text"}}}}))


# The whole answer of ``mc ls --json`` for a bucket that does not exist, as the pinned AIStor client gave it on the host; only the bucket's name is masked.
LIVE_NO_BUCKET = (
    '{"status": "error", "error": {"message": "Unable to list bucket.", '
    '"cause": {"message": "Bucket `<BUCKET>` does not exist.", "error": {"Bucket": "<BUCKET>"}}, "type": "error"}}'
)
REMOVED = object()


def no_bucket_answer(bucket: str, status: int = 1) -> Any:
    return bootstrap.Result(status, LIVE_NO_BUCKET.replace("<BUCKET>", bucket) + "\n")


def changed_answer(*path: str, to: Any) -> Any:
    """The live answer for the project's bucket with one field set to another value, added or removed."""
    document = json.loads(no_bucket_answer(BUCKET).output)
    holder = document
    for key in path[:-1]:
        holder = holder[key]
    if to is REMOVED:
        del holder[path[-1]]
    else:
        holder[path[-1]] = to
    return bootstrap.Result(1, json.dumps(document) + "\n")


@dataclass
class Server:
    """A stand-in for the calls of the module: it keeps the policies, the users, each user's policies, groups, status and password, as AIStor would.

    A call of the kind ``audit`` is answered as AIStor answers an admin call of that user: an unknown or disabled user or a wrong password is an S3 error
    code, and a user whose policies do not allow the admin action is ``AccessDenied``."""

    policies: dict[str, dict[str, Any]] = field(default_factory=dict)
    users: dict[str, dict[str, Any]] = field(default_factory=dict)
    calls: list[Any] = field(default_factory=list)
    fail_on: str = ""
    fail_words: tuple[str, ...] = ()
    unreadable: tuple[str, ...] = ()
    order: list[str] = field(default_factory=list)
    bucket_exists: bool = True
    sign_in_answer: Any = None
    audit_answer: Any = None

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
        if kind == "audit":
            return self.audit(words[0], words[1:], call.stdin.rstrip("\n"))
        if kind == "audit_user":
            return self.raw_user(words[0], words[1], call.stdin.rstrip("\n"))
        if kind == "read" and words[:3] in (["admin", "policy", "info"], ["admin", "user", "info"]):
            return self.info(words)
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

    def info(self, words: list[str]) -> Any:
        """The answer of ``mc --json admin user info`` or ``admin policy info``."""
        if words[:3] == ["admin", "policy", "info"]:
            document = self.policies.get(words[4])
            if document is None:
                return bootstrap.Result(1, NO_SUCH_POLICY)
            return bootstrap.Result(0, json.dumps({"status": "success", "policy": words[4], "policyInfo": {"PolicyName": words[4], "Policy": document}}))
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
        return bootstrap.Result(0, "") if self.bucket_exists else no_bucket_answer(words[1])

    def allows(self, user: dict[str, Any], action: str) -> bool:
        """Whether one of the policies that the user holds allows the action, as AIStor decides an admin call."""
        for name in user["policies"]:
            for statement in self.policies.get(name, {}).get("Statement", []):
                if statement.get("Effect") == "Allow" and (action in statement.get("Action", []) or "admin:*" in statement.get("Action", [])):
                    return True
        return False

    def audit(self, name: str, words: list[str], password: str) -> Any:
        """The answer of an admin call that the audit identity makes: ``words`` are the words of the ``mc`` call."""
        if self.audit_answer is not None:
            return self.audit_answer
        user = self.users.get(name)
        if user is None or user["status"] != "enabled":
            return error_answer(AUTH_ERROR)
        if user["password"] != password:
            return error_answer("SignatureDoesNotMatch")
        needed = {"user": "admin:GetUser", "policy": "admin:GetPolicy"}[words[1]]
        if words[0] != "admin" or words[2] != "info" or not self.allows(user, needed):
            return error_answer("AccessDenied")
        if words[1] == "user" and self.users.get(words[4], {}).get("groups") and not self.allows(user, "admin:GetGroup"):
            # ``mc admin user info`` also fetches each group's description (GetGroupDescription, which needs admin:GetGroup) and stops with that error.
            return error_answer("AccessDenied")
        return self.info(words)

    def raw_user(self, name: str, target: str, password: str) -> Any:
        """The answer of the raw ``GetUserInfo`` request that the audit identity makes, as the host's curl prints it: the body, a new line and the HTTP status.

        The handler allows a user to read its own record without the action (``checkDenyOnly``) and needs ``admin:GetUser`` for any other. It gives the policy
        names, the status and the group names, and reads no group: ``admin:GetGroup`` is not needed."""
        forced = self.audit_answer
        if forced is not None:
            code = bootstrap.error_code(forced)
            return raw_error(code, 403) if code else bootstrap.Result(forced.status, forced.output)
        user = self.users.get(name)
        if user is None or user["status"] != "enabled":
            return raw_error(AUTH_ERROR, 403)
        if user["password"] != password:
            return raw_error("SignatureDoesNotMatch", 403)
        if target != name and not self.allows(user, "admin:GetUser"):
            return raw_error("AccessDenied", 403)
        found = self.users.get(target)
        if found is None:
            return raw_error("XMinioAdminNoSuchUser", 404)
        body: dict[str, Any] = {"policyName": ",".join(found["policies"]), "status": found["status"], "updatedAt": "2026-11-25T00:00:00Z"}
        if found["groups"]:
            body["memberOf"] = list(found["groups"])
        return bootstrap.Result(0, json.dumps(body) + "\n200")

    def root_calls(self) -> list[Any]:
        return [call for call in self.calls if call.kind not in ("sign_in", "audit", "audit_user")]

    def writes(self) -> list[str]:
        return [call.kind if call.kind != "read" else " ".join(call.arguments[:3]) for call in self.calls if self.is_write(call)]

    @staticmethod
    def is_write(call: Any) -> bool:
        written = {("admin", "policy", "attach"), ("admin", "policy", "detach"), ("admin", "user", "enable"), ("admin", "group", "remove")}
        return call.kind not in ("read", "sign_in", "audit", "audit_user") or tuple(call.arguments[:3]) in written


@dataclass
class Bao:
    """A stand-in for the sign-in that OpenBao keeps at one path: ``read`` gives None for a path that holds nothing."""

    stored: dict[str, Any] | None = None
    written: list[dict[str, Any]] = field(default_factory=list)
    unreadable: bool = False
    lose_writes: bool = False

    def read(self) -> dict[str, Any] | None:
        if self.unreadable:
            raise bootstrap.BootstrapError("OpenBao refused the read of the project user's sign-in (HTTP 503)")
        return None if self.stored is None else dict(self.stored)

    def write(self, user: str, password: str) -> None:
        self.written.append({"user": user, "password": password})
        if not self.lose_writes:
            self.stored = {"user": user, "password": password}


@dataclass
class FakeMarker:
    """A stand-in for the first-bootstrap marker on the host."""

    exists: bool = False
    writes: int = 0
    before_write: list[Callable[[], None]] = field(default_factory=list)

    def present(self) -> bool:
        return self.exists

    def write(self) -> None:
        for check in self.before_write:
            check()
        self.writes += 1
        self.exists = True


@dataclass
class World:
    """One host: the server, the two sign-ins that OpenBao keeps, the marker, and the run of the module against them."""

    server: Server = field(default_factory=Server)
    bao: Bao = field(default_factory=Bao)
    audit_bao: Bao = field(default_factory=Bao)
    marker: FakeMarker = field(default_factory=FakeMarker)

    def run(
        self,
        *,
        check_only: bool = False,
        repair: bool = False,
        password: str = PROJECT_VALUE,
        audit_password: str = AUDIT_VALUE,
        runner: Callable[[Any], Any] | None = None,
    ) -> list[str]:
        project = bootstrap.project_account(USER, POLICY, BUCKET, password, self.bao)
        audit = bootstrap.audit_account(AUDIT_USER, AUDIT_POLICY, audit_password, self.audit_bao)
        return list(bootstrap.reconcile(runner or self.server, self.marker, project, audit, BUCKET, check_only=check_only, repair=repair))

    def settle(self) -> World:
        """Forget the calls of earlier runs, so a case sees only what its own run does."""
        self.server.calls.clear()
        self.server.order.clear()
        self.bao.written.clear()
        self.audit_bao.written.clear()
        self.marker.writes = 0
        return self


def deployed() -> World:
    """A host after its first bootstrap: both accounts made, both sign-ins in OpenBao, the marker written."""
    world = World()
    world.run()
    if not (world.marker.exists and world.bao.stored and world.audit_bao.stored):
        raise AssertionError("the first bootstrap did not finish")
    return world.settle()


def kinds(server: Server) -> list[str]:
    return [call.kind if call.kind != "read" else " ".join(call.arguments[:3]) for call in server.calls]


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


class AuditPolicyTest(unittest.TestCase):
    """The audit identity may read a user and a policy and do nothing else: no S3 call, no write and no other admin action."""

    def document(self) -> dict[str, Any]:
        document: dict[str, Any] = bootstrap.audit_policy_document()
        return document

    def test_the_policy_holds_exactly_the_two_admin_reads_and_each_has_its_source(self) -> None:
        actions = {action for statement in self.document()["Statement"] for action in statement["Action"]}
        self.assertEqual(set(ADMIN_CALLS), actions)
        self.assertEqual(sorted(ADMIN_CALLS), sorted(bootstrap.AUDIT_ACTIONS))

    def test_the_policy_allows_no_s3_call_no_wildcard_no_write_action_and_names_no_resource(self) -> None:
        document = self.document()
        for statement in document["Statement"]:
            self.assertEqual("Allow", statement["Effect"])
            self.assertNotIn("Resource", statement, "an admin action has no resource, and a resource would add no S3 reach")
            for action in statement["Action"]:
                self.assertRegex(action, r"^admin:(Get|List)[A-Za-z]+$")
                self.assertNotIn("*", action)
        text = json.dumps(document)
        for forbidden in ("s3:", "kms:", "Create", "Delete", "Update", "Add", "Set", "Put", "Attach", "Detach", "Enable", "Disable"):
            self.assertNotIn(forbidden, text)

    def test_the_audit_policy_is_not_the_projects_policy_and_reaches_no_bucket(self) -> None:
        self.assertNotEqual(bootstrap.normalised(self.document()), bootstrap.normalised(bootstrap.policy_document(BUCKET)))

    def test_the_audit_identity_cannot_read_a_bucket_and_the_project_user_cannot_read_a_user_in_the_stand_in(self) -> None:
        world = deployed()
        self.assertFalse(world.server.allows(world.server.users[AUDIT_USER], "s3:ListBucket"))
        self.assertTrue(world.server.allows(world.server.users[AUDIT_USER], "admin:GetUser"))
        self.assertFalse(world.server.allows(world.server.users[USER], "admin:GetUser"))


class CommandLineTest(unittest.TestCase):
    """The host's command line holds the container's service and the script's fixed text and arguments, and no secret value."""

    def calls(self) -> list[Any]:
        world = World()
        world.run()
        return world.server.calls

    def test_root_enters_only_as_a_read_of_the_containers_own_secret_files_in_the_script_and_never_as_a_value(self) -> None:
        for call in self.calls():
            line = bootstrap.command_line(COMPOSE, "aistor", call)
            text = "\n".join(line)
            self.assertEqual([*COMPOSE, "exec", "-T", "aistor", "sh", "-c"], line[: len(COMPOSE) + 5])
            if call.kind in ("sign_in", "audit", "audit_user"):
                self.assertNotIn("/run/secrets/aistor_root", text, "the sign-in as the project user or the audit identity never reads the root pair")
                continue
            self.assertIn("/run/secrets/aistor_root_user", text)
            self.assertIn("/run/secrets/aistor_root_password", text)
            for value in (ROOT_FIRST, ROOT_SECOND):
                self.assertNotIn(value, text)

    def test_the_passwords_travel_on_standard_input_and_never_in_the_host_command_line(self) -> None:
        calls = self.calls()
        adds = [call for call in calls if call.kind == "user_add"]
        self.assertEqual([PROJECT_VALUE + "\n", AUDIT_VALUE + "\n"], [call.stdin for call in adds])
        for call in calls:
            for value in (PROJECT_VALUE, AUDIT_VALUE):
                self.assertNotIn(value, "\n".join(bootstrap.command_line(COMPOSE, "aistor", call)))

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

    def test_the_audit_script_gives_the_audit_identitys_pair_to_one_mc_process_as_its_environment_and_takes_the_words_of_the_call_after_the_user(self) -> None:
        script = bootstrap.SCRIPTS["audit"]
        self.assertIn("IFS= read -r secret_value", script)
        self.assertRegex(script, r'MC_HOST_aistor="http://\$user:\$secret_value@127\.0\.0\.1:9000" exec mc ')
        self.assertIn("shift", script)
        self.assertNotRegex(script, r'mc [^"]*\$secret_value')
        self.assertNotIn("aistor_root", script)
        self.assertNotIn("ROOT_SIGN_IN", script)

    def test_the_raw_user_script_reads_one_users_record_with_curl_and_takes_the_password_from_standard_input_into_curls_own_config_and_never_an_argument(
        self,
    ) -> None:
        script = bootstrap.SCRIPTS["audit_user"]
        self.assertIn("IFS= read -r secret_value", script)
        self.assertIn('printf \'user = "%s:%s"\\n\' "$1" "$secret_value" | curl', script, "the pair is the text of a config on curl's standard input")
        self.assertIn("--config -", script)
        self.assertIn("--aws-sigv4 aws:amz:us-east-1:s3", script)
        self.assertIn('"http://127.0.0.1:9000/minio/admin/v3/user-info?accessKey=$2"', script, "the admin call GetUserInfo, which reads no group")
        self.assertNotRegex(script, r"--user|-u |--data|\$secret_value[^\"]", "the password is in no argument of curl")
        self.assertNotIn("aistor_root", script)
        self.assertNotIn("ROOT_SIGN_IN", script)
        self.assertNotIn("--fail", script, "an answer of a refused call keeps its body, which holds the S3 code")

    def test_the_audit_call_holds_the_user_and_the_words_as_arguments_and_the_password_on_standard_input(self) -> None:
        world = deployed()
        world.run()
        first = world.server.calls[0]
        self.assertEqual(("audit_user", AUDIT_VALUE + "\n"), (first.kind, first.stdin))
        self.assertEqual((AUDIT_USER, AUDIT_USER), first.arguments)
        self.assertNotIn(AUDIT_VALUE, "\n".join(bootstrap.command_line(COMPOSE, "aistor", first)))
        policy_read = world.server.calls[1]
        self.assertEqual(("audit", AUDIT_VALUE + "\n"), (policy_read.kind, policy_read.stdin))
        self.assertEqual((AUDIT_USER, "admin", "policy", "info", "aistor", AUDIT_POLICY), policy_read.arguments)

    def test_the_sign_in_call_holds_the_user_and_the_bucket_as_arguments_and_the_password_on_standard_input(self) -> None:
        world = deployed()
        world.run()
        call = world.server.calls[-1]
        self.assertEqual(("sign_in", (USER, BUCKET), PROJECT_VALUE + "\n"), (call.kind, call.arguments, call.stdin))
        self.assertNotIn(PROJECT_VALUE, "\n".join(bootstrap.command_line(COMPOSE, "aistor", call)))

    def test_the_policy_text_travels_on_standard_input_and_not_in_the_command_line(self) -> None:
        creates = [call for call in self.calls() if call.kind == "policy_create"]
        self.assertEqual([bootstrap.policy_document(BUCKET), bootstrap.audit_policy_document()], [json.loads(call.stdin) for call in creates])
        for call in creates:
            self.assertNotIn("Statement", "\n".join(bootstrap.command_line(COMPOSE, "aistor", call)))


class FirstBootstrapTest(unittest.TestCase):
    """The first bootstrap of a host is the one run that uses root: it makes both accounts, keeps both sign-ins in OpenBao and ends with the marker."""

    def test_a_fresh_server_gets_both_policies_both_users_and_both_attachments_then_openbao_keeps_both_sign_ins_and_the_marker_is_written_last(self) -> None:
        world = World()
        world.marker.before_write.append(lambda: self.assertTrue(world.bao.written and world.audit_bao.written, "the marker follows both writes to OpenBao"))
        self.assertEqual(FIRST_ACTIONS, world.run())
        self.assertEqual(bootstrap.normalised(bootstrap.policy_document(BUCKET)), bootstrap.normalised(world.server.policies[POLICY]))
        self.assertEqual(bootstrap.normalised(bootstrap.audit_policy_document()), bootstrap.normalised(world.server.policies[AUDIT_POLICY]))
        self.assertEqual(
            {
                USER: {"status": "enabled", "policies": [POLICY], "groups": [], "password": PROJECT_VALUE},
                AUDIT_USER: {"status": "enabled", "policies": [AUDIT_POLICY], "groups": [], "password": AUDIT_VALUE},
            },
            world.server.users,
        )
        self.assertEqual([{"user": USER, "password": PROJECT_VALUE}], world.bao.written)
        self.assertEqual([{"user": AUDIT_USER, "password": AUDIT_VALUE}], world.audit_bao.written)
        self.assertEqual(1, world.marker.writes)

    def test_a_failure_before_the_end_leaves_no_marker_so_the_next_deploy_is_still_the_first_bootstrap(self) -> None:
        cases: list[tuple[str, Callable[[Any], object]]] = [
            ("a failed policy call", lambda world: setattr(world.server, "fail_on", "policy_create")),
            ("a failed user call", lambda world: setattr(world.server, "fail_on", "user_add")),
            ("a write to OpenBao that is not kept", lambda world: setattr(world.audit_bao, "lose_writes", True)),
            ("an OpenBao that cannot be read", lambda world: setattr(world.bao, "unreadable", True)),
        ]
        for label, change in cases:
            with self.subTest(label):
                world = World()
                change(world)
                with self.assertRaises(bootstrap.BootstrapError):
                    world.run()
                self.assertEqual(0, world.marker.writes)
                self.assertFalse(world.marker.exists)

    def test_a_read_of_openbao_that_fails_stops_the_first_bootstrap_before_any_call_to_aistor(self) -> None:
        for which in ("bao", "audit_bao"):
            with self.subTest(which):
                world = World()
                getattr(world, which).unreadable = True
                with self.assertRaises(bootstrap.BootstrapError):
                    world.run()
                self.assertEqual([], world.server.calls)

    def test_the_run_after_a_failed_one_finishes_and_a_run_after_that_uses_no_root(self) -> None:
        world = World()
        world.server.fail_on = "user_add"
        with self.assertRaises(bootstrap.BootstrapError):
            world.run()
        world.server.fail_on = ""
        world.run()
        self.assertTrue(world.marker.exists)
        world.settle()
        self.assertEqual([], world.run())
        self.assertEqual([], world.server.root_calls())

    def test_a_check_run_names_the_actions_and_writes_nothing(self) -> None:
        world = World()
        self.assertEqual(FIRST_ACTIONS, world.run(check_only=True))
        self.assertEqual([], world.server.writes())
        self.assertEqual({}, world.server.users)
        self.assertEqual(([], [], 0), (world.bao.written, world.audit_bao.written, world.marker.writes))

    def test_a_host_made_before_the_marker_existed_gets_the_marker_with_one_run_and_nothing_else_changes(self) -> None:
        world = deployed()
        world.marker.exists = False
        self.assertEqual([RECORD], world.run())
        self.assertEqual(1, world.marker.writes)
        self.assertEqual(([], []), (world.bao.written, world.audit_bao.written))
        self.assertEqual([], world.server.writes())

    def test_the_names_that_are_the_root_user_a_built_in_policy_or_shared_between_the_two_accounts_are_refused_before_any_call(self) -> None:
        cases = {
            "root user": (("minioadmin", POLICY, AUDIT_USER, AUDIT_POLICY)),
            "empty user": (("", POLICY, AUDIT_USER, AUDIT_POLICY)),
            "built-in project policy": ((USER, "consoleAdmin", AUDIT_USER, AUDIT_POLICY)),
            "root audit user": ((USER, POLICY, "root", AUDIT_POLICY)),
            "built-in audit policy": ((USER, POLICY, AUDIT_USER, "readwrite")),
            "same user": ((USER, POLICY, USER, AUDIT_POLICY)),
            "same policy": ((USER, POLICY, AUDIT_USER, POLICY)),
        }
        for label, (user, policy, audit_user, audit_policy) in cases.items():
            with self.subTest(label):
                server = Server()
                project = bootstrap.project_account(user, policy, BUCKET, PROJECT_VALUE, Bao())
                audit = bootstrap.audit_account(audit_user, audit_policy, AUDIT_VALUE, Bao())
                with self.assertRaises(bootstrap.BootstrapError):
                    bootstrap.reconcile(server, FakeMarker(), project, audit, BUCKET, check_only=False, repair=False)
                self.assertEqual([], server.calls)

    def test_a_failed_call_is_a_fixed_message_with_the_exit_status_and_no_output_text(self) -> None:
        for kind in ("policy_create", "user_add"):
            with self.subTest(kind=kind):
                world = World(server=Server(fail_on=kind))
                with self.assertRaises(bootstrap.BootstrapError) as caught:
                    world.run()
                for value in (ROOT_SECOND, PROJECT_VALUE, AUDIT_VALUE, "leaked"):
                    self.assertNotIn(value, str(caught.exception))
                self.assertIn("exit status 1", str(caught.exception))

    def test_a_password_of_another_form_is_refused_before_any_call(self) -> None:
        for password in ("", "short", "has a space-0123456789", "line\nbreak-0123456789"):
            for which in ("password", "audit_password"):
                with self.subTest(password=password, which=which):
                    world = World()
                    with self.assertRaises(bootstrap.BootstrapError):
                        world.run(password=password) if which == "password" else world.run(audit_password=password)
                    self.assertEqual([], world.server.calls)


class AbsenceTest(unittest.TestCase):
    """A user or a policy is made only when the server says in its own words that it does not exist. Any other answer fails and makes nothing."""

    def refused(self, world: World, answer: Any) -> None:
        with self.assertRaises(bootstrap.BootstrapError):
            world.run(repair=True, runner=answer)
        self.assertEqual([], world.server.writes())

    def test_a_user_read_that_fails_for_another_reason_makes_no_user_and_no_write(self) -> None:
        world = deployed()
        world.server.unreadable = ("admin", "user", "info")
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            world.run(repair=True)
        self.assertEqual([], world.server.writes())
        self.assertNotIn("connection refused", str(caught.exception))

    def test_a_policy_read_that_fails_for_another_reason_makes_no_policy_and_no_write(self) -> None:
        world = deployed()
        world.server.unreadable = ("admin", "policy", "info")
        with self.assertRaises(bootstrap.BootstrapError):
            world.run(repair=True)
        self.assertEqual([], world.server.writes())

    def test_an_answer_that_is_not_the_documented_form_makes_nothing(self) -> None:
        for broken in ("", "not json", "[]", "{}", json.dumps({"status": "success"}), json.dumps({"status": "success", "userStatus": "enabled"})):
            with self.subTest(broken=broken):
                world = deployed()

                def answer(call: Any, world: World = world, broken: str = broken) -> Any:
                    if call.kind == "read" and call.arguments[:3] == ("admin", "user", "info"):
                        return bootstrap.Result(0, broken)
                    return world.server(call)

                self.refused(world, answer)

    def test_a_failure_whose_text_only_mentions_the_words_is_not_evidence_of_absence(self) -> None:
        world = deployed()

        def answer(call: Any) -> Any:
            if call.kind == "read" and call.arguments[:3] == ("admin", "user", "info"):
                return bootstrap.Result(1, "the specified user does not exist; XMinioAdminNoSuchUser is only in this sentence, not in an answer")
            return world.server(call)

        self.refused(world, answer)

    def test_an_exit_status_of_zero_with_the_absence_code_is_not_absence(self) -> None:
        world = deployed()

        def answer(call: Any) -> Any:
            if call.kind == "read" and call.arguments[:3] == ("admin", "policy", "info"):
                return bootstrap.Result(0, NO_SUCH_POLICY)
            return world.server(call)

        self.refused(world, answer)


class GrantsTest(unittest.TestCase):
    """In the root step each account holds exactly its policy and no group: an added policy or group is removed, and a disabled user is enabled last."""

    def test_a_user_that_holds_another_policy_loses_it_so_the_user_is_never_wider_than_the_project_policy(self) -> None:
        world = deployed()
        world.server.users[USER]["policies"] = ["consoleAdmin", POLICY]
        self.assertEqual(["detach the policy consoleAdmin from the user"], world.run(repair=True))
        self.assertEqual([POLICY], world.server.users[USER]["policies"])
        world.server.users[USER]["policies"] = ["consoleAdmin"]
        self.assertEqual(["detach the policy consoleAdmin from the user", "attach the policy to the user"], world.run(repair=True))
        self.assertEqual([POLICY], world.server.users[USER]["policies"])

    def test_the_audit_user_loses_a_policy_that_is_not_its_own_in_the_same_way(self) -> None:
        world = deployed()
        world.server.users[AUDIT_USER]["policies"] = [AUDIT_POLICY, "consoleAdmin"]
        world.server.users[AUDIT_USER]["groups"] = ["admins"]
        self.assertEqual(["detach the policy consoleAdmin from the audit user", "remove the audit user from the group admins"], world.run(repair=True))
        self.assertEqual([AUDIT_POLICY], world.server.users[AUDIT_USER]["policies"])

    def test_a_user_in_a_group_is_removed_from_it_because_the_group_policies_reach_the_user(self) -> None:
        world = deployed()
        world.server.users[USER]["groups"] = ["admins", "analysts"]
        self.assertEqual(["remove the user from the group admins", "remove the user from the group analysts"], world.run(repair=True))
        self.assertEqual([], world.server.users[USER]["groups"])
        self.assertEqual([], world.run(repair=True))

    def test_a_disabled_user_is_enabled_last_after_each_policy_and_group_that_is_not_the_projects_is_gone(self) -> None:
        world = deployed()
        world.server.users[USER].update(status="disabled", policies=["consoleAdmin"], groups=["admins"])
        world.server.order.clear()
        actions = world.run(repair=True)
        self.assertEqual(
            [
                "detach the policy consoleAdmin from the user",
                "remove the user from the group admins",
                "attach the policy to the user",
                "enable the user",
            ],
            actions,
        )
        self.assertEqual(["admin policy detach", "admin group remove", "admin policy attach", "admin user enable"], world.server.order)
        self.assertEqual({"status": "enabled", "policies": [POLICY], "groups": [], "password": PROJECT_VALUE}, world.server.users[USER])

    def test_a_failed_detach_leaves_a_disabled_user_disabled_and_nothing_after_it_runs(self) -> None:
        world = deployed()
        world.server.users[USER].update(status="disabled", policies=["consoleAdmin"], groups=["admins"])
        world.server.fail_words = ("admin", "policy", "detach")
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            world.run(repair=True)
        self.assertEqual("disabled", world.server.users[USER]["status"])
        self.assertNotIn("admin user enable", world.server.order)
        self.assertNotIn("admin policy attach", world.server.order)
        self.assertIn("exit status 1", str(caught.exception))
        self.assertNotIn(ROOT_SECOND, str(caught.exception))

    def test_a_failed_group_removal_leaves_a_disabled_user_disabled(self) -> None:
        world = deployed()
        world.server.users[USER].update(status="disabled", groups=["admins"])
        world.server.fail_words = ("admin", "group", "remove")
        with self.assertRaises(bootstrap.BootstrapError):
            world.run(repair=True)
        self.assertEqual("disabled", world.server.users[USER]["status"])
        self.assertNotIn("admin user enable", world.server.order)

    def test_a_check_run_names_each_removal_and_the_enabling_and_writes_nothing(self) -> None:
        world = deployed()
        world.server.users[USER].update(status="disabled", policies=["consoleAdmin"], groups=["admins"])
        actions = world.run(repair=True, check_only=True)
        self.assertEqual("enable the user", actions[-1])
        self.assertEqual([], world.server.writes())

    def test_the_group_names_that_the_server_gives_are_checked_before_they_reach_a_command(self) -> None:
        world = deployed()
        world.server.users[USER]["groups"] = ["bad group; rm"]
        with self.assertRaises(bootstrap.BootstrapError):
            world.run(repair=True)
        self.assertEqual([], world.server.writes())

    def test_a_policy_that_drifted_is_written_again_and_the_user_stays_when_the_root_step_runs(self) -> None:
        world = deployed()
        world.server.policies[POLICY]["Statement"][0]["Resource"].append("arn:aws:s3:::*")
        world.server.users[USER]["policies"] = []
        self.assertEqual(["create the policy", "attach the policy to the user"], world.run(repair=True))
        self.assertEqual(["policy_create", "admin policy attach"], world.server.writes())
        self.assertNotIn("arn:aws:s3:::*", json.dumps(world.server.policies[POLICY]))

    def test_a_user_without_the_policy_gets_it_attached_and_is_not_created_again(self) -> None:
        world = deployed()
        world.server.users[USER]["policies"] = []
        self.assertEqual(["attach the policy to the user"], world.run(repair=True))
        self.assertNotIn("user_add", world.server.writes())


class LiveCheckTest(unittest.TestCase):
    """A redeploy reads the live state, not a saved digest: any difference fails with a fixed message that names it, with no change and no root call."""

    def differs(self, mutate: Callable[[Server], object], *phrases: str, check_only: bool = False) -> str:
        world = deployed()
        mutate(world.server)
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            world.run(check_only=check_only)
        message = str(caught.exception)
        for phrase in phrases:
            self.assertIn(phrase, message)
        self.assertIn(APPROVAL, message, "the message names the one approval that lets the root pair repair it")
        self.assertEqual([], world.server.root_calls(), "no call as root")
        self.assertEqual([], world.server.writes())
        self.assertTrue(set(kinds(world.server)) <= {"audit", "audit_user", "sign_in"})
        self.assertEqual(([], []), (world.bao.written, world.audit_bao.written))
        for value in (PROJECT_VALUE, AUDIT_VALUE, ROOT_SECOND):
            self.assertNotIn(value, message)
        return message

    def test_the_reviewers_probe_console_admin_attached_is_found_although_the_sign_in_works_and_the_policy_document_is_right(self) -> None:
        self.differs(lambda server: server.users[USER]["policies"].append("consoleAdmin"), "the project user holds the policy consoleAdmin")

    def test_the_reviewers_probe_a_group_added_is_found(self) -> None:
        self.differs(lambda server: server.users[USER]["groups"].append("admins"), "the project user is a member of the group admins")

    def test_the_reviewers_probe_the_group_description_lookup_of_mc_needs_admin_getgroup_so_the_check_reads_the_raw_user_answer_and_names_the_group(
        self,
    ) -> None:
        world = deployed()
        world.server.users[USER]["groups"].append("admins")
        mc_call = bootstrap.Call("audit", (AUDIT_USER, "admin", "user", "info", "aistor", USER), AUDIT_VALUE + "\n")
        self.assertEqual("AccessDenied", bootstrap.error_code(world.server(mc_call)), "the probe: mc stops at the group's description without admin:GetGroup")
        self.assertEqual(["admin:GetUser", "admin:GetPolicy"], bootstrap.AUDIT_ACTIONS, "the grant stays the two actions")
        world.server.calls.clear()
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            world.run()
        self.assertIn("the project user is a member of the group admins", str(caught.exception))
        self.assertNotIn("AccessDenied", str(caught.exception))
        through_mc = [call for call in world.server.calls if call.kind == "audit" and call.arguments[1:4] == ("admin", "user", "info")]
        self.assertEqual([], through_mc, "no call reads a user through mc")

    def test_the_raw_answer_is_turned_into_the_form_that_the_rest_of_the_check_reads_and_a_failed_call_keeps_no_output(self) -> None:
        body = json.dumps({"policyName": f"{POLICY},b", "status": "disabled", "memberOf": ["g1", "g2"], "updatedAt": "2026-11-25T00:00:00Z"})
        done = bootstrap.audit_user_info(lambda call: bootstrap.Result(0, body + "\n200"), AUDIT_USER, AUDIT_VALUE, USER)
        self.assertEqual((0, ("disabled", [POLICY, "b"], ["g1", "g2"])), (done.status, bootstrap.parse_user_info(done.output, USER)))
        bare = bootstrap.audit_user_info(lambda call: bootstrap.Result(0, json.dumps({"status": "enabled"}) + "\n200"), AUDIT_USER, AUDIT_VALUE, USER)
        self.assertEqual(("enabled", [], []), bootstrap.parse_user_info(bare.output, USER))
        for code in ("XMinioAdminNoSuchUser", "AccessDenied", "SignatureDoesNotMatch"):
            with self.subTest(code=code):
                answer = bootstrap.audit_user_info(lambda call, code=code: raw_error(code, 403), AUDIT_USER, AUDIT_VALUE, USER)
                self.assertEqual(code, bootstrap.error_code(answer))
        failed = bootstrap.audit_user_info(lambda call: bootstrap.Result(7, f"curl: (7) {NOT_AN_ANSWER} {AUDIT_VALUE}"), AUDIT_USER, AUDIT_VALUE, USER)
        self.assertEqual((7, ""), (failed.status, failed.output))
        for text in ("<html>gateway</html>\n502", "garbage\n200", "[]\n200", json.dumps({"status": 5}) + "\n200", json.dumps({"memberOf": "g"}) + "\n200"):
            with self.subTest(text=text), self.assertRaises(bootstrap.BootstrapError):
                answer = bootstrap.audit_user_info(lambda call, text=text: bootstrap.Result(0, text), AUDIT_USER, AUDIT_VALUE, USER)
                self.assertIsNone(bootstrap.error_code(answer))
                bootstrap.checked(answer, "read the user") if answer.status else bootstrap.parse_user_info(answer.output, USER)

    def test_the_reviewers_probe_a_removed_bucket_versioning_read_in_the_policy_is_found(self) -> None:
        def mutate(server: Server) -> None:
            server.policies[POLICY]["Statement"][0]["Action"].remove("s3:GetBucketVersioning")

        self.differs(mutate, "the project policy's document differs from the desired one")

    def test_the_reviewers_probe_a_renamed_policy_with_an_identical_document_is_found_because_the_user_must_hold_exactly_the_project_policy_by_name(
        self,
    ) -> None:
        def mutate(server: Server) -> None:
            server.policies["renamed_policy"] = json.loads(json.dumps(server.policies[POLICY]))
            server.users[USER]["policies"] = ["renamed_policy"]

        self.differs(mutate, "the project user holds the policy renamed_policy", "the project user does not hold the project policy")

    def test_a_widened_policy_a_missing_policy_a_missing_user_and_a_disabled_user_are_each_found(self) -> None:
        def widen(server: Server) -> None:
            server.policies[POLICY]["Statement"][0]["Resource"].append("arn:aws:s3:::*")

        self.differs(widen, "the project policy's document differs from the desired one")
        self.differs(lambda server: server.policies.pop(POLICY), "the project policy does not exist")
        self.differs(lambda server: server.users.pop(USER), "the project user does not exist")
        self.differs(lambda server: server.users[USER].update(status="disabled"), "the project user is not enabled")

    def test_the_audit_identity_is_held_to_the_same_form_so_a_widened_auditor_is_found(self) -> None:
        self.differs(lambda server: server.users[AUDIT_USER]["policies"].append("consoleAdmin"), "the audit user holds the policy consoleAdmin")
        self.differs(lambda server: server.users[AUDIT_USER]["groups"].append("admins"), "the audit user is a member of the group admins")

        def widen(server: Server) -> None:
            server.policies[AUDIT_POLICY]["Statement"][0]["Action"].append("admin:*")

        self.differs(widen, "the audit policy's document differs from the desired one")

    def test_every_difference_is_named_in_one_message(self) -> None:
        def mutate(server: Server) -> None:
            server.users[USER]["policies"].append("consoleAdmin")
            server.users[USER]["groups"].append("admins")
            server.policies[POLICY]["Statement"][0]["Action"].remove("s3:ListBucket")

        self.differs(
            mutate,
            "the project user holds the policy consoleAdmin",
            "the project user is a member of the group admins",
            "the project policy's document differs from the desired one",
        )

    def test_a_policy_name_of_another_form_is_not_repeated_in_the_message(self) -> None:
        def mutate(server: Server) -> None:
            server.users[USER]["policies"] = ["bad name; rm -rf", POLICY]

        message = self.differs(mutate, "the project user holds a policy whose name is of another form")
        self.assertNotIn("rm -rf", message)

    def test_a_check_run_finds_the_same_differences(self) -> None:
        self.differs(lambda server: server.users[USER]["policies"].append("consoleAdmin"), "consoleAdmin", check_only=True)

    def test_a_matching_redeploy_reads_four_answers_as_the_audit_identity_lists_the_bucket_as_the_project_user_and_makes_no_root_call_and_no_write(
        self,
    ) -> None:
        world = deployed()
        self.assertEqual([], world.run())
        self.assertEqual(["audit_user", "audit", "audit_user", "audit", "sign_in"], [call.kind for call in world.server.calls])
        self.assertEqual([], world.server.root_calls())
        self.assertEqual([], world.server.writes())
        self.assertEqual(([], [], 0), (world.bao.written, world.audit_bao.written, world.marker.writes))

    def test_the_sign_ins_are_the_ones_that_openbao_keeps_and_the_password_files_are_not_used(self) -> None:
        world = deployed()
        self.assertEqual([], world.run(password=LOST_VALUE, audit_password=OTHER_VALUE))
        self.assertEqual([AUDIT_VALUE + "\n"] * 4 + [PROJECT_VALUE + "\n"], [call.stdin for call in world.server.calls])

    def test_a_bucket_that_does_not_exist_yet_still_counts_as_a_sign_in_because_the_listing_was_allowed_and_found_no_bucket(self) -> None:
        world = deployed()
        world.server.bucket_exists = False
        self.assertEqual([], world.run())
        self.assertEqual([], world.server.root_calls())

    def test_a_missing_bucket_hides_no_refused_sign_in_and_no_user_without_a_policy(self) -> None:
        cases: list[tuple[str, Callable[[Any], object]]] = [
            ("another password", lambda server: server.users[USER].update(password=OTHER_VALUE)),
            ("no policy", lambda server: server.users[USER].update(policies=[])),
        ]
        for label, change in cases:
            with self.subTest(label):
                world = deployed()
                world.server.bucket_exists = False
                change(world.server)
                with self.assertRaises(bootstrap.BootstrapError):
                    world.run()
                self.assertEqual([], world.server.root_calls())

    def test_a_refused_sign_in_of_the_project_user_fails_closed_without_root(self) -> None:
        world = deployed()
        world.server.users[USER]["password"] = OTHER_VALUE
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            world.run()
        self.assertIn("the sign-in that OpenBao keeps for the project user is refused", str(caught.exception))
        self.assertIn(APPROVAL, str(caught.exception))
        self.assertEqual([], world.server.root_calls())
        self.assertEqual([], world.server.writes())
        self.assertEqual(([], []), (world.bao.written, world.audit_bao.written))

    def test_access_denied_of_an_offline_or_unlicensed_server_is_a_failure_and_never_a_reason_to_use_root(self) -> None:
        cases: list[tuple[str, Callable[[Any], object]]] = [
            ("the S3 sign-in", lambda server: setattr(server, "sign_in_answer", error_answer("AccessDenied"))),
            ("the audit reads", lambda server: setattr(server, "audit_answer", error_answer("AccessDenied"))),
        ]
        for label, change in cases:
            with self.subTest(label):
                world = deployed()
                change(world.server)
                with self.assertRaises(bootstrap.BootstrapError) as caught:
                    world.run()
                self.assertIn("AccessDenied", str(caught.exception))
                self.assertEqual([], world.server.root_calls())
                self.assertEqual([], world.server.writes())

    def test_a_sign_in_that_is_neither_a_success_nor_a_refusal_fails_without_root_and_without_the_output(self) -> None:
        for answer in (bootstrap.Result(1, NOT_AN_ANSWER), bootstrap.Result(1, ""), bootstrap.Result(1, "[]"), error_answer("InternalError")):
            for which in ("sign_in_answer", "audit_answer"):
                with self.subTest(answer=answer, which=which):
                    world = deployed()
                    setattr(world.server, which, answer)
                    with self.assertRaises(bootstrap.BootstrapError) as caught:
                        world.run()
                    self.assertEqual([], world.server.root_calls())
                    self.assertNotIn("connection refused", str(caught.exception))
                    self.assertNotIn(PROJECT_VALUE, str(caught.exception))

    def test_a_refused_audit_sign_in_fails_closed_and_says_which_identity(self) -> None:
        world = deployed()
        world.server.users[AUDIT_USER]["password"] = OTHER_VALUE
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            world.run()
        self.assertIn("the audit identity's sign-in that OpenBao keeps is refused", str(caught.exception))
        self.assertIn(APPROVAL, str(caught.exception))
        self.assertEqual([], world.server.root_calls())

    def test_a_sign_in_that_openbao_does_not_keep_fails_closed_before_any_call_to_aistor(self) -> None:
        cases: list[tuple[str, Callable[[Any], object]]] = [
            ("no project sign-in", lambda world: setattr(world.bao, "stored", None)),
            ("no audit sign-in", lambda world: setattr(world.audit_bao, "stored", None)),
            ("a sign-in of another user", lambda world: setattr(world.bao, "stored", {"user": "someone_else", "password": OTHER_VALUE})),
            ("a document of another form", lambda world: setattr(world.audit_bao, "stored", {"user": AUDIT_USER, "password": 5})),
        ]
        for label, change in cases:
            with self.subTest(label):
                world = deployed()
                change(world)
                with self.assertRaises(bootstrap.BootstrapError) as caught:
                    world.run()
                self.assertIn(APPROVAL, str(caught.exception))
                self.assertEqual([], world.server.calls)

    def test_a_read_of_openbao_that_fails_stops_before_any_call_to_aistor(self) -> None:
        for which in ("bao", "audit_bao"):
            with self.subTest(which):
                world = deployed()
                getattr(world, which).unreadable = True
                with self.assertRaises(bootstrap.BootstrapError):
                    world.run()
                self.assertEqual([], world.server.calls)

    def test_a_live_state_check_after_a_host_made_with_the_old_digest_needs_no_digest(self) -> None:
        world = deployed()
        world.bao.stored = {"user": USER, "password": PROJECT_VALUE, "policy_sha256": "0" * 64}
        self.assertEqual([], world.run())


class RootOnlyOnceTest(unittest.TestCase):
    """Root runs on the first bootstrap of a host and after that only for one deploy that the owner approved."""

    def test_the_run_after_the_first_bootstrap_makes_no_call_as_root_whatever_differs(self) -> None:
        cases: list[tuple[str, Callable[[Any], object]]] = [
            ("a user that is gone", lambda server: server.users.pop(USER)),
            ("a policy that is gone", lambda server: server.policies.pop(POLICY)),
            ("an extra policy", lambda server: server.users[USER]["policies"].append("consoleAdmin")),
            ("a changed password", lambda server: server.users[USER].update(password=OTHER_VALUE)),
        ]
        for label, mutate in cases:
            with self.subTest(label):
                world = deployed()
                mutate(world.server)
                with self.assertRaises(bootstrap.BootstrapError):
                    world.run()
                self.assertEqual([], world.server.root_calls())

    def test_the_approval_runs_the_root_step_once_and_the_run_after_it_matches_without_root(self) -> None:
        world = deployed()
        world.server.users[USER]["policies"] = ["consoleAdmin"]
        world.server.users[USER]["groups"] = ["admins"]
        world.server.policies[POLICY]["Statement"][0]["Action"].remove("s3:GetBucketVersioning")
        self.assertEqual(
            ["create the policy", "detach the policy consoleAdmin from the user", "remove the user from the group admins", "attach the policy to the user"],
            world.run(repair=True),
        )
        world.settle()
        self.assertEqual([], world.run())
        self.assertEqual([], world.server.root_calls())

    def test_the_approval_on_a_host_that_matches_changes_nothing_and_writes_no_marker(self) -> None:
        world = deployed()
        self.assertEqual([], world.run(repair=True))
        self.assertEqual([], world.server.writes())
        self.assertEqual(0, world.marker.writes)
        self.assertEqual(([], []), (world.bao.written, world.audit_bao.written))

    def test_a_user_that_is_gone_is_made_again_only_with_the_approval(self) -> None:
        world = deployed()
        del world.server.users[USER]
        self.assertEqual(["create the user", "attach the policy to the user"], world.run(repair=True))
        self.assertEqual(PROJECT_VALUE, world.server.users[USER]["password"])

    def test_a_sign_in_that_openbao_lost_is_written_again_only_with_the_approval_after_a_sign_in_with_the_files_password(self) -> None:
        world = deployed()
        world.bao.stored = None
        world.audit_bao.stored = None
        self.assertEqual([PUBLISH, PUBLISH_AUDIT], world.run(repair=True))
        self.assertEqual([{"user": USER, "password": PROJECT_VALUE}], world.bao.written)
        self.assertEqual([{"user": AUDIT_USER, "password": AUDIT_VALUE}], world.audit_bao.written)
        world.settle()
        self.assertEqual([], world.run())

    def test_access_denied_in_the_first_bootstrap_is_a_failure_and_leaves_no_marker(self) -> None:
        world = World()
        world.server.sign_in_answer = error_answer("AccessDenied")
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            world.run()
        self.assertIn("AccessDenied", str(caught.exception))
        self.assertFalse(world.marker.exists)
        self.assertEqual(([], []), (world.bao.written, world.audit_bao.written))


class MissingBucketTest(unittest.TestCase):
    """The first deploy of a host runs before ``make storage-up`` makes the bucket. The client's answer for that holds no S3 code."""

    def answered(self, result: Any) -> Any:
        return bootstrap.signed_in(lambda call: result, USER, BUCKET, PROJECT_VALUE)

    def refused_as_unknown(self, result: Any) -> None:
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            self.answered(result)
        message = str(caught.exception)
        for text in (BUCKET, PROJECT_VALUE, "does not exist", "leaked"):
            self.assertNotIn(text, message)

    def test_the_first_bootstrap_of_a_host_without_the_bucket_finishes_keeps_both_sign_ins_and_writes_the_marker(self) -> None:
        world = World()
        world.server.bucket_exists = False
        self.assertEqual(FIRST_ACTIONS, world.run())
        self.assertEqual((1, 1, 1), (len(world.bao.written), len(world.audit_bao.written), world.marker.writes))
        self.assertEqual([], world.run(), "the next deploy, still without the bucket, matches")

    def test_the_clients_answer_for_the_projects_missing_bucket_is_a_sign_in(self) -> None:
        self.assertIs(True, self.answered(no_bucket_answer(BUCKET)))

    def test_a_code_in_the_answer_decides_it_whatever_the_rest_says(self) -> None:
        def coded(code: Any) -> Any:
            return changed_answer("error", "cause", "error", "Code", to=code)

        for code in ("InvalidAccessKeyId", "SignatureDoesNotMatch"):
            with self.subTest(code=code):
                self.assertIs(False, self.answered(coded(code)))
        for other in ("AccessDenied", "InternalError", "", None, 404):
            with self.subTest(code=other):
                self.refused_as_unknown(coded(other))

    def test_any_other_form_that_speaks_of_a_missing_bucket_is_not_a_sign_in(self) -> None:
        sentence = f"Bucket `{BUCKET}` does not exist."
        good = no_bucket_answer(BUCKET).output
        documents: list[tuple[str, Any]] = [
            ("another bucket", no_bucket_answer("another-bucket")),
            ("a bucket whose name holds the project's", no_bucket_answer(BUCKET + "-copy")),
            ("another exit status", no_bucket_answer(BUCKET, status=2)),
            ("a killed call", no_bucket_answer(BUCKET, status=137)),
            ("a status of success", changed_answer("status", to="success")),
            ("no status", changed_answer("status", to=REMOVED)),
            ("a field beside the error", changed_answer("other", to="text")),
            ("the sentence in the message", changed_answer("error", "message", to="Unable to list bucket. " + sentence)),
            ("another message", changed_answer("error", "message", to="Unable to list folder.")),
            ("no message", changed_answer("error", "message", to=REMOVED)),
            ("another type", changed_answer("error", "type", to="fatal")),
            ("no type", changed_answer("error", "type", to=REMOVED)),
            ("a field in the error", changed_answer("error", "other", to="text")),
            ("no cause", changed_answer("error", "cause", to=REMOVED)),
            ("a cause that is null", changed_answer("error", "cause", to=None)),
            ("a cause that is text", changed_answer("error", "cause", to=sentence)),
            ("the cause's message of another bucket", changed_answer("error", "cause", "message", to="Bucket `another-bucket` does not exist.")),
            ("text after the cause's message", changed_answer("error", "cause", "message", to=sentence + " Access Denied.")),
            ("no cause's message", changed_answer("error", "cause", "message", to=REMOVED)),
            ("a field in the cause", changed_answer("error", "cause", "other", to="text")),
            ("no cause's error", changed_answer("error", "cause", "error", to=REMOVED)),
            ("a cause's error that is null", changed_answer("error", "cause", "error", to=None)),
            ("a cause's error that is text", changed_answer("error", "cause", "error", to="InvalidAccessKeyId")),
            ("an empty cause's error", changed_answer("error", "cause", "error", to={})),
            ("the cause's error of another bucket", changed_answer("error", "cause", "error", "Bucket", to="another-bucket")),
            ("a field in the cause's error", changed_answer("error", "cause", "error", "Key", to="text")),
            ("an error that is text", changed_answer("error", to=sentence)),
            ("the message alone", bootstrap.Result(1, json.dumps({"status": "error", "error": {"message": "Unable to list bucket. " + sentence}}))),
            ("a second line", bootstrap.Result(1, good + good)),
            ("a line before the answer", bootstrap.Result(1, "leaked\n" + good)),
            ("the words alone", bootstrap.Result(1, sentence)),
        ]
        for label, result in documents:
            with self.subTest(label):
                self.refused_as_unknown(result)

    def test_the_missing_bucket_answer_is_no_sign_in_of_the_audit_identity_and_no_answer_to_its_reads(self) -> None:
        with self.assertRaises(bootstrap.BootstrapError):
            bootstrap.audit_signed_in(lambda call: no_bucket_answer(BUCKET), AUDIT_USER, AUDIT_VALUE)
        with self.assertRaises(bootstrap.BootstrapError):
            bootstrap.checked(no_bucket_answer(BUCKET), "read the policy")


class SignInTest(unittest.TestCase):
    def test_each_answer_of_a_sign_in_is_a_success_a_refusal_or_a_failure_and_access_denied_is_a_failure(self) -> None:
        def answered(result: Any) -> Any:
            return bootstrap.signed_in(lambda call: result, USER, BUCKET, PROJECT_VALUE)

        self.assertIs(True, answered(bootstrap.Result(0, "")))
        self.assertIs(True, answered(error_answer("NoSuchBucket")))
        for code in ("InvalidAccessKeyId", "SignatureDoesNotMatch"):
            with self.subTest(code=code):
                self.assertIs(False, answered(error_answer(code)))
        for result in (error_answer("AccessDenied"), error_answer("InternalError"), bootstrap.Result(1, NOT_AN_ANSWER)):
            with self.subTest(result=result), self.assertRaises(bootstrap.BootstrapError):
                answered(result)

    def test_each_answer_of_an_audit_sign_in_is_the_same(self) -> None:
        def answered(result: Any) -> Any:
            return bootstrap.audit_signed_in(lambda call: result, AUDIT_USER, AUDIT_VALUE)

        self.assertIs(True, answered(bootstrap.Result(0, json.dumps({"status": "enabled", "policyName": AUDIT_POLICY}) + "\n200")))
        for code in ("InvalidAccessKeyId", "SignatureDoesNotMatch"):
            with self.subTest(code=code):
                self.assertIs(False, answered(raw_error(code, 403)))
        for result in (raw_error("AccessDenied", 403), raw_error("InternalError", 500), bootstrap.Result(1, NOT_AN_ANSWER)):
            with self.subTest(result=result), self.assertRaises(bootstrap.BootstrapError):
                answered(result)


class VerifyBeforePublishTest(unittest.TestCase):
    """OpenBao is written only with a password that has just signed in. A password that nothing has verified never replaces the one that works."""

    def test_a_new_password_file_never_replaces_the_working_password_in_openbao(self) -> None:
        world = deployed()
        self.assertEqual([], world.run(password=LOST_VALUE, repair=True))
        self.assertEqual(([], []), (world.bao.written, world.audit_bao.written))
        self.assertEqual(PROJECT_VALUE, world.bao.stored["password"] if world.bao.stored else None)
        self.assertNotIn("user_add", world.server.writes())

    def test_openbao_that_holds_nothing_is_given_the_files_password_after_it_signed_in_and_not_before(self) -> None:
        world = deployed()
        world.bao.stored = None
        world.audit_bao.stored = None
        world.marker.exists = False
        self.assertEqual([PUBLISH, PUBLISH_AUDIT, RECORD], world.run())
        self.assertEqual(
            {"sign_in", "audit_user"}, {kind for kind in kinds(world.server) if kind in ("sign_in", "audit", "audit_user")}, "the passwords signed in"
        )

    def test_a_sign_in_in_openbao_that_no_longer_works_is_replaced_by_the_files_password_when_that_one_signs_in(self) -> None:
        world = deployed()
        world.bao.stored = {"user": USER, "password": OTHER_VALUE}
        self.assertEqual([PUBLISH], world.run(repair=True))
        self.assertEqual(PROJECT_VALUE, world.bao.stored["password"] if world.bao.stored else None)

    def test_a_sign_in_of_another_user_in_openbao_is_not_used_and_is_replaced_after_a_verified_sign_in(self) -> None:
        world = deployed()
        world.bao.stored = {"user": "someone_else", "password": OTHER_VALUE}
        self.assertEqual([PUBLISH], world.run(repair=True))
        self.assertEqual(USER, world.bao.stored["user"] if world.bao.stored else None)
        self.assertNotIn(OTHER_VALUE, [call.stdin.rstrip("\n") for call in world.server.calls if call.stdin])

    def test_a_document_in_openbao_of_another_form_is_not_used_and_is_replaced_after_a_verified_sign_in(self) -> None:
        for stored in ({}, {"user": USER}, {"password": 5, "user": USER}, {"user": 7, "password": "x"}):
            with self.subTest(stored=stored):
                world = deployed()
                world.bao.stored = stored
                self.assertEqual([PUBLISH], world.run(repair=True))
                self.assertEqual(PROJECT_VALUE, world.bao.stored["password"] if world.bao.stored else None)

    def test_nothing_that_signs_in_with_a_user_that_exists_refuses_by_name_of_the_approval_and_changes_no_password_and_no_openbao_value(self) -> None:
        for which, user, kept in (("the project user", USER, "bao"), ("the audit user", AUDIT_USER, "audit_bao")):
            with self.subTest(which):
                world = deployed()
                world.marker.exists = False
                world.server.users[user]["password"] = OTHER_VALUE
                getattr(world, kept).stored = {"user": user, "password": LOST_VALUE}
                with self.assertRaises(bootstrap.BootstrapError) as caught:
                    world.run()
                message = str(caught.exception)
                self.assertIn(APPROVAL, message)
                for value in (PROJECT_VALUE, AUDIT_VALUE, OTHER_VALUE, LOST_VALUE):
                    self.assertNotIn(value, message)
                self.assertNotIn("user_add", world.server.writes())
                self.assertEqual([], getattr(world, kept).written)
                self.assertEqual(OTHER_VALUE, world.server.users[user]["password"])
                self.assertEqual(0, world.marker.writes)

    def test_the_approved_repair_sets_the_files_password_on_the_user_and_then_keeps_it_in_openbao_after_a_sign_in(self) -> None:
        for user, kept, action, value, publish in (
            (USER, "bao", SET_VALUE, PROJECT_VALUE, PUBLISH),
            (AUDIT_USER, "audit_bao", SET_AUDIT_VALUE, AUDIT_VALUE, PUBLISH_AUDIT),
        ):
            with self.subTest(user):
                world = deployed()
                world.server.users[user]["password"] = OTHER_VALUE
                getattr(world, kept).stored = {"user": user, "password": LOST_VALUE}
                self.assertEqual([action, publish], world.run(repair=True))
                self.assertEqual(value, world.server.users[user]["password"])
                self.assertEqual(value, getattr(world, kept).stored["password"])
                add = next(call for call in world.server.calls if call.kind == "user_add")
                self.assertEqual(value + "\n", add.stdin)

    def test_a_user_that_was_made_in_this_run_and_cannot_sign_in_fails_and_publishes_nothing(self) -> None:
        world = World()
        original = Server.sign_in

        def never(self: Server, words: list[str], password: str) -> Any:
            return error_answer("SignatureDoesNotMatch")

        Server.sign_in = never  # type: ignore[method-assign]
        try:
            with self.assertRaises(bootstrap.BootstrapError):
                world.run()
        finally:
            Server.sign_in = original  # type: ignore[method-assign]
        self.assertEqual(([], [], 0), (world.bao.written, world.audit_bao.written, world.marker.writes))

    def test_the_audit_user_that_was_made_in_this_run_and_cannot_sign_in_fails_and_leaves_no_marker(self) -> None:
        world = World()
        world.server.audit_answer = error_answer("SignatureDoesNotMatch")
        with self.assertRaises(bootstrap.BootstrapError):
            world.run()
        self.assertEqual(0, world.marker.writes)
        self.assertEqual([], world.audit_bao.written)

    def test_a_write_that_openbao_did_not_keep_is_found_by_the_read_back_and_fails(self) -> None:
        world = World()
        world.bao.lose_writes = True
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            world.run()
        self.assertNotIn(PROJECT_VALUE, str(caught.exception))

    def test_a_check_run_does_not_write_openbao_and_names_the_writes(self) -> None:
        world = deployed()
        world.bao.stored = None
        world.audit_bao.stored = None
        world.marker.exists = False
        self.assertEqual([PUBLISH, PUBLISH_AUDIT, RECORD], world.run(check_only=True))
        self.assertEqual(([], [], 0), (world.bao.written, world.audit_bao.written, world.marker.writes))
        self.assertEqual([], world.server.writes())


class MainWiringTest(unittest.TestCase):
    """``main`` gives ``reconcile`` the approval, the check mode and the marker exactly as the task's parameters name them, and the passwords from the files."""

    def run_main(self, *, repair: bool, check_mode: bool) -> dict[str, Any]:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        folder = Path(directory.name)
        (folder / "project").write_text(PROJECT_VALUE + "\n")
        (folder / "audit").write_text(AUDIT_VALUE + "\n")
        params = {
            "compose_command": COMPOSE,
            "service": "aistor",
            "user": USER,
            "policy": POLICY,
            "bucket": BUCKET,
            "password_file": str(folder / "project"),
            "audit_user": AUDIT_USER,
            "audit_policy": AUDIT_POLICY,
            "audit_password_file": str(folder / "audit"),
            "marker_file": str(folder / "marker"),
            "openbao_address": "http://127.0.0.1:8200",
            "openbao_role": "bootstrap",
            "openbao_secret_id_file": str(folder / "secret_id"),
            "openbao_path": "secret/data/services/aistor/aistor",
            "audit_openbao_path": "secret/data/services/aistor/audit",
            "repair": repair,
        }
        seen: dict[str, Any] = {}

        class FakeModule:
            def __init__(self, **_: Any) -> None:
                self.params = params
                self.check_mode = check_mode

            def exit_json(self, **fields: Any) -> None:
                seen["exit"] = fields

            def fail_json(self, **fields: Any) -> None:
                seen["fail"] = fields

        def fake_reconcile(runner: Any, marker: Any, project: Any, audit: Any, bucket: str, *, check_only: bool, repair: bool) -> list[str]:
            seen.update(marker=marker.path, project=project, audit=audit, bucket=bucket, check_only=check_only, repair=repair)
            return []

        original = (bootstrap.AnsibleModule, bootstrap.reconcile)
        bootstrap.AnsibleModule, bootstrap.reconcile = FakeModule, fake_reconcile
        try:
            bootstrap.main()
        finally:
            bootstrap.AnsibleModule, bootstrap.reconcile = original
        seen["folder"] = folder
        return seen

    def test_the_approval_and_the_check_mode_are_passed_as_they_are_given(self) -> None:
        for repair in (False, True):
            for check_mode in (False, True):
                with self.subTest(repair=repair, check_mode=check_mode):
                    seen = self.run_main(repair=repair, check_mode=check_mode)
                    self.assertEqual((repair, check_mode), (seen["repair"], seen["check_only"]))
                    self.assertNotIn("fail", seen)

    def test_each_account_gets_its_own_password_file_and_its_own_openbao_path_and_the_marker_is_the_named_file(self) -> None:
        seen = self.run_main(repair=False, check_mode=False)
        self.assertEqual(
            (USER, POLICY, PROJECT_VALUE, "secret/data/services/aistor/aistor"),
            (seen["project"].user, seen["project"].policy, seen["project"].password, seen["project"].store.path),
        )
        self.assertEqual(
            (AUDIT_USER, AUDIT_POLICY, AUDIT_VALUE, "secret/data/services/aistor/audit"),
            (seen["audit"].user, seen["audit"].policy, seen["audit"].password, seen["audit"].store.path),
        )
        self.assertEqual(str(seen["folder"] / "marker"), str(seen["marker"]))
        self.assertEqual(BUCKET, seen["bucket"])


class MarkerTest(unittest.TestCase):
    """The marker is a root-only plain file beside the AIStor state. It holds no secret, is written whole or not at all and is checked before it is trusted."""

    def folder(self) -> Path:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        return Path(directory.name)

    def test_an_absent_marker_is_absent_and_a_written_one_is_a_root_only_plain_file_without_a_secret(self) -> None:
        path = self.folder() / "aistor.bootstrap"
        marker = bootstrap.Marker(str(path))
        self.assertIs(False, marker.present())
        marker.write()
        self.assertIs(True, marker.present())
        status = path.lstat()
        self.assertEqual((stat.S_IFREG, 0o400, os.geteuid()), (stat.S_IFMT(status.st_mode), stat.S_IMODE(status.st_mode), status.st_uid))
        self.assertEqual("the first bootstrap of the AIStor project user and its audit identity finished on this host\n", path.read_text())

    def test_a_second_write_changes_nothing(self) -> None:
        path = self.folder() / "aistor.bootstrap"
        marker = bootstrap.Marker(str(path))
        marker.write()
        before = path.lstat()
        marker.write()
        after = path.lstat()
        self.assertEqual((before.st_ino, before.st_mtime_ns), (after.st_ino, after.st_mtime_ns))

    def test_a_marker_that_is_not_a_root_only_plain_file_is_refused_and_never_taken_for_absent(self) -> None:
        folder = self.folder()
        loose = folder / "loose"
        loose.write_text("x")
        loose.chmod(0o644)
        link = folder / "link"
        link.symlink_to(loose)
        directory = folder / "directory"
        directory.mkdir()
        for label, path in (("a mode of 0644", loose), ("a link", link), ("a folder", directory)):
            with self.subTest(label), self.assertRaises(bootstrap.BootstrapError):
                bootstrap.Marker(str(path)).present()

    def test_a_marker_of_another_owner_is_refused(self) -> None:
        path = self.folder() / "aistor.bootstrap"
        bootstrap.Marker(str(path)).write()
        original = os.geteuid
        os.geteuid = lambda: original() + 1
        try:
            with self.assertRaises(bootstrap.BootstrapError):
                bootstrap.Marker(str(path)).present()
        finally:
            os.geteuid = original

    def test_a_marker_that_cannot_be_looked_at_is_a_failure_and_not_absence(self) -> None:
        path = self.folder() / "missing_folder" / "aistor.bootstrap"
        self.assertIs(False, bootstrap.Marker(str(path)).present(), "a missing file is absent")
        with self.assertRaises(bootstrap.BootstrapError):
            bootstrap.Marker(str(path)).write()

    def test_the_sibling_that_an_interrupted_write_left_is_removed_at_the_next_write_and_a_failed_write_leaves_none(self) -> None:
        folder = self.folder()
        path = folder / "aistor.bootstrap"
        sibling = folder / ".aistor.bootstrap.0123abcd.new"
        sibling.write_text("left behind")
        sibling.chmod(0o400)
        bootstrap.Marker(str(path)).write()
        self.assertEqual(["aistor.bootstrap"], sorted(entry.name for entry in folder.iterdir()))
        path.unlink()
        original = os.link

        def failing(source: str, target: str) -> None:
            raise OSError(5, "input/output error")

        os.link = failing  # type: ignore[assignment]
        try:
            with self.assertRaises(bootstrap.BootstrapError):
                bootstrap.Marker(str(path)).write()
        finally:
            os.link = original
        self.assertEqual([], sorted(entry.name for entry in folder.iterdir()))


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
