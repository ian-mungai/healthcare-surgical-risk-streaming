"""Ansible module: the AIStor project user and its policy, made once with the root pair, and checked live at every later deploy without it.

THE ONLY USE OF THE ROOT PAIR, AND ONLY ONCE PER HOST. The owner decided that root credentials are kept in a file, are secured on the disk and are not used
for the project's work. This module's root calls make the project user, the project policy and the attachment of one to the other, and the read-only audit
identity of the next paragraph. They run only on the FIRST bootstrap of a host, which ends when a root-only marker file is written on the host
(``marker_file``), and again only for one run that the owner approved with ``python -m scripts.infra.host up --aistor-repair``.
The launcher gives that approval as the extra
variable ``aistor_repair_once`` of that one run, which is the module's ``repair``, and no inventory file holds it. Everything
else (Terraform, the storage command, the checks, the workloads) signs in as the project user, or through STS, and never as root. After the marker exists, a
refused sign-in, a missing OpenBao entry or any difference in the live state fails the deploy with a fixed message that names what differs, and changes
nothing. A refusal whose S3 code is ``AccessDenied`` (which an offline or unlicensed server also answers) is a failure and never a reason to use root.

THE LIVE CHECK. A redeploy trusts no saved digest. It reads the live state as a second identity, the audit identity: an AIStor user whose one policy allows
the two admin actions ``admin:GetUser`` (the raw ``GetUserInfo`` request of the next paragraph) and ``admin:GetPolicy`` (``mc admin policy info``) and no S3
call (``GetUserAdminAction`` and ``GetPolicyAdminAction`` of github.com/minio/pkg ``policy/admin-action.go``, commit e87292ba, lines 104-105 and 175-176; the
handlers ``GetUserInfo`` and ``InfoCannedPolicy`` of github.com/minio/minio ``cmd/admin-handlers-users.go``, commit 7aac2a2c, lines 204 and 1558, require
exactly them). Its sign-in is in OpenBao at its own path, which only the deploy's admin role reads. Terraform's identity stays the bucket-only project user.
The check holds that the project user is enabled, holds exactly the project policy by name and no other, is in no group, and that the project policy's
document equals the desired one; it then lists the project's bucket as the project user with the sign-in that OpenBao keeps. The audit identity is held to the
same exact form.

THE RAW USER READ. ``mc admin user info`` also fetches each group's description (``GetGroupDescription``, which needs ``admin:GetGroup``;
``cmd/admin-user-info.go`` lines 67-91 of github.com/minio/mc), so a user in an added group made the check fail with a bare ``AccessDenied`` that named no
group. The check sends the one admin request ``GET /minio/admin/v3/user-info?accessKey=<user>`` itself, with the AIStor container's own curl (its health
check in compose/services/aistor.yml uses it) (``--aws-sigv4``, the same signature that ``mc`` makes), and reads the policy names, the status and the group
names of the JSON answer (``GetUserInfo`` of ``cmd/iam-store.go``, lines 1658-1669 of github.com/minio/minio, written with ``madmin.UserInfo``), so the two
admin actions are enough and an added group is named. The password reaches curl as the text of a config on its standard input, never as an argument. The
answer is turned into the form of ``mc --json admin user info``, so one reader serves both, and a failed call is reported by its exit status alone.

OpenBao is written with a sign-in only after that pair has signed in, so a password file that was lost and made again never replaces the working value. When
the user exists and no password signs in, the module changes no password and writes nothing: it refuses, unless the owner approved the repair for one deploy.

How the pair is kept out of the host's processes: the module runs ``mc`` inside the AIStor container (the image ships it) through ``docker compose exec``. A
fixed script there reads the two root files from the container's own secrets mount (``/run/secrets/aistor_root_user`` and ``..._password``, the files that the
server reads through ``MINIO_ROOT_USER_FILE`` and ``MINIO_ROOT_PASSWORD_FILE``) and gives the pair to the one ``mc`` process as its ``MC_HOST_aistor``
variable, which is the only way ``mc`` takes a sign-in without writing it to a configuration file or reading it from its own command line. So the pair is
never a module argument, never in Ansible's or the host shell's environment and never in a host command line; it exists for the length of one ``mc`` call, in
that call's environment inside the container.

The project user's password reaches ``mc`` on standard input and never as an argument: the pinned client (``mc.RELEASE.2026-09-19T15-24-59Z``, SHA-256
``83938eded435e7c28c397ece1dc590b2a79626dd9354aae2107060fec559ae2b``, in the image ``RELEASE.2026-09-19T17-05-25Z``, whose layer holds the help text of the
form) takes ``mc admin user add ALIAS ACCESSKEY`` and then reads the secret key from standard input when that is not a terminal (``fetchUserKeys`` in
``cmd/admin-user-add.go``: two arguments and no terminal read one line). ``docker compose exec -T`` gives no terminal. So the password is in no process list.
No secret is printed, logged or part of a message: a failed call is reported by its fixed name and exit status.

The project policy holds exactly the S3 calls that the pinned Terraform provider (aminueza/minio v3.46.2) makes for the two resources of ``infra/storage`` and
nothing else; each action has its source file and line in ``tests/fast/test_aistor_project_user.py`` (``PROVIDER_CALLS``). It publishes no policy (the
bucket's ``acl = "private"`` only removes one), writes no object and deletes no version. Each account holds its policy, no other policy and no group: in the
root step a policy or a group that someone added is removed before a disabled user is enabled, and a failed removal leaves the user disabled. A user or a
policy is made only when the server answers in its own code that it does not exist (``XMinioAdminNoSuchUser`` or ``XMinioAdminNoSuchPolicy`` with a failed
exit status); any other answer fails. A check run reports the root step's actions without making any call that writes. It uses only the standard library and
runs on the host's Python.
"""

from __future__ import annotations

import json
import os
import re
import stat
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.openbao_client import OpenBaoClient, OpenBaoError
from ansible.module_utils.secret_files import SecretFileError, write_private_file

ALIAS = "aistor"
BUCKET_NAME = re.compile(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]")
ACCOUNT_NAME = re.compile(r"[A-Za-z][A-Za-z0-9_.-]{2,63}")
VALUE_FORM = re.compile(r"[A-Za-z0-9_-]{16,128}")
RESERVED_USERS = {"minioadmin", "root", "admin"}
BUILT_IN_POLICIES = {"consoleAdmin", "readwrite", "readonly", "writeonly", "diagnostics"}
ROOT_PAIR_FILES = ("/run/secrets/aistor_root_user", "/run/secrets/aistor_root_password")
# The one place that gives the root pair to a process: the environment of one ``mc`` call, read inside the container from its own mounted files.
ROOT_SIGN_IN = f'MC_HOST_{ALIAS}="http://$(cat {ROOT_PAIR_FILES[0]}):$(cat {ROOT_PAIR_FILES[1]})@127.0.0.1:9000"'
GROUP_NAME = re.compile(r"[A-Za-z0-9_.-]{1,64}")
NO_SUCH_USER = "XMinioAdminNoSuchUser"
NO_SUCH_POLICY = "XMinioAdminNoSuchPolicy"
# The S3 codes of a call that was refused for who made it (no such user, not the password, not enabled). ``AccessDenied`` is not among them: an offline or
# unlicensed server answers it too, and a user without its policy is found by the live check, so it is a failure and never a verdict on the sign-in. The code of
# an unknown access key is written in two parts: a scan keeps the field name of an exported credential out of every deployment file.
SIGN_IN_REFUSALS = {"Invalid" + "Access" + "KeyId", "SignatureDoesNotMatch"}
SIGNED_IN_CODES = {"NoSuchBucket"}
EMPTY_BODY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
APPROVAL = "--aistor-repair on the one run of python -m scripts.infra.host up"
PUBLISH = "write the project user's sign-in to OpenBao"
PUBLISH_AUDIT = "write the audit identity's sign-in to OpenBao"
RECORD = "record the first bootstrap on the host"
SCRIPTS = {
    "read": f'{ROOT_SIGN_IN} exec mc --no-color --json "$@" 2>&1',
    "user_add": f'{ROOT_SIGN_IN} exec mc --no-color admin user add {ALIAS} "$1"',
    # The one call as the project user. Its pair is the environment of the one mc process inside the container: the user is an argument of the script, the
    # password comes on standard input, and neither is an argument of mc. The root files are not read here.
    "sign_in": f'IFS= read -r secret_value && MC_HOST_{ALIAS}="http://$1:$secret_value@127.0.0.1:9000" exec mc --no-color ls --json {ALIAS}/"$2" 2>&1',
    # The calls as the audit identity: its user is the first argument, the rest are the words of one ``mc`` call, and its password comes on standard input.
    "audit": (
        f'IFS= read -r secret_value && user=$1 && shift && MC_HOST_{ALIAS}="http://$user:$secret_value@127.0.0.1:9000" exec mc --no-color --json "$@" 2>&1'
    ),
    # The raw ``GetUserInfo`` request as the audit identity (``mc admin user info`` also fetches each group's description, which needs ``admin:GetGroup``). Its
    # user is the first argument and the account to read the second. Its password comes on standard input and goes into the config that curl reads from standard
    # input, so it is no argument of curl. The answer is the body, a new line and the HTTP status.
    "audit_user": (
        'IFS= read -r secret_value && printf \'user = "%s:%s"\\n\' "$1" "$secret_value" | curl --silent --show-error --max-time 20 --config - '
        f"--aws-sigv4 aws:amz:us-east-1:s3 --header 'x-amz-content-sha256: {EMPTY_BODY_SHA256}' --write-out '\\n%{{http_code}}' "
        '"http://127.0.0.1:9000/minio/admin/v3/user-info?accessKey=$2" 2>&1'
    ),
    "policy_create": (
        f'file=$(mktemp) && cat > "$file" && {ROOT_SIGN_IN} mc --no-color admin policy create {ALIAS} "$1" "$file"; status=$?; rm -f "$file"; exit $status'
    ),
}
# Every S3 call of the pinned provider for the two resources (bucket and versioning), with private ACL and no forced destroy, on the project's one bucket and on
# no object. The source of each action (file and line of aminueza/terraform-provider-minio v3.46.2) is the table PROVIDER_CALLS of the unit test, which a test
# holds equal to this list: a new action needs a new row, a new resource needs a new test.
BUCKET_ACTIONS = [
    "s3:CreateBucket",
    "s3:DeleteBucket",
    "s3:ListBucket",
    "s3:ListBucketVersions",
    "s3:GetBucketLocation",
    "s3:GetBucketVersioning",
    "s3:PutBucketVersioning",
    "s3:GetBucketPolicy",
    "s3:DeleteBucketPolicy",
    "s3:GetBucketTagging",
]
# The audit identity's actions: the raw ``GetUserInfo`` request (which also gives the group names) and ``mc admin policy info``. The sources are in
# the module's docstring and in the table ADMIN_CALLS of the unit test, which a test holds equal to this list.
AUDIT_ACTIONS = ["admin:GetUser", "admin:GetPolicy"]
MARKER_TEXT = "the first bootstrap of the AIStor project user and its audit identity finished on this host\n"


class BootstrapError(RuntimeError):
    """A name, a value or a call is not usable. The message names the call and its exit status and holds no output, no password and no root value."""


@dataclass(frozen=True)
class Call:
    """One ``mc`` call in the container: which fixed script runs, its arguments and the text on its standard input."""

    kind: str
    arguments: tuple[str, ...]
    stdin: str | None = None


@dataclass(frozen=True)
class Result:
    status: int
    output: str


Runner = Callable[[Call], Result]


@dataclass(frozen=True)
class Account:
    """One AIStor account that the module keeps: its user, its one policy and the policy's document, the value of its password file and the OpenBao entry that
    keeps its sign-in (an object with ``read`` and ``write(user, password)``). ``name`` is ``project`` or ``audit`` and words every action and message."""

    name: str
    user: str
    policy: str
    document: dict[str, Any]
    password: str
    store: Any

    @property
    def user_noun(self) -> str:
        return "user" if self.name == "project" else f"{self.name} user"

    @property
    def policy_noun(self) -> str:
        return "policy" if self.name == "project" else f"{self.name} policy"

    @property
    def label(self) -> str:
        return f"the {self.name} user" if self.name == "project" else "the audit user"

    @property
    def policy_label(self) -> str:
        return f"the {self.name} policy"

    @property
    def publish(self) -> str:
        return PUBLISH if self.name == "project" else PUBLISH_AUDIT

    @property
    def set_value(self) -> str:
        return f"set the {self.name} user's password to the one of the password file"


def policy_document(bucket: str) -> dict[str, Any]:
    """The project's policy: the provider's bucket actions on the one bucket, and nothing on its objects."""
    if BUCKET_NAME.fullmatch(bucket) is None:
        raise BootstrapError("the bucket name is not a bucket name: 3 to 63 lower case letters, digits and hyphens")
    return {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": list(BUCKET_ACTIONS), "Resource": [f"arn:aws:s3:::{bucket}"]}],
    }


def audit_policy_document() -> dict[str, Any]:
    """The audit identity's policy: the two admin reads and no S3 call. An admin action takes no resource."""
    return {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": list(AUDIT_ACTIONS)}]}


def project_account(user: str, policy: str, bucket: str, password: str, store: Any) -> Account:
    return Account("project", user, policy, policy_document(bucket), password, store)


def audit_account(user: str, policy: str, password: str, store: Any) -> Account:
    return Account("audit", user, policy, audit_policy_document(), password, store)


def normalised(document: Any) -> str:
    """A policy as text that does not depend on the order of its lists: AIStor may return a list in another order."""

    def ordered(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: ordered(item) for key, item in sorted(value.items())}
        if isinstance(value, list):
            return sorted((ordered(item) for item in value), key=lambda item: json.dumps(item, sort_keys=True))
        return value

    return json.dumps(ordered(document), sort_keys=True)


def command_line(compose: list[str], service: str, call: Call) -> list[str]:
    """The host's command line of a call. It holds the service, the script's fixed text and the call's arguments, and no secret value."""
    return [*compose, "exec", "-T", service, "sh", "-c", SCRIPTS[call.kind], "mc", *call.arguments]


def answer_of(text: str) -> dict[str, Any]:
    """The JSON object of one ``mc --json`` answer, or an error that holds no part of the text."""
    try:
        document = json.loads(text)
    except ValueError:
        raise BootstrapError("the answer of the server is not the documented JSON form") from None
    if not isinstance(document, dict):
        raise BootstrapError("the answer of the server is not the documented JSON form")
    return document


def parse_user_info(text: str, user: str | None = None) -> tuple[str, list[str], list[str]]:
    """The status, the policies and the groups of a user from the JSON of ``mc --json admin user info``. Any other form is refused and nothing is made."""
    document = answer_of(text)
    status, name, held, groups = document.get("userStatus"), document.get("accessKey"), document.get("policyName", ""), document.get("memberOf", [])
    if document.get("status") != "success" or not isinstance(status, str) or not status or not isinstance(name, str) or not name:
        raise BootstrapError("the answer for the user is not user information")
    if (user is not None and name != user) or not isinstance(held, str) or not isinstance(groups, list):
        raise BootstrapError("the answer for the user is not user information")
    names = [item.get("name") if isinstance(item, dict) else None for item in groups]
    if not all(isinstance(group, str) and GROUP_NAME.fullmatch(group) for group in names):
        raise BootstrapError("the answer for the user holds a group name of another form")
    return status, [policy.strip() for policy in held.split(",") if policy.strip()], [str(group) for group in names]


def error_code(result: Result) -> str | None:
    """The code of the JSON error in a failed answer (``error.cause.error.Code``), or None when the call did not fail or the answer holds no such code."""
    if result.status == 0:
        return None
    try:
        found = answer_of(result.output)
    except BootstrapError:
        return None
    cause = ((found.get("error") or {}).get("cause") or {}).get("error") or {}
    code = cause.get("Code") if isinstance(cause, dict) else None
    return code if found.get("status") == "error" and isinstance(code, str) else None


def is_absent(result: Result, code: str) -> bool:
    """Whether the answer is the server's own statement that the thing does not exist: a failed call whose JSON error holds exactly this code."""
    return error_code(result) == code


def verdict(result: Result, who: str) -> bool:
    """Whether a call as a non-root account was allowed. A refusal of who made it is False. ``AccessDenied`` and every other failure raise: the deploy
    never goes to root for them."""
    code = error_code(result)
    if result.status == 0 or code in SIGNED_IN_CODES:
        return True
    if code in SIGN_IN_REFUSALS:
        return False
    if code == "AccessDenied":
        raise BootstrapError(
            f"the call as {who} was answered with AccessDenied, which an offline or unlicensed server also gives: the deploy does not use the root pair for it"
        )
    raise BootstrapError(f"the call as {who} failed with exit status {result.status} and an answer that is not a refusal")


def signed_in(runner: Runner, user: str, bucket: str, password: str) -> bool:
    """Whether the project user signs in with this password: a listing of the project's bucket as that user."""
    return verdict(runner(Call("sign_in", (user, bucket), password + "\n")), "the project user")


def audit_read(runner: Runner, user: str, password: str, *words: str) -> Result:
    return runner(Call("audit", (user, *words), password + "\n"))


def audit_user_info(runner: Runner, user: str, password: str, name: str) -> Result:
    """One user's information as the audit identity, from the raw ``GetUserInfo`` answer, in the form of ``mc --json admin user info``.

    The admin call needs ``admin:GetUser`` and nothing else (cmd/iam-store.go ``GetUserInfo`` of github.com/minio/minio gives the policy names, the status and
    the group names), where ``mc admin user info`` also reads each group's description and so needs ``admin:GetGroup`` (cmd/admin-user-info.go of
    github.com/minio/mc). A failed call is reported by its exit status alone: curl's own text is never kept."""
    result = runner(Call("audit_user", (user, name), password + "\n"))
    if result.status != 0:
        return Result(result.status, "")
    body, _, code = result.output.rstrip("\n").rpartition("\n")
    if code != "200":
        try:
            found = answer_of(body).get("Code")
        except BootstrapError:
            found = None
        failure = {"status": "error", "error": {"cause": {"error": {"Code": found if isinstance(found, str) else None}}}}
        return Result(1, json.dumps(failure))
    document = answer_of(body)
    names = document.get("memberOf") or []
    if not isinstance(names, list) or not isinstance(document.get("policyName", ""), str) or not isinstance(document.get("status"), str):
        raise BootstrapError("the answer for the user is not user information")
    shaped = {"status": "success", "accessKey": name, "policyName": document.get("policyName", ""), "userStatus": document["status"]}
    return Result(0, json.dumps(shaped | {"memberOf": [{"name": group} for group in names]}))


def audit_signed_in(runner: Runner, user: str, password: str) -> bool:
    """Whether the audit identity signs in with this password: it reads its own user information, which its policy allows."""
    return verdict(audit_user_info(runner, user, password, user), "the audit identity")


def read(runner: Runner, *words: str) -> Result:
    return runner(Call("read", tuple(words)))


def write(runner: Runner, call: Call, what: str) -> None:
    result = runner(call)
    if result.status != 0:
        raise BootstrapError(f"the call to {what} failed with exit status {result.status}")


def policy_matches(result: Result, document: dict[str, Any]) -> bool | None:
    """Whether the answer of a policy read holds the document: None when the server says the policy does not exist, and an error for any other failed or
    unreadable answer."""
    if is_absent(result, NO_SUCH_POLICY):
        return None
    if result.status != 0:
        raise BootstrapError(f"the call to read the policy failed with exit status {result.status}")
    found = answer_of(result.output)
    info = found.get("policyInfo")
    stored = info.get("Policy") if isinstance(info, dict) else None
    if found.get("status") != "success" or not isinstance(stored, dict):
        raise BootstrapError("the answer for the policy is not policy information")
    return normalised(stored) == normalised(document)


def reconcile_root(runner: Runner, account: Account, *, check_only: bool) -> tuple[list[str], bool]:
    """The root step for one account: make its policy, user and attachment match with root's calls, and return what changed (or would) and whether it is new.

    The password is given to a user that is explicitly absent and to no other user."""
    actions: list[str] = []
    user, policy, noun_user, noun_policy = account.user, account.policy, account.user_noun, account.policy_noun
    if policy_matches(read(runner, "admin", "policy", "info", ALIAS, policy), account.document) is not True:
        actions.append(f"create the {noun_policy}")
        if not check_only:
            write(runner, Call("policy_create", (policy,), json.dumps(account.document)), "create the policy")
    answer = read(runner, "admin", "user", "info", ALIAS, user)
    created = is_absent(answer, NO_SUCH_USER)
    if created:
        actions.append(f"create the {noun_user}")
        if not check_only:
            write(runner, Call("user_add", (user,), account.password + "\n"), "create the user")
        status, held, groups = "enabled", list[str](), list[str]()
    elif answer.status != 0:
        raise BootstrapError(f"the call to read the user failed with exit status {answer.status}")
    else:
        status, held, groups = parse_user_info(answer.output, user)
    # Access that is not the account's goes first, its policy next and the enabling last: a user that is disabled is never enabled with extra access.
    for other in held:
        if other != policy:
            actions.append(f"detach the policy {other} from the {noun_user}")
            if not check_only:
                write(runner, Call("read", ("admin", "policy", "detach", ALIAS, other, "--user", user)), "detach a policy")
    for group in groups:
        actions.append(f"remove the {noun_user} from the group {group}")
        if not check_only:
            write(runner, Call("read", ("admin", "group", "remove", ALIAS, group, user)), "remove the user from a group")
    if policy not in held:
        actions.append(f"attach the {noun_policy} to the {noun_user}")
        if not check_only:
            write(runner, Call("read", ("admin", "policy", "attach", ALIAS, policy, "--user", user)), "attach the policy")
    if status != "enabled":
        actions.append(f"enable the {noun_user}")
        if not check_only:
            write(runner, Call("read", ("admin", "user", "enable", ALIAS, user)), "enable the user")
    return actions, created


def kept_sign_in(stored: dict[str, Any] | None, user: str) -> str | None:
    """The password in the sign-in that OpenBao keeps, when that is the account's and has the form of a password. Any other document is not used."""
    password = stored.get("password") if isinstance(stored, dict) and stored.get("user") == user else None
    return password if isinstance(password, str) and VALUE_FORM.fullmatch(password) else None


def refusal(account: Account) -> BootstrapError:
    return BootstrapError(
        f"{account.label} exists, but neither the sign-in that OpenBao keeps nor the password file signs in: no password is changed and nothing is written to "
        f"OpenBao. To set the password file's value on the user, the owner approves it with {APPROVAL}"
    )


def settle(
    runner: Runner,
    account: Account,
    stored: dict[str, Any] | None,
    probe: Callable[[str], bool],
    *,
    created: bool,
    changed: bool,
    check_only: bool,
    repair: bool,
) -> list[str]:
    """Find the password of one account that signs in and keep it in OpenBao, and return the actions (a write to OpenBao, or the setting of the password).

    OpenBao's sign-in is tried first and the password file's next. OpenBao is written with the pair only after that pair has signed in, so a new password
    file never replaces a working value. When nothing signs in, an account that was made now is an error; one that existed is left as it is and the run
    refuses, unless the owner approved the repair."""
    kept = kept_sign_in(stored, account.user)
    candidates = ([("openbao", kept)] if kept else []) + ([("file", account.password)] if account.password != kept else [])
    if check_only and (created or changed):
        # A sign-in cannot be tried before the root step's changes are made.
        return [account.publish] if created or kept is None else []
    verified = next(((source, password) for source, password in candidates if probe(password)), None)
    actions: list[str] = []
    if verified is None:
        if created:
            raise BootstrapError(f"{account.label} cannot sign in with the password of the password file right after it was made")
        if not repair:
            raise refusal(account)
        actions.append(account.set_value)
        if not check_only:
            write(runner, Call("user_add", (account.user,), account.password + "\n"), "set the password")
            if not probe(account.password):
                raise BootstrapError(f"{account.label} cannot sign in with the password of the password file after the password was set")
        verified = ("file", account.password)
    if verified[0] == "file":
        actions.append(account.publish)
        if not check_only:
            account.store.write(account.user, verified[1])
            after = account.store.read()
            if not (isinstance(after, dict) and after.get("user") == account.user and after.get("password") == verified[1]):
                raise BootstrapError(f"OpenBao did not keep {account.label}'s sign-in that was written to it")
    return actions


def root_step(runner: Runner, marker: Any, project: Account, audit: Account, bucket: str, *, check_only: bool, repair: bool) -> list[str]:
    """The first bootstrap, or a repair that the owner approved: the only use of the root pair.

    OpenBao is read before any call. Both accounts are made to match with root's calls, then each account's password that signs in is found and kept in OpenBao,
    and the marker is written last, so a run that fails before the end is the first bootstrap again."""
    stored = {account.name: account.store.read() for account in (project, audit)}
    found = {account.name: reconcile_root(runner, account, check_only=check_only) for account in (project, audit)}
    actions = [action for account in (project, audit) for action in found[account.name][0]]
    probes: dict[str, Callable[[str], bool]] = {
        "project": lambda password: signed_in(runner, project.user, bucket, password),
        "audit": lambda password: audit_signed_in(runner, audit.user, password),
    }
    for account in (project, audit):
        changed_actions, created = found[account.name]
        actions += settle(
            runner, account, stored[account.name], probes[account.name], created=created, changed=bool(changed_actions), check_only=check_only, repair=repair
        )
    if not marker.present():
        actions.append(RECORD)
        if not check_only:
            marker.write()
    return actions


def safe_name(name: str) -> str:
    return name if GROUP_NAME.fullmatch(name) else "a policy whose name is of another form"


def live_differences(read_as_audit: Callable[..., Result], user_as_audit: Callable[[str], Result], account: Account) -> list[str]:
    """What differs between one account's live state and its definition, from the answers of the audit identity: the user is there and enabled, holds
    exactly its policy by name and no group, and the policy's document is the desired one."""
    differences: list[str] = []
    answer = user_as_audit(account.user)
    if is_absent(answer, NO_SUCH_USER):
        differences.append(f"{account.label} does not exist")
    else:
        checked(answer, "read the user")
        status, held, groups = parse_user_info(answer.output, account.user)
        if status != "enabled":
            differences.append(f"{account.label} is not enabled")
        differences += [
            f"{account.label} holds the policy {safe_name(other)}" if GROUP_NAME.fullmatch(other) else f"{account.label} holds {safe_name(other)}"
            for other in held
            if other != account.policy
        ]
        if account.policy not in held:
            differences.append(f"{account.label} does not hold {account.policy_label}")
        differences += [f"{account.label} is a member of the group {group}" for group in groups]
    answer = read_as_audit("admin", "policy", "info", ALIAS, account.policy)
    if not is_absent(answer, NO_SUCH_POLICY):
        checked(answer, "read the policy")
    matches = policy_matches(answer, account.document)
    if matches is None:
        differences.append(f"{account.policy_label} does not exist")
    elif not matches:
        differences.append(f"{account.policy_label}'s document differs from the desired one")
    return differences


def checked(result: Result, what: str) -> None:
    """A failed read as the audit identity: a refusal or ``AccessDenied`` is its own message, and any other failure is named by its exit status."""
    if result.status == 0:
        return
    code = error_code(result)
    if code in SIGN_IN_REFUSALS:
        raise BootstrapError(
            "the audit identity's sign-in that OpenBao keeps is refused: nothing was changed. To repair it with the root pair, the owner approves it "
            f"with {APPROVAL}"
        )
    if code == "AccessDenied":
        raise BootstrapError(
            f"the call to {what} as the audit identity was answered with AccessDenied, which an offline or unlicensed server also gives: nothing was "
            "changed and the root pair is not used"
        )
    raise BootstrapError(f"the call to {what} as the audit identity failed with exit status {result.status}")


def verify_live(runner: Runner, project: Account, audit: Account, bucket: str) -> None:
    """Every deploy after the first bootstrap: read the live state as the audit identity, then list the bucket as the project user, and make no change.

    A sign-in that OpenBao does not keep, a refused sign-in or any difference fails with a fixed message that names it and the one approval that lets the root
    pair repair it."""
    pairs: dict[str, str] = {}
    for account in (project, audit):
        password = kept_sign_in(account.store.read(), account.user)
        if password is None:
            raise BootstrapError(
                f"OpenBao keeps no sign-in for {account.label}: nothing was changed. To repair it with the root pair, the owner approves it with {APPROVAL}"
            )
        pairs[account.name] = password

    def as_audit(*words: str) -> Result:
        return audit_read(runner, audit.user, pairs["audit"], *words)

    def user_as_audit(name: str) -> Result:
        return audit_user_info(runner, audit.user, pairs["audit"], name)

    differences = live_differences(as_audit, user_as_audit, audit) + live_differences(as_audit, user_as_audit, project)
    if not differences and not signed_in(runner, project.user, bucket, pairs["project"]):
        differences.append(f"the sign-in that OpenBao keeps for {project.label} is refused")
    if differences:
        raise BootstrapError(
            f"AIStor differs from the project's definition: {'; '.join(differences)}. Nothing was changed and the root pair was not used. "
            f"To repair it with the root pair, the owner approves it with {APPROVAL}"
        )


def reconcile(runner: Runner, marker: Any, project: Account, audit: Account, bucket: str, *, check_only: bool, repair: bool = False) -> list[str]:
    """Return what changed (or, in a check run, what would). The first bootstrap of a host and an approved repair use root; every other run only reads.

    1. The names and passwords are checked before any call.
    2. With the marker on the host and no approval, the live state is read as the audit identity and the project user and must match: no call as root, no write.
    3. Otherwise the root step runs (``root_step``)."""
    for account in (project, audit):
        if ACCOUNT_NAME.fullmatch(account.user) is None or account.user.lower() in RESERVED_USERS:
            raise BootstrapError("a user's name is not an account name or is a reserved name: neither account is ever the root user")
        if ACCOUNT_NAME.fullmatch(account.policy) is None or account.policy in BUILT_IN_POLICIES:
            raise BootstrapError("a policy's name is not a policy name or is a built-in policy's name")
        if VALUE_FORM.fullmatch(account.password) is None:
            raise BootstrapError("a password file does not hold a password of the expected form")
    if project.user == audit.user or project.policy == audit.policy:
        raise BootstrapError("the project user and the audit identity must have names of their own: a user and a policy each")
    if marker.present() and not repair:
        verify_live(runner, project, audit, bucket)
        return []
    return root_step(runner, marker, project, audit, bucket, check_only=check_only, repair=repair)


class Marker:
    """The root-only file that ends the first bootstrap of a host. It is a plain file of the module's user with mode 0400 and holds no secret.

    It is written with the shared helper of the secret files: the text goes to a temporary sibling that takes its place whole or not at all, a failed write
    removes the sibling, and the leftover of an interrupted write (a second 0400 file named ``.<name>.<8 hex>.new``) is removed by the next write."""

    def __init__(self, path: str) -> None:
        self.path = Path(path)

    def present(self) -> bool:
        try:
            status = os.lstat(self.path)
        except FileNotFoundError:
            return False
        except OSError as error:
            raise BootstrapError(f"the first-bootstrap marker cannot be looked at: {error.strerror}") from None
        if not stat.S_ISREG(status.st_mode) or stat.S_IMODE(status.st_mode) != 0o400 or status.st_uid != os.geteuid():
            raise BootstrapError(
                "the first-bootstrap marker is not a plain file of the deploy's user with mode 0400, so it is not trusted: look at it and remove it by hand"
            )
        return True

    def write(self) -> None:
        if self.present():
            return
        if not self.path.parent.is_dir():
            raise BootstrapError("the folder of the first-bootstrap marker does not exist")
        try:
            write_private_file(self.path, MARKER_TEXT, replace=False, owner=(os.geteuid(), os.getegid()))
        except FileExistsError:
            return
        except (SecretFileError, OSError) as error:
            raise BootstrapError(f"the first-bootstrap marker cannot be written: {error}") from None


class OpenBaoStore:
    """One account's sign-in at one OpenBao path, read and written as one AppRole whose secret ID is in a root-only file. Failures are fixed messages."""

    def __init__(self, client: OpenBaoClient, role: str, secret_id_file: str, path: str) -> None:
        self.client, self.role, self.secret_id_file, self.path = client, role, secret_id_file, path

    def login(self) -> str:
        try:
            with open(self.secret_id_file, encoding="utf-8") as handle:
                secret_id = handle.read().rstrip("\n")
        except OSError as error:
            raise BootstrapError(f"the OpenBao secret ID file cannot be read: {error.strerror}") from None
        try:
            return self.client.login(self.role, secret_id)
        except OpenBaoError as error:
            raise BootstrapError(str(error)) from None

    def read(self) -> dict[str, Any] | None:
        try:
            return self.client.read(self.login(), self.path)
        except OpenBaoError as error:
            raise BootstrapError(str(error)) from None

    def write(self, user: str, password: str) -> None:
        try:
            self.client.write(self.login(), self.path, {"user": user, "password": password})
        except OpenBaoError as error:
            raise BootstrapError(str(error)) from None


def module_runner(module: AnsibleModule, compose: list[str], service: str) -> Runner:
    """Run each call through Ansible's own command runner: standard input carries the text, and the module never logs the output."""

    def run(call: Call) -> Result:
        status, output, _ = module.run_command(command_line(compose, service, call), data=call.stdin, check_rc=False, binary_data=False)
        return Result(status, output)

    return run


def read_password(path: str, what: str) -> str:
    try:
        with open(path, encoding="utf-8") as handle:
            return handle.read().rstrip("\n")
    except OSError as error:
        raise BootstrapError(f"the {what} password file cannot be read: {error.strerror}") from None


def main() -> None:
    module = AnsibleModule(
        argument_spec={
            "compose_command": {"type": "list", "elements": "str", "required": True},
            "service": {"type": "str", "required": True},
            "user": {"type": "str", "required": True},
            "policy": {"type": "str", "required": True},
            "bucket": {"type": "str", "required": True},
            "password_file": {"type": "path", "required": True},
            "audit_user": {"type": "str", "required": True},
            "audit_policy": {"type": "str", "required": True},
            "audit_password_file": {"type": "path", "required": True},
            "marker_file": {"type": "path", "required": True},
            "openbao_address": {"type": "str", "required": True},
            "openbao_role": {"type": "str", "default": "bootstrap"},
            "openbao_secret_id_file": {"type": "path", "required": True},
            "openbao_path": {"type": "str", "required": True},
            "audit_openbao_path": {"type": "str", "required": True},
            "repair": {"type": "bool", "default": False},
        },
        supports_check_mode=True,
    )
    params = module.params
    try:
        try:
            client = OpenBaoClient(params["openbao_address"])
        except OpenBaoError as error:
            raise BootstrapError(str(error)) from None
        project = project_account(
            params["user"],
            params["policy"],
            params["bucket"],
            read_password(params["password_file"], "project user's"),
            OpenBaoStore(client, params["openbao_role"], params["openbao_secret_id_file"], params["openbao_path"]),
        )
        audit = audit_account(
            params["audit_user"],
            params["audit_policy"],
            read_password(params["audit_password_file"], "audit identity's"),
            OpenBaoStore(client, params["openbao_role"], params["openbao_secret_id_file"], params["audit_openbao_path"]),
        )
        actions = reconcile(
            module_runner(module, list(params["compose_command"]), params["service"]),
            Marker(params["marker_file"]),
            project,
            audit,
            params["bucket"],
            check_only=module.check_mode,
            repair=bool(params["repair"]),
        )
        module.exit_json(changed=bool(actions), actions=actions)
    except BootstrapError as error:
        module.fail_json(msg=str(error))


if __name__ == "__main__":
    main()
