"""Ansible module: the one-time AIStor bootstrap that makes the project's user and its policy, and nothing else.

THE ONLY USE OF THE ROOT PAIR. The owner decided that root credentials are kept in a file, are secured on the disk and are not used for the project's work. This
module's calls are the one place where the pair is used: only to create the project user, its policy and the attachment of one to the other. Everything else
(Terraform, the storage command, the checks, the workloads) signs in as the project user, or through STS, and never as root. A deploy first verifies the
project user AS that user: it reads the sign-in that OpenBao keeps for the aistor service and lists the project's bucket with it. When that works and OpenBao
holds the digest of the current policy, the module makes no call as root and no write, so after the first deploy root is not used again. Only a sign-in that
is refused (no user, wrong password, disabled, no policy) or a policy digest that changed sends the deploy to the root step.

OpenBao is written with the sign-in only after that pair has signed in, so a password file that was lost and made again never replaces the working value.
When the user exists and no password signs in, the module changes no password and writes nothing: it refuses, unless the owner approved setting the password
file's value on the user for one deploy with ``compose_stack_aistor_rotate_password: true``.

How the pair is kept out of the host's processes: the module runs ``mc`` inside the AIStor container (the image ships it) through ``docker compose exec``. A
fixed script there reads the two root files from the container's own secrets mount (``/run/secrets/aistor_root_user`` and ``..._password``, the files that
the server reads through ``MINIO_ROOT_USER_FILE`` and ``MINIO_ROOT_PASSWORD_FILE``) and gives the pair to the one ``mc`` process as its ``MC_HOST_aistor``
variable, which is the only way ``mc`` takes a sign-in without writing it to a configuration file or reading it from its own command line. So the pair is
never a module argument, never in Ansible's or the host shell's environment and never in a host command line; it exists for the length of one ``mc`` call,
in that call's environment inside the container.

The project user's password reaches ``mc`` on standard input and never as an argument: the pinned client (``mc.RELEASE.2026-09-19T15-24-59Z``, SHA-256
``83938eded435e7c28c397ece1dc590b2a79626dd9354aae2107060fec559ae2b``, in the image ``RELEASE.2026-09-19T17-05-25Z``, whose layer holds the help text of the
form) takes ``mc admin user add ALIAS ACCESSKEY`` and then reads the secret key from standard input when that is not a terminal (``fetchUserKeys`` in
``cmd/admin-user-add.go``: two arguments and no terminal read one line). ``docker compose exec -T`` gives no terminal. So the password is in no process list.
No secret is printed, logged or part of a message: a failed call is reported by its fixed name and exit status.

The policy holds exactly the S3 calls that the pinned Terraform provider (aminueza/minio v3.46.2) makes for the two resources of ``infra/storage`` and
nothing else; each action has its source file and line in ``tests/fast/test_aistor_project_user.py`` (``PROVIDER_CALLS``). It publishes no policy (the bucket's
``acl = "private"`` only removes one), writes no object and deletes no version. The user holds that policy, no other policy and no group: a policy or a group
that someone added is removed before a disabled user is enabled, and a failed removal leaves the user disabled. A user or a policy is made only when the server
answers in its own code that it does not exist (``XMinioAdminNoSuchUser`` or ``XMinioAdminNoSuchPolicy`` with a failed exit status); any other answer fails.
The module changes nothing when everything matches, and a check run reports the actions without making any call that writes. It uses only the standard library
and runs on the host's Python.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.openbao_client import OpenBaoClient, OpenBaoError

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
# The S3 codes of a call that was refused for who made it (not a user, not the password, not enabled, no policy) and the code of a bucket that is not there yet:
# the listing was allowed and found no bucket, so the sign-in worked. Any other answer is not a verdict on the sign-in.
SIGN_IN_REFUSALS = {"InvalidAccessKeyId", "SignatureDoesNotMatch", "AccessDenied"}
SIGNED_IN_CODES = {"NoSuchBucket"}
SET_VALUE_ACTION = "set the project user's password to the one of the password file"
PUBLISH = "write the project user's sign-in to OpenBao"
APPROVAL = "compose_stack_aistor_rotate_password"
SCRIPTS = {
    "read": f'{ROOT_SIGN_IN} exec mc --no-color --json "$@" 2>&1',
    "user_add": f'{ROOT_SIGN_IN} exec mc --no-color admin user add {ALIAS} "$1"',
    # The one call as the project user. Its pair is the environment of the one mc process inside the container: the user is an argument of the script, the
    # password comes on standard input, and neither is an argument of mc. The root files are not read here.
    "sign_in": f'IFS= read -r secret_value && MC_HOST_{ALIAS}="http://$1:$secret_value@127.0.0.1:9000" exec mc --no-color ls --json {ALIAS}/"$2" 2>&1',
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


def policy_document(bucket: str) -> dict[str, Any]:
    """The project's policy: the provider's bucket actions on the one bucket, and nothing on its objects."""
    if BUCKET_NAME.fullmatch(bucket) is None:
        raise BootstrapError("the bucket name is not a bucket name: 3 to 63 lower case letters, digits and hyphens")
    return {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": list(BUCKET_ACTIONS), "Resource": [f"arn:aws:s3:::{bucket}"]}],
    }


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


def policy_digest(bucket: str) -> str:
    """The SHA-256 of the project's policy for the bucket. OpenBao keeps it beside the sign-in, so a changed policy or bucket is applied again."""
    return hashlib.sha256(normalised(policy_document(bucket)).encode()).hexdigest()


def signed_in(runner: Runner, user: str, bucket: str, password: str) -> bool:
    """Whether the project user signs in with this password: a listing of the project's bucket as that user. A refusal is False, and any other answer fails."""
    result = runner(Call("sign_in", (user, bucket), password + "\n"))
    code = error_code(result)
    if result.status == 0 or code in SIGNED_IN_CODES:
        return True
    if code in SIGN_IN_REFUSALS:
        return False
    raise BootstrapError(f"the sign-in as the project user failed with exit status {result.status} and an answer that is not a refusal")


def read(runner: Runner, *words: str) -> Result:
    return runner(Call("read", tuple(words)))


def write(runner: Runner, call: Call, what: str) -> None:
    result = runner(call)
    if result.status != 0:
        raise BootstrapError(f"the call to {what} failed with exit status {result.status}")


def policy_state(runner: Runner, policy: str, document: dict[str, Any]) -> bool:
    """Whether the policy exists and matches. A policy that does not exist, by the server's own code, is False; any other failed or unreadable answer fails."""
    result = read(runner, "admin", "policy", "info", ALIAS, policy)
    if is_absent(result, NO_SUCH_POLICY):
        return False
    if result.status != 0:
        raise BootstrapError(f"the call to read the policy failed with exit status {result.status}")
    found = answer_of(result.output)
    info = found.get("policyInfo")
    stored = info.get("Policy") if isinstance(info, dict) else None
    if found.get("status") != "success" or not isinstance(stored, dict):
        raise BootstrapError("the answer for the policy is not policy information")
    return normalised(stored) == normalised(document)


def reconcile_root(runner: Runner, user: str, policy: str, document: dict[str, Any], password: str, *, check_only: bool) -> tuple[list[str], bool]:
    """The root step: make the project's policy, user and attachment match with root's calls, and return what changed (or would) and whether the user is new.

    The password is given to a user that is explicitly absent and to no other user."""
    actions: list[str] = []
    if not policy_state(runner, policy, document):
        actions.append("create the policy")
        if not check_only:
            write(runner, Call("policy_create", (policy,), json.dumps(document)), "create the policy")
    answer = read(runner, "admin", "user", "info", ALIAS, user)
    created = is_absent(answer, NO_SUCH_USER)
    if created:
        actions.append("create the user")
        if not check_only:
            write(runner, Call("user_add", (user,), password + "\n"), "create the user")
        status, held, groups = "enabled", list[str](), list[str]()
    elif answer.status != 0:
        raise BootstrapError(f"the call to read the user failed with exit status {answer.status}")
    else:
        status, held, groups = parse_user_info(answer.output, user)
    # Access that is not the project's goes first, the project's policy next and the enabling last: a user that is disabled is never enabled with extra access.
    for other in held:
        if other != policy:
            actions.append(f"detach the policy {other} from the user")
            if not check_only:
                write(runner, Call("read", ("admin", "policy", "detach", ALIAS, other, "--user", user)), "detach a policy")
    for group in groups:
        actions.append(f"remove the user from the group {group}")
        if not check_only:
            write(runner, Call("read", ("admin", "group", "remove", ALIAS, group, user)), "remove the user from a group")
    if policy not in held:
        actions.append("attach the policy to the user")
        if not check_only:
            write(runner, Call("read", ("admin", "policy", "attach", ALIAS, policy, "--user", user)), "attach the policy")
    if status != "enabled":
        actions.append("enable the user")
        if not check_only:
            write(runner, Call("read", ("admin", "user", "enable", ALIAS, user)), "enable the user")
    return actions, created


def kept_sign_in(stored: dict[str, Any] | None, user: str) -> str | None:
    """The password in the sign-in that OpenBao keeps, when that is the project user's and has the form of a password. Any other document is not used."""
    password = stored.get("password") if isinstance(stored, dict) and stored.get("user") == user else None
    return password if isinstance(password, str) and VALUE_FORM.fullmatch(password) else None


def first_that_signs_in(runner: Runner, user: str, bucket: str, candidates: list[tuple[str, str]]) -> tuple[str, str] | None:
    for source, password in candidates:
        if signed_in(runner, user, bucket, password):
            return source, password
    return None


def refusal() -> BootstrapError:
    return BootstrapError(
        "the project user exists, but neither the sign-in that OpenBao keeps nor the password file signs in: no password is changed and nothing is written to "
        f"OpenBao. To set the password file's value on the user, the owner approves it for one deploy with {APPROVAL}: true"
    )


def reconcile(runner: Runner, bao: Any, user: str, policy: str, bucket: str, password: str, *, check_only: bool, rotate: bool = False) -> list[str]:
    """Verify the project user as itself and use root only when that fails, and return what changed (or, in a check run, what would).

    1. OpenBao's sign-in is read, and the project user signs in with it. When that works and OpenBao's digest is the one of the current policy, nothing else
       happens: no call as root, no write.
    2. Otherwise the root step makes the policy, the user (only when absent), its one policy and no group match, enabling a disabled user last.
    3. Then a password that signs in is found: OpenBao's first, the password file's next. OpenBao is written with the pair only after that pair has signed
       in, so a new password file never replaces a working value. When nothing signs in, a user that was made now is an error; a user that existed is left as
       it is and the run refuses, unless the owner approved setting the password file's value on it (``rotate``)."""
    if ACCOUNT_NAME.fullmatch(user) is None or user.lower() in RESERVED_USERS:
        raise BootstrapError("the project user's name is not an account name or is a reserved name: the project user is never the root user")
    if ACCOUNT_NAME.fullmatch(policy) is None or policy in BUILT_IN_POLICIES:
        raise BootstrapError("the project policy's name is not a policy name or is a built-in policy's name")
    if VALUE_FORM.fullmatch(password) is None:
        raise BootstrapError("the project user's password file does not hold a password of the expected form")
    document, digest = policy_document(bucket), policy_digest(bucket)
    stored = bao.read()
    kept = kept_sign_in(stored, user)
    fresh = kept is not None and isinstance(stored, dict) and stored.get("policy_sha256") == digest
    candidates = ([("openbao", kept)] if kept else []) + ([("file", password)] if password != kept else [])
    verified = first_that_signs_in(runner, user, bucket, candidates)
    if verified is not None and verified[0] == "openbao" and fresh:
        return []
    actions, created = reconcile_root(runner, user, policy, document, password, check_only=check_only)
    if check_only:
        if verified is None and not created and not actions:
            if not rotate:
                raise refusal()
            actions.append(SET_VALUE_ACTION)
        source = (verified or candidates[0])[0] if verified or actions else "file"
        if source == "file" or not fresh:
            actions.append(PUBLISH)
        return actions
    verified = first_that_signs_in(runner, user, bucket, candidates) if actions or verified is None else verified
    if verified is None:
        if created:
            raise BootstrapError("the project user cannot sign in with the password of the password file right after it was made")
        if not rotate:
            raise refusal()
        write(runner, Call("user_add", (user,), password + "\n"), "set the password")
        actions.append(SET_VALUE_ACTION)
        if not signed_in(runner, user, bucket, password):
            raise BootstrapError("the project user cannot sign in with the password of the password file after the password was set")
        verified = ("file", password)
    if verified[0] == "file" or not fresh:
        bao.write(user, verified[1], digest)
        after = bao.read()
        if not (isinstance(after, dict) and after.get("user") == user and after.get("password") == verified[1] and after.get("policy_sha256") == digest):
            raise BootstrapError("OpenBao did not keep the project user's sign-in that was written to it")
        actions.append(PUBLISH)
    return actions


class OpenBaoStore:
    """The project user's sign-in at one OpenBao path, read and written as one AppRole whose secret ID is in a root-only file. Failures are fixed messages."""

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

    def write(self, user: str, password: str, digest: str) -> None:
        try:
            self.client.write(self.login(), self.path, {"user": user, "password": password, "policy_sha256": digest})
        except OpenBaoError as error:
            raise BootstrapError(str(error)) from None


def module_runner(module: AnsibleModule, compose: list[str], service: str) -> Runner:
    """Run each call through Ansible's own command runner: standard input carries the text, and the module never logs the output."""

    def run(call: Call) -> Result:
        status, output, _ = module.run_command(command_line(compose, service, call), data=call.stdin, check_rc=False, binary_data=False)
        return Result(status, output)

    return run


def main() -> None:
    module = AnsibleModule(
        argument_spec={
            "compose_command": {"type": "list", "elements": "str", "required": True},
            "service": {"type": "str", "required": True},
            "user": {"type": "str", "required": True},
            "policy": {"type": "str", "required": True},
            "bucket": {"type": "str", "required": True},
            "password_file": {"type": "path", "required": True},
            "openbao_address": {"type": "str", "required": True},
            "openbao_role": {"type": "str", "default": "bootstrap"},
            "openbao_secret_id_file": {"type": "path", "required": True},
            "openbao_path": {"type": "str", "required": True},
            "rotate": {"type": "bool", "default": False},
        },
        supports_check_mode=True,
    )
    params = module.params
    try:
        try:
            with open(params["password_file"], encoding="utf-8") as handle:
                password = handle.read().rstrip("\n")
        except OSError as error:
            raise BootstrapError(f"the project user's password file cannot be read: {error.strerror}") from None
        try:
            store = OpenBaoStore(OpenBaoClient(params["openbao_address"]), params["openbao_role"], params["openbao_secret_id_file"], params["openbao_path"])
        except OpenBaoError as error:
            raise BootstrapError(str(error)) from None
        actions = reconcile(
            module_runner(module, list(params["compose_command"]), params["service"]),
            store,
            params["user"],
            params["policy"],
            params["bucket"],
            password,
            check_only=module.check_mode,
            rotate=bool(params["rotate"]),
        )
        module.exit_json(changed=bool(actions), actions=actions)
    except BootstrapError as error:
        module.fail_json(msg=str(error))


if __name__ == "__main__":
    main()
