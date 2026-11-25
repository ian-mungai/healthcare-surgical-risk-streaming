"""Ansible module: the one-time AIStor bootstrap that makes the project's user and its policy, and nothing else.

THE ONLY USE OF THE ROOT PAIR. The owner decided that root credentials are kept in a file, are secured on the disk and are not used for the project's work. This
module's calls are the one place where the pair is used: only to create the project user, its policy and the attachment of one to the other. Everything else
(Terraform, the storage command, the checks, the workloads) signs in as the project user, or through STS, and never as root. The module makes no call when the
user and the policy exist and match, so after the first deploy root is not used again.

How the pair is kept out of the host's processes: the module runs ``mc`` inside the AIStor container (the image ships it) through ``docker compose exec``. A
fixed script there reads the two root files from the container's own secrets mount (``/run/secrets/aistor_root_user`` and ``..._password``, the files that
the server reads through ``MINIO_ROOT_USER_FILE`` and ``MINIO_ROOT_PASSWORD_FILE``) and gives the pair to the one ``mc`` process as its ``MC_HOST_aistor``
variable, which is the only way ``mc`` takes a sign-in without writing it to a configuration file or reading it from its own command line. So the pair is
never a module argument, never in Ansible's or the host shell's environment and never in a host command line; it exists for the length of one ``mc`` call,
in that call's environment inside the container.

The project user's password is read from the root-only file on the host and travels to the container on standard input. It is an ``mc`` argument for the one
``mc admin user add`` call (the command takes no other form: ``mc admin user add ALIAS ACCESSKEY SECRETKEY``), so it is visible in the container's process list
for that call only. No secret is printed, logged or part of a message: a failed call is reported by its fixed name and exit status.

The policy reaches the project's one bucket and the objects in it, and no admin action. The user holds that policy and no other: a policy that someone
attached later is detached. The module changes nothing when everything matches, and a check run reports the actions without making any call that writes.
It uses only the standard library and runs on the host's Python.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ansible.module_utils.basic import AnsibleModule

ALIAS = "aistor"
BUCKET_NAME = re.compile(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]")
ACCOUNT_NAME = re.compile(r"[A-Za-z][A-Za-z0-9_.-]{2,63}")
VALUE_FORM = re.compile(r"[A-Za-z0-9_-]{16,128}")
RESERVED_USERS = {"minioadmin", "root", "admin"}
BUILT_IN_POLICIES = {"consoleAdmin", "readwrite", "readonly", "writeonly", "diagnostics"}
ROOT_PAIR_FILES = ("/run/secrets/aistor_root_user", "/run/secrets/aistor_root_password")
# The one place that gives the root pair to a process: the environment of one ``mc`` call, read inside the container from its own mounted files.
ROOT_SIGN_IN = f'MC_HOST_{ALIAS}="http://$(cat {ROOT_PAIR_FILES[0]}):$(cat {ROOT_PAIR_FILES[1]})@127.0.0.1:9000"'
SCRIPTS = {
    "read": f'{ROOT_SIGN_IN} exec mc --no-color "$@"',
    "user_add": f'IFS= read -r secret_value && {ROOT_SIGN_IN} exec mc --no-color admin user add {ALIAS} "$1" "$secret_value"',
    "policy_create": (
        f'file=$(mktemp) && cat > "$file" && {ROOT_SIGN_IN} mc --no-color admin policy create {ALIAS} "$1" "$file"; status=$?; rm -f "$file"; exit $status'
    ),
}
BUCKET_ACTIONS = [
    "s3:CreateBucket",
    "s3:DeleteBucket",
    "s3:ListBucket",
    "s3:ListBucketVersions",
    "s3:ListBucketMultipartUploads",
    "s3:GetBucketLocation",
    "s3:GetBucketVersioning",
    "s3:PutBucketVersioning",
    "s3:GetBucketPolicy",
    "s3:PutBucketPolicy",
    "s3:DeleteBucketPolicy",
    "s3:GetBucketTagging",
    "s3:PutBucketTagging",
]
OBJECT_ACTIONS = [
    "s3:GetObject",
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:GetObjectVersion",
    "s3:DeleteObjectVersion",
    "s3:GetObjectTagging",
    "s3:PutObjectTagging",
    "s3:AbortMultipartUpload",
    "s3:ListMultipartUploadParts",
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
    """The project's policy: bucket actions on the bucket, object actions on its objects, and nothing else."""
    if BUCKET_NAME.fullmatch(bucket) is None:
        raise BootstrapError("the bucket name is not a bucket name: 3 to 63 lower case letters, digits and hyphens")
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": list(BUCKET_ACTIONS), "Resource": [f"arn:aws:s3:::{bucket}"]},
            {"Effect": "Allow", "Action": list(OBJECT_ACTIONS), "Resource": [f"arn:aws:s3:::{bucket}/*"]},
        ],
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


def parse_user_info(text: str) -> tuple[str, list[str]]:
    """The status and the policies of a user from the text of ``mc admin user info``."""
    status = re.search(r"^\s*Status:\s*(\S*)", text, re.MULTILINE)
    held = re.search(r"^\s*PolicyName:[ \t]*(.*)$", text, re.MULTILINE)
    if status is None or held is None:
        raise BootstrapError("the answer for the user is not user information")
    return status.group(1), [name.strip() for name in held.group(1).split(",") if name.strip()]


def read(runner: Runner, *words: str) -> Result:
    return runner(Call("read", tuple(words)))


def write(runner: Runner, call: Call, what: str) -> None:
    result = runner(call)
    if result.status != 0:
        raise BootstrapError(f"the call to {what} failed with exit status {result.status}")


def current_policy_matches(runner: Runner, policy: str, document: dict[str, Any]) -> bool:
    result = read(runner, "admin", "policy", "info", ALIAS, policy)
    if result.status != 0:
        return False
    try:
        return normalised(json.loads(result.output)) == normalised(document)
    except ValueError:
        return False


def reconcile(runner: Runner, user: str, policy: str, bucket: str, password: str, *, check_only: bool) -> list[str]:
    """Make the project's policy, user and attachment match, and return what changed (or, in a check run, what would). A matching server gets no write call."""
    if ACCOUNT_NAME.fullmatch(user) is None or user.lower() in RESERVED_USERS:
        raise BootstrapError("the project user's name is not an account name or is a reserved name: the project user is never the root user")
    if ACCOUNT_NAME.fullmatch(policy) is None or policy in BUILT_IN_POLICIES:
        raise BootstrapError("the project policy's name is not a policy name or is a built-in policy's name")
    if VALUE_FORM.fullmatch(password) is None:
        raise BootstrapError("the project user's password file does not hold a password of the expected form")
    document = policy_document(bucket)
    actions: list[str] = []
    if not current_policy_matches(runner, policy, document):
        actions.append("create the policy")
        if not check_only:
            write(runner, Call("policy_create", (policy,), json.dumps(document)), "create the policy")
    answer = read(runner, "admin", "user", "info", ALIAS, user)
    if answer.status != 0:
        actions.append("create the user")
        if not check_only:
            write(runner, Call("user_add", (user,), password + "\n"), "create the user")
        status, held = "enabled", list[str]()
    else:
        status, held = parse_user_info(answer.output)
        if status != "enabled":
            actions.append("enable the user")
            if not check_only:
                write(runner, Call("read", ("admin", "user", "enable", ALIAS, user)), "enable the user")
    if policy not in held:
        actions.append("attach the policy to the user")
        if not check_only:
            write(runner, Call("read", ("admin", "policy", "attach", ALIAS, policy, "--user", user)), "attach the policy")
    for other in held:
        if other != policy:
            actions.append(f"detach the policy {other} from the user")
            if not check_only:
                write(runner, Call("read", ("admin", "policy", "detach", ALIAS, other, "--user", user)), "detach a policy")
    return actions


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
        actions = reconcile(
            module_runner(module, list(params["compose_command"]), params["service"]),
            params["user"],
            params["policy"],
            params["bucket"],
            password,
            check_only=module.check_mode,
        )
        module.exit_json(changed=bool(actions), actions=actions)
    except BootstrapError as error:
        module.fail_json(msg=str(error))


if __name__ == "__main__":
    main()
