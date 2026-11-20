"""Plan, apply and export the project profile's IAM user and its per-service policies.

The owner runs this under the administrator profile, from the repository root:

``.venv/bin/python -m infra.iam.scripts.manage_policies plan --admin-profile <ADMIN_PROFILE>``

``plan`` is read-only. It lists each step and prints a confirmation code. ``apply --confirm <CODE>`` computes the plan
again and applies it only when its code equals the reviewed one. It creates the user, creates missing policies,
publishes a new default version only when a document changed and attaches each policy to the user. A rerun plans
nothing. ``key`` gives the user its one access key: it creates the key only when the user has none, writes it to an
owner-only backup file beside the AWS command line's own files, in the column layout of the console's download,
imports it as the project profile and waits until AWS accepts it. It refuses to create a key while ``cli_history`` is
enabled for the administrator profile, because the AWS command line would record the secret in its history database.
It confirms the project profile by the exact user ARN and account that the administrator's ``get-user`` returned. A
refusal by IAM removes the empty backup that this call reserved. A call with no clear answer keeps it, and the owner
checks the user's keys before a retry. It never prints the secret, never overwrites a backup file and never deletes a
key. ``export`` compares the live documents, the tags of the policies, the boundary
and the user, the user's attached policies by ARN, its inline policies and its group memberships with the templates and
exits 1 on a difference. Only ``key`` creates an access key. The tool never removes a grant or a tag: a live project
policy with no template, an extra attachment, an inline policy, a group membership and an extra policy or user tag are
reported, and ``plan`` and ``apply`` exit 1 while one remains.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from infra.iam.scripts.render_policy import BOUNDARY_DIRECTORY, PolicyRenderError, load_documents, scope_problems
from scripts.aws_cli import AWS_TIMEOUT_SECONDS, Aws, AwsError
from scripts.process import TimeoutExpired, run_command, timeout_text
from scripts.project_config import ConfigurationError, load_configuration

MAX_POLICY_VERSIONS = 5
KEY_FOLDER = Path.home() / ".aws"
KEY_HEADER = "User Name,Access key ID,Secret access key"
KEY_READY_SECONDS = 120.0
KEY_POLL_SECONDS = 5.0
CONFIG_READ_SECONDS = 30.0
REFUSED_BY_IAM = re.compile(r"^aws iam create-access-key failed: (AccessDenied|LimitExceeded|NoSuchEntity|InvalidInput)(;|$)")
UNCERTAIN_CREATE = (
    "the create call gave no clear answer and a key may exist: check the access keys of {user} as the owner before a retry; the empty backup file is kept"
)
UNSTORED_KEY = (
    "the create call answered, but its answer could not be read or its backup could not be written, and a key may exist: "
    "check the access keys of {user} as the owner before a retry; the backup file is kept as it is"
)
LIST_KEYS = {"Action", "NotAction", "Resource", "NotResource"}


@dataclass(frozen=True)
class Names:
    project: str
    environment: str

    @property
    def user(self) -> str:
        return f"{self.project}_{self.environment}"

    def policy(self, service: str) -> str:
        return f"{self.project}_{service}_policy"

    def boundary(self, kind: str) -> str:
        return f"{self.project}_{kind}_boundary"

    @property
    def tags(self) -> dict[str, str]:
        return {"Project": self.project, "Environment": self.environment}

    def tag_arguments(self) -> list[str]:
        return ["--tags", *[f"Key={key},Value={value}" for key, value in self.tags.items()]]

    def owns(self, policy_name: str) -> bool:
        return policy_name.startswith(f"{self.project}_") and policy_name.endswith(("_policy", "_boundary"))


@dataclass(frozen=True)
class Step:
    action: str
    target: str


def normalized(value: Any, key: str = "") -> Any:
    if isinstance(value, dict):
        return {name: normalized(child, name) for name, child in sorted(value.items())}
    if key in LIST_KEYS:
        return sorted(value) if isinstance(value, list) else [value]
    if isinstance(value, list):
        return [normalized(item) for item in value]
    return value


def canonical(document: Mapping[str, Any]) -> str:
    """One text for every equal policy: sorted keys, and a single action or resource equal to a one-item list."""
    return json.dumps(normalized(document), separators=(",", ":"))


@dataclass(frozen=True)
class LiveState:
    """What the account holds now: the user and its tags, the project policies and their tags by name, and the user's attached ARNs, inline policies, groups."""

    user_exists: bool
    documents: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    attached: frozenset[str] = frozenset()
    inline: frozenset[str] = frozenset()
    groups: frozenset[str] = frozenset()
    tags: Mapping[str, Mapping[str, str]] = field(default_factory=dict)
    user_tags: Mapping[str, str] = field(default_factory=dict)


def policy_arn(account: str, name: str) -> str:
    return f"arn:aws:iam::{account}:policy/{name}"


def plan_steps(
    names: Names, documents: Mapping[str, Mapping[str, Any]], live: LiveState, *, account: str, unattached: frozenset[str] | set[str] = frozenset()
) -> list[Step]:
    """The steps that bring the user, its policies and its attachments to the documents, keyed by policy name.

    A name in ``unattached`` is a permissions boundary: it is created and updated and never attached to the user. A
    step that starts with ``report-`` names a difference that the tool never changes.
    """
    steps = [] if live.user_exists else [Step("create-user", names.user)]
    if live.user_exists and any(live.user_tags.get(key) != value for key, value in names.tags.items()):
        steps.append(Step("tag-user", names.user))
    for name in sorted(documents):
        arn = policy_arn(account, name)
        if name not in live.documents:
            steps.append(Step("create-policy", name))
        else:
            if canonical(live.documents[name]) != canonical(documents[name]):
                steps.append(Step("update-policy", name))
            if any(live.tags.get(name, {}).get(key) != value for key, value in names.tags.items()):
                steps.append(Step("tag-policy", name))
        if name not in unattached and arn not in live.attached:
            steps.append(Step("attach-policy", name))
        if name in unattached and arn in live.attached:
            steps.append(Step("report-attached-boundary", name))
    ours = {policy_arn(account, name) for name in documents}
    return [
        *steps,
        *[Step("report-unexpected-attachment", arn.split(":", 5)[5]) for arn in sorted(live.attached - ours)],
        *[Step("report-inline-policy", name) for name in sorted(live.inline)],
        *[Step("report-group-membership", name) for name in sorted(live.groups)],
        *[Step("report-extra-policy-tag", f"{name} {key}") for name in sorted(documents) for key in sorted(set(live.tags.get(name, {})) - set(names.tags))],
        *[Step("report-extra-user-tag", f"{names.user} {key}") for key in sorted(set(live.user_tags) - set(names.tags)) if live.user_exists],
        *[Step("report-unlisted", name) for name in sorted(set(live.documents) - set(documents))],
    ]


def differences(steps: list[Step]) -> list[Step]:
    """The steps that show the live account differs from the templates: every one but a policy that nothing attaches and no template lists."""
    return [step for step in steps if step.action != "report-unlisted"]


def unchanged_differences(steps: list[Step]) -> list[Step]:
    """Every report-only step, an unlisted policy included: what plan and apply never change and still fail on."""
    return [step for step in steps if step.action.startswith("report-")]


def confirmation_code(account: str, documents: Mapping[str, Mapping[str, Any]], steps: list[Step]) -> str:
    content = [account, [[step.action, step.target] for step in steps], {name: canonical(document) for name, document in sorted(documents.items())}]
    return hashlib.sha256(json.dumps(content).encode()).hexdigest()[:12]


def live_state(aws: Aws, names: Names) -> LiveState:
    user_exists = aws.call("iam", "get-user", "--user-name", names.user, missing=("NoSuchEntity",)) is not None
    documents = {}
    tags = {}
    for policy in aws.call("iam", "list-policies", "--scope", "Local")["Policies"]:
        if names.owns(policy["PolicyName"]):
            version = aws.call("iam", "get-policy-version", "--policy-arn", policy["Arn"], "--version-id", policy["DefaultVersionId"])
            documents[policy["PolicyName"]] = version["PolicyVersion"]["Document"]
            tags[policy["PolicyName"]] = {tag["Key"]: tag["Value"] for tag in aws.call("iam", "list-policy-tags", "--policy-arn", policy["Arn"])["Tags"]}
    if not user_exists:
        return LiveState(False, documents, tags=tags)
    user_tags = {tag["Key"]: tag["Value"] for tag in aws.call("iam", "list-user-tags", "--user-name", names.user)["Tags"]}
    attached = aws.call("iam", "list-attached-user-policies", "--user-name", names.user)["AttachedPolicies"]
    inline = aws.call("iam", "list-user-policies", "--user-name", names.user)["PolicyNames"]
    groups = aws.call("iam", "list-groups-for-user", "--user-name", names.user)["Groups"]
    return LiveState(
        True,
        documents,
        frozenset(item["PolicyArn"] for item in attached),
        frozenset(inline),
        frozenset(group["GroupName"] for group in groups),
        tags,
        user_tags,
    )


def apply_step(aws: Aws, names: Names, account: str, documents: Mapping[str, Mapping[str, Any]], step: Step) -> None:
    by_name = {name: json.dumps(document) for name, document in documents.items()}
    arn = policy_arn(account, step.target)
    if step.action == "create-user":
        aws.call("iam", "create-user", "--user-name", step.target, *names.tag_arguments())
    elif step.action == "tag-user":
        aws.call("iam", "tag-user", "--user-name", step.target, *names.tag_arguments())
    elif step.action == "create-policy":
        aws.call("iam", "create-policy", "--policy-name", step.target, "--policy-document", by_name[step.target], *names.tag_arguments())
    elif step.action == "tag-policy":
        aws.call("iam", "tag-policy", "--policy-arn", arn, *names.tag_arguments())
    elif step.action == "update-policy":
        versions = aws.call("iam", "list-policy-versions", "--policy-arn", arn)["Versions"]
        if len(versions) >= MAX_POLICY_VERSIONS:
            oldest = min((version for version in versions if not version["IsDefaultVersion"]), key=lambda version: version["CreateDate"])
            aws.call("iam", "delete-policy-version", "--policy-arn", arn, "--version-id", oldest["VersionId"])
        aws.call("iam", "create-policy-version", "--policy-arn", arn, "--policy-document", by_name[step.target], "--set-as-default")
    elif step.action == "attach-policy":
        aws.call("iam", "attach-user-policy", "--user-name", names.user, "--policy-arn", arn)


def profile_is_the_user(project: Aws, user_arn: str, *, timeout: float = AWS_TIMEOUT_SECONDS) -> bool:
    """Whether the project profile signs in as the exact user ARN that the administrator profile's ``get-user`` returned, in that ARN's account."""
    try:
        identity = project.call("sts", "get-caller-identity", timeout=timeout)
    except AwsError, TimeoutExpired:
        return False
    return bool(identity.get("Arn") == user_arn and identity.get("Account") == user_arn.split(":")[4])


def refuse_while_history_records(admin: Aws) -> None:
    """Stop before the key call when the AWS command line would record its answer, secret key included, in its history database."""
    advice = f"turn it off with: aws configure set cli_history disabled --profile {admin.profile}"
    unreadable = ConfigurationError(f"the cli_history setting of the profile {admin.profile} could not be read, so key stops before it creates a key; {advice}")
    try:
        result = run_command("aws", ["configure", "get", "cli_history", "--profile", admin.profile], timeout=CONFIG_READ_SECONDS)
    except TimeoutExpired:
        raise ConfigurationError(f"{timeout_text('aws configure get', CONFIG_READ_SECONDS)}, so key stops before it creates a key; run key again") from None
    except OSError:
        raise unreadable from None
    value = result.stdout.strip()
    if result.returncode not in (0, 1):
        raise unreadable
    if value == "enabled":
        raise ConfigurationError(
            f"cli_history is enabled for the profile {admin.profile}: the AWS command line would record the new secret key; {advice}, then run key again"
        )


def write_new_key(admin: Aws, user: str, backup: Path) -> None:
    """Reserve the backup file, create the key and write it. A definite refusal by IAM releases the reservation; any other failure keeps it."""
    descriptor = os.open(backup, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    stored = False
    try:
        with os.fdopen(descriptor, "w") as handle:
            try:
                key = admin.single_attempt().call("iam", "create-access-key", "--user-name", user)["AccessKey"]
                handle.write(f"{KEY_HEADER}\n{user},{key['AccessKeyId']},{key['SecretAccessKey']}\n")
                handle.flush()
                stored = True
            except AwsError as error:
                if not REFUSED_BY_IAM.search(str(error)):
                    raise ConfigurationError(UNCERTAIN_CREATE.format(user=user)) from error
                backup.unlink()
                raise
            except ValueError, LookupError, TypeError, OSError:
                pass
    except OSError:
        stored = False
    if not stored:
        raise ConfigurationError(UNSTORED_KEY.format(user=user))


def wait_until_the_profile_works(project: Aws, user_arn: str) -> None:
    """Poll until the project profile works. Each call gets the time left, no sleep passes the deadline and an answer after it is refused."""
    deadline = time.monotonic() + KEY_READY_SECONDS
    while (remaining := deadline - time.monotonic()) > 0:
        if profile_is_the_user(project, user_arn, timeout=remaining) and time.monotonic() <= deadline:
            return
        time.sleep(max(0.0, min(KEY_POLL_SECONDS, deadline - time.monotonic())))
    raise ConfigurationError(f"the new key did not start to work within {KEY_READY_SECONDS:g} seconds; run key again to check it")


def create_key(admin: Aws, configuration: dict[str, str], names: Names) -> int:
    """Give the project user its one access key, a private backup file and a working profile. A second run changes nothing."""
    user = names.user
    if configuration["AWS_PROFILE"] != user:
        raise ConfigurationError(f"AWS_PROFILE must equal the project user's name, {user}, for the key to be imported as that profile")
    found = admin.call("iam", "get-user", "--user-name", user, missing=("NoSuchEntity",))
    if found is None:
        raise ConfigurationError(f"the user {user} does not exist: run apply first")
    user_arn = found["User"]["Arn"]
    project = Aws(user, configuration["AWS_REGION"])
    backup = KEY_FOLDER / f"{user}_accessKeys.csv"
    if admin.call("iam", "list-access-keys", "--user-name", user)["AccessKeyMetadata"]:
        if profile_is_the_user(project, user_arn):
            sys.stdout.write(f"no change: {user} has its access key and the profile {user} works\n")
            return 0
        raise ConfigurationError(f"{user} already has an access key and the project profile does not work: this tool never deletes a key; review it")
    if backup.exists():
        raise ConfigurationError(f"the backup file already exists and is never overwritten: {backup.name} in {KEY_FOLDER.name}; move it away as the owner")
    refuse_while_history_records(admin)
    KEY_FOLDER.mkdir(mode=0o700, exist_ok=True)
    write_new_key(admin, user, backup)
    admin.call("configure", "import", "--csv", f"file://{backup}")
    for name, value in (("region", configuration["AWS_REGION"]), ("output", "json")):
        project.call("configure", "set", name, value)
    sys.stdout.write(f"access key created for {user}; backup written, readable by its owner only: {backup.name} in {KEY_FOLDER.name}\n")
    wait_until_the_profile_works(project, user_arn)
    sys.stdout.write(f"the profile {user} works\n")
    return 0


def run(arguments: argparse.Namespace) -> int:
    configuration = load_configuration()
    if arguments.admin_profile == configuration["AWS_PROFILE"]:
        raise ConfigurationError("the administrator profile must differ from the project profile")
    names = Names(configuration["PROJECT_NAME"], configuration["ENVIRONMENT"])
    account = configuration["AWS_ACCOUNT_ID"]
    boundaries = {names.boundary(kind): document for kind, document in load_documents(configuration, BOUNDARY_DIRECTORY, suffix="_boundary").items()}
    documents = {names.policy(service): document for service, document in load_documents(configuration).items()} | boundaries
    problems = [f"{name}: {problem}" for name, document in documents.items() for problem in scope_problems(document)]
    if problems:
        raise PolicyRenderError("; ".join(problems))
    aws = Aws(arguments.admin_profile, configuration["AWS_REGION"])
    if aws.call("sts", "get-caller-identity")["Account"] != account:
        raise ConfigurationError("the administrator profile is in a different account from AWS_ACCOUNT_ID")
    if arguments.command == "key":
        return create_key(aws, configuration, names)
    steps = plan_steps(names, documents, live_state(aws, names), account=account, unattached=set(boundaries))
    code = confirmation_code(account, documents, steps)
    for step in steps:
        sys.stdout.write(f"{step.action}: {step.target}\n")
    if arguments.command == "export":
        sys.stdout.write(f"{len(differences(steps))} differences between the live account and the templates\n")
        return 1 if differences(steps) else 0
    changes = [step for step in steps if not step.action.startswith("report-")]
    remaining = unchanged_differences(steps)
    if not changes:
        if remaining:
            sys.stdout.write(f"nothing to apply, but {len(remaining)} differences remain that this tool never removes: review each as the owner\n")
            return 1
        sys.stdout.write("no change: the user, its policies and its attachments equal the templates\n")
        return 0
    if arguments.command == "plan":
        sys.stdout.write(f"{len(changes)} steps planned. To apply exactly this plan: apply --admin-profile <ADMIN_PROFILE> --confirm {code}\n")
        if remaining:
            sys.stdout.write(f"{len(remaining)} differences remain that this tool never removes: review each as the owner\n")
        return 1 if remaining else 0
    if arguments.confirm != code:
        sys.stderr.write("the confirmation code does not equal the current plan; run plan again and review it\n")
        return 1
    for step in changes:
        apply_step(aws, names, account, documents, step)
        sys.stdout.write(f"done: {step.action}: {step.target}\n")
    if remaining:
        sys.stdout.write(f"{len(remaining)} differences remain that this tool never removes: review each as the owner\n")
    return 1 if remaining else 0


def main() -> int:
    """Run plan, apply, key or export and return a non-zero code on a refusal, a difference or a failure."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["plan", "apply", "key", "export"])
    parser.add_argument("--admin-profile", required=True, help="the owner's administrator AWS profile; never the project profile")
    parser.add_argument("--confirm", default="", help="apply: the confirmation code that plan printed")
    return guarded(parser.parse_args())


def guarded(arguments: argparse.Namespace) -> int:
    try:
        return run(arguments)
    except (ConfigurationError, PolicyRenderError, AwsError) as error:
        sys.stderr.write(f"{error}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
