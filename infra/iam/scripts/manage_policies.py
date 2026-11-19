"""Plan, apply and export the project profile's IAM user and its per-service policies.

The owner runs this under the administrator profile, from the repository root:

``.venv/bin/python -m infra.iam.scripts.manage_policies plan --admin-profile <ADMIN_PROFILE>``

``plan`` is read-only. It lists each step and prints a confirmation code. ``apply --confirm <CODE>`` computes the plan
again and applies it only when its code equals the reviewed one. It creates the user, creates missing policies,
publishes a new default version only when a document changed and attaches each policy to the user. A rerun plans
nothing. ``export`` compares the live documents, the user's attached policies by ARN, its inline policies and its
group memberships with the templates and exits 1 on a difference. The tool never creates an access key and never
removes a grant: a live project policy with no template, an extra attachment, an inline policy and a group membership are
reported, and ``plan`` and ``apply`` exit 1 while one remains.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from infra.iam.scripts.render_policy import BOUNDARY_DIRECTORY, PolicyRenderError, load_documents, scope_problems
from scripts.aws_cli import Aws, AwsError
from scripts.project_config import ConfigurationError, load_configuration

MAX_POLICY_VERSIONS = 5
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
    """What the account holds now: the user, the project policies by name, and the user's attached ARNs, inline policies and groups."""

    user_exists: bool
    documents: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    attached: frozenset[str] = frozenset()
    inline: frozenset[str] = frozenset()
    groups: frozenset[str] = frozenset()


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
    for name in sorted(documents):
        arn = policy_arn(account, name)
        if name not in live.documents:
            steps.append(Step("create-policy", name))
        elif canonical(live.documents[name]) != canonical(documents[name]):
            steps.append(Step("update-policy", name))
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
        *[Step("report-unlisted", name) for name in sorted(set(live.documents) - set(documents))],
    ]


def differences(steps: list[Step]) -> list[Step]:
    """The steps that show the live account differs from the templates: every one but a policy that nothing attaches and no template lists."""
    return [step for step in steps if step.action != "report-unlisted"]


def unchanged_differences(steps: list[Step]) -> list[Step]:
    """The differences that apply never changes."""
    return [step for step in differences(steps) if step.action.startswith("report-")]


def confirmation_code(account: str, documents: Mapping[str, Mapping[str, Any]], steps: list[Step]) -> str:
    content = [account, [[step.action, step.target] for step in steps], {name: canonical(document) for name, document in sorted(documents.items())}]
    return hashlib.sha256(json.dumps(content).encode()).hexdigest()[:12]


def live_state(aws: Aws, names: Names) -> LiveState:
    user_exists = aws.call("iam", "get-user", "--user-name", names.user, missing=("NoSuchEntity",)) is not None
    documents = {}
    for policy in aws.call("iam", "list-policies", "--scope", "Local")["Policies"]:
        if names.owns(policy["PolicyName"]):
            version = aws.call("iam", "get-policy-version", "--policy-arn", policy["Arn"], "--version-id", policy["DefaultVersionId"])
            documents[policy["PolicyName"]] = version["PolicyVersion"]["Document"]
    if not user_exists:
        return LiveState(False, documents)
    attached = aws.call("iam", "list-attached-user-policies", "--user-name", names.user)["AttachedPolicies"]
    inline = aws.call("iam", "list-user-policies", "--user-name", names.user)["PolicyNames"]
    groups = aws.call("iam", "list-groups-for-user", "--user-name", names.user)["Groups"]
    return LiveState(
        True,
        documents,
        frozenset(item["PolicyArn"] for item in attached),
        frozenset(inline),
        frozenset(group["GroupName"] for group in groups),
    )


def apply_step(aws: Aws, names: Names, account: str, documents: Mapping[str, Mapping[str, Any]], step: Step) -> None:
    by_name = {name: json.dumps(document) for name, document in documents.items()}
    arn = policy_arn(account, step.target)
    if step.action == "create-user":
        tags = [f"Key=Project,Value={names.project}", f"Key=Environment,Value={names.environment}"]
        aws.call("iam", "create-user", "--user-name", step.target, "--tags", *tags)
    elif step.action == "create-policy":
        aws.call("iam", "create-policy", "--policy-name", step.target, "--policy-document", by_name[step.target])
    elif step.action == "update-policy":
        versions = aws.call("iam", "list-policy-versions", "--policy-arn", arn)["Versions"]
        if len(versions) >= MAX_POLICY_VERSIONS:
            oldest = min((version for version in versions if not version["IsDefaultVersion"]), key=lambda version: version["CreateDate"])
            aws.call("iam", "delete-policy-version", "--policy-arn", arn, "--version-id", oldest["VersionId"])
        aws.call("iam", "create-policy-version", "--policy-arn", arn, "--policy-document", by_name[step.target], "--set-as-default")
    elif step.action == "attach-policy":
        aws.call("iam", "attach-user-policy", "--user-name", names.user, "--policy-arn", arn)


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
        return 0
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
    """Run plan, apply or export and return a non-zero code on a refusal, a difference or a failure."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["plan", "apply", "export"])
    parser.add_argument("--admin-profile", required=True, help="the owner's administrator AWS profile; never the project profile")
    parser.add_argument("--confirm", default="", help="apply: the confirmation code that plan printed")
    try:
        return run(parser.parse_args())
    except (ConfigurationError, PolicyRenderError, AwsError) as error:
        sys.stderr.write(f"{error}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
