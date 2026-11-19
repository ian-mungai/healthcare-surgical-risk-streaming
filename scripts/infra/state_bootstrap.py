"""Create and verify the project's one S3 bucket and bind this run to its Terraform state location.

Run from the repository root under the project profile in ``.env``:

``.venv/bin/python -m scripts.infra.state_bootstrap create``

The project has one bucket, named by ``S3_BUCKET`` in ``.env``. The Terraform state lives under ``terraform_state/``
and the data under its own prefixes. ``create`` binds a new run ID, the bucket, the workspace and the state keys in
the binding file before it creates anything, so an interrupted run is never left with an unrecorded bucket. It
creates the bucket when it is absent, uses the project's bucket when this account already owns it, and sets only
what differs: all four public access blocks, versioning and the tags. Only the call that created the bucket marks
it as created by this run, and the teardown removes no other bucket. ``create --short-lived-bucket`` gives a test
its own bucket, named with the suffix ``-test-`` and the run, which must not exist yet. A second run changes
nothing. ``verify`` reads the settings back and exits 1 naming each one that differs. Neither command writes an
object to the bucket.
"""

from __future__ import annotations

import argparse
import json
import secrets
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.aws_cli import Aws, AwsError
from scripts.process import run_command
from scripts.project_config import ROOT, ConfigurationError, load_configuration

DEFAULT_BINDING = ROOT / "runs" / "run_binding.json"
STATE_PREFIX = "terraform_state"
BUCKET_NAME_LIMIT = 63
WORKSPACE = "default"
BLOCK_NAMES = ("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")
MISSING_BUCKET = ("(404)", "NoSuchBucket")
GIT_TIMEOUT_SECONDS = 60


@dataclass
class RunBinding:
    run_id: str
    commit: str
    backend: str
    region: str
    bucket: str
    workspace: str
    state_keys: list[str]
    status: str
    provision_host: bool = True
    bucket_creation: str = "none"
    bucket_mode: str = "permanent"

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(asdict(self), indent=2) + "\n")


def state_keys(environment: str) -> list[str]:
    """The state key and its lock key for one environment, under the state prefix of the bucket."""
    key = f"{STATE_PREFIX}/{environment}/terraform.tfstate"
    return [key, f"{key}.tflock"]


def short_lived_bucket_name(project_bucket: str, run_id: str) -> str:
    """A test's own bucket: the project's bucket name, the test suffix and the run ID without its year and separators, to fit an S3 name."""
    name = f"{project_bucket}-test-{run_id[4:].replace('t', '').replace('-', '')}"
    if len(name) > BUCKET_NAME_LIMIT:
        raise ConfigurationError(f"the test bucket name would be longer than {BUCKET_NAME_LIMIT} characters; shorten S3_BUCKET")
    return name


def new_binding(configuration: dict[str, str], backend: str = "s3", provision_host: bool = True, short_lived: bool = False) -> RunBinding:
    run_id = datetime.now(UTC).strftime("%Y%m%dt%H%M") + "-" + secrets.token_hex(3)
    commit = run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, timeout=GIT_TIMEOUT_SECONDS, check=True).stdout.strip()
    bucket = short_lived_bucket_name(configuration["S3_BUCKET"], run_id) if short_lived else configuration["S3_BUCKET"]
    region = configuration["AWS_REGION"]
    mode = "short_lived" if short_lived else "permanent"
    if backend == "local":
        keys = [str(ROOT / "runs" / run_id / "terraform.tfstate")]
        return RunBinding(run_id, commit, "local", region, bucket, WORKSPACE, keys, "planned", provision_host=False, bucket_mode=mode)
    keys = state_keys(configuration["ENVIRONMENT"])
    return RunBinding(run_id, commit, "s3", region, bucket, WORKSPACE, keys, "planned", provision_host=provision_host, bucket_mode=mode)


def bucket_problem(binding: RunBinding, configuration: dict[str, str], *, short_lived: bool | None = None) -> str | None:
    """Why the binding's bucket may not be used: it does not fit its recorded mode, or the caller asked for the other mode. Checked before any S3 call."""
    if binding.bucket_mode not in ("permanent", "short_lived"):
        return "the binding's bucket mode is neither permanent nor short_lived"
    if short_lived is True and binding.bucket_mode == "permanent":
        return "the run is bound to the permanent bucket, and a short-lived test bucket was asked for"
    if short_lived is False and binding.bucket_mode == "short_lived":
        return "the run is bound to a short-lived test bucket, and the permanent bucket was asked for"
    if binding.bucket_mode == "permanent" and binding.bucket != configuration["S3_BUCKET"]:
        return "the binding's permanent bucket is not the bucket named by S3_BUCKET"
    if binding.bucket_mode == "short_lived" and binding.bucket != short_lived_bucket_name(configuration["S3_BUCKET"], binding.run_id):
        return "the binding's test bucket is not the short-lived bucket of this run"
    return None


def adoption_problem(creation: str, *, exists: bool, owned: bool, empty: bool, short_lived: bool) -> str | None:
    """Why this run may not use the bucket with its bucket name, or None when it may create it, keep using it or share the project's."""
    if creation == "none" and exists and short_lived:
        return "a bucket with this test's bucket name already exists and this run did not create it"
    if creation == "none" and exists and not owned:
        return "the project's bucket name belongs to a bucket this account does not own"
    if creation == "attempted" and exists and not (owned and empty):
        return "the bucket from this run's interrupted creation is not an empty bucket of this account"
    if creation == "created" and not exists:
        return "this run's bucket no longer exists"
    return None


def owned(aws: Aws, bucket: str, account: str) -> bool:
    return aws.call("s3api", "head-bucket", "--bucket", bucket, "--expected-bucket-owner", account, missing=("(403)", "AccessDenied")) is not None


def empty(aws: Aws, bucket: str) -> bool:
    listing = aws.call("s3api", "list-object-versions", "--bucket", bucket, "--max-items", "1")
    return not listing.get("Versions") and not listing.get("DeleteMarkers")


def required_tags(configuration: dict[str, str]) -> dict[str, str]:
    return {"Project": configuration["PROJECT_NAME"], "Environment": configuration["ENVIRONMENT"]}


def bucket_exists(aws: Aws, bucket: str) -> bool:
    return aws.call("s3api", "head-bucket", "--bucket", bucket, missing=MISSING_BUCKET) is not None


def read_settings(aws: Aws, bucket: str) -> dict[str, Any]:
    blocks = aws.call("s3api", "get-public-access-block", "--bucket", bucket, missing=("NoSuchPublicAccessBlockConfiguration",)) or {}
    tags = aws.call("s3api", "get-bucket-tagging", "--bucket", bucket, missing=("NoSuchTagSet",)) or {}
    return {
        "blocks": blocks.get("PublicAccessBlockConfiguration", {}),
        "versioning": aws.call("s3api", "get-bucket-versioning", "--bucket", bucket).get("Status", "never enabled"),
        "tags": {tag["Key"]: tag["Value"] for tag in tags.get("TagSet", [])},
    }


def problems(settings: dict[str, Any], tags: dict[str, str]) -> dict[str, str]:
    """Each required setting that S3 does not report, by the name of its repair."""
    found = {}
    off = [name for name in BLOCK_NAMES if settings["blocks"].get(name) is not True]
    if off:
        found["blocks"] = f"public access block not enabled: {', '.join(off)}"
    if settings["versioning"] != "Enabled":
        found["versioning"] = f"versioning is {settings['versioning']}, not Enabled"
    if settings["tags"] != tags:
        found["tags"] = "the bucket tags differ from the required tags"
    return found


def repair(aws: Aws, bucket: str, name: str, tags: dict[str, str]) -> None:
    if name == "blocks":
        aws.call(
            "s3api", "put-public-access-block", "--bucket", bucket, "--public-access-block-configuration", ",".join(f"{block}=true" for block in BLOCK_NAMES)
        )
    elif name == "versioning":
        aws.call("s3api", "put-bucket-versioning", "--bucket", bucket, "--versioning-configuration", "Status=Enabled")
    elif name == "tags":
        tag_set = json.dumps({"TagSet": [{"Key": key, "Value": value} for key, value in tags.items()]})
        aws.call("s3api", "put-bucket-tagging", "--bucket", bucket, "--tagging", tag_set)


def create(aws: Aws, configuration: dict[str, str], path: Path, backend: str = "s3", provision_host: bool = True, short_lived: bool = False) -> int:
    binding = RunBinding(**json.loads(path.read_text())) if path.is_file() else new_binding(configuration, backend, provision_host, short_lived)
    if binding.backend != backend:
        sys.stderr.write(f"the run is bound to the {binding.backend} backend, not {backend}\n")
        return 1
    mode_problem = bucket_problem(binding, configuration, short_lived=short_lived)
    if mode_problem:
        sys.stderr.write(f"{mode_problem}\n")
        return 1
    if not path.is_file():
        binding.write(path)
    tags = required_tags(configuration)
    changes = []
    exists = bucket_exists(aws, binding.bucket)
    is_owned = exists and owned(aws, binding.bucket, configuration["AWS_ACCOUNT_ID"])
    is_empty = is_owned and binding.bucket_creation == "attempted" and empty(aws, binding.bucket)
    problem = adoption_problem(binding.bucket_creation, exists=exists, owned=is_owned, empty=is_empty, short_lived=binding.bucket_mode == "short_lived")
    if problem:
        sys.stderr.write(f"{problem}\n")
        return 1
    if not exists:
        binding.bucket_creation = "attempted"
        binding.write(path)
        aws.call("s3api", "create-bucket", "--bucket", binding.bucket, "--create-bucket-configuration", f"LocationConstraint={binding.region}")
        changes.append("bucket created")
    if binding.bucket_creation == "attempted":
        binding.bucket_creation = "created"
        binding.write(path)
    for name in problems(read_settings(aws, binding.bucket), tags):
        repair(aws, binding.bucket, name, tags)
        changes.append(f"{name} set")
    left = problems(read_settings(aws, binding.bucket), tags)
    if left:
        sys.stderr.write("".join(f"{problem}\n" for problem in left.values()))
        return 1
    if binding.status != "verified":
        binding.status = "verified"
        binding.write(path)
    state = "local state" if binding.backend == "local" else f"state under {STATE_PREFIX}/"
    sys.stdout.write(
        f"run {binding.run_id}: {', '.join(changes) if changes else 'no change'}; {state}; S3 reports all four public access blocks and versioning enabled\n"
    )
    return 0


def verify(aws: Aws, configuration: dict[str, str], path: Path) -> int:
    if not path.is_file():
        sys.stderr.write("no run binding: run create first\n")
        return 1
    binding = RunBinding(**json.loads(path.read_text()))
    mode_problem = bucket_problem(binding, configuration)
    if mode_problem:
        sys.stderr.write(f"{mode_problem}\n")
        return 1
    if not bucket_exists(aws, binding.bucket):
        sys.stderr.write("the bound bucket does not exist\n")
        return 1
    found = problems(read_settings(aws, binding.bucket), required_tags(configuration))
    sys.stderr.write("".join(f"{problem}\n" for problem in found.values()))
    if not found:
        sys.stdout.write(f"run {binding.run_id}: the bucket has every required setting\n")
    return 1 if found else 0


def main() -> int:
    """Run create or verify and return a non-zero code on a difference or a failure."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["create", "verify"])
    parser.add_argument("--binding", type=Path, default=DEFAULT_BINDING, help="the run binding file; the default is runs/run_binding.json")
    parser.add_argument(
        "--backend", choices=["s3", "local"], default="s3", help="create: s3 for a host in AWS, local for a host that already exists outside AWS"
    )
    parser.add_argument("--given-host", action="store_true", help="create: the host already exists, so Terraform plans and removes no host")
    parser.add_argument("--short-lived-bucket", action="store_true", help="create: a test's own bucket, named with the test suffix and the run")
    arguments = parser.parse_args()
    try:
        configuration = load_configuration()
        aws = Aws(configuration["AWS_PROFILE"], configuration["AWS_REGION"])
        if arguments.command == "create":
            return create(aws, configuration, arguments.binding, arguments.backend, not arguments.given_host, arguments.short_lived_bucket)
        return verify(aws, configuration, arguments.binding)
    except (ConfigurationError, AwsError) as error:
        sys.stderr.write(f"{error}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
