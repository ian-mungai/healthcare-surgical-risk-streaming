"""Create and verify the S3 bucket that holds this run's Terraform state.

Run from the repository root under the project profile in ``.env``:

``.venv/bin/python -m scripts.infra.state_bootstrap create``

``create`` binds a new run ID, the state bucket, the workspace and the state keys in the binding file before it
creates anything, so an interrupted run is never left with an unrecorded bucket. It then creates the bucket when it
is absent and sets only what differs: all four public access blocks, versioning and the tags. It marks the binding
verified only after S3 reports the required settings. A second run changes nothing. ``verify`` reads the settings
back and exits 1 naming each one that differs. Neither command writes an object to the bucket.
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
STATE_KEY = "environment/terraform.tfstate"
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
    state_bucket: str
    workspace: str
    state_keys: list[str]
    status: str

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(asdict(self), indent=2) + "\n")


def new_binding(configuration: dict[str, str], backend: str = "s3") -> RunBinding:
    run_id = datetime.now(UTC).strftime("%Y%m%dt%H%M") + "-" + secrets.token_hex(3)
    commit = run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, timeout=GIT_TIMEOUT_SECONDS, check=True).stdout.strip()
    if backend == "local":
        return RunBinding(run_id, commit, "local", configuration["AWS_REGION"], "", WORKSPACE, [str(ROOT / "runs" / run_id / "terraform.tfstate")], "verified")
    bucket = f"{configuration['BUCKET_PREFIX']}-state-{configuration['ENVIRONMENT']}-{run_id}"
    return RunBinding(run_id, commit, "s3", configuration["AWS_REGION"], bucket, WORKSPACE, [STATE_KEY, f"{STATE_KEY}.tflock"], "planned")


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


def create(aws: Aws, configuration: dict[str, str], path: Path, backend: str = "s3") -> int:
    binding = RunBinding(**json.loads(path.read_text())) if path.is_file() else new_binding(configuration, backend)
    if binding.backend != backend:
        sys.stderr.write(f"the run is bound to the {binding.backend} backend, not {backend}\n")
        return 1
    if not path.is_file():
        binding.write(path)
    if binding.backend == "local":
        sys.stdout.write(f"run {binding.run_id}: local state at {binding.state_keys[0]}; no state bucket is used\n")
        return 0
    tags = required_tags(configuration)
    changes = []
    if not bucket_exists(aws, binding.state_bucket):
        aws.call("s3api", "create-bucket", "--bucket", binding.state_bucket, "--create-bucket-configuration", f"LocationConstraint={binding.region}")
        changes.append("bucket created")
    for name in problems(read_settings(aws, binding.state_bucket), tags):
        repair(aws, binding.state_bucket, name, tags)
        changes.append(f"{name} set")
    left = problems(read_settings(aws, binding.state_bucket), tags)
    if left:
        sys.stderr.write("".join(f"{problem}\n" for problem in left.values()))
        return 1
    if binding.status != "verified":
        binding.status = "verified"
        binding.write(path)
    sys.stdout.write(
        f"run {binding.run_id}: {', '.join(changes) if changes else 'no change'}; S3 reports all four public access blocks and versioning enabled\n"
    )
    return 0


def verify(aws: Aws, configuration: dict[str, str], path: Path) -> int:
    if not path.is_file():
        sys.stderr.write("no run binding: run create first\n")
        return 1
    binding = RunBinding(**json.loads(path.read_text()))
    if binding.backend == "local":
        sys.stdout.write(f"run {binding.run_id}: local state; no state bucket to verify\n")
        return 0
    if not bucket_exists(aws, binding.state_bucket):
        sys.stderr.write("the bound state bucket does not exist\n")
        return 1
    found = problems(read_settings(aws, binding.state_bucket), required_tags(configuration))
    sys.stderr.write("".join(f"{problem}\n" for problem in found.values()))
    if not found:
        sys.stdout.write(f"run {binding.run_id}: the state bucket has every required setting\n")
    return 1 if found else 0


def main() -> int:
    """Run create or verify and return a non-zero code on a difference or a failure."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["create", "verify"])
    parser.add_argument("--binding", type=Path, default=DEFAULT_BINDING, help="the run binding file; the default is runs/run_binding.json")
    parser.add_argument(
        "--backend", choices=["s3", "local"], default="s3", help="create: s3 for a host in AWS, local for a host that already exists outside AWS"
    )
    arguments = parser.parse_args()
    try:
        configuration = load_configuration()
        aws = Aws(configuration["AWS_PROFILE"], configuration["AWS_REGION"])
        if arguments.command == "create":
            return create(aws, configuration, arguments.binding, arguments.backend)
        return verify(aws, configuration, arguments.binding)
    except (ConfigurationError, AwsError) as error:
        sys.stderr.write(f"{error}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
