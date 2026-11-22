"""The guard on object expiration: no S3 lifecycle rule may expire the live table paths, the ``warehouse`` prefix of the project's bucket.

Iceberg files are live while Nessie names their metadata, and an object that a lifecycle rule expires is gone with no Iceberg or Nessie step. Two
checks guard it.

The project declares no S3 lifecycle configuration. The declared-rules check does not read Terraform: it searches the text of every file under
``infra/`` whose name ends in ``.tf`` or ``.tf.json`` for ``aws_s3_bucket_lifecycle_configuration`` and ``lifecycle_rule`` (comments included, case-
sensitive) and names the file and line of each. A lifecycle rule then needs a deliberate change of this check.

The live check reads the bucket's configuration in the form that S3 returns it. A rule is a problem when its ``Status`` is not exactly ``Enabled`` or
``Disabled``, or when it is on, it expires current objects (``Expiration`` with days or a date) and its scope can reach the warehouse prefix. A scope
that cannot be shown to stay clear is a problem too: a rule with no filter, and a filter of tags or a size only. A rule that only aborts incomplete
uploads, expires old versions or delete markers, or moves objects between storage classes expires no live object, so it passes. "No configuration" is
accepted only when the AWS error code is exactly ``NoSuchLifecycleConfiguration``; every other error, and every successful answer that is not a JSON
object with a list of rules, is a failure.

``.venv/bin/python -m scripts.stack.lifecycle check`` runs both under the project profile in ``.env``.
It exits 0 when no rule reaches the warehouse, and 1 naming each problem, or when the bucket cannot be read.

"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from scripts.aws_cli import RAW_ERRORS, Aws, AwsError
from scripts.project_config import ROOT, ConfigurationError, load_configuration
from scripts.stack.warehouse import WAREHOUSE_PREFIX

INFRA_FOLDER = ROOT / "infra"
PROTECTED_PREFIX = f"{WAREHOUSE_PREFIX}/"
NO_CONFIGURATION_CODE = "NoSuchLifecycleConfiguration"
READ_OPERATION = "GetBucketLifecycleConfiguration"
STATUSES = ("Enabled", "Disabled")
DECLARATION_WORDS = ("aws_s3_bucket_lifecycle_configuration", "lifecycle_rule")
DECLARATION_ENDINGS = (".tf", ".tf.json")
ERROR_LINE_START = "An error occurred ("
ERROR_LINE = re.compile(r"An error occurred \(([A-Za-z0-9.]+)\) when calling the ([A-Za-z0-9]+) operation: ")
RAW_TEXT_PATH = re.compile(r"its full error text is in (\S+)$")


def rule_prefix(rule: Mapping[str, Any]) -> str:
    """The prefix that bounds a rule's scope: empty when tags, a size or nothing at all bounds it."""
    scope = rule.get("Filter")
    if scope is None:
        return str(rule.get("Prefix") or "")
    if not isinstance(scope, Mapping):
        raise ValueError("a rule's Filter is not an object")
    inner = scope.get("And")
    return str((inner if isinstance(inner, Mapping) else scope).get("Prefix") or "")


def expiring_rule_problems(configuration: Mapping[str, Any], protected_prefix: str) -> list[str]:
    """One message for each rule with a status that is not exact, and for each enabled rule that expires current objects and can reach ``protected_prefix``."""
    rules = configuration.get("Rules", [])
    if not isinstance(rules, list) or not all(isinstance(item, Mapping) for item in rules):
        raise ValueError("the lifecycle configuration's Rules is not a list of rules")
    found = []
    for position, item in enumerate(rules, start=1):
        name = item.get("ID") or position
        status = item.get("Status")
        if not isinstance(status, str) or status not in STATUSES:
            found.append(f"rule {name!r} has the Status {status!r}, which is neither Enabled nor Disabled")
            continue
        expiration = item.get("Expiration")
        if status != "Enabled" or not isinstance(expiration, Mapping) or not ({"Days", "Date"} & set(expiration)):
            continue
        prefix = rule_prefix(item)
        if prefix.startswith(protected_prefix) or protected_prefix.startswith(prefix):
            found.append(f"rule {name!r} expires objects under {prefix!r}, which reaches the live table paths {protected_prefix!r}")
    return found


def declared_problems(infra: str | Path) -> list[str]:
    """One message for each line of each ``.tf`` or ``.tf.json`` file under ``infra`` that holds the text of a lifecycle configuration.

    The folders are walked without following a symbolic link to a folder. Every file is searched as plain text, so a comment, an escaped quote or the JSON form
    of Terraform changes nothing.
    """
    root = Path(infra)
    found = []
    for directory, names, files in os.walk(root, followlinks=False):
        names.sort()
        for file_name in sorted(files):
            if not file_name.endswith(DECLARATION_ENDINGS):
                continue
            path = Path(directory, file_name)
            for number, line in enumerate(path.read_bytes().decode(errors="replace").splitlines(), start=1):
                for word in DECLARATION_WORDS:
                    if word in line:
                        found.append(
                            f"{path.relative_to(root)}:{number}: found {word}: the project declares no lifecycle rule, "
                            "and a lifecycle rule needs a deliberate change of this check"
                        )
    return found


def live_rules(document: Any) -> dict[str, Any]:
    """The answer of a successful read of the bucket's lifecycle configuration, or ``ValueError`` when it is not a JSON object that holds a list of rules.

    S3 answers with at least one rule or with the error ``NoSuchLifecycleConfiguration``, so an empty or null answer is not read as no rules.
    """
    rules = document.get("Rules") if isinstance(document, dict) else None
    if not isinstance(rules, list) or not rules or not all(isinstance(item, dict) for item in rules):
        raise ValueError(
            "the bucket's lifecycle configuration came back as something that is not a JSON object with a list of rules, and is not read as no rules"
        )
    return dict(document)


def no_configuration(stderr: str) -> bool:
    """Whether the CLI's error output is exactly one error line, in the CLI's form, for the code ``NoSuchLifecycleConfiguration`` and the operation asked for.

    A line counts as an error line when it begins with ``An error occurred (``. Zero such lines, more than one, another code, another operation and a mention of
    the code anywhere else in the text are not "no configuration": each is a failed read.
    """
    lines = [line for line in stderr.splitlines() if line.startswith(ERROR_LINE_START)]
    if len(lines) != 1:
        return False
    found = ERROR_LINE.match(lines[0])
    return found is not None and found.groups() == (NO_CONFIGURATION_CODE, READ_OPERATION)


def raw_error_text(error: AwsError) -> str | None:
    """The CLI's whole error output, which the shared client keeps in a file and names at the end of its message, or None when it cannot be found or read.

    The shared client words its message from the first error line it finds, so this guard reads the whole text itself.
    """
    found = RAW_TEXT_PATH.search(str(error))
    if found is None:
        return None
    path = RAW_ERRORS.parent.parent / found.group(1)
    if path.parent != RAW_ERRORS:
        return None
    try:
        return path.read_bytes().decode(errors="replace")
    except OSError:
        return None


def read_live(aws: Aws, bucket: str) -> dict[str, Any] | None:
    """The bucket's lifecycle configuration, or None when the error output is exactly the one error for no configuration. Any other error or answer raises."""
    try:
        document = aws.call("s3api", "get-bucket-lifecycle-configuration", "--bucket", bucket)
    except AwsError as error:
        text = raw_error_text(error)
        if text is not None and no_configuration(text):
            return None
        raise AwsError(f"{error}; the error output is not exactly one error line for no configuration, so the read failed") from error
    return live_rules(document)


def main(argv: Sequence[str] | None = None) -> int:
    """Check the declared rules and the live bucket's rules. Exit 0 when none reaches the warehouse prefix, 1 when one does or the bucket cannot be read."""
    parser = argparse.ArgumentParser(prog="scripts.stack.lifecycle", description="Check that no S3 lifecycle rule expires the live table paths.")
    parser.add_subparsers(dest="command", required=True).add_parser("check")
    parser.parse_args(argv)
    try:
        configuration = load_configuration()
        aws = Aws(configuration["AWS_PROFILE"], configuration["AWS_REGION"])
        live = read_live(aws, configuration["S3_BUCKET"])
        bucket_problems = [] if live is None else expiring_rule_problems(live, PROTECTED_PREFIX)
        found = declared_problems(INFRA_FOLDER) + [f"bucket: {problem}" for problem in bucket_problems]
    except (ConfigurationError, AwsError, ValueError) as error:
        sys.stderr.write(f"{error}\n")
        return 1
    if found:
        sys.stderr.write("".join(f"{problem}\n" for problem in found))
        return 1
    held = "no lifecycle configuration" if live is None else f"{len(live['Rules'])} rules"
    sys.stdout.write(f"ok: no rule, declared or on the bucket ({held}), expires the live table paths {PROTECTED_PREFIX}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
