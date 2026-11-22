"""The guard on object expiration: no S3 lifecycle rule may expire the live table paths, the ``warehouse`` prefix of the project's bucket.

Iceberg files are live while Nessie names their metadata, and an object that a lifecycle rule expires is gone with no Iceberg or Nessie step. This
module reads a lifecycle configuration in the form that S3 returns it, reads the rules that Terraform declares in ``infra/`` and reads the live
bucket's rules. A rule is a problem when it is on, it expires current objects (``Expiration`` with days or a date) and its scope can reach the
warehouse prefix. A scope that cannot be shown to stay clear is a problem too: a rule with no filter, a filter of tags or a size only, and a prefix
that is not a literal. A rule that only aborts incomplete uploads, expires old versions or delete markers, or moves objects between storage classes
expires no live object, so it passes.

``.venv/bin/python -m scripts.stack.lifecycle check`` reads the declared rules and the live bucket under the project profile in ``.env``.
It exits 0 when no rule reaches the warehouse, and 1 naming each rule that does, or when the bucket cannot be read.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from scripts.aws_cli import Aws, AwsError
from scripts.project_config import ROOT, ConfigurationError, load_configuration
from scripts.stack.warehouse import WAREHOUSE_PREFIX

INFRA_FOLDER = ROOT / "infra"
PROTECTED_PREFIX = f"{WAREHOUSE_PREFIX}/"
NO_CONFIGURATION = ("NoSuchLifecycleConfiguration",)
CONFIGURATION_RESOURCE = re.compile(r'resource\s+"aws_s3_bucket_lifecycle_configuration"\s+"[^"]+"\s*\{')
RULE_OPENER = re.compile(r"\brule\s*\{")
FILTER_OPENER = re.compile(r"\bfilter\s*\{")
EXPIRATION_OPENER = re.compile(r"\bexpiration\s*\{")
OLD_FORM = re.compile(r"\blifecycle_rule\b")
ASSIGNMENT = r"\b{name}\s*=\s*(.+)"


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
    """One message for each enabled rule that expires current objects and whose scope can reach ``protected_prefix``, in the order of the rules."""
    rules = configuration.get("Rules", [])
    if not isinstance(rules, list) or not all(isinstance(item, Mapping) for item in rules):
        raise ValueError("the lifecycle configuration's Rules is not a list of rules")
    found = []
    for position, item in enumerate(rules, start=1):
        expiration = item.get("Expiration")
        if item.get("Status") != "Enabled" or not isinstance(expiration, Mapping) or not ({"Days", "Date"} & set(expiration)):
            continue
        prefix = rule_prefix(item)
        if prefix.startswith(protected_prefix) or protected_prefix.startswith(prefix):
            found.append(f"rule {item.get('ID') or position!r} expires objects under {prefix!r}, which reaches the live table paths {protected_prefix!r}")
    return found


def block_bodies(text: str, opener: re.Pattern[str]) -> list[str]:
    """The text inside each block that ``opener`` starts, up to its matching closing brace. Braces inside a quoted string do not count."""
    bodies = []
    for match in opener.finditer(text):
        depth, quoted, index = 1, False, match.end()
        while index < len(text) and depth:
            character = text[index]
            if character == '"':
                quoted = not quoted
            elif not quoted and character == "{":
                depth += 1
            elif not quoted and character == "}":
                depth -= 1
            index += 1
        bodies.append(text[match.end() : index - 1] if depth == 0 else text[match.end() :])
    return bodies


def assigned(body: str, name: str) -> str | None:
    """The text on the right of ``name =`` in ``body``, or None."""
    found = re.search(ASSIGNMENT.format(name=name), body)
    return found.group(1).strip() if found else None


def terraform_rules(text: str) -> list[dict[str, Any]]:
    """The rules of every ``aws_s3_bucket_lifecycle_configuration`` resource in ``text``, in the form of S3, with only the fields that decide the check."""
    rules: list[dict[str, Any]] = []
    for resource in block_bodies(text, CONFIGURATION_RESOURCE):
        for body in block_bodies(resource, RULE_OPENER):
            status = (assigned(body, "status") or '"Enabled"').strip('"')
            rule: dict[str, Any] = {"ID": (assigned(body, "id") or f"rule {len(rules) + 1}").strip('"'), "Status": status}
            prefix = assigned(body, "prefix")
            if FILTER_OPENER.search(body) or prefix is not None:
                literal = re.fullmatch(r'"([^"]*)"', prefix) if prefix is not None else None
                rule["Filter"] = {"Prefix": literal.group(1)} if literal else {}
            for expiration in block_bodies(body, EXPIRATION_OPENER):
                days, date = assigned(expiration, "days"), assigned(expiration, "date")
                if days is not None:
                    rule["Expiration"] = {"Days": int(days) if days.isdigit() else days}
                elif date is not None:
                    rule["Expiration"] = {"Date": date.strip('"')}
            rules.append(rule)
    return rules


def declared_problems(infra: str | Path, protected_prefix: str) -> list[str]:
    """The problems of the rules that Terraform files under ``infra`` declare. A block of the old ``lifecycle_rule`` form cannot be read: it is one."""
    root = Path(infra)
    found = []
    for path in sorted(root.rglob("*.tf")):
        if ".terraform" in path.parts:
            continue
        text = path.read_text()
        name = str(path.relative_to(root))
        if OLD_FORM.search(text):
            found.append(f"{name}: a lifecycle_rule block cannot be checked: declare rules with aws_s3_bucket_lifecycle_configuration")
        found += [f"{name}: {problem}" for problem in expiring_rule_problems({"Rules": terraform_rules(text)}, protected_prefix)]
    return found


def main(argv: Sequence[str] | None = None) -> int:
    """Check the declared rules and the live bucket's rules. Exit 0 when none reaches the warehouse prefix, 1 when one does or the bucket cannot be read."""
    parser = argparse.ArgumentParser(prog="scripts.stack.lifecycle", description="Check that no S3 lifecycle rule expires the live table paths.")
    parser.add_subparsers(dest="command", required=True).add_parser("check")
    parser.parse_args(argv)
    try:
        configuration = load_configuration()
        aws = Aws(configuration["AWS_PROFILE"], configuration["AWS_REGION"])
        live = aws.call("s3api", "get-bucket-lifecycle-configuration", "--bucket", configuration["S3_BUCKET"], missing=NO_CONFIGURATION)
        found = declared_problems(INFRA_FOLDER, PROTECTED_PREFIX) + [f"bucket: {problem}" for problem in expiring_rule_problems(live or {}, PROTECTED_PREFIX)]
    except (ConfigurationError, AwsError, ValueError) as error:
        sys.stderr.write(f"{error}\n")
        return 1
    if found:
        sys.stderr.write("".join(f"{problem}\n" for problem in found))
        return 1
    held = "no lifecycle configuration" if live is None else f"{len(live.get('Rules', []))} rules"
    sys.stdout.write(f"ok: no rule, declared or on the bucket ({held}), expires the live table paths {PROTECTED_PREFIX}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
