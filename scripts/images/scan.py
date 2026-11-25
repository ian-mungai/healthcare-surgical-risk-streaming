"""The scan gate of the publisher: Docker Scout's report of an image's fixable findings, held against the committed list of accepted findings.

The publisher scans each image that it built before it pushes any image. Docker Scout needs a Docker sign-in, which the computer that publishes already has
for its push. A finding is the vulnerability's ID and the package URL with its version, without qualifiers. A finding passes only when the accepted list
names that exact pair, so an entry never carries over to another version of the package. ``image_scan/accepted_findings.yml`` is outside every image's
sources, so a change in it changes no image tag.

Everything that is not a complete report is refused and is never read as a clean image: a missing or unreadable file, another format or tool, and a result
that does not name exactly one package. Only an ID and a package URL that match their forms are ever shown.
"""

from __future__ import annotations

import datetime
import json
import re
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.ingestion.notice import FixedTextError
from scripts.stack.yaml_text import YamlError, load_yaml

ACCEPTED_FILE = Path(__file__).resolve().parents[2] / "image_scan" / "accepted_findings.yml"
FIELDS = {"cve", "package", "reason", "date"}
CVE_FORM = re.compile(r"(CVE|GHSA|GO)-[A-Za-z0-9-]{4,40}")
PACKAGE_FORM = re.compile(r"pkg:[a-z]+/[A-Za-z0-9._+~:/-]{1,200}@[A-Za-z0-9._+~:-]{1,100}")
DATE_FORM = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")


class ScanError(FixedTextError, ValueError):
    """A fixed-text failure of a report or of the accepted list. The text holds no value that was read from either."""


@dataclass(frozen=True, order=True)
class Finding:
    cve: str
    package: str


def scan_arguments(tag: str, report: Path) -> list[str]:
    """The ``docker scout`` arguments: the fixable findings of the linux/amd64 image that the build loaded on this computer, as SARIF in ``report``."""
    return ["scout", "cves", "--platform", "linux/amd64", "--only-fixed", "--format", "sarif", "--output", str(report), f"local://{tag}"]


def checked(cve: object, package: object) -> Finding:
    if not (isinstance(cve, str) and CVE_FORM.fullmatch(cve) and isinstance(package, str) and PACKAGE_FORM.fullmatch(package)):
        raise ScanError("a finding does not name a vulnerability ID and a package URL with its version in their forms")
    return Finding(cve, package)


def reported(result: Any) -> Finding:
    """The finding of one SARIF result: its rule is the vulnerability's ID, and one line of its message names the package."""
    try:
        cve = result["ruleId"]
        lines = str(result["message"]["text"]).split("\n")
    except (KeyError, TypeError) as error:
        raise ScanError("a result of the scan report lacks its rule or its message") from error
    packages = [match.group(1) for line in lines if (match := re.fullmatch(r"Package\s*:(\S+)\s*", line))]
    if len(packages) != 1:
        raise ScanError("a result of the scan report does not name exactly one package")
    return checked(cve, urllib.parse.unquote(packages[0].partition("?")[0]))


def read_report(text: str) -> frozenset[Finding]:
    """The findings of a complete SARIF 2.1.0 report of Docker Scout."""
    try:
        document = json.loads(text)
        version = document["version"]
        (run,) = document["runs"]
        tool = run["tool"]["driver"]["name"]
        results = run["results"]
    except (ValueError, KeyError, TypeError) as error:
        raise ScanError("the scan report is not one complete SARIF run") from error
    if version != "2.1.0" or tool != "docker scout" or not isinstance(results, list):
        raise ScanError("the scan report is not a SARIF 2.1.0 report of docker scout with a list of results")
    return frozenset(reported(result) for result in results)


def load_accepted(path: Path) -> frozenset[Finding]:
    """The findings that the list accepts. Each entry holds exactly the vulnerability's ID, the package URL with its version, the reason and the date."""
    try:
        document = load_yaml(path.read_text())
    except (YamlError, ValueError, OSError) as error:
        raise ScanError("the accepted-findings file cannot be read as YAML") from error
    if not isinstance(document, dict) or set(document) != {"findings"} or not isinstance(document["findings"], list):
        raise ScanError("the accepted-findings file must hold one list named findings")
    found: set[Finding] = set()
    for value in document["findings"]:
        if not isinstance(value, dict) or set(value) != FIELDS:
            raise ScanError("each accepted finding holds exactly the fields cve, package, reason and date")
        finding = checked(value["cve"], value["package"])
        if not isinstance(value["reason"], str) or not value["reason"].strip():
            raise ScanError("each accepted finding gives its reason")
        date = value["date"]
        if not isinstance(date, str) or DATE_FORM.fullmatch(date) is None:
            raise ScanError("each accepted finding gives its date as the quoted text YYYY-MM-DD")
        try:
            datetime.date.fromisoformat(date)
        except ValueError as error:
            raise ScanError("each accepted finding gives a date that exists") from error
        if finding in found:
            raise ScanError("the accepted-findings file names a finding twice")
        found.add(finding)
    return frozenset(found)


def unaccepted(found: frozenset[Finding], accepted: frozenset[Finding]) -> list[Finding]:
    """The findings that stop a push, in order."""
    return sorted(found - accepted)
