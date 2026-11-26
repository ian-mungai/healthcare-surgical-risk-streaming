"""The scan gate of the publisher: Docker Scout's report of an image's fixable findings, held against the committed list of accepted findings.

The publisher scans each image that it built before it pushes any image. Docker Scout needs a Docker sign-in, which the computer that publishes already has
for its push. A finding is the vulnerability's ID, the package URL with its version, without qualifiers, and the artifacts that hold the package: the paths
that the report's locations name, where a library inside a jar is located at the jar. An accepted entry names one image, one artifact, the ID and the package
URL. A finding passes only when every one of its artifacts has an entry for the scanned image, so an entry never carries over to another image, artifact or
version of the package.

``image_scan/accepted_findings.yml`` holds the active list, ``findings``, and may hold a list named ``pending`` of entries that wait for the owner's
decision. The gate never reads ``pending``. The file is outside every image's sources, so a change in it changes no image tag.

Everything that is not a complete report is refused and is never read as a clean image: a missing or unreadable file, another format or tool, an object that
holds a key twice (a second, empty list of results would hide the first), a scan whose exit status is not the number 0, a run whose invocations, when it holds
the key, do not all report ``executionSuccessful`` as true, and a result that does not name exactly one package and at least one location. Only an ID, a
package URL and an artifact path that match their forms are ever shown.

The reports of Docker Scout 1.28.0 hold no ``invocations`` (read on Nov 25 2026), so for them the exit status of the scan alone says that it completed.
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
FIELDS = {"image", "artifact", "cve", "package", "reason", "date"}
IMAGE_FORM = re.compile(r"[a-z][a-z0-9_]{0,39}")
ARTIFACT_FORM = re.compile(r"[A-Za-z0-9._+~@=,%:\[\]/-]{1,300}")
# Docker Scout names a library inside a jar twice: the jar, and the jar followed by the library's group and name.
INSIDE_JAR = re.compile(r"(.+\.jar):[A-Za-z0-9._-]+:[A-Za-z0-9._-]+")
SHOWN_ARTIFACTS = 3
CVE_FORM = re.compile(r"(CVE|GHSA|GO)-[A-Za-z0-9-]{4,40}")
PACKAGE_FORM = re.compile(r"pkg:[a-z]+/[A-Za-z0-9._+~:/-]{1,200}@[A-Za-z0-9._+~:-]{1,100}")
DATE_FORM = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")


class ScanError(FixedTextError, ValueError):
    """A fixed-text failure of a report or of the accepted list. The text holds no value that was read from either."""


@dataclass(frozen=True, order=True)
class Finding:
    cve: str
    package: str
    artifacts: tuple[str, ...]


@dataclass(frozen=True, order=True)
class Accepted:
    image: str
    artifact: str
    cve: str
    package: str


def scan_arguments(tag: str, report: Path) -> list[str]:
    """The ``docker scout`` arguments: the fixable findings of the linux/amd64 image that the build loaded on this computer, as SARIF in ``report``."""
    return ["scout", "cves", "--platform", "linux/amd64", "--only-fixed", "--format", "sarif", "--output", str(report), f"local://{tag}"]


def checked(cve: object, package: object) -> tuple[str, str]:
    if not (isinstance(cve, str) and CVE_FORM.fullmatch(cve) and isinstance(package, str) and PACKAGE_FORM.fullmatch(package)):
        raise ScanError("a finding does not name a vulnerability ID and a package URL with its version in their forms")
    return cve, package


def checked_artifact(artifact: object) -> str:
    if not (isinstance(artifact, str) and ARTIFACT_FORM.fullmatch(artifact)):
        raise ScanError("an artifact is not a path in its form")
    return artifact


def located(result: Any) -> tuple[str, ...]:
    """The artifacts of one SARIF result, each once and in order."""
    try:
        uris = [location["physicalLocation"]["artifactLocation"]["uri"] for location in result["locations"]]
    except (KeyError, TypeError) as error:
        raise ScanError("a result of the scan report lacks a location") from error
    if not uris:
        raise ScanError("a result of the scan report lacks a location")
    paths = [checked_artifact(uri) for uri in uris]
    return tuple(sorted({match.group(1) if (match := INSIDE_JAR.fullmatch(path)) else path for path in paths}))


def reported(result: Any) -> Finding:
    """The finding of one SARIF result: its rule is the vulnerability's ID, one line of its message names the package and its locations name the artifacts."""
    try:
        cve = result["ruleId"]
        lines = str(result["message"]["text"]).split("\n")
    except (KeyError, TypeError) as error:
        raise ScanError("a result of the scan report lacks its rule or its message") from error
    packages = [match.group(1) for line in lines if (match := re.fullmatch(r"Package\s*:(\S+)\s*", line))]
    if len(packages) != 1:
        raise ScanError("a result of the scan report does not name exactly one package")
    return Finding(*checked(cve, urllib.parse.unquote(packages[0].partition("?")[0])), located(result))


def unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """One JSON object. ``json.loads`` keeps the last value of a repeated key without a word, so a repeated key is refused."""
    found = dict(pairs)
    if len(found) != len(pairs):
        raise ScanError("the scan report holds a key twice in one object")
    return found


def require_success(run: dict[str, Any], exit_status: object) -> None:
    """The scan completed: its exit status is the number 0, and a run that holds invocations says so in every one of them, of which there is at least one."""
    if type(exit_status) is not int or exit_status != 0:
        raise ScanError("the scan did not end with the exit status 0")
    if "invocations" not in run:
        return
    invocations = run["invocations"]
    if not (isinstance(invocations, list) and invocations and all(isinstance(one, dict) and one.get("executionSuccessful") is True for one in invocations)):
        raise ScanError("the scan report does not say that its execution succeeded")


def read_report(text: str, exit_status: object) -> frozenset[Finding]:
    """The findings of a complete SARIF 2.1.0 report of Docker Scout, from a scan that ended with ``exit_status``."""
    try:
        document = json.loads(text, object_pairs_hook=unique_keys)
        version = document["version"]
        (run,) = document["runs"]
        tool = run["tool"]["driver"]["name"]
        results = run["results"]
    except ScanError:
        raise
    except (ValueError, KeyError, TypeError, RecursionError) as error:
        raise ScanError("the scan report is not one complete SARIF run") from error
    if version != "2.1.0" or tool != "docker scout" or not isinstance(results, list):
        raise ScanError("the scan report is not a SARIF 2.1.0 report of docker scout with a list of results")
    require_success(run, exit_status)
    return frozenset(reported(result) for result in results)


def accepted_entry(value: Any) -> Accepted:
    """One entry of a list: exactly the image's name, the artifact, the vulnerability's ID, the package URL with its version, the reason and the date."""
    if not isinstance(value, dict) or set(value) != FIELDS:
        raise ScanError("each accepted finding holds exactly the fields image, artifact, cve, package, reason and date")
    cve, package = checked(value["cve"], value["package"])
    image = value["image"]
    if not isinstance(image, str) or IMAGE_FORM.fullmatch(image) is None:
        raise ScanError("each accepted finding gives the name of its image")
    artifact = checked_artifact(value["artifact"])
    if not isinstance(value["reason"], str) or not value["reason"].strip():
        raise ScanError("each accepted finding gives its reason")
    date = value["date"]
    if not isinstance(date, str) or DATE_FORM.fullmatch(date) is None:
        raise ScanError("each accepted finding gives its date as the quoted text YYYY-MM-DD")
    try:
        datetime.date.fromisoformat(date)
    except ValueError as error:
        raise ScanError("each accepted finding gives a date that exists") from error
    return Accepted(image, artifact, cve, package)


def load_accepted(path: Path) -> frozenset[Accepted]:
    """The entries of the active list. The list named ``pending`` may be there beside it and is never read."""
    try:
        document = load_yaml(path.read_text())
    except (YamlError, ValueError, OSError) as error:
        raise ScanError("the accepted-findings file cannot be read as YAML") from error
    if not isinstance(document, dict) or set(document) - {"pending"} != {"findings"} or not isinstance(document["findings"], list):
        raise ScanError("the accepted-findings file must hold one list named findings, and may hold one named pending beside it")
    found: set[Accepted] = set()
    for value in document["findings"]:
        accepted = accepted_entry(value)
        if accepted in found:
            raise ScanError("the accepted-findings file names a finding twice")
        found.add(accepted)
    return frozenset(found)


def unaccepted(image: str, found: frozenset[Finding], accepted: frozenset[Accepted]) -> list[Finding]:
    """The findings of one image that stop a push, in order. Each names only its artifacts that no entry for this image accepts."""
    left: list[Finding] = []
    for finding in found:
        missing = tuple(artifact for artifact in finding.artifacts if Accepted(image, artifact, finding.cve, finding.package) not in accepted)
        if missing:
            left.append(Finding(finding.cve, finding.package, missing))
    return sorted(left)


def shown(finding: Finding) -> str:
    """The text of a refused finding. A system package has many files, so only the first artifacts are named and the others are counted."""
    more = len(finding.artifacts) - SHOWN_ARTIFACTS
    text = f"{finding.cve} {finding.package} in {' '.join(finding.artifacts[:SHOWN_ARTIFACTS])}"
    return f"{text} and {more} more" if more > 0 else text
