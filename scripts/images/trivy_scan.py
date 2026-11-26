"""The second scanner of the publisher's gate: the pinned Trivy's JSON report of an image's fixable HIGH and CRITICAL findings, and its pinned databases.

``--ignore-unfixed`` matches the rule of the Docker Scout side (``--only-fixed``): both count only a finding that a release fixes, because an unfixed one
cannot be fixed by a change here. The severities are the owner's: HIGH and CRITICAL (Docker Scout still counts every severity). Every vulnerability that the
report holds is a finding; this module drops none of them.

Trivy reads only the image that the local Docker engine holds, never a registry. It runs in a new folder with an empty configuration and an empty module
folder, with no ignore file and with no ``TRIVY_`` variable of the caller, so nothing on the computer can hide a finding.

A finding is matched to ``image_scan/accepted_findings.yml`` in the same way as one of Docker Scout (``scripts.images.scan``):

- A library file that Trivy names (``PkgPath``) is the artifact, with a leading slash. A library that is shaded into a jar is at the jar, which is also the
  artifact of Docker Scout. A path that goes further into a jar stays whole, so an entry for the outer jar never covers it.
- A Go or Rust program file is its own result, and that file is the artifact.
- Every other finding without a path has no artifact that an entry could name, so no entry ever accepts it. Two kinds have none: an operating system package,
  and a library that Trivy reads from an SBOM file in the image. The package URL of an operating system package is Trivy's: the binary package, with the epoch
  in the version as Docker Scout writes it.

Everything that is not a complete report of the pinned Trivy for the scanned image is refused, as in ``scripts.images.scan``: a key held twice, a number that
standard JSON does not have, another tool or version, another image, an exit status other than 0, an operating system that was not detected, is not
supported or is past its end of life, and a result without its list of packages, each of which is a mapping with its name. Only an ID, a package URL and an
artifact path in their forms are shown.

The vulnerability database and the Java database (which names jars without their own metadata) are pinned by digest in
``image_scan/trivy_databases.yml``, so the same pins give the same findings. ``make trivy-db`` (``python -m scripts.images.trivy_scan``) downloads both into
a new folder under ``.tools/trivy_db`` that is named after those digests, and writes the file ``complete`` in it after the last download, with the size and the
SHA-256 of each database file. Trivy reads a damaged Java database as one without findings, so a folder is read only while each file has the recorded nonzero
size and SHA-256. The publisher checks this before each build and again before each Trivy scan, and ``make trivy-db`` does nothing only when such a folder is
there. The record shows what the download wrote, not that the registry's content was whole: the pinned digest and the exit status of Trivy stand for that. A
pinned database holds nothing published after its date: refresh the pins as the README says. Docker Scout is not pinned.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import urllib.parse
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.images.scan import ACCEPTED_FILE, Finding, ScanError, checked, checked_artifact, refused_constant, require_exit_zero, unique_keys
from scripts.ingestion.folder import list_folder
from scripts.ingestion.notice import describe_error
from scripts.ingestion.output_file import write_new_file
from scripts.process import TimeoutExpired, run_command, timeout_text
from scripts.quality import install_tools
from scripts.stack.yaml_text import YamlError, load_yaml

VERSION = install_tools.TRIVY.version
DATABASE_FILE = ACCEPTED_FILE.with_name("trivy_databases.yml")
CACHE = install_tools.TOOLS / "trivy_db"
DATABASE_FILES = ("db/trivy.db", "db/metadata.json", "java-db/trivy-java.db", "java-db/metadata.json")
COMPLETE = "complete"
REFERENCE = re.compile(r"[a-z0-9.-]+(?::[0-9]+)?(?:/[a-z0-9._-]+)+@sha256:[0-9a-f]{64}")
DOWNLOAD_SECONDS = 1800
OS_FAMILIES = ("debian", "ubuntu")
# Trivy gives each program file of these types its own result, named by the file's path.
FILE_TARGETS = {"gobinary", "rustbinary"}
NO_PATH = "(no path in Trivy's report)"
INCOMPLETE = "the report is not one complete report of the pinned Trivy"


@dataclass(frozen=True)
class Databases:
    db: str
    java_db: str


def load_databases(path: Path) -> Databases:
    try:
        document = load_yaml(path.read_text())
    except (YamlError, ValueError, OSError) as error:
        raise ScanError("the Trivy database pins cannot be read as YAML") from error
    if not (
        isinstance(document, dict)
        and set(document) == {"db", "java_db"}
        and all(isinstance(value, str) and REFERENCE.fullmatch(value) for value in document.values())
    ):
        raise ScanError("the Trivy database pins must hold exactly db and java_db, each a repository pinned by digest (repository@sha256:<64 hex digits>)")
    return Databases(document["db"], document["java_db"])


def folder_prefix(databases: Databases) -> str:
    """The start of the name of each download folder of these pins."""
    return "_".join(reference.rpartition(":")[2][:16] for reference in (databases.db, databases.java_db)) + "."


def written(folder: Path) -> bool:
    """Whether ``folder`` holds every database file, none of them empty."""
    return all((folder / name).is_file() and (folder / name).stat().st_size > 0 for name in DATABASE_FILES)


def file_record(path: Path) -> dict[str, object]:
    with path.open("rb") as file:
        return {"size": path.stat().st_size, "sha256": hashlib.file_digest(file, "sha256").hexdigest()}


def intact(folder: Path) -> bool:
    """Whether the download of ``folder`` completed and each database file still has the nonzero size and the SHA-256 that the mark records. The files are
    hashed only after every size matches. A mark or a file that cannot be read is not intact."""
    try:
        recorded = json.loads((folder / COMPLETE).read_text(), object_pairs_hook=unique_keys)
        if not (isinstance(recorded, dict) and set(recorded) == set(DATABASE_FILES) and written(folder)):
            return False
        if not all(isinstance(value, dict) and value.get("size") == (folder / name).stat().st_size for name, value in recorded.items()):
            return False
        return all(file_record(folder / name) == value for name, value in recorded.items())
    except OSError, ValueError, RecursionError:
        return False


def downloaded_folder(cache: Path, databases: Databases) -> Path | None:
    """A folder of these pins whose download completed and whose files are intact, or None."""
    try:
        folders = list_folder(cache, f"{folder_prefix(databases)}*")
    except OSError as error:
        raise ScanError("the Trivy database cache cannot be read") from error
    return next((folder for folder in folders if intact(folder)), None)


def require_databases(cache: Path, databases: Databases) -> Path:
    folder = downloaded_folder(cache, databases)
    if folder is None:
        raise ScanError("the pinned Trivy databases are not downloaded, or a file is not as its download wrote it: run make trivy-db")
    return folder


def clean_environment() -> dict[str, str]:
    """The caller's environment without any TRIVY_ variable, each of which sets an option of Trivy."""
    return {name: value for name, value in os.environ.items() if not name.upper().startswith("TRIVY")}


def workspace(folder: Path) -> Path:
    """A new working folder for Trivy: an empty configuration and an empty module folder, and no ignore file."""
    (folder / "modules").mkdir(parents=True)
    write_new_file(folder / "trivy.yaml", "")
    return folder


def own_files(work: Path) -> list[str]:
    return [f"--config={work / 'trivy.yaml'}", f"--module-dir={work / 'modules'}"]


def scan_arguments(tag: str, report: Path, databases: Path, work: Path) -> list[str]:
    """The ``trivy`` arguments: the fixable HIGH and CRITICAL findings of the local linux/amd64 image, as JSON in ``report``."""
    return [
        "image",
        *own_files(work),
        f"--cache-dir={databases}",
        "--cache-backend=memory",
        "--skip-db-update",
        "--skip-java-db-update",
        "--offline-scan",
        "--skip-version-check",
        "--disable-telemetry",
        "--image-src=docker",
        "--platform=linux/amd64",
        "--scanners=vuln",
        "--pkg-types=os,library",
        "--severity=HIGH,CRITICAL",
        "--ignore-unfixed",
        "--list-all-pkgs",
        "--ignorefile=",
        "--secret-config=",
        "--no-progress",
        "--format=json",
        f"--output={report}",
        tag,
    ]


def run_trivy(trivy: str, arguments: Sequence[str], work: Path, seconds: int, label: str) -> int:
    """Run Trivy in ``work`` and return its exit status. None of its output is returned."""
    try:
        return run_command(trivy, arguments, cwd=work, env=clean_environment(), timeout=seconds).returncode
    except TimeoutExpired as error:
        raise ScanError(timeout_text(label, seconds)) from error
    except OSError as error:
        raise ScanError("trivy could not be started: it is not installed, not on the path or not executable: run make setup") from error


def download(trivy: str, folder: Path, databases: Databases) -> None:
    """Download both databases into ``folder``, and mark it complete only when every file is there and not empty, with the record of each file."""
    downloads = (("--download-db-only", "--db-repository", databases.db), ("--download-java-db-only", "--java-db-repository", databases.java_db))
    with tempfile.TemporaryDirectory(prefix="trivy_") as scratch:
        work = workspace(Path(scratch) / "work")
        for only, option, reference in downloads:
            arguments = ["image", *own_files(work), f"--cache-dir={folder}", only, f"{option}={reference}"]
            arguments += ["--skip-version-check", "--disable-telemetry", "--no-progress"]
            status = run_trivy(trivy, arguments, work, DOWNLOAD_SECONDS, "the download of a Trivy database")
            if status != 0:
                raise ScanError(f"the download of a Trivy database failed with the exit status {status} of trivy")
    if not written(folder):
        raise ScanError("the download of the Trivy databases did not write every database file")
    write_new_file(folder / COMPLETE, json.dumps({name: file_record(folder / name) for name in DATABASE_FILES}))


def fetch_databases(trivy: str, cache: Path, databases: Databases) -> bool:
    """Download both pinned databases into a new folder under ``cache``, and return False when an intact one is there. A download that fails or is
    interrupted removes its folder."""
    if downloaded_folder(cache, databases) is not None:
        return False
    cache.mkdir(parents=True, exist_ok=True)
    folder = Path(tempfile.mkdtemp(prefix=folder_prefix(databases), dir=cache))
    try:
        download(trivy, folder, databases)
    finally:
        if not (folder / COMPLETE).is_file():
            shutil.rmtree(folder)
    return True


def package_url(purl: object) -> object:
    """Trivy's package URL in the form of Docker Scout's: without qualifiers, with percent codes read, and with a Debian epoch inside the version."""
    if not isinstance(purl, str):
        return purl
    base, _, query = purl.partition("?")
    base = urllib.parse.unquote(base)
    epochs = urllib.parse.parse_qs(query, keep_blank_values=True).get("epoch")
    if epochs is None:
        return base
    name, at, version = base.rpartition("@")
    if not (at and name.startswith("pkg:deb/") and len(epochs) == 1 and re.fullmatch(r"[0-9]{1,9}", epochs[0])):
        raise ScanError("a package URL holds an epoch that is not one number of a Debian package")
    return f"{name}@{epochs[0]}:{version}"


def absolute(path: object) -> str:
    """A path of the report, which Trivy writes from the image's root without the leading slash."""
    if not (isinstance(path, str) and path and not path.startswith("/")):
        raise ScanError("an artifact is not a path in its form")
    return checked_artifact(f"/{path}")


def reported(kind: str, type_: str, target: str, value: Any) -> Finding:
    try:
        cve, purl = value["VulnerabilityID"], value["PkgIdentifier"]["PURL"]
    except (KeyError, TypeError) as error:
        raise ScanError("a vulnerability of the report lacks its ID or its package URL") from error
    cve, package = checked(cve, package_url(purl))
    if kind == "os-pkgs":
        if "PkgPath" in value:
            raise ScanError("an operating system package gives a path, which the pinned Trivy does not write")
        artifact = NO_PATH
    elif "PkgPath" in value:
        artifact = absolute(value["PkgPath"])
    elif type_ in FILE_TARGETS:
        artifact = absolute(target)
    else:
        artifact = NO_PATH
    return Finding(cve, package, (artifact,))


def os_family(metadata: Any) -> str:
    found = metadata.get("OS") if isinstance(metadata, dict) else None
    if not (
        isinstance(found, dict)
        and isinstance(found.get("Family"), str)
        and found["Family"] in OS_FAMILIES
        and isinstance(found.get("Name"), str)
        and found["Name"]
        and found.get("EOSL", False) is False
    ):
        raise ScanError("the report does not name a supported operating system within its support life, so its packages may not have been checked")
    return str(found["Family"])


def checked_result(value: Any) -> tuple[str, str, str, list[Any]]:
    """The class, type, target and vulnerabilities of one result, which lists the packages that it checked."""
    try:
        kind, type_, target, packages = value["Class"], value["Type"], value["Target"], value["Packages"]
        found = value.get("Vulnerabilities", [])
    except (KeyError, TypeError) as error:
        raise ScanError("a result of the report lacks its class, its type, its target or its packages") from error
    if not (
        kind in ("os-pkgs", "lang-pkgs")
        and isinstance(type_, str)
        and isinstance(target, str)
        and isinstance(packages, list)
        and packages
        and all(isinstance(package, dict) and isinstance(package.get("Name"), str) and package["Name"] for package in packages)
        and isinstance(found, list)
    ):
        raise ScanError("a result of the report is not a list of checked packages and their vulnerabilities")
    return kind, type_, target, found


def read_report(text: str, exit_status: object, tag: str) -> frozenset[Finding]:
    """The findings of a complete JSON report of the pinned Trivy about ``tag``, from a scan that ended with ``exit_status``."""
    try:
        document = json.loads(text, object_pairs_hook=unique_keys, parse_constant=refused_constant)
        schema, version = document["SchemaVersion"], document["Trivy"]["Version"]
        scanned = (document["ArtifactType"], document["ArtifactName"])
        metadata, results = document["Metadata"], document["Results"]
    except ScanError:
        raise
    except (ValueError, KeyError, TypeError, RecursionError) as error:
        raise ScanError(INCOMPLETE) from error
    if type(schema) is not int or schema != 2 or version != VERSION or scanned != ("container_image", tag) or not isinstance(results, list):
        raise ScanError(INCOMPLETE)
    require_exit_zero(exit_status)
    family = os_family(metadata)
    checked_results = [checked_result(value) for value in results]
    if [type_ for kind, type_, _, _ in checked_results if kind == "os-pkgs"] != [family]:
        raise ScanError("the report does not hold exactly one result of the packages of the operating system")
    return frozenset(reported(kind, type_, target, value) for kind, type_, target, found in checked_results for value in found)


def main(argv: Sequence[str] | None = None, trivy: str = str(install_tools.TRIVY.path), cache: Path = CACHE, database_file: Path = DATABASE_FILE) -> int:
    """Download the pinned databases of ``database_file`` with ``trivy`` into ``cache``."""
    if list(sys.argv[1:] if argv is None else argv):
        sys.stderr.write("usage: python -m scripts.images.trivy_scan\n")
        return 2
    try:
        fetched = fetch_databases(trivy, cache, load_databases(database_file))
    except ScanError as error:
        sys.stderr.write(f"trivy-db: {describe_error(error)}\n")
        return 1
    sys.stdout.write(f"the pinned Trivy databases are {'downloaded' if fetched else 'already downloaded'}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
