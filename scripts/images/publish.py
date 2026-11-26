"""Build each project image on this computer from the tracked files of HEAD, scan it, push it to the private Docker Hub repository and pin its pushed digest.

The images come from ``compose_stack_built_images`` in the group variables. Each image's folder is staged from ``git archive`` of HEAD, so an untracked
file and a Git-ignored file (the handoff, the project standard, ``.env``) cannot enter an image. The pin file is left out of every folder, because a pin of an
image must not change the hash of the image that holds the Compose folder. The tag holds the hash of the folder, so the same files give the same tag.

The pin file is the authority and the registry is never read. An image whose tag the pin file already holds is neither built nor pushed, and its pin stays as
it is. An image whose tag the pin file lacks is built, pushed and pinned. The digest is the one that ``docker push`` reports for the exact tag, in its line
``<tag>: digest: sha256:<64 hex digits> size: <number>`` on standard output. The push asks for the one linux/amd64 manifest (``--platform``), which the build
loaded (``buildx build --platform linux/amd64 --load``), so the pinned digest is the manifest that the host pulls by digest. Any output without exactly one
such line for the tag is refused. The pin file is written once, after every image has resolved, so a failure leaves every pin as it was.

Accepted behavior: a push that succeeded but whose pin was not saved is pushed again on the next run and may overwrite that unpinned tag. Nothing pins it, so
no host pulls it.

Accepted limit (owner, Nov 25 2026): two publishes at the same time can replace the digest of a pinned tag, because each reads the pin file before the other has
written it. The command takes no lock. Trigger to revisit: more than one person or process publishes. Today only the owner publishes, by hand.

Scan gate: every image that the run builds is scanned with Docker Scout (``scripts.images.scan``) and with the pinned Trivy and its pinned databases
(``scripts.images.trivy_scan``) before any image is pushed. One finding of either scanner that the committed list ``image_scan/accepted_findings.yml`` does
not name, in any of the images, refuses the whole push, and so does a scan that fails or writes no complete report. The refusal names each such finding of
each image and the scanner that reported it. An image to build needs the pinned databases (``make trivy-db``) before its build. An image whose tag the pin
file already holds is not pushed and is not scanned again, so a finding published after its push shows only at a scan run by hand or when its sources change.

The command relies on the ``docker login`` that this computer already has, which Docker Scout uses too. It reads, prints and writes no token, and every
message is fixed text: no output of Docker, Trivy or Git is ever shown. The one exception is a refused finding's vulnerability ID, package URL and artifact
paths, each shown only when it matches its form.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import tarfile
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.images import pins, scan, trivy_scan
from scripts.ingestion.notice import FixedTextError, describe_error
from scripts.process import TimeoutExpired, run_command, timeout_text
from scripts.project_config import ENV_FILE, ConfigurationError, load_configuration
from scripts.quality import install_tools

PLATFORM = "linux/amd64"
GIT_SECONDS = 120
BUILD_SECONDS = 1800
PUSH_SECONDS = 1800
SCAN_SECONDS = 900
STARTED_TEXT = "could not be started: it is not installed, not on the path or not executable"
NOT_PLAIN = "a source holds a symbolic link or another file that is not a plain file"


class PublishError(FixedTextError):
    """A fixed-text failure. The text holds no output of Docker or Git. ``exit_status`` is the exit status of the Docker command that failed, if any.
    ``findings`` holds one line for each finding that refused a push."""

    def __init__(self, message: str, exit_status: int | None = None, findings: tuple[str, ...] = ()) -> None:
        super().__init__(message)
        self.exit_status = exit_status
        self.findings = findings


@dataclass(frozen=True)
class Settings:
    root: Path
    docker: str
    repository: str
    pin_file: Path
    images: list[dict[str, Any]]
    accepted_file: Path = scan.ACCEPTED_FILE
    trivy: str = str(install_tools.TRIVY.path)
    trivy_databases: Path = trivy_scan.DATABASE_FILE
    trivy_cache: Path = trivy_scan.CACHE


def default_settings(env_file: Path = ENV_FILE) -> Settings:
    """The settings of this computer: the repository comes from IMAGE_REPOSITORY in the ignored ``.env`` file, like the bucket."""
    try:
        repository = load_configuration(env_file).get("IMAGE_REPOSITORY")
    except ConfigurationError as error:
        raise PublishError(str(error)) from error
    if not repository:
        raise PublishError("IMAGE_REPOSITORY is missing in .env: copy the line from .env.example and fill it in")
    return Settings(root=pins.ROOT, docker="docker", repository=repository, pin_file=pins.PIN_FILE, images=pins.load_images())


def git_environment() -> dict[str, str]:
    """The caller's environment without any GIT_ variable, which a hook sets to its own repository."""
    return {name: value for name, value in os.environ.items() if not name.startswith("GIT_")}


def git(settings: Settings, arguments: Sequence[str]) -> str:
    try:
        done = run_command("git", arguments, cwd=settings.root, env=git_environment(), timeout=GIT_SECONDS)
    except TimeoutExpired as error:
        raise PublishError(timeout_text("git", GIT_SECONDS)) from error
    except OSError as error:
        raise PublishError(f"git {STARTED_TEXT}") from error
    if done.returncode != 0:
        raise PublishError("git could not read the repository")
    return done.stdout


def pin_path(settings: Settings) -> str:
    return settings.pin_file.relative_to(settings.root).as_posix()


def require_clean(settings: Settings) -> None:
    """Every change but one in the pin file, which this command writes and the owner commits afterwards, stops the command."""
    if git(settings, ["status", "--porcelain", "--untracked-files=all", "--", ".", f":(exclude){pin_path(settings)}"]).strip():
        raise PublishError("the working tree is not clean: commit or remove every change first")


def stage(settings: Settings, image: dict[str, Any], folder: Path) -> str:
    """Put the image's sources from HEAD in ``folder`` and return the hash of the folder: each path, whether it is executable and the hash of its content."""
    name = image["name"]
    archive = folder.parent / f"{name}.tar"
    try:
        git(settings, ["archive", "--format=tar", f"--output={archive}", "HEAD", "--", *image["sources"]])
    except PublishError as error:
        raise PublishError(f"{name}: a source is not a tracked path of HEAD") from error
    entries: list[list[Any]] = []
    with tarfile.open(archive) as tar:
        members = [member for member in tar.getmembers() if member.name != pin_path(settings)]
        for member in members:
            if not (member.isfile() or member.isdir()):
                raise PublishError(f"{name}: {NOT_PLAIN}")
        tar.extractall(folder, members=members, filter="data")
        for member in sorted(members, key=lambda item: item.name):
            if member.isfile():
                content = (folder / member.name).read_bytes()
                entries.append([member.name, bool(member.mode & 0o111), hashlib.sha256(content).hexdigest()])
    archive.unlink()
    if not entries:
        raise PublishError(f"{name}: the sources hold no file")
    return hashlib.sha256(json.dumps(entries).encode()).hexdigest()


def image_tag(settings: Settings, image: dict[str, Any], folder_hash: str) -> str:
    version = str(image["tag"]).partition(":")[2]
    return f"{settings.repository}:{image['name']}-{version}-{folder_hash[:12]}"


def build_arguments(image: dict[str, Any], folder: Path, tag: str) -> list[str]:
    """The ``docker buildx build`` arguments, which have the same sources, context, Dockerfile and named contexts as the host role used."""
    context = image.get("context", image["sources"][0])
    arguments = ["buildx", "build", "--platform", PLATFORM, "--load", "--tag", tag]
    if "dockerfile" in image:
        arguments.append(f"--file={folder / image['dockerfile']}")
    arguments += [f"--build-context={key}={folder / value}" for key, value in image.get("build_contexts", {}).items()]
    return [*arguments, str(folder if context == "." else folder / context)]


def docker(settings: Settings, arguments: Sequence[str], seconds: int, label: str) -> tuple[int, str, str]:
    try:
        done = run_command(settings.docker, arguments, timeout=seconds)
    except TimeoutExpired as error:
        raise PublishError(timeout_text(label, seconds)) from error
    except OSError as error:
        raise PublishError(f"docker {STARTED_TEXT}") from error
    return done.returncode, done.stdout, done.stderr


def build_image(settings: Settings, image: dict[str, Any], folder: Path, tag: str) -> None:
    code, _, _ = docker(settings, build_arguments(image, folder, tag), BUILD_SECONDS, "docker build")
    if code != 0:
        raise PublishError(f"{image['name']}: the build failed")


def pushed_digest(name: str, tag: str, output: str) -> str:
    """The digest in the one line of ``docker push`` output that has the form ``<tag>: digest: sha256:<64 hex digits> size: <number>`` for this tag.
    The tag in the line is the part after the repository. No such line, or more than one, is refused."""
    label = re.escape(tag.rpartition(":")[2])
    found = [match.group(1) for line in re.split(r"\r?\n", output) if (match := re.fullmatch(rf"{label}: digest: (sha256:[0-9a-f]{{64}}) size: [0-9]+", line))]
    if len(found) != 1:
        raise PublishError(f"{name}: the push output holds no digest line of the form <tag>: digest: sha256:<64 hex digits> size: <number> for the pushed tag")
    return found[0]


def push_image(settings: Settings, name: str, tag: str) -> str:
    """Push the tag as its one linux/amd64 manifest and return the digest that the push reported for it. A failed push ends with the CLI's own exit status."""
    code, out, _ = docker(settings, ["push", "--platform", PLATFORM, tag], PUSH_SECONDS, "docker push")
    if code != 0:
        raise PublishError(
            f"{name}: the push failed with the exit status {code} of docker: check docker login on this computer, then run the command again", code
        )
    return pushed_digest(name, tag, out)


def new_tag(settings: Settings, image: dict[str, Any], found: dict[str, dict[str, str]], folder: Path) -> str | None:
    """Stage the image in ``folder`` and return its tag. A tag that the pin file holds keeps its pin: None comes back."""
    folder.mkdir()
    tag = image_tag(settings, image, stage(settings, image, folder))
    return None if found.get(image["name"], {}).get("tag") == tag else tag


def scout_findings(settings: Settings, name: str, tag: str, scratch: Path) -> frozenset[scan.Finding]:
    """Scan the built image with Docker Scout into a new report and return its findings."""
    report = scratch / f"{name}.sarif"
    code, _, _ = docker(settings, scan.scan_arguments(tag, report), SCAN_SECONDS, "docker scout cves")
    if code != 0:
        raise PublishError(
            f"{name}: the scan failed with the exit status {code} of docker: check that docker scout works and is signed in on this computer", code
        )
    try:
        return scan.read_report(report.read_text(), code)
    except (OSError, UnicodeDecodeError) as error:
        raise PublishError(f"{name}: the scan wrote no readable report") from error
    except scan.ScanError as error:
        raise PublishError(f"{name}: {error}") from error


def trivy_findings(settings: Settings, name: str, tag: str, databases: Path, scratch: Path) -> frozenset[scan.Finding]:
    """Scan the built image with Trivy and the pinned databases in ``databases`` into a new report and return its findings."""
    work = trivy_scan.workspace(scratch / f"{name}.trivy")
    report = scratch / f"{name}.trivy.json"
    try:
        code = trivy_scan.run_trivy(settings.trivy, trivy_scan.scan_arguments(tag, report, databases, work), work, SCAN_SECONDS, "trivy image")
    except scan.ScanError as error:
        raise PublishError(str(error)) from error
    if code != 0:
        raise PublishError(f"{name}: the Trivy scan failed with the exit status {code}", code)
    try:
        return trivy_scan.read_report(report.read_text(), code, tag)
    except (OSError, UnicodeDecodeError) as error:
        raise PublishError(f"{name}: the Trivy scan wrote no readable report") from error
    except scan.ScanError as error:
        raise PublishError(f"{name}: Trivy: {error}") from error


def build_and_scan(
    settings: Settings, found: dict[str, dict[str, str]], accepted: frozenset[scan.Accepted], databases: trivy_scan.Databases, scratch: Path
) -> dict[str, str]:
    """Build every image whose tag the pin file lacks, scan it with both scanners and return each one's tag by its name. Every image is scanned before the
    refusal, so that it names every finding at once."""
    built: dict[str, str] = {}
    refused: list[str] = []
    for image in settings.images:
        name = image["name"]
        tag = new_tag(settings, image, found, scratch / name)
        if tag is None:
            continue
        try:
            folder = trivy_scan.require_databases(settings.trivy_cache, databases)
        except scan.ScanError as error:
            raise PublishError(str(error)) from error
        build_image(settings, image, scratch / name, tag)
        built[name] = tag
        reports = (("Docker Scout", scout_findings(settings, name, tag, scratch)), ("Trivy", trivy_findings(settings, name, tag, folder, scratch)))
        for scanner, findings in reports:
            refused += [f"{name}: {scanner}: {scan.shown(finding)}" for finding in scan.unaccepted(name, findings, accepted)]
    if refused:
        raise PublishError(
            "nothing was pushed: a scan found findings that image_scan/accepted_findings.yml does not name: fix each one, or accept one that no release can "
            "fix with its reason and date",
            findings=tuple(refused),
        )
    return built


def publish(settings: Settings) -> list[str]:
    """Publish every image of the list in order and write the pin file once, after the last image. Every image is built and scanned before the first push.
    A failure before the pin file is written changes no pin: the next run pushes again every tag that the pin file still lacks."""
    require_clean(settings)
    try:
        found = pins.load(settings.pin_file)
        accepted = scan.load_accepted(settings.accepted_file)
        databases = trivy_scan.load_databases(settings.trivy_databases)
    except (pins.PinError, scan.ScanError) as error:
        raise PublishError(str(error)) from error
    if any(not value["digest"].startswith(settings.repository + "@") for value in found.values()):
        raise PublishError("the pin file holds another repository than IMAGE_REPOSITORY")
    resolved = dict(found)
    messages: list[str] = []
    with tempfile.TemporaryDirectory(prefix="images_") as scratch:
        built = build_and_scan(settings, found, accepted, databases, Path(scratch))
        for image in settings.images:
            name = image["name"]
            if name not in built:
                messages.append(f"{name}: already pinned")
                continue
            digest = push_image(settings, name, built[name])
            resolved[name] = {"tag": built[name], "digest": f"{settings.repository}@{digest}"}
            messages.append(f"{name}: pushed and pinned")
    if resolved != found:
        try:
            pins.write(settings.pin_file, resolved)
        except pins.PinError as error:
            raise PublishError(str(error)) from error
    return messages


def main(argv: Sequence[str] | None = None, settings: Settings | None = None) -> int:
    if list(sys.argv[1:] if argv is None else argv):
        sys.stderr.write("usage: python -m scripts.images.publish\n")
        return 2
    try:
        for message in publish(settings or default_settings()):
            sys.stdout.write(message + "\n")
    except PublishError as error:
        sys.stderr.write(f"publish: {describe_error(error)}\n")
        for line in error.findings:
            sys.stderr.write(f"publish: {line}\n")
        status = error.exit_status
        return status if status is not None and 0 < status < 256 else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
