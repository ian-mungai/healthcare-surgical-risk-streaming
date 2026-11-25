"""Build each project image on this computer from the tracked files of HEAD, push it to the private Docker Hub repository and pin the digest it was given.

The images come from ``compose_stack_built_images`` in the group variables. Each image's folder is staged from ``git archive`` of HEAD, so an untracked
file and a Git-ignored file (the handoff, the project standard, ``.env``) cannot enter an image. The pin file is left out of every folder, because a pin of an
image must not change the hash of the image that holds the Compose folder. The tag holds the hash of the folder, so the same files give the same tag, and a tag
that the repository already holds is never built or pushed again, and a pin that the pin file already holds must equal the digest that the repository gives
its tag. A push follows only when the repository's own answer says that the tag is absent. The pin file is written once, after every image has resolved.
The command relies on the ``docker login`` that this computer already has.
It reads, prints and writes no token, and every message is fixed text: no output of Docker or Git is ever shown.
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

from scripts.images import pins
from scripts.process import TimeoutExpired, run_command, timeout_text
from scripts.project_config import ENV_FILE, ConfigurationError, load_configuration

PLATFORM = "linux/amd64"
GIT_SECONDS = 120
BUILD_SECONDS = 1800
PUSH_SECONDS = 1800
INSPECT_SECONDS = 120
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
LOGIN_WORDS = ("denied", "unauthorized", "authentication required", "requested access")
STARTED_TEXT = "could not be started: it is not installed, not on the path or not executable"
LOGIN_TEXT = "the push was refused: run docker login on this computer, then run the command again"
NOT_PLAIN = "a source holds a symbolic link or another file that is not a plain file"


class PublishError(Exception):
    """A fixed-text failure. The text holds no output of Docker or Git."""


@dataclass(frozen=True)
class Settings:
    root: Path
    docker: str
    repository: str
    pin_file: Path
    images: list[dict[str, Any]]


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


def docker(settings: Settings, arguments: Sequence[str], seconds: int, label: str) -> tuple[int, str]:
    try:
        done = run_command(settings.docker, arguments, timeout=seconds)
    except TimeoutExpired as error:
        raise PublishError(timeout_text(label, seconds)) from error
    except OSError as error:
        raise PublishError(f"docker {STARTED_TEXT}") from error
    return done.returncode, (done.stdout + done.stderr).lower()


def build_image(settings: Settings, image: dict[str, Any], folder: Path, tag: str) -> None:
    code, _ = docker(settings, build_arguments(image, folder, tag), BUILD_SECONDS, "docker build")
    if code != 0:
        raise PublishError(f"{image['name']}: the build failed")


def push_image(settings: Settings, name: str, tag: str) -> None:
    code, output = docker(settings, ["push", tag], PUSH_SECONDS, "docker push")
    if code != 0:
        raise PublishError(LOGIN_TEXT if any(word in output for word in LOGIN_WORDS) else f"{name}: the push failed")


def remote_digest(settings: Settings, name: str, tag: str) -> str | None:
    """The digest that the repository holds for the tag, or None only when its own answer says that this tag is absent (``ERROR: <tag>: not found``).

    Any other failure of the read (the network, a timeout, another not-found text, a missing login) is a refusal, so a push never follows a read that failed."""
    code, output = docker(settings, ["buildx", "imagetools", "inspect", "--format", "{{.Manifest.Digest}}", tag], INSPECT_SECONDS, "docker inspect")
    if code == 0:
        return output.strip()
    if re.fullmatch(rf"error: (?:docker\.io/)?{re.escape(tag.lower())}: not found", output.strip()):
        return None
    raise PublishError(LOGIN_TEXT if any(word in output for word in LOGIN_WORDS) else f"{name}: the repository could not be read")


def pin_image(found: dict[str, dict[str, str]], name: str, tag: str, digest: str, repository: str) -> dict[str, dict[str, str]]:
    """The pins with this image's entry set. A tag that is already pinned with another digest is refused: a pushed tag never changes."""
    if not DIGEST.fullmatch(digest):
        raise PublishError(f"{name}: the repository returned a digest that is not sha256 and 64 hex digits")
    current = found.get(name)
    if current is not None and current["tag"] == tag and current["digest"] != f"{repository}@{digest}":
        raise PublishError(f"{name}: the tag already has a different digest in the pin file")
    return {**found, name: {"tag": tag, "digest": f"{repository}@{digest}"}}


def publish_image(settings: Settings, image: dict[str, Any], found: dict[str, dict[str, str]], scratch: Path) -> tuple[dict[str, dict[str, str]], str]:
    name = image["name"]
    folder = scratch / name
    folder.mkdir()
    tag = image_tag(settings, image, stage(settings, image, folder))
    pinned = found.get(name, {}).get("tag") == tag
    digest = remote_digest(settings, name, tag)
    if pinned:
        if digest is None:
            raise PublishError(f"{name}: the repository does not hold the pinned tag: remove its entry from the pin file and run the command again")
        return pin_image(found, name, tag, digest, settings.repository), f"{name}: already pinned"
    if digest is not None:
        return pin_image(found, name, tag, digest, settings.repository), f"{name}: pinned from the repository"
    build_image(settings, image, folder, tag)
    push_image(settings, name, tag)
    digest = remote_digest(settings, name, tag)
    if digest is None:
        raise PublishError(f"{name}: the repository does not hold the tag after the push")
    return pin_image(found, name, tag, digest, settings.repository), f"{name}: pushed and pinned"


def publish(settings: Settings) -> list[str]:
    """Publish every image of the list in order and write the pin file once, after the last image. A failure before then changes no pin: the next run finds the
    tags that were pushed in the repository and pins them from there."""
    require_clean(settings)
    try:
        found = pins.load(settings.pin_file)
    except pins.PinError as error:
        raise PublishError(str(error)) from error
    if any(not value["digest"].startswith(settings.repository + "@") for value in found.values()):
        raise PublishError("the pin file holds another repository than IMAGE_REPOSITORY")
    resolved = found
    messages: list[str] = []
    with tempfile.TemporaryDirectory(prefix="images_") as scratch:
        for image in settings.images:
            resolved, message = publish_image(settings, image, resolved, Path(scratch))
            messages.append(message)
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
        sys.stderr.write(f"publish: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
