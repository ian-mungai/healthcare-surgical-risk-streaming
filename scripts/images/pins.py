"""The pin file: each project image, by the tag that was pushed and the digest that the repository gave it.

``compose/image_pins.yml`` is the one source of the digests and the authority on what was pushed: the publish command pushes only a tag that
this file lacks, takes the digest from the push, never reads the registry and writes this file. ``render`` turns it into the ``IMAGE_<NAME>`` settings that
the host's Compose reads, and stops when an image has no pin. The list of images is ``compose_stack_built_images`` in the group variables,
which nothing else copies.
"""

from __future__ import annotations

import contextlib
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any

import yaml

from scripts.ingestion.notice import FixedTextError, describe_error
from scripts.project_config import IMAGE_REPOSITORY_PATTERN
from scripts.stack.yaml_text import YamlError, load_yaml

ROOT = Path(__file__).resolve().parents[2]
PIN_FILE = ROOT / "compose" / "image_pins.yml"
GROUP_VARIABLES = ROOT / "ansible" / "group_vars" / "all.yml"
FIELDS = {"tag", "digest"}
HEADER = (
    "# Written by make images-push: each project image by the tag that was pushed and the digest that Docker Hub gave it. Do not edit by hand.\n"
    "# An empty list means that nothing was pushed yet. The host's Compose settings (IMAGE_<NAME>) are rendered from this file.\n"
)


class PinError(FixedTextError, ValueError):
    """A fixed-text failure of the pin file or of the render. The text holds no value that was read from outside."""


def load_images() -> list[dict[str, Any]]:
    """The images of ``compose_stack_built_images``, the one list that the host role and the publish command share."""
    document = yaml.safe_load(GROUP_VARIABLES.read_text())
    return list(document["compose_stack_built_images"])


def validate(found: dict[str, Any]) -> None:
    """Each entry holds a tag ``<repository>:<name>-<version>-<12 hex>`` and a digest ``<repository>@sha256:<64 hex>`` of one repository."""
    repositories: set[str] = set()
    for name, value in found.items():
        if not isinstance(value, dict) or set(value) != FIELDS:
            raise PinError(f"the pin of {name} must hold exactly the fields tag and digest")
        tag = re.fullmatch(rf"({IMAGE_REPOSITORY_PATTERN}):{re.escape(str(name))}-[0-9][A-Za-z0-9._]*-[0-9a-f]{{12}}", str(value["tag"]))
        if tag is None:
            raise PinError(f"the tag of {name} must be <repository>:{name}-<version>-<12 hex digits>")
        digest = re.fullmatch(rf"({IMAGE_REPOSITORY_PATTERN})@sha256:[0-9a-f]{{64}}", str(value["digest"]))
        if digest is None:
            raise PinError(f"the digest of {name} must be <repository>@sha256:<64 hex digits>")
        if tag.group(1) != digest.group(1):
            raise PinError(f"the tag and the digest of {name} name different repositories")
        repositories.add(tag.group(1))
    if len(repositories) > 1:
        raise PinError("the pin file names more than one repository")


def load(path: Path = PIN_FILE) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    try:
        document = load_yaml(path.read_text())
    except (YamlError, yaml.YAMLError, OSError, UnicodeDecodeError) as error:
        raise PinError("the pin file cannot be read as YAML") from error
    if not isinstance(document, dict) or set(document) != {"images"} or not isinstance(document["images"], dict):
        raise PinError("the pin file must hold one mapping named images")
    found: dict[str, dict[str, str]] = document["images"]
    validate(found)
    return found


def write(path: Path, found: dict[str, dict[str, str]]) -> None:
    """Replace the file in one step: a temporary file in the same folder, synced, then renamed over the file."""
    validate(found)
    body = "images:\n" + "".join(f"  {name}:\n    tag: {found[name]['tag']}\n    digest: {found[name]['digest']}\n" for name in sorted(found))
    text = HEADER + (body if found else "images: {}\n")
    temporary: str | None = None
    try:
        descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix=".image_pins_")
        with os.fdopen(descriptor, "w") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except OSError as error:
        if temporary is not None:
            with contextlib.suppress(OSError):
                os.unlink(temporary)
        raise PinError("the pin file could not be written") from error


def variable_name(name: str) -> str:
    return "IMAGE_" + name.upper()


def render_environment(found: dict[str, dict[str, str]], names: list[str]) -> str:
    """One ``IMAGE_<NAME>=<repository>@sha256:<digest>`` line for each image, in name order. An image with no pin stops it."""
    validate(found)
    for name in names:
        if name not in found:
            raise PinError(f"the image {name} has no pin: run make images-push")
    extra = sorted(set(found) - set(names))
    if extra:
        raise PinError(f"the pin file holds {extra[0]}, which compose_stack_built_images does not name")
    return "".join(f"{variable_name(name)}={found[name]['digest']}\n" for name in sorted(names))


def main(arguments: list[str] | None = None) -> int:
    given = sys.argv[1:] if arguments is None else arguments
    if given == ["render"]:
        path = PIN_FILE
    elif len(given) == 3 and given[:2] == ["render", "--pins"]:
        path = Path(given[2])
    else:
        sys.stderr.write("usage: python -m scripts.images.pins render [--pins <file>]\n")
        return 2
    try:
        sys.stdout.write(render_environment(load(path), [image["name"] for image in load_images()]))
    except PinError as error:
        sys.stderr.write(f"image pins: {describe_error(error)}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
