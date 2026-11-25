"""Ansible module: the source manifest of a runner checkout, and the check of a checkout against it.

``state: write`` hashes every file that ``git archive`` placed in the folder and writes ``runner_manifest.json`` there, in this form and no other:
``{"commit": "<40 lower-case hexadecimal digits>", "files": {"<path relative to the folder>": "<SHA-256, 64 lower-case hexadecimal digits>"}}``.
The paths use forward slashes and are in sorted order. The top-level ``.venv`` (the environment), the finished marker and the manifest itself are not archive
files and are never listed. A runner that the launcher places later is not in the manifest either, because the manifest is written before any placement.
A link or another file that is not a plain file stops the step with its path, because a link can lead to other content later.

``state: verify`` reads the manifest and compares every listed file with it. It never fails on a bad folder: it reports ``intact: false`` and the problems,
so that the role can build the folder again. A file that the manifest does not list (a placed runner, the environment) is not a problem. The problems are
fixed text with a path of the folder, at most twenty and a count of the rest, and never a file's content. It uses only the standard library and runs on the
host's Python.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ansible.module_utils.basic import AnsibleModule

MANIFEST = "runner_manifest.json"
MARKER = ".runner_checkout.json"
ENVIRONMENT = ".venv"
COMMIT_FORM = re.compile(r"[0-9a-f]{40}")
DIGEST_FORM = re.compile(r"[0-9a-f]{64}")
COMPONENT_FORM = re.compile(r"[^/\\\x00]+")
BLOCK_BYTES = 1024 * 1024
SHOWN_PROBLEMS = 20
KEYS = ("commit", "files")


class ManifestError(RuntimeError):
    """The folder holds an entry that a manifest cannot describe, or the commit is of the wrong form."""


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(BLOCK_BYTES), b""):
            digest.update(block)
    return digest.hexdigest()


def raise_error(error: OSError) -> None:
    """The handler that makes a walk fail on a folder it cannot read, which it skips otherwise."""
    raise error


def archived_files(folder: Path) -> dict[str, str]:
    """The SHA-256 of every file below the folder except the top-level environment, the marker and the manifest, by its path relative to the folder."""
    found: dict[str, str] = {}
    for top, directories, names in os.walk(folder, followlinks=False, onerror=raise_error):
        here = Path(top)
        at_top = here == folder
        if at_top and ENVIRONMENT in directories:
            directories.remove(ENVIRONMENT)
        for directory in directories:
            if (here / directory).is_symlink():
                raise ManifestError(f"{(here / directory).relative_to(folder).as_posix()} is a link: only plain files and folders are archive content")
        for name in names:
            path = here / name
            relative = path.relative_to(folder).as_posix()
            if at_top and name in (MARKER, MANIFEST):
                continue
            if not stat.S_ISREG(path.lstat().st_mode):
                raise ManifestError(f"{relative} is a link or a special file: only plain files and folders are archive content")
            found[relative] = sha256_of(path)
    return dict(sorted(found.items()))


def manifest_text(commit: str, files: Mapping[str, str]) -> str:
    """The manifest's text: the commit and the files in sorted order, with one final newline."""
    if COMMIT_FORM.fullmatch(commit) is None:
        raise ManifestError("the commit must be 40 lower-case hexadecimal digits")
    return json.dumps({"commit": commit, "files": dict(sorted(files.items()))}, indent=2) + "\n"


def path_form_problem(path: object) -> str | None:
    if not isinstance(path, str) or not path:
        return "a path is not text"
    parts = path.split("/")
    if any(COMPONENT_FORM.fullmatch(part) is None or part in (".", "..") for part in parts):
        return "a path is not relative, in forward slashes and without . or .. parts"
    if parts[0] == ENVIRONMENT or path in (MARKER, MANIFEST):
        return "a path names the environment, the marker or the manifest, which are not archive files"
    return None


def read_manifest(folder: Path, commit: str) -> tuple[dict[str, Any] | None, str | None]:
    """The manifest's files, or the one problem with the manifest as fixed text."""
    path = folder / MANIFEST
    if path.is_symlink() or not path.is_file():
        return None, "no manifest"
    try:
        document = json.loads(path.read_bytes().decode("utf-8"))
    except (OSError, ValueError):
        return None, "manifest: not JSON text"
    if not isinstance(document, dict) or tuple(sorted(document)) != KEYS:
        return None, "manifest: not an object with the keys commit and files"
    if document["commit"] != commit:
        return None, "manifest: names another commit"
    files = document["files"]
    if not isinstance(files, dict):
        return None, "manifest: files is not an object"
    for name, digest in files.items():
        reason = path_form_problem(name)
        if reason is not None:
            return None, f"manifest: {reason}"
        if not isinstance(digest, str) or DIGEST_FORM.fullmatch(digest) is None:
            return None, "manifest: a hash is not 64 lower-case hexadecimal digits"
    return files, None


def problems(folder: Path, commit: str) -> list[str]:
    """What differs between the folder and its manifest, as fixed text with paths: at most twenty lines and a count of the rest."""
    files, reason = read_manifest(folder, commit)
    if files is None:
        return [reason or "no manifest"]
    root = Path(os.path.realpath(folder))
    found: list[str] = []
    for relative, digest in files.items():
        path = folder / relative
        if not os.path.lexists(path):
            found.append(f"missing: {relative}")
        elif Path(os.path.realpath(path)) != root / relative or not path.is_file():
            found.append(f"not a plain file: {relative}")
        elif sha256_of(path) != digest:
            found.append(f"changed: {relative}")
    if len(found) > SHOWN_PROBLEMS:
        return [*found[:SHOWN_PROBLEMS], f"and {len(found) - SHOWN_PROBLEMS} more"]
    return found


def write_manifest(module: AnsibleModule, folder: Path, commit: str) -> dict[str, Any]:
    destination = folder / MANIFEST
    files = archived_files(folder)
    text = manifest_text(commit, files)
    changed = False
    if destination.is_symlink() or not destination.is_file() or destination.read_text() != text:
        handle, temporary = tempfile.mkstemp(dir=folder, prefix=".runner_manifest_")
        with os.fdopen(handle, "w") as written:
            written.write(text)
        module.atomic_move(temporary, str(destination))
        changed = True
    arguments = module.load_file_common_arguments(module.params, path=str(destination))
    changed = module.set_fs_attributes_if_different(arguments, changed)
    return {"changed": changed, "files": len(files)}


def main() -> None:
    module = AnsibleModule(
        argument_spec={
            "folder": {"type": "path", "required": True},
            "commit": {"type": "str", "required": True},
            "state": {"type": "str", "required": True, "choices": ["write", "verify"]},
        },
        add_file_common_args=True,
        supports_check_mode=False,
    )
    folder = Path(module.params["folder"])
    commit = module.params["commit"]
    try:
        if COMMIT_FORM.fullmatch(commit) is None:
            raise ManifestError("the commit must be 40 lower-case hexadecimal digits")
        if module.params["state"] == "write":
            module.exit_json(**write_manifest(module, folder, commit))
        found = problems(folder, commit)
        module.exit_json(changed=False, intact=not found, problems=found)
    except (ManifestError, OSError) as error:
        module.fail_json(msg=str(error))


if __name__ == "__main__":
    main()
