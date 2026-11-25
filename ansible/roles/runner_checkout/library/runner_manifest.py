"""Ansible module: the source manifest of a runner checkout, and the check of a checkout against the controller's archive.

The expected files are never read from the host. ``state: archive`` runs on the controller: it reads the tar file that ``git archive`` made there and returns
``files``, the SHA-256 of every file in it, after it refused a member that is not a plain file or a folder, a path of another form and a name that comes
twice. The role sends that map with the deploy to the two states that run on the host:

``state: write`` hashes every file that the unpacked archive placed in the folder, fails unless they are exactly the files and hashes that the controller
sent, and writes ``runner_manifest.json`` there, in this form and no other: ``{"commit": "<40 lower-case hexadecimal digits>", "files": {"<path relative to
the folder>": "<SHA-256, 64 lower-case hexadecimal digits>"}}``. The paths use forward slashes and are in sorted order. The top-level ``.venv`` (the
environment), the finished marker and the manifest itself are not archive files and are never listed. A runner that the launcher places later is not in the
manifest either, because the manifest is written before any placement.

``state: verify`` reads the manifest and requires it to hold exactly the files and hashes that the controller sent (an empty, a partial or an extended map
fails), then compares every listed file with it. It never fails on a bad folder: it reports ``intact: false`` and the problems, so that the role can build the
folder again. A file that the manifest does not list (a placed runner, the environment) is not a problem. The problems are fixed text with a path of the
folder, at most twenty and a count of the rest, and never a file's content. The two states of the host use only the standard library.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tarfile
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import IO, Any

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
    """The folder or the archive holds an entry that a manifest cannot describe, or the commit is of the wrong form."""


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(BLOCK_BYTES), b""):
            digest.update(block)
    return digest.hexdigest()


def sha256_of_stream(handle: IO[bytes]) -> str:
    digest = hashlib.sha256()
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


def files_form_problem(files: object) -> str | None:
    """Why a map of files is not one that the controller could have sent, or None. An empty map is refused: an archive of no file is no checkout."""
    if not isinstance(files, dict) or not files:
        return "the files are not a map with at least one file"
    for name, digest in files.items():
        reason = path_form_problem(name)
        if reason is not None:
            return reason
        if not isinstance(digest, str) or DIGEST_FORM.fullmatch(digest) is None:
            return "a hash is not 64 lower-case hexadecimal digits"
    return None


def archive_files(archive: Path) -> dict[str, str]:
    """The SHA-256 of every file in the tar file, by its path. Only plain files and folders are archive content: a link, a device, a pipe, a path of another
    form and a name that comes twice stop the step with the path, because the host would place what the controller never read."""
    found: dict[str, str] = {}
    with tarfile.open(archive, "r:") as opened:
        for member in opened:
            name = member.name.removeprefix("./").rstrip("/") if member.isdir() else member.name
            if member.isdir():
                if name and any(COMPONENT_FORM.fullmatch(part) is None or part in (".", "..") for part in name.split("/")):
                    raise ManifestError("a folder of the archive has a path that is not relative, in forward slashes and without . or .. parts")
                continue
            if not member.isreg():
                raise ManifestError(f"{member.name} is a link or a special file: only plain files and folders are archive content")
            reason = path_form_problem(member.name)
            if reason is not None:
                raise ManifestError(f"{member.name}: {reason}")
            if member.name in found:
                raise ManifestError(f"{member.name} is in the archive twice")
            handle = opened.extractfile(member)
            if handle is None:
                raise ManifestError(f"{member.name} cannot be read")
            found[member.name] = sha256_of_stream(handle)
    if not found:
        raise ManifestError("the archive holds no file")
    return dict(sorted(found.items()))


def differences(found: Mapping[str, str], expected: Mapping[str, str], what: str) -> list[str]:
    """Where two maps of files differ, in the order of the expected map and then of the extra names."""
    problems = [f"{what} lacks: {name}" for name in expected if name not in found]
    problems += [f"{what} has a different hash: {name}" for name, digest in expected.items() if name in found and found[name] != digest]
    problems += [f"{what} has an extra file: {name}" for name in found if name not in expected]
    return problems


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
    reason = files_form_problem(files)
    if reason is not None:
        return None, f"manifest: {reason}"
    return files, None


def shown(found: list[str]) -> list[str]:
    if len(found) > SHOWN_PROBLEMS:
        return [*found[:SHOWN_PROBLEMS], f"and {len(found) - SHOWN_PROBLEMS} more"]
    return found


def problems(folder: Path, commit: str, expected: object) -> list[str]:
    """What differs between the folder and the files that the controller sent, through its manifest: the manifest must hold exactly those files and hashes, and
    each file must be a plain file with that hash. Fixed text with paths: at most twenty lines and a count of the rest."""
    reason = files_form_problem(expected)
    if reason is not None or not isinstance(expected, dict):
        return [f"expected files: {reason}"]
    files, reason = read_manifest(folder, commit)
    if files is None:
        return [reason or "no manifest"]
    mismatch = differences(files, expected, "manifest")
    if mismatch:
        return shown(mismatch)
    root = Path(os.path.realpath(folder))
    found: list[str] = []
    for relative, digest in expected.items():
        path = folder / relative
        if not os.path.lexists(path):
            found.append(f"missing: {relative}")
        elif Path(os.path.realpath(path)) != root / relative or not path.is_file():
            found.append(f"not a plain file: {relative}")
        elif sha256_of(path) != digest:
            found.append(f"changed: {relative}")
    return shown(found)


def write_manifest(module: AnsibleModule, folder: Path, commit: str, expected: object) -> dict[str, Any]:
    reason = files_form_problem(expected)
    if reason is not None or not isinstance(expected, dict):
        raise ManifestError(f"the files that the controller sent are not usable: {reason}")
    placed = archived_files(folder)
    if placed != expected:
        listed = shown(differences(placed, expected, "the unpacked folder"))
        raise ManifestError("the unpacked folder is not the archive that the controller read: " + "; ".join(listed))
    destination = folder / MANIFEST
    text = manifest_text(commit, expected)
    changed = False
    if destination.is_symlink() or not destination.is_file() or destination.read_text() != text:
        handle, temporary = tempfile.mkstemp(dir=folder, prefix=".runner_manifest_")
        with os.fdopen(handle, "w") as written:
            written.write(text)
        module.atomic_move(temporary, str(destination))
        changed = True
    arguments = module.load_file_common_arguments(module.params, path=str(destination))
    changed = module.set_fs_attributes_if_different(arguments, changed)
    return {"changed": changed, "files": len(placed)}


def main() -> None:
    module = AnsibleModule(
        argument_spec={
            "folder": {"type": "path"},
            "commit": {"type": "str"},
            "archive": {"type": "path"},
            "files": {"type": "dict"},
            "state": {"type": "str", "required": True, "choices": ["archive", "write", "verify"]},
        },
        required_if=[("state", "archive", ("archive",)), ("state", "write", ("folder", "commit", "files")), ("state", "verify", ("folder", "commit", "files"))],
        add_file_common_args=True,
        supports_check_mode=False,
    )
    state = module.params["state"]
    try:
        if state == "archive":
            files = archive_files(Path(module.params["archive"]))
            module.exit_json(changed=False, files=files, count=len(files))
        folder, commit, expected = Path(module.params["folder"]), module.params["commit"], module.params["files"]
        if COMMIT_FORM.fullmatch(commit) is None:
            raise ManifestError("the commit must be 40 lower-case hexadecimal digits")
        if state == "write":
            module.exit_json(**write_manifest(module, folder, commit, expected))
        found = problems(folder, commit, expected)
        module.exit_json(changed=False, intact=not found, problems=found)
    except (ManifestError, OSError, tarfile.TarError) as error:
        module.fail_json(msg=str(error))


if __name__ == "__main__":
    main()
