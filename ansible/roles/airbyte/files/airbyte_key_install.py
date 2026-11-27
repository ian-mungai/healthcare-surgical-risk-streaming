#!/usr/bin/python3 -I
"""Place the project profile's access key for Airbyte's two connectors as two root-only files, and for the code location's record load as two more files for
the container's user. Run as root on the host by the launcher.

The launcher reads the key on the owner's computer and sends one JSON document on standard input: ``{"aws_access_key_id": ...,
"aws_secret_access_key": ...}``. The script takes no secret in an argument or the environment, and no Ansible task or module ever holds the key. It
refuses anything else by a fixed code: input over 4 KiB (``oversized``), text that is not a JSON object (``not_json``), a key whose name holds "token"
(``session_token``), another set of keys than the two (``bad_keys``), values that are not text or not in the form of an AWS access key (``bad_form``),
empty values (``empty``), and an owner that is not a numeric ``uid:gid`` of ids up to 4294967294 or a container owner that is root (``bad_owner``, checked
first, before anything is written). It writes ``airbyte/aws_access_key_id`` and ``airbyte/aws_secret_access_key`` under the secrets folder with the shared
atomic helper: each file is written through a fresh temporary file that is flushed and synced before it takes its place, mode 0400, so a file is whole
or absent and never empty. It writes the same two files again as ``airbyte_api/aws_access_key_id`` and ``airbyte_api/aws_secret_access_key``, owned by the
code location container's user and group (``--container-owner``, by default 10001:10001, the user that ``compose/services/dagster_code_location.yml`` runs
as), in the folder that the Airbyte role makes for the other four files of that container. The script is the only writer of the key, so no Ansible task,
variable or result holds it. A folder that is missing is made for root with mode 0700, as the role makes it. The leftovers of an interrupted write are removed
at the start, an empty or short file is replaced like a changed one (a rotated key; so is a file whose bytes are not text), a file with another mode or
owner is given the right ones, and a second run changes nothing. A file that cannot be written is ``cannot_write``. The script prints one code (``placed``,
``unchanged`` or a refusal) and nothing else, and exits 0 for the first two and 3 otherwise.
It uses only the standard library and the helper that the role installs beside it, and runs on the host's Python.

The launcher starts it as ``sudo /usr/bin/python3 -I <this file>``: the host's interpreter in isolated mode (no environment variable, no user site
folder and no script folder on the import path), so that the command that sudo matches, and the sudoers entry that turns off its input recording, name
one fixed command line. The interpreter line makes the file a program that starts by itself too, and names the same interpreter. Isolated mode drops
the script's folder from the import path, so the script loads the helper by its path beside itself."""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sys
from pathlib import Path
from typing import NoReturn


def load_helper() -> object:
    """The shared file helper that the role installs beside this script, loaded by its path because isolated mode has no script folder on the import path."""
    path = Path(__file__).resolve().parent / "secret_files.py"
    spec = importlib.util.spec_from_file_location("secret_files", path)
    if spec is None or spec.loader is None:
        raise ImportError("secret_files")
    module = importlib.util.module_from_spec(spec)
    sys.modules["secret_files"] = module
    spec.loader.exec_module(module)
    return module


helper = load_helper()
SecretFileError = helper.SecretFileError  # type: ignore[attr-defined]
access_differs = helper.access_differs  # type: ignore[attr-defined]
parse_owner = helper.parse_owner  # type: ignore[attr-defined]
remove_leftovers = helper.remove_leftovers  # type: ignore[attr-defined]
set_access = helper.set_access  # type: ignore[attr-defined]
write_private_file = helper.write_private_file  # type: ignore[attr-defined]

DEFAULT_DIRECTORY = "/opt/platform/secrets"
DEFAULT_OWNER = "0:0"
DEFAULT_CONTAINER_OWNER = "10001:10001"
MAX_INPUT_BYTES = 4096
FOLDER = "airbyte"
CONTAINER_FOLDER = "airbyte_api"
ID_FILE = "aws_access_key_id"
VALUE_FILE = "aws_secret_access_key"
KEY_ID_FORM = re.compile(r"[A-Z0-9]{16,128}")
KEY_VALUE_FORM = re.compile(r"[A-Za-z0-9/+=]{20,128}")
HIGHEST_ID = 4294967294
CODES = ("placed", "unchanged", "oversized", "not_json", "bad_keys", "session_token", "empty", "bad_form", "bad_owner", "cannot_write")


class Refusal(RuntimeError):
    """The document cannot be used or the files cannot be written. The error holds a code and no other text."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def parse_document(data: bytes) -> tuple[str, str]:
    """The access key ID and the secret of the document. Raise ``Refusal`` with a code for anything that is not exactly the two text values of a static key."""
    if len(data) > MAX_INPUT_BYTES:
        raise Refusal("oversized")
    try:
        document = json.loads(data.decode())
    except (ValueError, UnicodeDecodeError):
        raise Refusal("not_json") from None
    if not isinstance(document, dict):
        raise Refusal("not_json")
    if any("token" in str(name).lower() for name in document):
        raise Refusal("session_token")
    if set(document) != {ID_FILE, VALUE_FILE}:
        raise Refusal("bad_keys")
    key_id, value = document[ID_FILE], document[VALUE_FILE]
    if not isinstance(key_id, str) or not isinstance(value, str):
        raise Refusal("bad_form")
    if not key_id or not value:
        raise Refusal("empty")
    if KEY_ID_FORM.fullmatch(key_id) is None or KEY_VALUE_FORM.fullmatch(value) is None:
        raise Refusal("bad_form")
    return key_id, value


def check_owner(text: str, *, allow_root: bool) -> tuple[int, int]:
    """The numeric user and group of ``uid:gid``, each no higher than ``HIGHEST_ID`` (the system call takes no more), or ``Refusal`` with ``bad_owner``.

    It is called for both owners before the first file is written, so a bad owner leaves no file and no folder.
    """
    try:
        owner = parse_owner(text, allow_root=allow_root)
    except (SecretFileError, ValueError):
        raise Refusal("bad_owner") from None
    if max(owner) > HIGHEST_ID:
        raise Refusal("bad_owner")
    return owner


def current_value(path: Path) -> str | None:
    """What the file holds, or ``None`` when it is absent. The empty text, which no key equals, stands for an empty file and for bytes that are not text."""
    try:
        return path.read_text().rstrip("\n")
    except FileNotFoundError:
        return None
    except UnicodeDecodeError:
        return ""
    except OSError:
        raise Refusal("cannot_write") from None


def place(directory: Path, owner: tuple[int, int], container_owner: tuple[int, int], key_id: str, value: str) -> str:
    """Make the four files hold the key and return ``placed`` or ``unchanged``. The root files go first, and in each folder the secret goes before the ID."""
    changed = False
    for folder, holder, name, wanted in (
        (FOLDER, owner, VALUE_FILE, value),
        (FOLDER, owner, ID_FILE, key_id),
        (CONTAINER_FOLDER, container_owner, VALUE_FILE, value),
        (CONTAINER_FOLDER, container_owner, ID_FILE, key_id),
    ):
        path = directory / folder / name
        try:
            if path.parent.exists():
                remove_leftovers(path)
            if current_value(path) != wanted:
                write_private_file(path, wanted, replace=True, owner=holder)
            elif access_differs(path, holder):
                set_access(path, holder)
            else:
                continue
        except (SecretFileError, OSError):
            raise Refusal("cannot_write") from None
        changed = True
    return "placed" if changed else "unchanged"


class QuietParser(argparse.ArgumentParser):
    """An argument parser whose only error output is the code ``usage``. It runs alone on the host, so it holds its own copy."""

    def error(self, message: str) -> NoReturn:
        sys.stderr.write("usage\n")
        raise SystemExit(3)


def main(argv: list[str] | None = None) -> int:
    parser = QuietParser(description="Place the project profile's access key from standard input.")
    parser.add_argument("--directory", default=DEFAULT_DIRECTORY, help="the secrets folder")
    parser.add_argument("--owner", default=DEFAULT_OWNER, help="the numeric user and group of the root-only files, uid:gid")
    parser.add_argument("--container-owner", default=DEFAULT_CONTAINER_OWNER, help="the numeric user and group of the code location's two copies, uid:gid")
    arguments = parser.parse_args(argv)
    try:
        owner, container_owner = check_owner(arguments.owner, allow_root=True), check_owner(arguments.container_owner, allow_root=False)
        key_id, value = parse_document(sys.stdin.buffer.read(MAX_INPUT_BYTES + 1))
        code = place(Path(arguments.directory), owner, container_owner, key_id, value)
    except Refusal as error:
        code = error.code
    except SecretFileError:
        code = "cannot_write"
    sys.stdout.write(f"{code}\n")
    return 0 if code in ("placed", "unchanged") else 3


if __name__ == "__main__":
    sys.exit(main())
