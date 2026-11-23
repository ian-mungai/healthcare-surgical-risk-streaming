#!/usr/bin/python3 -I
"""Place the project profile's access key for Airbyte's two connectors as two root-only files. Run as root on the host by the launcher.

The launcher reads the key on the owner's computer and sends one JSON document on standard input: ``{"aws_access_key_id": ...,
"aws_secret_access_key": ...}``. The script takes no secret in an argument or the environment, and no Ansible task or module ever holds the key. It
refuses anything else by a fixed code: input over 4 KiB (``oversized``), text that is not a JSON object (``not_json``), a key whose name holds "token"
(``session_token``), another set of keys than the two (``bad_keys``), values that are not text or not in the form of an AWS access key (``bad_form``)
and empty values (``empty``). It writes ``airbyte/aws_access_key_id`` and ``airbyte/aws_secret_access_key`` under the secrets folder with the shared
atomic helper: each file is written through a fresh temporary file that is flushed and synced before it takes its place, mode 0400, so a file is whole
or absent and never empty. The leftovers of an interrupted write are removed at the start, an empty or short file is replaced like a changed one (a
rotated key), a file with another mode or owner is given the right ones, and a second run changes nothing. A file that cannot be written is
``cannot_write``. The script prints one code (``placed``, ``unchanged`` or a refusal) and nothing else, and exits 0 for the first two and 3 otherwise.
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
MAX_INPUT_BYTES = 4096
FOLDER = "airbyte"
ID_FILE = "aws_access_key_id"
VALUE_FILE = "aws_secret_access_key"
KEY_ID_FORM = re.compile(r"[A-Z0-9]{16,128}")
KEY_VALUE_FORM = re.compile(r"[A-Za-z0-9/+=]{20,128}")
CODES = ("placed", "unchanged", "oversized", "not_json", "bad_keys", "session_token", "empty", "bad_form", "cannot_write")


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


def current_value(path: Path) -> str | None:
    """What the file holds, or ``None`` when it is absent. An empty file holds the empty text, which no key equals."""
    try:
        return path.read_text().rstrip("\n")
    except FileNotFoundError:
        return None
    except (OSError, UnicodeDecodeError):
        raise Refusal("cannot_write") from None


def place(directory: Path, owner: tuple[int, int], key_id: str, value: str) -> str:
    """Make the two files hold the key and return ``placed`` or ``unchanged``. The secret goes first, so the ID changes last."""
    changed = False
    for name, wanted in ((VALUE_FILE, value), (ID_FILE, key_id)):
        path = directory / FOLDER / name
        try:
            if path.parent.exists():
                remove_leftovers(path)
            if current_value(path) != wanted:
                write_private_file(path, wanted, replace=True, owner=owner)
            elif access_differs(path, owner):
                set_access(path, owner)
            else:
                continue
        except (SecretFileError, OSError):
            raise Refusal("cannot_write") from None
        changed = True
    return "placed" if changed else "unchanged"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Place the project profile's access key from standard input.")
    parser.add_argument("--directory", default=DEFAULT_DIRECTORY, help="the secrets folder")
    parser.add_argument("--owner", default=DEFAULT_OWNER, help="the numeric user and group of the files, uid:gid")
    arguments = parser.parse_args(argv)
    try:
        key_id, value = parse_document(sys.stdin.buffer.read(MAX_INPUT_BYTES + 1))
        code = place(Path(arguments.directory), parse_owner(arguments.owner, allow_root=True), key_id, value)
    except Refusal as error:
        code = error.code
    except SecretFileError:
        code = "cannot_write"
    sys.stdout.write(f"{code}\n")
    return 0 if code in ("placed", "unchanged") else 3


if __name__ == "__main__":
    sys.exit(main())
