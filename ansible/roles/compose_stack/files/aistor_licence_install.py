#!/usr/bin/python3 -I
"""Place AIStor's licence on the host as one root-only file. Run as root on the host by the launcher.

The launcher reads the licence on the owner's computer (from its OpenBao) and sends it as plain text on standard input. The script takes no secret in
an argument or the environment, and no Ansible task or module ever holds the licence. It refuses anything that cannot be a licence by a fixed code:
input over 16 KiB (``oversized``), bytes that are not ASCII text or hold a control character other than a line break, a carriage return or a tab
(``bad_form``), text that is empty or only white space (``empty``) and an owner that is not a numeric ``uid:gid`` of ids up to 4294967294
(``bad_owner``, checked first, before anything is written). It writes ``aistor/licence`` under the secrets folder with the shared atomic helper: the
file is written through a fresh temporary file that is flushed and synced before it takes its place, mode 0400, so the file is whole or absent and
never empty. The text is kept as it was sent with its edge white space trimmed. The leftovers of an interrupted write are removed at the start, a file
with other text is replaced, a file with another mode or owner is given the right ones, and a second run with the same licence changes nothing. A file
that cannot be written is ``cannot_write``. The script prints one code (``placed``, ``unchanged`` or a refusal) and nothing else, and exits 0 for the
first two and 3 otherwise: the licence is in no output, no error and no log. It uses only the standard library and the helper that the role installs
beside it, and runs on the host's Python.

The launcher starts it as ``sudo /usr/bin/python3 -I <this file>``: the host's interpreter in isolated mode (no environment variable, no user site
folder and no script folder on the import path), so that the command that sudo matches, and the sudoers entry that turns off its input recording, name
one fixed command line. The interpreter line makes the file a program that starts by itself too, and names the same interpreter. Isolated mode drops
the script's folder from the import path, so the script loads the helper by its path beside itself."""

from __future__ import annotations

import argparse
import importlib.util
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
MAX_INPUT_BYTES = 16384
FOLDER = "aistor"
FILE = "licence"
ALLOWED_CONTROLS = {"\n", "\r", "\t"}
HIGHEST_ID = 4294967294
CODES = ("placed", "unchanged", "oversized", "bad_form", "empty", "bad_owner", "cannot_write")


class Refusal(RuntimeError):
    """The input cannot be used or the file cannot be written. The error holds a code and no other text."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def parse_licence(data: bytes) -> str:
    """The licence text with its edge white space trimmed, or ``Refusal`` with a code for anything that cannot be a licence."""
    if len(data) > MAX_INPUT_BYTES:
        raise Refusal("oversized")
    try:
        text = data.decode("ascii")
    except UnicodeDecodeError:
        raise Refusal("bad_form") from None
    if any((ord(character) < 32 or ord(character) == 127) and character not in ALLOWED_CONTROLS for character in text):
        raise Refusal("bad_form")
    if not text.strip():
        raise Refusal("empty")
    return text.strip()


def check_owner(text: str) -> tuple[int, int]:
    """The numeric user and group of ``uid:gid``, each no higher than ``HIGHEST_ID`` (the system call takes no more), or ``Refusal`` with ``bad_owner``."""
    try:
        owner = parse_owner(text, allow_root=True)
    except (SecretFileError, ValueError):
        raise Refusal("bad_owner") from None
    if max(owner) > HIGHEST_ID:
        raise Refusal("bad_owner")
    return owner


def current_text(path: Path) -> str | None:
    """What the file holds, or ``None`` when it is absent. The empty text, which no licence equals, stands for an empty file and for bytes that are not text."""
    try:
        return path.read_text()
    except FileNotFoundError:
        return None
    except UnicodeDecodeError:
        return ""
    except OSError:
        raise Refusal("cannot_write") from None


def place(directory: Path, owner: tuple[int, int], licence: str) -> str:
    """Make the file hold the licence and return ``placed`` or ``unchanged``."""
    path = directory / FOLDER / FILE
    try:
        if path.parent.exists():
            remove_leftovers(path)
        if current_text(path) != licence:
            write_private_file(path, licence, replace=True, owner=owner)
        elif access_differs(path, owner):
            set_access(path, owner)
        else:
            return "unchanged"
    except (SecretFileError, OSError):
        raise Refusal("cannot_write") from None
    return "placed"


class QuietParser(argparse.ArgumentParser):
    """An argument parser whose only error output is the code ``usage``. It runs alone on the host, so it holds its own copy."""

    def error(self, message: str) -> NoReturn:
        sys.stderr.write("usage\n")
        raise SystemExit(3)


def main(argv: list[str] | None = None) -> int:
    parser = QuietParser(description="Place AIStor's licence from standard input.")
    parser.add_argument("--directory", default=DEFAULT_DIRECTORY, help="the secrets folder")
    parser.add_argument("--owner", default=DEFAULT_OWNER, help="the numeric user and group of the root-only file, uid:gid")
    arguments = parser.parse_args(argv)
    try:
        owner = check_owner(arguments.owner)
        licence = parse_licence(sys.stdin.buffer.read(MAX_INPUT_BYTES + 1))
        code = place(Path(arguments.directory), owner, licence)
    except Refusal as error:
        code = error.code
    except SecretFileError:
        code = "cannot_write"
    sys.stdout.write(f"{code}\n")
    return 0 if code in ("placed", "unchanged") else 3


if __name__ == "__main__":
    sys.exit(main())
