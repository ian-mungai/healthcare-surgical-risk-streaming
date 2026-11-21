"""The one place that knows where a secret file is, who owns it and how it is made. It is a module of the role ``secret_files``, which has no tasks.

Every secret file that a container reads is owned by the numeric user that container runs as, has mode 0400 and is read by nobody else. A file that no
container reads is owned by root, the same way. The owner of each file is data. The tracked service list names the owner for each service, as a numeric
``uid:gid`` at the end of its line in ``compose/openbao_services.txt``, and the variables of the playbook name the owner for OpenBao and for root.

The role of the stack and the role of OpenBao each depend on this role, so Ansible finds this module for either role alone. A module imports it as
``ansible.module_utils.secret_files``. It uses only the standard library and runs on the host's Python.
"""

from __future__ import annotations

import os
import re
import secrets
import stat
from dataclasses import dataclass
from pathlib import Path

POSTGRES_FOLDER = "postgres"
PASSWORD_BYTES = 32
SECRET_FILE_MODE = 0o400
SERVICE_NAME = re.compile(r"[a-z][a-z0-9_]{0,30}")
OWNER = re.compile(r"(0|[1-9][0-9]*):(0|[1-9][0-9]*)")
RESERVED_NAMES = {"bootstrap", "default", "root"}


class SecretFileError(RuntimeError):
    """A list, an owner or a secret file is not usable. The message never holds a secret."""


@dataclass(frozen=True)
class Entry:
    name: str
    owner: tuple[int, int] | None


def parse_owner(text: str, *, allow_root: bool = False) -> tuple[int, int]:
    """The numeric user and group in ``uid:gid``. A user of a container is never root: a container that reads a secret file runs as a user of its own."""
    parsed = OWNER.fullmatch(text)
    if parsed is None or (not allow_root and int(parsed[1]) == 0):
        raise SecretFileError(f"owner {text!r} must be a numeric user and group, as uid:gid" + ("" if allow_root else ", with a user other than 0"))
    return int(parsed[1]), int(parsed[2])


def parse_entries(text: str, what: str) -> list[Entry]:
    """The entries of a tracked list: one for each line, with comments and blank lines ignored. A service line may end with the owner of its files."""
    entries: list[Entry] = []
    for line in text.splitlines():
        words = line.split("#", 1)[0].split()
        if not words:
            continue
        name = words[0]
        if not SERVICE_NAME.fullmatch(name) or name in RESERVED_NAMES:
            raise SecretFileError(f"{what} name {name!r} must be 1 to 31 lower case letters, digits or underscores, start with a letter and not be reserved")
        if len(words) > (2 if what == "service" else 1):
            raise SecretFileError(f"the line of {what} {name!r} holds more than its name" + (" and an owner" if what == "service" else ""))
        if name in [entry.name for entry in entries]:
            raise SecretFileError(f"{what} name {name!r} is listed more than once")
        entries.append(Entry(name, parse_owner(words[1]) if len(words) == 2 else None))
    if not entries:
        raise SecretFileError(f"the {what} list names no {what}")
    return entries


def parse_names(text: str, what: str) -> list[str]:
    return [entry.name for entry in parse_entries(text, what)]


def read_file(path: Path, what: str) -> str:
    try:
        text = path.read_text().rstrip("\n")
    except OSError as error:
        raise SecretFileError(f"the {what} cannot be read: {error.strerror}") from error
    if not text:
        raise SecretFileError(f"the {what} is empty")
    return text


def make_private_folder(folder: Path) -> None:
    for missing in reversed([path for path in (folder, *folder.parents) if not path.exists()]):
        missing.mkdir(mode=0o700)


def write_private_file(path: Path, text: str, *, replace: bool, owner: tuple[int, int]) -> None:
    """Write ``text`` to a new file with mode 0400 from the moment it exists, given to ``owner`` before the text is written.

    ``replace`` swaps an existing file atomically. A file that cannot be given to its owner is removed again, so no half-made file stays.
    """
    make_private_folder(path.parent)
    target = path.with_name(f".{path.name}.{secrets.token_hex(4)}.new") if replace else path
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, SECRET_FILE_MODE)
    try:
        with os.fdopen(descriptor, "w") as handle:
            os.fchown(handle.fileno(), *owner)
            handle.write(text)
        if replace:
            os.replace(target, path)
    except OSError as error:
        target.unlink(missing_ok=True)
        raise SecretFileError(f"the file {path.name} cannot be made for owner {owner[0]}:{owner[1]}: {error.strerror}") from error


def access_differs(path: Path, owner: tuple[int, int]) -> bool:
    """Whether an existing secret file has another mode than 0400 or another owner than ``owner``."""
    status = path.stat()
    return stat.S_IMODE(status.st_mode) != SECRET_FILE_MODE or (status.st_uid, status.st_gid) != owner


def set_access(path: Path, owner: tuple[int, int]) -> None:
    try:
        os.chmod(path, SECRET_FILE_MODE)
        os.chown(path, *owner)
    except OSError as error:
        raise SecretFileError(f"the file {path.name} cannot be given to owner {owner[0]}:{owner[1]} with mode 0400: {error.strerror}") from error


class PasswordStore:
    """A file for each PostgreSQL account in one private folder: ``<secrets folder>/postgres/<account>.password``."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory

    @classmethod
    def in_secrets(cls, secrets_directory: Path) -> PasswordStore:
        return cls(secrets_directory / POSTGRES_FOLDER)

    def path(self, account: str) -> Path:
        return self.directory / f"{account}.password"

    def read(self, account: str) -> str:
        return read_file(self.path(account), f"password file of {account}")

    def ensure(self, account: str, owner: tuple[int, int]) -> bool:
        """Create the account's password file for ``owner`` when it is missing and return whether it did. An existing file is never replaced."""
        try:
            write_private_file(self.path(account), secrets.token_urlsafe(PASSWORD_BYTES), replace=False, owner=owner)
        except FileExistsError:
            self.read(account)
            return False
        return True

    def protect(self, account: str, owner: tuple[int, int]) -> bool:
        """Give an existing password file its owner and mode 0400 and return whether that changed anything. The password is kept."""
        path = self.path(account)
        self.read(account)
        if not access_differs(path, owner):
            return False
        set_access(path, owner)
        return True
