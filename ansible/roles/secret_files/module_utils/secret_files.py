"""The one place that knows where a PostgreSQL password file is and how it is made. It is a module of the role ``secret_files``, which has no tasks.

The role of the stack and the role of OpenBao each depend on that role, so Ansible finds this module for either role alone. A module imports it as
``ansible.module_utils.secret_files``. It uses only the standard library and runs on the host's Python.
"""

from __future__ import annotations

import os
import secrets
from pathlib import Path

POSTGRES_FOLDER = "postgres"
PASSWORD_BYTES = 32


class SecretFileError(RuntimeError):
    """A secret file is missing, empty or unreadable. The message never holds a secret."""


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
        try:
            text = self.path(account).read_text().rstrip("\n")
        except OSError as error:
            raise SecretFileError(f"the password file of {account} cannot be read: {error.strerror}") from error
        if not text:
            raise SecretFileError(f"the password file of {account} is empty")
        return text

    def ensure(self, account: str) -> bool:
        """Create the account's password file when it is missing and return whether it did. An existing file is never replaced."""
        for missing in reversed([folder for folder in (self.directory, *self.directory.parents) if not folder.exists()]):
            missing.mkdir(mode=0o700)
        try:
            descriptor = os.open(self.path(account), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            self.read(account)
            return False
        with os.fdopen(descriptor, "w") as handle:
            handle.write(secrets.token_urlsafe(PASSWORD_BYTES))
        return True
