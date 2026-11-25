"""Ansible module: the secret files that Airbyte needs and the Kubernetes secrets made from them.

``step: files`` creates each missing generated file under ``<secrets_directory>/airbyte`` (the instance admin's password and the object store's two
keys), keeps existing files and requires the database password file that the PostgreSQL step made. No container reads these files, so each one is owned by
the root owner (``root_owner``) with mode 0400, as every secret file is: an existing file with another mode or owner is given the right ones, its value kept.
``step: secrets`` makes three secrets in the namespace match those files: ``airbyte-database``, ``airbyte-storage`` and ``airbyte-auth-secrets``. A secret
is applied only when a key it holds differs, and keys that Airbyte added to ``airbyte-auth-secrets`` itself are left alone.

A value goes to the cluster on the standard input of ``kubectl apply --server-side``, so no value is in a command line, a rendered file or a last-applied
annotation. No result or message holds a value or any text of kubectl: a failed call shows one fixed word (refused, not found,
unreachable or failed). The module uses only the standard library and runs on the host's Python.
"""

from __future__ import annotations

import base64
import json
import secrets
from collections.abc import Callable
from pathlib import Path

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.secret_files import SecretFileError, access_differs, parse_owner, set_access, write_private_file

ACCOUNT_FOLDER = "airbyte"
DATABASE_FILE = "postgres/airbyte.password"
PASSWORD_BYTES = 32
KEY_ID_BYTES = 10
FIELD_MANAGER = "platform_airbyte"
GENERATED: dict[str, Callable[[], str]] = {
    "admin.password": lambda: secrets.token_urlsafe(PASSWORD_BYTES),
    "storage_access_key_id": lambda: secrets.token_hex(KEY_ID_BYTES),
    "storage_secret_access_key": lambda: secrets.token_urlsafe(PASSWORD_BYTES),
}
Runner = Callable[[list[str], "str | None"], "tuple[int, str, str]"]


class SecretError(RuntimeError):
    """A file or the cluster is not usable. The message never holds a secret value."""


class SecretFiles:
    """The one place that knows where each secret value comes from: a file under one private folder."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory

    def path(self, relative: str) -> Path:
        return self.directory / relative

    def read(self, relative: str) -> str:
        try:
            text = self.path(relative).read_text().rstrip("\n")
        except OSError as error:
            raise SecretError(f"the secret file {relative} cannot be read: {error.strerror}") from error
        if not text:
            raise SecretError(f"the secret file {relative} is empty")
        return text

    def missing(self) -> list[str]:
        return [name for name in GENERATED if not self.path(f"{ACCOUNT_FOLDER}/{name}").exists()]

    def unprotected(self, owner: tuple[int, int]) -> list[str]:
        """The generated files that exist with another mode or owner than the one every secret file has."""
        return [name for name in GENERATED if self.path(f"{ACCOUNT_FOLDER}/{name}").exists() and access_differs(self.path(f"{ACCOUNT_FOLDER}/{name}"), owner)]

    def ensure_generated(self, owner: tuple[int, int]) -> tuple[list[str], list[str]]:
        """Create each missing generated file for ``owner``, give each existing one its mode and owner, and return the created and the changed names.

        An existing file is never replaced.
        """
        self.read(DATABASE_FILE)
        folder = self.directory / ACCOUNT_FOLDER
        folder.mkdir(mode=0o700, exist_ok=True)
        folder.chmod(0o700)
        created: list[str] = []
        protected: list[str] = []
        for name, generate in GENERATED.items():
            path = folder / name
            if not path.exists():
                write_private_file(path, generate(), replace=False, owner=owner)
                created.append(name)
                continue
            self.read(f"{ACCOUNT_FOLDER}/{name}")
            if access_differs(path, owner):
                set_access(path, owner)
                protected.append(name)
        return created, protected


def wanted_secrets(files: SecretFiles, database_user: str) -> dict[str, dict[str, str]]:
    """Each secret's name and its keys with their values, read from the files."""
    return {
        "airbyte-auth-secrets": {"instance-admin-password": files.read(f"{ACCOUNT_FOLDER}/admin.password")},
        "airbyte-database": {"database-user": database_user, "database-password": files.read(DATABASE_FILE)},
        "airbyte-storage": {
            "AWS_ACCESS_KEY_ID": files.read(f"{ACCOUNT_FOLDER}/storage_access_key_id"),
            "AWS_SECRET_ACCESS_KEY": files.read(f"{ACCOUNT_FOLDER}/storage_secret_access_key"),
        },
    }


def reason(error: str) -> str:
    """One fixed word for kubectl's standard error. Its text can hold a kubeconfig's path and token, which no list of known values covers, so none is shown."""
    if "orbidden" in error or "nauthorized" in error:
        return "refused"
    if "(NotFound)" in error:
        return "not found"
    if any(
        text in error for text in ("connection refused", "was refused", "no such host", "i/o timeout", "Unable to connect", "couldn't get current server API")
    ):
        return "unreachable"
    return "failed"


def stored_data(output: str) -> dict[str, str]:
    try:
        data = json.loads(output).get("data") or {}
    except (json.JSONDecodeError, AttributeError) as error:
        raise SecretError("the cluster answered with a secret that cannot be read") from error
    return {str(key): str(value) for key, value in data.items()}


def differs(stored: dict[str, str] | None, wanted: dict[str, str]) -> bool:
    """Whether a key is missing or holds another value. Keys that the secret has beyond the wanted ones do not count."""
    return stored is None or any(stored.get(key) != base64.b64encode(value.encode()).decode() for key, value in wanted.items())


def manifest(name: str, namespace: str, wanted: dict[str, str]) -> str:
    data = {key: base64.b64encode(value.encode()).decode() for key, value in wanted.items()}
    return json.dumps({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": name, "namespace": namespace}, "type": "Opaque", "data": data})


def apply_secrets(run: Runner, kubectl: list[str], namespace: str, files: SecretFiles, database_user: str, *, check_only: bool = False) -> list[str]:
    """Make each secret match its files and return the names of those that changed."""
    wanted = wanted_secrets(files, database_user)
    changed: list[str] = []
    for name in sorted(wanted):
        code, output, error = run([*kubectl, "get", "secret", name, "--namespace", namespace, "--output", "json"], None)
        if code != 0 and "NotFound" not in error:
            raise SecretError(f"cannot read the secret {name} (reason: {reason(error)})")
        if not differs(stored_data(output) if code == 0 else None, wanted[name]):
            continue
        changed.append(name)
        if check_only:
            continue
        arguments = [*kubectl, "apply", "--server-side", "--force-conflicts", "--field-manager", FIELD_MANAGER, "--namespace", namespace, "--filename", "-"]
        code, _, error = run(arguments, manifest(name, namespace, wanted[name]))
        if code != 0:
            raise SecretError(f"cannot apply the secret {name} (reason: {reason(error)})")
    return changed


def main() -> None:
    module = AnsibleModule(
        argument_spec={
            "step": {"type": "str", "required": True, "choices": ["files", "secrets"]},
            "secrets_directory": {"type": "path", "required": True},
            "namespace": {"type": "str", "default": "airbyte"},
            "kubectl": {"type": "list", "elements": "str", "default": ["k3s", "kubectl"]},
            "database_user": {"type": "str", "default": "airbyte"},
            "root_owner": {"type": "str", "default": "0:0"},
        },
        supports_check_mode=True,
    )
    files = SecretFiles(Path(module.params["secrets_directory"]))
    try:
        if module.params["step"] == "files":
            owner = parse_owner(module.params["root_owner"], allow_root=True)
            created, protected = (files.missing(), files.unprotected(owner)) if module.check_mode else files.ensure_generated(owner)
            module.exit_json(changed=bool(created or protected), created=created, protected=protected)
        changed = apply_secrets(
            lambda arguments, data: module.run_command(arguments, data=data, check_rc=False),
            module.params["kubectl"],
            module.params["namespace"],
            files,
            module.params["database_user"],
            check_only=module.check_mode,
        )
        module.exit_json(changed=bool(changed), secrets=changed)
    except (SecretError, SecretFileError) as error:
        module.fail_json(msg=str(error))


if __name__ == "__main__":
    main()
