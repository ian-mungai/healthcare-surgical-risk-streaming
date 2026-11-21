"""Ansible module: the OpenBao admin secret ID file, and the policy, AppRole, secret ID file and PostgreSQL password of each listed service.

``step: prepare`` creates the admin secret ID file when it is missing, before OpenBao starts for the first time. The server's first start reads it (see
``compose/openbao/openbao.hcl``). The file is owned by the numeric user that the container runs as and has mode 0400. An existing file keeps its content.

``step: reconcile`` logs in as the ``bootstrap`` AppRole and makes the running server match the service list: for each service one policy that reads
only its own path, one AppRole named for the service with a renewable token, one secret ID in a file that only the service reads (mode 0400, owned by
the numeric user on its list line) and, for a service that is also a PostgreSQL tool, its PostgreSQL password at its own path. A server that already
matches needs no change and the step reports none. A service that left the list keeps its role and is reported as unlisted, never removed.

It uses only the standard library and runs on the host's Python. No secret is a module argument, a command line, a log line or a message.
"""

from __future__ import annotations

import http.client
import json
import os
import re
import secrets
import stat
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.secret_files import PasswordStore, SecretFileError

SERVICE_NAME = re.compile(r"[a-z][a-z0-9_]{0,30}")
OWNER = re.compile(r"([1-9][0-9]*):([0-9]+)")
RESERVED_NAMES = {"bootstrap", "default", "root"}
ADMIN_ROLE = "bootstrap"
OPENBAO_FOLDER = "openbao"
ADMIN_ID_FILE = "bootstrap.secret_id"
SECRET_BYTES = 32
SECRET_FILE_MODE = 0o400
HTTP_SECONDS = 10
READY_POLL_SECONDS = 1
ERROR_CHARACTERS = 300


class BootstrapError(RuntimeError):
    """A list, a file or the server's answer is not usable. The message never holds a secret."""


@dataclass(frozen=True)
class Entry:
    name: str
    owner: tuple[int, int] | None


def parse_owner(text: str) -> tuple[int, int]:
    """The numeric user and group in ``uid:gid``. The user is never root: a container that reads a secret file runs as a user of its own."""
    parsed = OWNER.fullmatch(text)
    if parsed is None:
        raise BootstrapError(f"owner {text!r} must be a numeric user and group, as uid:gid, with a user other than 0")
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
            raise BootstrapError(f"{what} name {name!r} must be 1 to 31 lower case letters, digits or underscores, start with a letter and not be reserved")
        if len(words) > (2 if what == "service" else 1):
            raise BootstrapError(f"the line of {what} {name!r} holds more than its name" + (" and an owner" if what == "service" else ""))
        if name in [entry.name for entry in entries]:
            raise BootstrapError(f"{what} name {name!r} is listed more than once")
        entries.append(Entry(name, parse_owner(words[1]) if len(words) == 2 else None))
    if not entries:
        raise BootstrapError(f"the {what} list names no {what}")
    return entries


def parse_names(text: str, what: str) -> list[str]:
    return [entry.name for entry in parse_entries(text, what)]


def policy_name(service: str) -> str:
    return f"service_{service}"


def policy_text(service: str) -> str:
    """The one policy of a service: it reads and lists its own path and nothing else."""
    return (
        f'path "secret/data/services/{service}/*" {{ capabilities = ["read"] }}\n'
        f'path "secret/metadata/services/{service}/*" {{ capabilities = ["read", "list"] }}\n'
    )


def role_settings(service: str, token_ttl: int, token_max_ttl: int) -> dict[str, Any]:
    """The AppRole of a service: its own policy, a secret ID that never expires and a renewable token with a set lifetime."""
    return {
        "role_id": service,
        "token_policies": [policy_name(service)],
        "token_ttl": token_ttl,
        "token_max_ttl": token_max_ttl,
        "token_num_uses": 0,
        "secret_id_ttl": 0,
        "secret_id_num_uses": 0,
        "bind_secret_id": True,
    }


@dataclass
class State:
    policies: dict[str, str]
    roles: dict[str, dict[str, Any]]
    secret_id_valid: dict[str, bool]
    passwords: dict[str, str | None]


@dataclass(frozen=True)
class Action:
    kind: str
    service: str
    description: str


def plan(services: list[str], tools: list[str], state: State, password_of: Callable[[str], str], token_ttl: int, token_max_ttl: int) -> list[Action]:
    """The changes that make the server match the list, and none when it already does."""
    actions: list[Action] = []
    for service in services:
        stored = state.policies.get(policy_name(service))
        if stored is None or stored.strip() != policy_text(service).strip():
            actions.append(Action("policy", service, f"write policy {policy_name(service)}"))
        role = state.roles.get(service)
        if role is None or any(role.get(key) != value for key, value in role_settings(service, token_ttl, token_max_ttl).items()):
            actions.append(Action("role", service, f"write role {service}"))
        if not state.secret_id_valid.get(service, False):
            actions.append(Action("secret_id", service, f"issue a secret ID for {service}"))
        if service in tools and state.passwords.get(service) != password_of(service):
            actions.append(Action("password", service, f"write the PostgreSQL password of {service}"))
    return actions


def secret_file(secrets_directory: Path, name: str) -> Path:
    return secrets_directory / OPENBAO_FOLDER / name


def read_file(path: Path, what: str) -> str:
    try:
        text = path.read_text().rstrip("\n")
    except OSError as error:
        raise BootstrapError(f"the {what} cannot be read: {error.strerror}") from error
    if not text:
        raise BootstrapError(f"the {what} is empty")
    return text


def make_private_folder(folder: Path) -> None:
    for missing in reversed([path for path in (folder, *folder.parents) if not path.exists()]):
        missing.mkdir(mode=0o700)


def write_private_file(path: Path, text: str, *, replace: bool, owner: tuple[int, int] | None) -> None:
    """Write ``text`` to a new file with mode 0400 from the moment it exists, owned by ``owner`` when given. ``replace`` swaps an existing file atomically."""
    make_private_folder(path.parent)
    target = path.with_name(f".{path.name}.{secrets.token_hex(4)}.new") if replace else path
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, SECRET_FILE_MODE)
    with os.fdopen(descriptor, "w") as handle:
        if owner is not None:
            os.fchown(handle.fileno(), *owner)
        handle.write(text)
    if replace:
        os.replace(target, path)


def access_differs(path: Path, owner: tuple[int, int] | None) -> bool:
    """Whether an existing secret file has another mode than 0400 or another owner than ``owner``."""
    status = path.stat()
    return stat.S_IMODE(status.st_mode) != SECRET_FILE_MODE or (owner is not None and (status.st_uid, status.st_gid) != owner)


def set_access(path: Path, owner: tuple[int, int] | None) -> None:
    if owner is not None:
        os.chown(path, *owner)
    os.chmod(path, SECRET_FILE_MODE)


def prepare(secrets_directory: Path, unlock_key_file: Path | None, owner: tuple[int, int] | None, *, check_only: bool) -> bool:
    """Create the admin secret ID file when it is missing, or set its mode and owner when they differ, and return whether anything changed."""
    if unlock_key_file is not None and not unlock_key_file.is_file():
        raise BootstrapError("the unlock key file is missing: OpenBao cannot start without it")
    path = secret_file(secrets_directory, ADMIN_ID_FILE)
    if not path.exists():
        if not check_only:
            write_private_file(path, secrets.token_urlsafe(SECRET_BYTES), replace=False, owner=owner)
        return True
    read_file(path, "admin secret ID file")
    if not access_differs(path, owner):
        return False
    if not check_only:
        set_access(path, owner)
    return True


class Api:
    """The OpenBao HTTP API on one address. Secrets travel in request bodies and headers only."""

    def __init__(self, address: str) -> None:
        parts = urlsplit(address)
        if parts.scheme != "http" or not parts.hostname:
            raise BootstrapError("the OpenBao address must be an http address")
        self.address = address
        self.host = parts.hostname
        self.port = parts.port or 80

    def call(self, method: str, path: str, token: str | None = None, body: dict[str, Any] | None = None) -> tuple[int, dict[str, Any]]:
        connection = http.client.HTTPConnection(self.host, self.port, timeout=HTTP_SECONDS)
        headers = {"X-Vault-Token": token} if token else {}
        try:
            connection.request(method, f"/v1/{path}", json.dumps(body) if body is not None else None, headers)
            response = connection.getresponse()
            raw = response.read().decode(errors="replace")
        except (OSError, http.client.HTTPException) as error:
            raise BootstrapError(f"{method} {path}: no answer from {self.address} ({type(error).__name__})") from error
        finally:
            connection.close()
        try:
            document = json.loads(raw) if raw else {}
        except ValueError:
            document = {}
        return response.status, document if isinstance(document, dict) else {}

    def must(self, method: str, path: str, token: str | None = None, body: dict[str, Any] | None = None) -> dict[str, Any]:
        status, document = self.call(method, path, token, body)
        if status not in (200, 204):
            errors = " ".join(str(item) for item in document.get("errors") or [])[:ERROR_CHARACTERS]
            raise BootstrapError(f"{method} {path}: HTTP {status}: {errors}")
        return document

    def wait_ready(self, deadline_seconds: int) -> None:
        """Wait until the server is initialised and unsealed, or fail at the deadline with the last answer."""
        deadline = time.monotonic() + deadline_seconds
        last = "no answer"
        while True:
            try:
                status, document = self.call("GET", "sys/seal-status")
                last = f"HTTP {status}, initialized {document.get('initialized')}, sealed {document.get('sealed')}"
                if status == 200 and document.get("initialized") is True and document.get("sealed") is False:
                    return
            except (BootstrapError, SecretFileError) as error:
                last = str(error)
            if time.monotonic() >= deadline:
                raise BootstrapError(f"OpenBao at {self.address} did not become ready within {deadline_seconds} seconds (last answer: {last})")
            time.sleep(READY_POLL_SECONDS)

    def login(self, role: str, secret_id: str) -> str:
        status, document = self.call("POST", "auth/approle/login", body={"role_id": role, "secret_id": secret_id})
        token = (document.get("auth") or {}).get("client_token")
        if status != 200 or not token:
            raise BootstrapError(
                f"the login of role {role} was refused (HTTP {status}); if OpenBao started with another admin secret ID, its data volume must be reset"
            )
        return str(token)


def read_state(api: Api, token: str, services: list[str], tools: list[str], secrets_directory: Path) -> State:
    state = State(policies={}, roles={}, secret_id_valid={}, passwords={})
    for service in services:
        status, document = api.call("GET", f"sys/policies/acl/{policy_name(service)}", token)
        if status == 200:
            state.policies[policy_name(service)] = str((document.get("data") or {}).get("policy") or "")
        elif status != 404:
            raise BootstrapError(f"GET sys/policies/acl/{policy_name(service)}: HTTP {status}")
        status, document = api.call("GET", f"auth/approle/role/{service}", token)
        if status == 200:
            _, identifier = api.call("GET", f"auth/approle/role/{service}/role-id", token)
            state.roles[service] = {**(document.get("data") or {}), "role_id": (identifier.get("data") or {}).get("role_id")}
        elif status != 404:
            raise BootstrapError(f"GET auth/approle/role/{service}: HTTP {status}")
        file = secret_file(secrets_directory, f"{service}.secret_id")
        if service in state.roles and file.exists():
            status, document = api.call(
                "POST", f"auth/approle/role/{service}/secret-id/lookup", token, {"secret_id": read_file(file, f"secret ID file of {service}")}
            )
            state.secret_id_valid[service] = status == 200 and bool(document.get("data"))
        if service in tools:
            status, document = api.call("GET", f"secret/data/services/{service}/postgres", token)
            if status == 200:
                state.passwords[service] = str(((document.get("data") or {}).get("data") or {}).get("password") or "")
            elif status != 404:
                raise BootstrapError(f"GET secret/data/services/{service}/postgres: HTTP {status}")
    return state


def apply(api: Api, token: str, action: Action, secrets_directory: Path, owner: tuple[int, int] | None, token_ttl: int, token_max_ttl: int) -> None:
    service = action.service
    if action.kind == "policy":
        api.must("PUT", f"sys/policies/acl/{policy_name(service)}", token, {"policy": policy_text(service)})
    elif action.kind == "role":
        api.must("POST", f"auth/approle/role/{service}", token, role_settings(service, token_ttl, token_max_ttl))
    elif action.kind == "secret_id":
        issued = str((api.must("POST", f"auth/approle/role/{service}/secret-id", token).get("data") or {}).get("secret_id") or "")
        if not issued:
            raise BootstrapError(f"OpenBao returned no secret ID for {service}")
        write_private_file(secret_file(secrets_directory, f"{service}.secret_id"), issued, replace=True, owner=owner)
    else:
        password = PasswordStore.in_secrets(secrets_directory).read(service)
        api.must("POST", f"secret/data/services/{service}/postgres", token, {"data": {"password": password}})


def unlisted_roles(api: Api, token: str, services: list[str]) -> list[str]:
    status, document = api.call("GET", "auth/approle/role?list=true", token)
    if status != 200:
        return []
    keys = [str(key) for key in (document.get("data") or {}).get("keys") or []]
    return sorted(key for key in keys if key != ADMIN_ROLE and key not in services)


def access_actions(entries: list[Entry], secrets_directory: Path, issued: list[str]) -> list[Action]:
    """The secret ID files that exist with another mode or owner than the list line asks for. A file issued in this run is already right."""
    actions: list[Action] = []
    for entry in entries:
        path = secret_file(secrets_directory, f"{entry.name}.secret_id")
        if entry.name not in issued and path.exists() and access_differs(path, entry.owner):
            actions.append(Action("access", entry.name, f"set the mode and owner of the secret ID file of {entry.name}"))
    return actions


def reconcile(params: dict[str, Any], *, check_only: bool) -> dict[str, Any]:
    secrets_directory = Path(params["secrets_directory"])
    entries = parse_entries(Path(params["services_file"]).read_text(), "service")
    services = [entry.name for entry in entries]
    owners = {entry.name: entry.owner for entry in entries}
    tools = parse_names(Path(params["tools_file"]).read_text(), "tool")
    if missing := [tool for tool in tools if tool not in services]:
        raise BootstrapError(f"PostgreSQL tool {missing[0]!r} has no entry in the service list, so its password would never reach OpenBao")
    api = Api(params["address"])
    api.wait_ready(int(params["ready_deadline_seconds"]))
    token = api.login(ADMIN_ROLE, read_file(secret_file(secrets_directory, ADMIN_ID_FILE), "admin secret ID file"))
    ttl, max_ttl = int(params["token_ttl_seconds"]), int(params["token_max_ttl_seconds"])
    state = read_state(api, token, services, tools, secrets_directory)
    actions = plan(services, tools, state, PasswordStore.in_secrets(secrets_directory).read, ttl, max_ttl)
    actions += access_actions(entries, secrets_directory, [action.service for action in actions if action.kind == "secret_id"])
    if not check_only:
        for action in actions:
            if action.kind == "access":
                set_access(secret_file(secrets_directory, f"{action.service}.secret_id"), owners[action.service])
            else:
                apply(api, token, action, secrets_directory, owners[action.service], ttl, max_ttl)
    return {"changed": bool(actions), "actions": [action.description for action in actions], "unlisted": unlisted_roles(api, token, services)}


def main() -> None:
    module = AnsibleModule(
        argument_spec={
            "step": {"type": "str", "required": True, "choices": ["prepare", "reconcile"]},
            "secrets_directory": {"type": "path", "required": True},
            "unlock_key_file": {"type": "path"},
            "file_owner": {"type": "str"},
            "services_file": {"type": "path"},
            "tools_file": {"type": "path"},
            "address": {"type": "str"},
            "ready_deadline_seconds": {"type": "int", "default": 120},
            "token_ttl_seconds": {"type": "int", "default": 3600},
            "token_max_ttl_seconds": {"type": "int", "default": 86400},
        },
        supports_check_mode=True,
    )
    params = module.params
    try:
        if params["step"] == "prepare":
            unlock = Path(params["unlock_key_file"]) if params["unlock_key_file"] else None
            owner = parse_owner(params["file_owner"]) if params["file_owner"] else None
            created = prepare(Path(params["secrets_directory"]), unlock, owner, check_only=module.check_mode)
            module.exit_json(changed=created, created=[ADMIN_ID_FILE] if created else [])
        if not all(params[key] for key in ("services_file", "tools_file", "address")):
            raise BootstrapError("the reconcile step needs services_file, tools_file and address")
        module.exit_json(**reconcile(params, check_only=module.check_mode))
    except (BootstrapError, SecretFileError) as error:
        module.fail_json(msg=str(error))


if __name__ == "__main__":
    main()
