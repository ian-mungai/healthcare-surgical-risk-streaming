"""Ansible module: the OpenBao admin secret ID file, and the policy, AppRole, secret ID file and PostgreSQL password of each listed service.

``step: prepare`` creates the admin secret ID file when it is missing, before OpenBao starts for the first time. The server's first start reads it (see
``compose/openbao/openbao.hcl``). The file is owned by the numeric user that the container runs as and has mode 0400. An existing file keeps its content
and is given that owner and mode when it has others.

``step: reconcile`` logs in as the ``bootstrap`` AppRole and makes the running server match the service list: for each service one policy that reads
only its own path, one AppRole named for the service with a renewable token, one secret ID in a file that only the service reads (mode 0400, owned by
the numeric user on its list line, or by root when the line has none) and, for a service that is also a PostgreSQL tool, its PostgreSQL password at its own
path. A server that already matches needs no change and the step reports none. It reports every secret file with its owner, for the role's check. A
service that left the list keeps its role and is reported as unlisted, never removed.

``step: fetch`` writes one external secret (``compose/external_secrets.txt``) from OpenBao to a file for its consumer, after the consumer logs in with
its own AppRole: the pull of the images reads the Docker Hub token that way and removes the file right after. ``step: generated`` keeps a secret that
the host makes for itself (the Grafana administrator password): made once, stored in OpenBao while its path is empty and written from OpenBao to the
file that its container reads.

It uses only the standard library and runs on the host's Python. No secret is a module argument, a command line, a log line or a message.
"""

from __future__ import annotations

import http.client
import json
import re
import secrets
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.external_secrets import ExternalSecretError, form_ok, parse_external, value_digest
from ansible.module_utils.secret_files import (
    Entry,
    PasswordStore,
    SecretFileError,
    access_differs,
    parse_entries,
    parse_names,
    parse_owner,
    read_file,
    set_access,
    write_private_file,
)

ADMIN_ROLE = "bootstrap"
GENERATED_NAME = re.compile(r"[a-z][a-z0-9_]*(?:/[a-z][a-z0-9_]*)+")
OPENBAO_FOLDER = "openbao"
ADMIN_ID_FILE = "bootstrap.secret_id"
SECRET_BYTES = 32
HTTP_SECONDS = 10
READY_POLL_SECONDS = 1
ERROR_CHARACTERS = 300
# The form of a token that goes into a header: the characters of OpenBao's tokens and of a bearer token (RFC 6750, b64token), and nothing else. A control
# character such as a line break makes the HTTP library's header error carry the token's text.
TOKEN_FORM = re.compile(r"[A-Za-z0-9\-._~+/]+=*")


class BootstrapError(RuntimeError):
    """A list, a file or the server's answer is not usable. The message never holds a secret."""


def policy_name(service: str) -> str:
    return f"service_{service}"


def policy_text(service: str, external_paths: Sequence[str] = ()) -> str:
    """The one policy of a service: it reads and lists its own path, and reads each external secret that the list gives it as consumer, and nothing else."""
    return (
        f'path "secret/data/services/{service}/*" {{ capabilities = ["read"] }}\n'
        f'path "secret/metadata/services/{service}/*" {{ capabilities = ["read", "list"] }}\n'
        + "".join(f'path "{path}" {{ capabilities = ["read"] }}\n' for path in external_paths)
    )


def external_paths(list_text: str, services: Sequence[str]) -> dict[str, list[str]]:
    """The paths of the external secrets that each consumer reads, from the tracked list. A consumer that is no listed service has no policy: it is refused."""
    try:
        declared = parse_external(list_text)
    except ExternalSecretError as error:
        raise BootstrapError(str(error)) from error
    paths: dict[str, list[str]] = {}
    for item in declared:
        if item.consumer not in services:
            raise BootstrapError(f"the consumer {item.consumer!r} of external secret {item.name!r} is not in the service list, so it has no policy")
        paths.setdefault(item.consumer, []).append(item.path)
    return paths


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


def plan(
    services: list[str],
    tools: list[str],
    state: State,
    password_of: Callable[[str], str],
    token_ttl: int,
    token_max_ttl: int,
    external: Mapping[str, Sequence[str]] | None = None,
) -> list[Action]:
    """The changes that make the server match the list, and none when it already does."""
    actions: list[Action] = []
    for service in services:
        stored = state.policies.get(policy_name(service))
        if stored is None or stored.strip() != policy_text(service, (external or {}).get(service, ())).strip():
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


def prepare(secrets_directory: Path, unlock_key_file: Path | None, owner: tuple[int, int], *, check_only: bool) -> bool:
    """Create the admin secret ID file when it is missing, or give it its owner and mode 0400 when it has others, and return whether anything changed."""
    if unlock_key_file is not None and not unlock_key_file.is_file():
        raise BootstrapError("the unlock key file is missing: OpenBao cannot start without it")
    path = secret_file(secrets_directory, ADMIN_ID_FILE)
    if not path.exists():
        if not check_only:
            write_private_file(path, secrets.token_urlsafe(SECRET_BYTES), replace=False, owner=owner)
        return True
    read_file(path, "admin secret ID file")
    if path.read_bytes().endswith((b"\n", b"\r")):
        raise BootstrapError(
            "the admin secret ID file ends with a line break; the first start keeps the file's content and the login trims it, so remove the break"
        )
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
        if token and TOKEN_FORM.fullmatch(token) is None:
            raise BootstrapError("the token that OpenBao gave is not a valid token")
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
        if status == 200 and token and type(token) is not str:
            raise BootstrapError(f"the login of role {role} gave a token that is not text")
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


def apply(
    api: Api,
    token: str,
    action: Action,
    secrets_directory: Path,
    owner: tuple[int, int],
    token_ttl: int,
    token_max_ttl: int,
    external: Mapping[str, Sequence[str]] | None = None,
) -> None:
    service = action.service
    if action.kind == "policy":
        api.must("PUT", f"sys/policies/acl/{policy_name(service)}", token, {"policy": policy_text(service, (external or {}).get(service, ()))})
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


def secret_files(secrets_directory: Path, entries: list[Entry], admin_owner: tuple[int, int], root_owner: tuple[int, int]) -> dict[Path, tuple[int, int]]:
    """Every secret file of OpenBao with its owner: the admin file, then the secret ID file of each service (root owns it when the line names no user)."""
    files = {secret_file(secrets_directory, ADMIN_ID_FILE): admin_owner}
    for entry in entries:
        files[secret_file(secrets_directory, f"{entry.name}.secret_id")] = entry.owner or root_owner
    return files


def access_actions(entries: list[Entry], secrets_directory: Path, issued: list[str], root_owner: tuple[int, int]) -> list[Action]:
    """The secret ID files that exist with another mode or owner than the list line asks for. A file issued in this run is already right."""
    actions: list[Action] = []
    for entry in entries:
        path = secret_file(secrets_directory, f"{entry.name}.secret_id")
        if entry.name not in issued and path.exists() and access_differs(path, entry.owner or root_owner):
            actions.append(Action("access", entry.name, f"set the mode and owner of the secret ID file of {entry.name}"))
    return actions


def kv_value(api: Api, token: str, path: str) -> str | None:
    """The text in the field ``value`` at ``secret/data/<path>``, or None when the path is empty."""
    status, document = api.call("GET", f"secret/data/{path}", token)
    if status == 404:
        return None
    if status != 200:
        raise BootstrapError(f"GET secret/data/{path}: HTTP {status}")
    value = ((document.get("data") or {}).get("data") or {}).get("value")
    if not isinstance(value, str):
        raise BootstrapError(f"the value at secret/data/{path} is not text")
    return value


def kv_store(api: Api, token: str, path: str, value: str) -> None:
    """Store ``value`` at ``secret/data/<path>`` with check-and-set on the current version, read it back and compare it by SHA-256.

    A stored text that differs is an error."""
    status, document = api.call("GET", f"secret/metadata/{path}", token)
    if status not in (200, 404):
        raise BootstrapError(f"GET secret/metadata/{path}: HTTP {status}")
    version = int((document.get("data") or {}).get("current_version") or 0) if status == 200 else 0
    api.must("POST", f"secret/data/{path}", token, {"options": {"cas": version}, "data": {"value": value}})
    read_back = kv_value(api, token, path)
    if read_back is None or value_digest(read_back) != value_digest(value):
        raise BootstrapError(f"OpenBao stored another text at secret/data/{path} than the one sent")


def write_when_different(dest: Path, value: str, owner: tuple[int, int], *, check_only: bool) -> bool:
    """Make the file hold ``value`` for ``owner`` with mode 0400 (a final line break counts as the same text) and return whether it had to change."""
    same = dest.is_file() and dest.read_text().rstrip("\n") == value
    if same and not access_differs(dest, owner):
        return False
    if not check_only:
        if same:
            set_access(dest, owner)
        else:
            write_private_file(dest, value, replace=True, owner=owner)
    return True


def fetch_external(params: dict[str, Any], *, check_only: bool) -> dict[str, Any]:
    """Write an external secret from OpenBao to a file for its consumer, after the consumer logs in with its own AppRole.

    The list names the consumer of each secret, and a consumer reads only its own. The value goes to the file and to no result, argument or message."""
    name, consumer = str(params["name"]), str(params["consumer"])
    try:
        declared = {item.name: item for item in parse_external(Path(params["external_secrets_file"]).read_text())}
    except ExternalSecretError as error:
        raise BootstrapError(str(error)) from error
    if name not in declared:
        raise BootstrapError(f"external secret {name!r} is not declared in the list")
    if declared[name].consumer != consumer:
        raise BootstrapError(f"external secret {name!r} is for the consumer {declared[name].consumer!r}, not {consumer!r}")
    api = Api(params["address"])
    token = api.login(consumer, read_file(secret_file(Path(params["secrets_directory"]), f"{consumer}.secret_id"), f"secret ID file of {consumer}"))
    value = kv_value(api, token, f"external/{name}")
    if value is None:
        raise BootstrapError(f"external secret {name} is empty in OpenBao: the launcher's intake (make host-up) stores it before the images are pulled")
    if not form_ok(declared[name].check, value):
        raise BootstrapError(f"the value stored for external secret {name} is not in the form of its check")
    changed = write_when_different(Path(params["dest"]), value, parse_owner(params["file_owner"], allow_root=True), check_only=check_only)
    return {"changed": changed}


def generated_secret(params: dict[str, Any], *, check_only: bool) -> dict[str, Any]:
    """Keep a secret that the host makes for itself in OpenBao, and write it from there to the file that its container reads.

    While the path ``secret/data/services/<name>`` is empty, the value is the file's text when the file exists (a host made before the secret moved to
    OpenBao keeps its password) and a new random one otherwise. The admin login stores it. The file is then made to hold what OpenBao holds.
    The value goes to no result or message."""
    name = str(params["name"])
    if GENERATED_NAME.fullmatch(name) is None:
        raise BootstrapError(f"the name {name!r} of a generated secret is not folders and a name of lower case letters, digits and underscores")
    secrets_directory, dest = Path(params["secrets_directory"]), Path(params["dest"])
    owner = parse_owner(params["file_owner"], allow_root=True)
    api = Api(params["address"])
    token = api.login(ADMIN_ROLE, read_file(secret_file(secrets_directory, ADMIN_ID_FILE), "admin secret ID file"))
    path = f"services/{name}"
    value, stored = kv_value(api, token, path), True
    if value is None:
        stored = False
        value = read_file(dest, f"password file {dest.name}") if dest.is_file() and dest.stat().st_size else secrets.token_urlsafe(SECRET_BYTES)
        if value != value.strip():
            raise BootstrapError(f"the file {dest.name} starts or ends with white space, which a password of this host never does")
        if not check_only:
            kv_store(api, token, path, value)
    changed = write_when_different(dest, value, owner, check_only=check_only)
    return {"changed": changed or not stored}


def reconcile(params: dict[str, Any], *, check_only: bool) -> dict[str, Any]:
    secrets_directory = Path(params["secrets_directory"])
    entries = parse_entries(Path(params["services_file"]).read_text(), "service")
    services = [entry.name for entry in entries]
    root_owner = parse_owner(params["root_owner"], allow_root=True)
    admin_owner = parse_owner(params["file_owner"])
    owners = {entry.name: entry.owner or root_owner for entry in entries}
    tools = parse_names(Path(params["tools_file"]).read_text(), "tool")
    if missing := [tool for tool in tools if tool not in services]:
        raise BootstrapError(f"PostgreSQL tool {missing[0]!r} has no entry in the service list, so its password would never reach OpenBao")
    external = external_paths(Path(params["external_secrets_file"]).read_text(), services) if params.get("external_secrets_file") else {}
    api = Api(params["address"])
    api.wait_ready(int(params["ready_deadline_seconds"]))
    token = api.login(ADMIN_ROLE, read_file(secret_file(secrets_directory, ADMIN_ID_FILE), "admin secret ID file"))
    ttl, max_ttl = int(params["token_ttl_seconds"]), int(params["token_max_ttl_seconds"])
    state = read_state(api, token, services, tools, secrets_directory)
    actions = plan(services, tools, state, PasswordStore.in_secrets(secrets_directory).read, ttl, max_ttl, external)
    actions += access_actions(entries, secrets_directory, [action.service for action in actions if action.kind == "secret_id"], root_owner)
    if not check_only:
        for action in actions:
            if action.kind == "access":
                set_access(secret_file(secrets_directory, f"{action.service}.secret_id"), owners[action.service])
            else:
                apply(api, token, action, secrets_directory, owners[action.service], ttl, max_ttl, external)
    files = [
        {"path": str(path), "owner": f"{owner[0]}:{owner[1]}"} for path, owner in secret_files(secrets_directory, entries, admin_owner, root_owner).items()
    ]
    return {
        "changed": bool(actions),
        "actions": [action.description for action in actions],
        "unlisted": unlisted_roles(api, token, services),
        "secret_files": files,
    }


def main() -> None:
    module = AnsibleModule(
        argument_spec={
            "step": {"type": "str", "required": True, "choices": ["prepare", "reconcile", "fetch", "generated"]},
            "consumer": {"type": "str"},
            "name": {"type": "str"},
            "dest": {"type": "path"},
            "secrets_directory": {"type": "path", "required": True},
            "unlock_key_file": {"type": "path"},
            "file_owner": {"type": "str", "required": True},
            "root_owner": {"type": "str", "default": "0:0"},
            "services_file": {"type": "path"},
            "tools_file": {"type": "path"},
            "external_secrets_file": {"type": "path"},
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
            owner = parse_owner(params["file_owner"])
            created = prepare(Path(params["secrets_directory"]), unlock, owner, check_only=module.check_mode)
            files = [{"path": str(secret_file(Path(params["secrets_directory"]), ADMIN_ID_FILE)), "owner": params["file_owner"]}]
            module.exit_json(changed=created, created=[ADMIN_ID_FILE] if created else [], secret_files=files)
        if params["step"] in ("fetch", "generated"):
            needed = ("address", "dest", "name") + (("consumer", "external_secrets_file") if params["step"] == "fetch" else ())
            if not all(params[key] for key in needed):
                raise BootstrapError(f"the {params['step']} step needs {', '.join(needed)}")
            run = fetch_external if params["step"] == "fetch" else generated_secret
            module.exit_json(**run(params, check_only=module.check_mode))
        if not all(params[key] for key in ("services_file", "tools_file", "address")):
            raise BootstrapError("the reconcile step needs services_file, tools_file and address")
        module.exit_json(**reconcile(params, check_only=module.check_mode))
    except (BootstrapError, SecretFileError) as error:
        module.fail_json(msg=str(error))


if __name__ == "__main__":
    main()
