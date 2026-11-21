"""Ansible module: the PostgreSQL password files and the one account and one database for each tool in the tracked list.

``step: passwords`` creates each missing password file, for the server's administrator and for each tool, readable by its owner only, and keeps
existing files. ``step: provision`` makes the running server match the tool list: it creates a missing account and database, repairs an account
whose password or attributes differ from the file and removes every grant on a tool's database, so that only the tool's own account (and the
administrator) can connect. A server that already matches needs no change and the step reports none.

The module computes each password's SCRAM-SHA-256 verifier itself and sends only the verifier to the server, so no password reaches a command
line, a statement or a log. It uses only the standard library and runs on the host's Python.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.secret_files import PasswordStore, SecretFileError

TOOL_NAME = re.compile(r"[a-z][a-z0-9_]{0,30}")
RESERVED_NAMES = {"postgres", "public", "template0", "template1"}
ADMINISTRATOR = "postgres"
SERVICE = "postgres"
ADMINISTRATOR_FILE_IN_CONTAINER = "/run/secrets/postgres_admin_password"
ITERATIONS = 4096
MAXIMUM_ITERATIONS = 1_000_000
SALT_BYTES = 16
ATTRIBUTES = "LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
VERIFIER = re.compile(r"SCRAM-SHA-256\$(\d+):([A-Za-z0-9+/=]+)\$([A-Za-z0-9+/=]+):([A-Za-z0-9+/=]+)")
ERROR_CHARACTERS = 400


class AccountError(RuntimeError):
    """The tool list, a password file or the server's answer is not usable. The message never holds a password."""


def parse_tools(text: str) -> list[str]:
    """The tool names in the tracked list: one for each line, with comments and blank lines ignored."""
    tools: list[str] = []
    for line in text.splitlines():
        name = line.split("#", 1)[0].strip()
        if not name:
            continue
        if not TOOL_NAME.fullmatch(name) or name in RESERVED_NAMES or name.startswith("pg_"):
            raise AccountError(
                f"tool name {name!r} must be 1 to 31 lower case letters, digits or underscores, start with a letter and not clash with the server"
            )
        if name in tools:
            raise AccountError(f"tool name {name!r} is listed more than once")
        tools.append(name)
    if not tools:
        raise AccountError("the tool list names no tool")
    return tools


def scram_verifier(password: str, salt: bytes | None = None, iterations: int = ITERATIONS) -> str:
    """The SCRAM-SHA-256 verifier that PostgreSQL stores for ``password``."""
    salt = os.urandom(SALT_BYTES) if salt is None else salt
    salted = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations)
    stored_key = hashlib.sha256(hmac.new(salted, b"Client Key", "sha256").digest()).digest()
    server_key = hmac.new(salted, b"Server Key", "sha256").digest()
    encode = base64.b64encode
    return f"SCRAM-SHA-256${iterations}:{encode(salt).decode()}${encode(stored_key).decode()}:{encode(server_key).decode()}"


def verifier_matches(stored: str, password: str) -> bool:
    """Whether the stored verifier belongs to ``password``. A missing or malformed verifier matches nothing."""
    parsed = VERIFIER.fullmatch(stored)
    if parsed is None or not 0 < int(parsed[1]) <= MAXIMUM_ITERATIONS:
        return False
    try:
        salt = base64.b64decode(parsed[2], validate=True)
    except ValueError:
        return False
    return hmac.compare_digest(scram_verifier(password, salt, int(parsed[1])), stored)


@dataclass(frozen=True)
class RoleState:
    verifier: str
    attributes_ok: bool


@dataclass(frozen=True)
class DatabaseState:
    owner: str
    grantees: tuple[str, ...]


@dataclass(frozen=True)
class Action:
    description: str
    sql: str


def plan(tools: list[str], roles: dict[str, RoleState], databases: dict[str, DatabaseState], password_of: Callable[[str], str]) -> list[Action]:
    """The statements that make the server match the tool list, and none when it already does."""
    actions: list[Action] = []
    for tool in tools:
        name = f'"{tool}"'
        role = roles.get(tool)
        if role is None:
            actions.append(Action(f"create account {tool}", f"CREATE ROLE {name} {ATTRIBUTES} PASSWORD '{scram_verifier(password_of(tool))}'"))
        else:
            if not verifier_matches(role.verifier, password_of(tool)):
                actions.append(Action(f"reset the password of account {tool}", f"ALTER ROLE {name} PASSWORD '{scram_verifier(password_of(tool))}'"))
            if not role.attributes_ok:
                actions.append(Action(f"narrow the attributes of account {tool}", f"ALTER ROLE {name} {ATTRIBUTES}"))
        database = databases.get(tool)
        if database is None:
            actions.append(Action(f"create database {tool}", f"CREATE DATABASE {name} OWNER {name}"))
            grantees: tuple[str, ...] = ("PUBLIC",)
        elif database.owner != tool:
            raise AccountError(f"database {tool} is owned by another account")
        else:
            grantees = database.grantees
        if grantees:
            actions.append(Action(f"refuse every other account on database {tool}", f"REVOKE ALL ON DATABASE {name} FROM {', '.join(grantees)}"))
    return actions


STATE_QUERY = """\
SELECT 'role', rolname, coalesce(rolpassword, ''),
       (rolcanlogin AND NOT (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls))::text
FROM pg_authid WHERE rolname = ANY (string_to_array(:'tools', ',')) ORDER BY rolname;
SELECT 'database', d.datname, pg_get_userbyid(d.datdba),
       coalesce((SELECT string_agg(DISTINCT CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END, ',')
                 FROM aclexplode(coalesce(d.datacl, acldefault('d', d.datdba))) a WHERE a.grantee <> d.datdba), '')
FROM pg_database d WHERE d.datname = ANY (string_to_array(:'tools', ',')) ORDER BY d.datname;
"""


def tool_variable(tools: list[str]) -> str:
    """The value of the psql variable ``tools``, which the state query splits into the listed names."""
    return ",".join(tools)


def parse_state(output: str) -> tuple[dict[str, RoleState], dict[str, DatabaseState]]:
    roles: dict[str, RoleState] = {}
    databases: dict[str, DatabaseState] = {}
    for line in output.splitlines():
        parts = line.split("|")
        if len(parts) == 4 and parts[0] == "role" and parts[3] in ("true", "false"):
            roles[parts[1]] = RoleState(parts[2], parts[3] == "true")
        elif len(parts) == 4 and parts[0] == "database":
            databases[parts[1]] = DatabaseState(parts[2], tuple(item for item in parts[3].split(",") if item))
        else:
            raise AccountError("the state query returned a line of an unexpected shape")
    return roles, databases


class Server:
    """The running PostgreSQL service, reached through ``docker compose exec`` as its administrator with the password that Compose mounted."""

    def __init__(self, run: Callable[[list[str], str], tuple[int, str, str]], compose_command: list[str]) -> None:
        self.run = run
        self.compose_command = compose_command

    def psql(self, sql: str, tools: list[str]) -> str:
        options = f'-X -q -A -t -F "|" -v ON_ERROR_STOP=1 -v tools={tool_variable(tools)} -U {ADMINISTRATOR} -d postgres'
        script = f'PGPASSWORD="$(cat {ADMINISTRATOR_FILE_IN_CONTAINER})" exec psql {options}'
        code, output, error = self.run([*self.compose_command, "exec", "-T", SERVICE, "sh", "-c", script], sql)
        if code != 0:
            shown = re.sub(r"SCRAM-SHA-256\$\S+", "<verifier>", error.strip())[:ERROR_CHARACTERS]
            raise AccountError(f"service {SERVICE}: psql exited with {code}: {shown}")
        return output


def create_missing_passwords(store: PasswordStore, tools: list[str], *, check_only: bool) -> list[str]:
    created: list[str] = []
    for account in (ADMINISTRATOR, *tools):
        if check_only:
            if not store.path(account).exists():
                created.append(account)
        elif store.ensure(account):
            created.append(account)
    return created


def main() -> None:
    module = AnsibleModule(
        argument_spec={
            "step": {"type": "str", "required": True, "choices": ["passwords", "provision"]},
            "tools_file": {"type": "path", "required": True},
            "secrets_directory": {"type": "path", "required": True},
            "compose_command": {"type": "list", "elements": "str"},
        },
        supports_check_mode=True,
    )
    try:
        tools = parse_tools(Path(module.params["tools_file"]).read_text())
        store = PasswordStore.in_secrets(Path(module.params["secrets_directory"]))
        accounts = [ADMINISTRATOR, *tools]
        if module.params["step"] == "passwords":
            created = create_missing_passwords(store, tools, check_only=module.check_mode)
            module.exit_json(changed=bool(created), accounts=accounts, created=created)
        if not module.params["compose_command"]:
            raise AccountError("the provision step needs compose_command")
        server = Server(lambda args, data: module.run_command(args, data=data, check_rc=False), module.params["compose_command"])
        roles, databases = parse_state(server.psql(STATE_QUERY, tools))
        actions = plan(tools, roles, databases, store.read)
        if actions and not module.check_mode:
            server.psql("".join(f"{action.sql};\n" for action in actions), tools)
        module.exit_json(changed=bool(actions), accounts=accounts, actions=[action.description for action in actions])
    except (AccountError, SecretFileError) as error:
        module.fail_json(msg=str(error))


if __name__ == "__main__":
    main()
