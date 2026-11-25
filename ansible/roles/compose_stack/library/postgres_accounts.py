"""Ansible module: the PostgreSQL password files and the account and databases of each tool in the tracked list.

``step: passwords`` creates each missing password file, for the server's administrator and for each tool, and keeps existing files. A file is owned
by the numeric user of the container that reads it and has mode 0400: the tool's line in the service list names that user, and the administrator's file
(which the image reads as root, before it drops privileges) and the file of a tool with no user of its own are owned by root. An existing file with another
mode or owner is given the right ones, and its password is kept.

``step: provision`` makes the running server match the tool list: it creates a missing account and each missing database, repairs an account whose
password or attributes differ from the file and removes every grant on a tool's databases, so that only the tool's own account (and the administrator)
can connect. A server that already matches needs no change and the step reports none.

``step: slots`` reads how many client sessions each account holds (a superuser's too, but not this read's own) and how many the server admits for
ordinary accounts, and reports whether they are all held. It changes nothing and prints counts by account name only, never a query, an address or a password.

A line of the tool list is a bare tool name, which owns one database named for the tool, or ``tool: database database ...``, which owns the databases it
lists. A database belongs to one tool: a name listed twice or owned by another tool is refused.

The module computes each password's SCRAM-SHA-256 verifier itself and sends only the verifier to the server, so no password reaches a command
line, a statement or a log. A failed psql call shows its exit code and one fixed word (refused, unreachable, not found or failed), never psql's text.
It uses only the standard library and runs on the host's Python.
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
from ansible.module_utils.secret_files import Entry, PasswordStore, SecretFileError, access_differs, parse_entries, parse_owner

TOOL_NAME = re.compile(r"[a-z][a-z0-9_]{0,30}")
RESERVED_NAMES = {"postgres", "public", "template0", "template1"}
ADMINISTRATOR = "postgres"
SERVICE = "postgres"
ADMINISTRATOR_FILE_IN_CONTAINER = "/run/secrets/postgres_admin_password"
# psql reads the administrator's password from a private file on the container's memory file system, which the script removes when psql ends, so the
# password is in no process environment and no argument.
PSQL_SCRIPT = r"""set -eu; umask 077; file=$(mktemp /dev/shm/pgpass.XXXXXX); trap 'rm -f "$file"' EXIT
{ printf '*:*:*:@ACCOUNT@:'; sed -e 's/\\/\\\\/g' -e 's/:/\\:/g' @PASSWORD_FILE@; printf '\n'; } > "$file"
PGPASSFILE="$file" psql @OPTIONS@"""
# A tool's reader cannot take every character: SmallRye Config 3.17.2 (Nessie) expands ``${...}`` and drops a backslash before ``$`` after the parse.
UNREADABLE_CHARACTERS = {"nessie": "$\\"}
ITERATIONS = 4096
MAXIMUM_ITERATIONS = 1_000_000
SALT_BYTES = 16
ATTRIBUTES = "LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
VERIFIER = re.compile(r"SCRAM-SHA-256\$(\d+):([A-Za-z0-9+/=]+)\$([A-Za-z0-9+/=]+):([A-Za-z0-9+/=]+)")
# One fixed word for the text that psql prints on standard error. psql repeats the failed statement and the server names roles, databases and paths, so none of
# the text is shown.
REASONS = (
    ("refused", ("authentication failed", "permission denied", "must be superuser", "no pg_hba.conf entry")),
    (
        "unreachable",
        (
            "could not connect to server",
            "Connection refused",
            "is starting up",
            "is shutting down",
            "closed the connection unexpectedly",
            "Is the server running",
        ),
    ),
    ("not found", ("does not exist", "is not running", "no such service", "executable file not found")),
)


class AccountError(RuntimeError):
    """The tool list, a password file or the server's answer is not usable. The message never holds a password."""


def check_name(kind: str, name: str) -> None:
    if not TOOL_NAME.fullmatch(name) or name in RESERVED_NAMES or name.startswith("pg_"):
        raise AccountError(f"{kind} name {name!r} must be 1 to 31 lower case letters, digits or underscores, start with a letter and not clash with the server")


def parse_tools(text: str) -> dict[str, list[str]]:
    """Each tool's databases from the tracked list, one line each. A bare name owns one database named for it."""
    tools: dict[str, list[str]] = {}
    owners: dict[str, str] = {}
    for line in text.splitlines():
        entry = line.split("#", 1)[0].strip()
        if not entry:
            continue
        name, colon, rest = entry.partition(":")
        name = name.strip()
        check_name("tool", name)
        if name in tools:
            raise AccountError(f"tool name {name!r} is listed more than once")
        databases = rest.split() if colon else [name]
        if not databases:
            raise AccountError(f"tool {name!r} lists no database")
        for database in databases:
            check_name("database", database)
            if database in owners:
                reason = "is listed more than once" if owners[database] == name else f"belongs to another tool ({owners[database]!r})"
                raise AccountError(f"database name {database!r} {reason}")
            owners[database] = name
        tools[name] = databases
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


def plan(tools: dict[str, list[str]], roles: dict[str, RoleState], databases: dict[str, DatabaseState], password_of: Callable[[str], str]) -> list[Action]:
    """The statements that make the server match the tool list, and none when it already does."""
    actions: list[Action] = []
    for tool, owned in tools.items():
        name = f'"{tool}"'
        role = roles.get(tool)
        if role is None:
            actions.append(Action(f"create account {tool}", f"CREATE ROLE {name} {ATTRIBUTES} PASSWORD '{scram_verifier(password_of(tool))}'"))
        else:
            if not verifier_matches(role.verifier, password_of(tool)):
                actions.append(Action(f"reset the password of account {tool}", f"ALTER ROLE {name} PASSWORD '{scram_verifier(password_of(tool))}'"))
            if not role.attributes_ok:
                actions.append(Action(f"narrow the attributes of account {tool}", f"ALTER ROLE {name} {ATTRIBUTES}"))
        for database_name in owned:
            database = databases.get(database_name)
            quoted = f'"{database_name}"'
            if database is None:
                actions.append(Action(f"create database {database_name}", f"CREATE DATABASE {quoted} OWNER {name}"))
                grantees: tuple[str, ...] = ("PUBLIC",)
            elif database.owner != tool:
                raise AccountError(f"database {database_name} is owned by another account")
            else:
                grantees = database.grantees
            if grantees:
                actions.append(Action(f"refuse every other account on database {database_name}", f"REVOKE ALL ON DATABASE {quoted} FROM {', '.join(grantees)}"))
    return actions


STATE_QUERY = """\
SELECT 'role', rolname, coalesce(rolpassword, ''),
       (rolcanlogin AND NOT (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls))::text
FROM pg_authid WHERE rolname = ANY (string_to_array(:'tools', ',')) ORDER BY rolname;
SELECT 'database', d.datname, pg_get_userbyid(d.datdba),
       coalesce((SELECT string_agg(DISTINCT CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END, ',')
                 FROM aclexplode(coalesce(d.datacl, acldefault('d', d.datdba))) a WHERE a.grantee <> d.datdba), '')
FROM pg_database d WHERE d.datname = ANY (string_to_array(:'databases', ',')) ORDER BY d.datname;
"""


SLOT_SETTINGS = ("max_connections", "reserved_connections", "superuser_reserved_connections")
SLOTS_QUERY = """\
SELECT 'setting', name, setting FROM pg_settings WHERE name IN ('max_connections', 'reserved_connections', 'superuser_reserved_connections') ORDER BY name;
SELECT 'held', r.rolname, count(*) FROM pg_stat_activity a JOIN pg_roles r ON r.oid = a.usesysid
WHERE a.backend_type = 'client backend' AND a.pid <> pg_backend_pid() GROUP BY r.rolname ORDER BY r.rolname;
"""
BUDGET_FILE_NAME = "compose/postgres_connections.yml"


@dataclass(frozen=True)
class Slots:
    max_connections: int
    reserved: int
    role_reserved: int
    held: dict[str, int]


def parse_slots(output: str) -> Slots:
    settings: dict[str, int] = {}
    held: dict[str, int] = {}
    for line in output.splitlines():
        parts = line.split("|")
        if len(parts) == 3 and parts[0] == "setting" and parts[1] in SLOT_SETTINGS and parts[2].isdigit():
            settings[parts[1]] = int(parts[2])
        elif len(parts) == 3 and parts[0] == "held" and parts[2].isdigit():
            held[parts[1]] = int(parts[2])
        else:
            raise AccountError("the slots query returned a line of an unexpected shape")
    if set(settings) != set(SLOT_SETTINGS):
        raise AccountError("the slots query did not return all three settings")
    return Slots(settings["max_connections"], settings["superuser_reserved_connections"], settings["reserved_connections"], held)


def slots_report(slots: Slots) -> tuple[bool, str]:
    """Whether PostgreSQL now refuses an ordinary account, and the sentence for a failed start: cause, figures, holders and budget file.

    PostgreSQL gives an ordinary account a slot while the sessions of every account, a superuser's too, number fewer than ``max_connections`` less
    ``superuser_reserved_connections`` and ``reserved_connections``. The count leaves out the session of this read, which ends before a tool connects.
    """
    usable = slots.max_connections - slots.reserved - slots.role_reserved
    used = sum(slots.held.values())
    holders = ", ".join(f"{name} {count}" for name, count in sorted(slots.held.items())) or "none"
    if used >= usable:
        return True, (
            f"PostgreSQL connection slots are full: {used} of {usable} slots that an ordinary account may use are held by sessions of every account "
            f"({holders}), "
            f"so a tool that connects now is refused with 'remaining connection slots are reserved for roles with the SUPERUSER attribute'. "
            f"Each tool's maximum is in {BUDGET_FILE_NAME}."
        )
    return (
        False,
        f"PostgreSQL connection slots are not full now: {used} of {usable} slots that an ordinary account may use are held by sessions of every account "
        f"({holders}).",
    )


def tool_variable(tools: dict[str, list[str]]) -> str:
    """The value of the psql variable ``tools``, which the state query splits into the listed accounts."""
    return ",".join(tools)


def database_variable(tools: dict[str, list[str]]) -> str:
    """The value of the psql variable ``databases``, which the state query splits into the listed databases."""
    return ",".join(database for owned in tools.values() for database in owned)


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

    @staticmethod
    def reason(error: str) -> str:
        """The one word of ``REASONS`` whose text psql's standard error holds, or ``failed``."""
        return next((word for word, texts in REASONS if any(text in error for text in texts)), "failed")

    def psql(self, sql: str, tools: dict[str, list[str]]) -> str:
        options = (
            f'-X -q -A -t -F "|" -v ON_ERROR_STOP=1 -v tools={tool_variable(tools)} -v databases={database_variable(tools)} -U {ADMINISTRATOR} -d postgres'
        )
        script = PSQL_SCRIPT.replace("@PASSWORD_FILE@", ADMINISTRATOR_FILE_IN_CONTAINER).replace("@ACCOUNT@", ADMINISTRATOR).replace("@OPTIONS@", options)
        code, output, error = self.run([*self.compose_command, "exec", "-T", SERVICE, "sh", "-c", script], sql)
        if code != 0:
            raise AccountError(f"service {SERVICE}: psql exited with {code} (reason: {self.reason(error)})")
        return output


def password_owners(tools: list[str], services: list[Entry], root_owner: tuple[int, int]) -> dict[str, tuple[int, int]]:
    """The owner of each password file: root for the server's administrator, and for each tool the owner on its line in the service list."""
    on_list = {entry.name: entry.owner for entry in services}
    owners = {ADMINISTRATOR: root_owner}
    for tool in tools:
        if tool not in on_list:
            raise AccountError(f"tool {tool} has no line in the service list, so no container user is known to own its password file")
        owners[tool] = on_list[tool] or root_owner
    return owners


def make_password_files(store: PasswordStore, owners: dict[str, tuple[int, int]], *, check_only: bool) -> tuple[list[str], list[str]]:
    """Create each missing password file and give each existing one its owner and mode. Return the accounts that were created and the ones that changed."""
    created: list[str] = []
    protected: list[str] = []
    for account, owner in owners.items():
        if store.path(account).exists() and any(character in store.read(account) for character in UNREADABLE_CHARACTERS.get(account, "")):
            raise AccountError(f"the password file of {account} holds a character that its reader treats as an escape ($ or a backslash); replace the password")
        if not store.path(account).exists():
            created.append(account)
            if not check_only:
                store.ensure(account, owner)
        elif check_only:
            store.read(account)
            if access_differs(store.path(account), owner):
                protected.append(account)
        elif store.protect(account, owner):
            protected.append(account)
    return created, protected


def main() -> None:
    module = AnsibleModule(
        argument_spec={
            "step": {"type": "str", "required": True, "choices": ["passwords", "provision", "slots"]},
            "tools_file": {"type": "path", "required": True},
            "secrets_directory": {"type": "path", "required": True},
            "services_file": {"type": "path"},
            "root_owner": {"type": "str", "default": "0:0"},
            "compose_command": {"type": "list", "elements": "str"},
        },
        supports_check_mode=True,
    )
    try:
        tools = parse_tools(Path(module.params["tools_file"]).read_text())
        store = PasswordStore.in_secrets(Path(module.params["secrets_directory"]))
        accounts = [ADMINISTRATOR, *tools]
        if module.params["step"] == "passwords":
            if not module.params["services_file"]:
                raise AccountError("the passwords step needs services_file")
            root_owner = parse_owner(module.params["root_owner"], allow_root=True)
            owners = password_owners(list(tools), parse_entries(Path(module.params["services_file"]).read_text(), "service"), root_owner)
            created, protected = make_password_files(store, owners, check_only=module.check_mode)
            module.exit_json(
                changed=bool(created or protected),
                accounts=accounts,
                created=created,
                protected=protected,
                owners={account: f"{owner[0]}:{owner[1]}" for account, owner in owners.items()},
            )
        if not module.params["compose_command"]:
            raise AccountError("the provision step needs compose_command")
        server = Server(lambda args, data: module.run_command(args, data=data, check_rc=False), module.params["compose_command"])
        if module.params["step"] == "slots":
            full, message = slots_report(parse_slots(server.psql(SLOTS_QUERY, tools)))
            module.exit_json(changed=False, full=full, message=message)
        roles, databases = parse_state(server.psql(STATE_QUERY, tools))
        actions = plan(tools, roles, databases, store.read)
        if actions and not module.check_mode:
            server.psql("".join(f"{action.sql};\n" for action in actions), tools)
        module.exit_json(changed=bool(actions), accounts=accounts, actions=[action.description for action in actions])
    except (AccountError, SecretFileError) as error:
        module.fail_json(msg=str(error))


if __name__ == "__main__":
    main()
