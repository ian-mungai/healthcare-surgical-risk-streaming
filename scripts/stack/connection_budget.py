"""Load and check the PostgreSQL connection budget, the one source of how many slots each client of the shared server may hold.

``compose/postgres_connections.yml`` states the server's ``max_connections``, its ``superuser_reserved_connections`` and a margin, then one entry for each
client: the largest number of connections it may hold at once, the settings in tracked files that cap it, and the connections that no setting caps (the
allotment). The checks here fail when the maxima and the margin do not fit under the slots that the server gives to ordinary accounts, when a service of
the limits file or an account of the tool list has no entry, and when a tracked file does not carry the figure that its entry states.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

from scripts.project_config import ROOT
from scripts.stack.yaml_text import YamlError, load_yaml

BUDGET_FILE = ROOT / "compose" / "postgres_connections.yml"
SERVER_SERVICE = "postgres"
DATABASE_USE = re.compile(r"/postgres/[a-z_]+\.password|postgres:\d+")


class BudgetError(ValueError):
    """The budget file is missing or malformed."""


@dataclass(frozen=True)
class Setting:
    file: str
    path: str
    value: int
    count: int
    flag: str | None


@dataclass(frozen=True)
class Client:
    name: str
    account: str
    services: tuple[str, ...]
    max: int
    allotment: int
    settings: tuple[Setting, ...]
    source: str


@dataclass(frozen=True)
class Budget:
    max_connections: int
    superuser_reserved_connections: int
    margin: int
    clients: Mapping[str, Client]
    not_clients: tuple[str, ...]


TOP_LEVEL_KEYS = {"postgres", "clients", "not_clients"}
POSTGRES_KEYS = {"max_connections", "superuser_reserved_connections", "margin"}
CLIENT_KEYS = {"account", "services", "max", "allotment", "settings", "source"}
SETTING_KEYS = {"file", "path", "value", "count", "flag"}
SETTING_REQUIRED = {"file", "path", "value", "count"}


def whole_number(value: object, *, minimum: int) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= minimum


def mapping(value: object, what: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise BudgetError(f"{what}: must be a mapping")
    return value


def exact_keys(entry: Mapping[str, object], allowed: set[str], required: set[str], what: str) -> None:
    for key in sorted(required - set(entry)):
        raise BudgetError(f"{what}: the key {key} is missing")
    for key in sorted(set(entry) - allowed):
        raise BudgetError(f"{what}: the key {key} is not known")


def parse_setting(client: str, entry: object) -> Setting:
    what = f"client {client}: a setting"
    setting = mapping(entry, what)
    exact_keys(setting, SETTING_KEYS, SETTING_REQUIRED, what)
    file, path, flag = setting["file"], setting["path"], setting.get("flag")
    if not (isinstance(file, str) and file and isinstance(path, str) and path and (flag is None or isinstance(flag, str) and flag)):
        raise BudgetError(f"{what}: file, path and flag must be text")
    if not whole_number(setting["value"], minimum=1) or not whole_number(setting["count"], minimum=0):
        raise BudgetError(f"{what}: value must be a positive whole number and count a whole number (0 for a setting that holds no connection)")
    return Setting(file, path, int(str(setting["value"])), int(str(setting["count"])), flag)


def parse_client(name: str, entry: object) -> Client:
    what = f"client {name}"
    client = mapping(entry, what)
    exact_keys(client, CLIENT_KEYS, CLIENT_KEYS - {"settings"}, what)
    services, settings = client["services"], client.get("settings", [])
    if not (isinstance(services, list) and services and all(isinstance(item, str) and item for item in services)):
        raise BudgetError(f"{what}: services must list at least one service name")
    if not isinstance(settings, list):
        raise BudgetError(f"{what}: settings must be a list")
    if not (isinstance(client["account"], str) and client["account"] and isinstance(client["source"], str) and client["source"]):
        raise BudgetError(f"{what}: account and source must be text")
    if not whole_number(client["max"], minimum=1) or not whole_number(client["allotment"], minimum=0):
        raise BudgetError(f"{what}: max must be a positive whole number and allotment a whole number")
    return Client(
        name,
        client["account"],
        tuple(services),
        int(str(client["max"])),
        int(str(client["allotment"])),
        tuple(parse_setting(name, item) for item in settings),
        client["source"],
    )


def parse_budget(text: str) -> Budget:
    """The budget in ``text``, or ``BudgetError`` naming the first thing that is missing, unknown or not a whole number."""
    try:
        document = mapping(load_yaml(text), "the budget file")
    except YamlError as error:
        raise BudgetError(str(error)) from error
    exact_keys(document, TOP_LEVEL_KEYS, TOP_LEVEL_KEYS, "the budget file")
    server = mapping(document["postgres"], "postgres")
    exact_keys(server, POSTGRES_KEYS, POSTGRES_KEYS, "postgres")
    for key in sorted(POSTGRES_KEYS):
        if not whole_number(server[key], minimum=1 if key == "max_connections" else 0):
            raise BudgetError(f"postgres: {key} must be a whole number" + (" above zero" if key == "max_connections" else ""))
    clients = mapping(document["clients"], "clients")
    if not clients:
        raise BudgetError("clients: at least one client is needed")
    not_clients = document["not_clients"]
    if not (isinstance(not_clients, list) and all(isinstance(item, str) and item for item in not_clients)):
        raise BudgetError("not_clients: must be a list of service names")
    return Budget(
        int(str(server["max_connections"])),
        int(str(server["superuser_reserved_connections"])),
        int(str(server["margin"])),
        {name: parse_client(name, entry) for name, entry in clients.items()},
        tuple(not_clients),
    )


def load_budget(path: Path = BUDGET_FILE) -> Budget:
    try:
        text = path.read_text()
    except OSError as error:
        raise BudgetError(f"{path.name}: the file cannot be read") from error
    return parse_budget(text)


def usable_slots(budget: Budget) -> int:
    """The slots that the server gives to ordinary accounts: the rest is kept for superusers, so that an administrator can still connect."""
    return budget.max_connections - budget.superuser_reserved_connections


def budget_problems(budget: Budget) -> list[str]:
    """Each way the figures do not add up: a reserve not below the slots, a maximum that is not its settings plus its allotment, a sum over the slots."""
    problems = []
    if budget.superuser_reserved_connections >= budget.max_connections:
        problems.append(f"superuser_reserved_connections {budget.superuser_reserved_connections} must be below max_connections {budget.max_connections}")
    for client in budget.clients.values():
        capped = sum(setting.value * setting.count for setting in client.settings)
        if client.max != capped + client.allotment:
            problems.append(f"{client.name}: max {client.max} is not the settings' {capped} plus the allotment {client.allotment}")
    total = sum(client.max for client in budget.clients.values())
    if total + budget.margin > usable_slots(budget):
        problems.append(
            f"the clients' maxima {total} plus the margin {budget.margin} come to {total + budget.margin}, over the {usable_slots(budget)} slots for "
            f"ordinary accounts (max_connections {budget.max_connections} less superuser_reserved_connections {budget.superuser_reserved_connections})"
        )
    return problems


def coverage_problems(budget: Budget, limit_services: Iterable[str], tool_accounts: Iterable[str], compose_texts: Mapping[str, str]) -> list[str]:
    """Each service, name, account and Compose file that the entries do not cover or cover twice."""
    problems = []
    known = set(limit_services)
    owners: dict[str, list[str]] = {}
    for client in budget.clients.values():
        for service in client.services:
            owners.setdefault(service, []).append(client.name)
            if service not in known:
                problems.append(f"{service}: client {client.name} names a service that the limits file does not have")
    for service in budget.not_clients:
        if service not in known:
            problems.append(f"{service}: not_clients names a service that the limits file does not have")
    for service in sorted(known):
        if service in owners and service in budget.not_clients:
            problems.append(f"{service}: listed both as a service of client {owners[service][0]} and in not_clients")
        elif service not in owners and service not in budget.not_clients:
            problems.append(
                f"{service}: no entry, neither in a client's services nor in not_clients; add the client with its maximum, or list it in not_clients"
            )
    for service, text in sorted(compose_texts.items()):
        if service != SERVER_SERVICE and DATABASE_USE.search(text) and service not in owners:
            problems.append(f"{service}: its Compose file reads a PostgreSQL password or address and no client lists the service")
    accounts = set(tool_accounts)
    for account in sorted(accounts - {client.account for client in budget.clients.values()}):
        problems.append(f"{account}: an account of the tool list has no client in the budget")
    for client in budget.clients.values():
        if client.account not in accounts:
            problems.append(f"{client.name}: the account {client.account} is not on the tool list")
    return problems


def find_setting(document: object, setting: Setting) -> object | None:
    node = document
    for part in setting.path.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    if setting.flag is None:
        return node
    if not isinstance(node, list) or setting.flag not in node or node.index(setting.flag) + 1 >= len(node):
        return None
    found: object = node[node.index(setting.flag) + 1]
    return found


def setting_problems(budget: Budget, read: Callable[[str], str]) -> list[str]:
    """Each tracked file that does not carry the cap that its client's entry states: the setting is missing, or its value is another figure."""
    problems = []
    for client in budget.clients.values():
        for setting in client.settings:
            where = f"{client.name}: {setting.file}"
            name = setting.path if setting.flag is None else f"{setting.path} {setting.flag}"
            try:
                found = find_setting(load_yaml(read(setting.file)), setting)
            except OSError:
                problems.append(f"{where} cannot be read")
                continue
            except YamlError as error:
                problems.append(f"{where} is not valid YAML: {error}")
                continue
            if found is None:
                problems.append(f"{where} has no setting {name}; the budget says {setting.value}")
            elif str(found) != str(setting.value):
                problems.append(f"{where} sets {name} to {found}; the budget says {setting.value}")
    return problems


def postgres_problems(budget: Budget, postgres_service_text: str) -> list[str]:
    """Each figure of the budget that the PostgreSQL service does not pass to the server as ``-c name=value``, so that the image default would decide."""
    try:
        document = load_yaml(postgres_service_text)
    except YamlError as error:
        return [f"the PostgreSQL service file is not valid YAML: {error}"]
    service = document.get("services", {}).get("postgres", {}) if isinstance(document, dict) else {}
    command = service.get("command", []) if isinstance(service, dict) else []
    passed = {item.split("=", 1)[0]: item.split("=", 1)[1] for item in command if isinstance(item, str) and "=" in item} if isinstance(command, list) else {}
    problems = []
    for name, wanted in (("max_connections", budget.max_connections), ("superuser_reserved_connections", budget.superuser_reserved_connections)):
        if name not in passed:
            problems.append(f"{name}: the postgres service does not pass it with -c, so the image default decides; the budget says {wanted}")
        elif passed[name] != str(wanted):
            problems.append(f"{name}: the postgres service passes {passed[name]}; the budget says {wanted}")
    return problems
