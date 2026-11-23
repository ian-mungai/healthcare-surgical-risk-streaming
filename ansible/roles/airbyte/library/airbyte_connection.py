"""Ansible module: make the raw connection in a running Airbyte match a document, and change nothing when it already does.

The document is what ``scripts.ingestion.connection render`` prints: a pinned definition, a source and a destination, and one connection. The module
reads the instance administrator's client credentials from the namespace's secret, asks Airbyte's public API for a token and then makes the definition
of each connector (the pinned version), the source, the destination and the connection match. An object is found by its name, created when it is
missing, and updated when its answer does not hold every value of the document. The platform adds fields of its own to an answer, so only the
document's values are compared. A drifted setting, a schedule that was switched on, a changed connector version and a dropped selected file are
repaired.

The run has two phases. It first reads every object, checks every item of every answer against a closed table of required keys and types
(``ANSWER_SHAPES``), and decides the whole plan: identities, definitions, links and drift. Anything that no update can repair (an object that belongs
to another definition, source or destination, a name that two objects hold, a workspace count other than one, an answer of the wrong form) is refused
there, before any write. Deleting a connection would delete its sync state, so such a drift is refused and never repaired. Only then are the writes
made, in order.

The access key of the project profile is never in the document, which holds no ``secret_fields`` and no value at a key field. The launcher placed the
key as two root-only files on the host, outside Ansible. The module holds one closed table (``CONNECTOR_KEY_FIELDS``): for each of the two pinned
connectors the image and the exact two field paths that receive the key ID and the secret. Before any call it checks that the outgoing configuration
differs from the document at exactly those paths and nowhere else. Airbyte answers every secret field as the mask ``**********``, so a secret cannot
be compared. The module compares the document's other values and keeps a record file with, for each object, a SHA-256 fingerprint of the actor's ID
and the key's ID, which changes when the key is rotated or the actor is replaced. A key whose fingerprint differs from the record is sent again, and
the record is written after the call succeeds. On every successful run the record's mode and owner are repaired, and that is reported as a change. A
secret changed by hand in Airbyte is not seen.

Every exchange with Airbyte has one elapsed deadline, enforced by a timer that shuts the socket down at the deadline, and the whole run has one
(``Budget``), so no exchange and no kubectl call runs past it. The token, the client credentials, the key and every body of a call stay out of every
message: a failed call names its method, its path and its status.

Accepted limit (owner decision, Nov 22 2026): for an answer that carries ``Connection: close`` the body read can outlast the exchange deadline, and
the kill grace of the ``timeout`` program around kubectl is outside the run budget.

The module uses only the standard library and runs on the host's Python.
"""

from __future__ import annotations

import base64
import contextlib
import copy
import hashlib
import http.client
import json
import math
import os
import re
import socket
import threading
import time
import urllib.parse
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.secret_files import SecretFileError, access_differs, set_access, write_private_file

AUTH_OBJECT = "airbyte-auth-secrets"
ID_FIELD = "instance-admin-client-id"
KEY_FIELD = "instance-admin-client-secret"
LOCAL_ADDRESSES = ("127.0.0.1", "localhost")
KEY_FOLDER = "airbyte"
KEY_FILES = {"aws_access_key_id": "aws_access_key_id", "aws_secret_access_key": "aws_secret_access_key"}
FINGERPRINT_OF = "aws_access_key_id"
CONNECTOR_KEY_FIELDS: dict[str, dict[str, Any]] = {
    "source": {
        "image": "airbyte/source-s3",
        "fields": {"credentials.aws_access_key_id": "aws_access_key_id", "credentials.aws_secret_access_key": "aws_secret_access_key"},
    },
    "destination": {
        "image": "airbyte/destination-s3",
        "fields": {"access_key_id": "aws_access_key_id", "secret_access_key": "aws_secret_access_key"},
    },
}
ID_FORM = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
DEFINITION_SHAPE: dict[str, type] = {"id": str, "name": str, "dockerRepository": str, "dockerImageTag": str}
ANSWER_SHAPES: dict[str, dict[str, type]] = {
    "workspace": {"workspaceId": str},
    "source_definition": DEFINITION_SHAPE,
    "destination_definition": DEFINITION_SHAPE,
    "source": {"sourceId": str, "name": str, "definitionId": str, "configuration": dict},
    "destination": {"destinationId": str, "name": str, "definitionId": str, "configuration": dict},
    "connection": {"connectionId": str, "name": str, "sourceId": str, "destinationId": str, "status": str, "schedule": dict, "configurations": dict},
}
ID_KEYS: dict[str, tuple[str, ...]] = {
    "workspace": ("workspaceId",),
    "source_definition": ("id",),
    "destination_definition": ("id",),
    "source": ("sourceId", "definitionId"),
    "destination": ("destinationId", "definitionId"),
    "connection": ("connectionId", "sourceId", "destinationId"),
}
ID_OF: dict[str, str] = {
    "source_definition": "id",
    "destination_definition": "id",
    "source": "sourceId",
    "destination": "destinationId",
    "connection": "connectionId",
}
WRITE_CALLS: tuple[tuple[str, str], ...] = (
    ("POST", "source_definition"),
    ("PUT", "source_definition"),
    ("POST", "destination_definition"),
    ("PUT", "destination_definition"),
    ("POST", "source"),
    ("PATCH", "source"),
    ("POST", "destination"),
    ("PATCH", "destination"),
    ("POST", "connection"),
    ("PATCH", "connection"),
)
KUBECTL_SECONDS = 60
KUBECTL_KILL_SECONDS = 5
TIMED_OUT = (124, 137)
PAGE_SIZE = 100
MAX_PAGES = 1000
DEFINITION_FIELDS = ("name", "dockerRepository", "dockerImageTag")
CONNECTION_FIELDS = ("name", "status", "schedule", "namespaceDefinition", "configurations")
Runner = Callable[[list[str]], "tuple[int, str, str]"]


class ApiError(RuntimeError):
    """A call or an object is not usable. The message holds a method, a path, a status or an object's name, and never a token, a secret or a body."""


class Budget:
    """The time left in the run. Every exchange and every call of another program takes its deadline from it."""

    def __init__(self, seconds: float, clock: Callable[[], float] = time.monotonic) -> None:
        self.clock = clock
        self.deadline = clock() + seconds

    def remaining(self) -> float:
        return self.deadline - self.clock()


class Api:
    """What the plan and the steps need: one call that returns the JSON object of the reply."""

    def request(self, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        raise NotImplementedError


class HttpApi(Api):
    """Calls over plain HTTP to Airbyte's local address only, so the bearer token never leaves the host.

    One exchange (connect, send, read the whole answer) has one deadline: the smaller of ``timeout_seconds`` and the time left in the run's budget. A
    timer shuts the socket down when it expires, so any read that is pending fails at once, however slowly the server sends its headers or its body,
    and an exchange that ended after the deadline is a timeout too."""

    def __init__(
        self,
        base_url: str,
        token: Callable[[], str] | None,
        *,
        timeout_seconds: float,
        budget: Budget | None = None,
        connect: Callable[..., Any] = http.client.HTTPConnection,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        address = urllib.parse.urlsplit(base_url)
        if address.scheme != "http" or address.hostname not in LOCAL_ADDRESSES or address.port is None:
            raise ApiError("the Airbyte address must be http on the host's local address with a port")
        self.host, self.port, self.prefix = address.hostname, address.port, address.path.rstrip("/")
        self.token = token
        self.timeout_seconds = timeout_seconds
        self.budget = budget
        self.connect = connect
        self.clock = clock

    def request(self, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        allowed = self.timeout_seconds
        if self.budget is not None:
            left = self.budget.remaining()
            if left <= 0:
                raise ApiError(f"{method} {path} was not made: the run deadline has passed")
            allowed = min(allowed, left)
        headers = {"Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if self.token is not None:
            headers["Authorization"] = f"Bearer {self.token()}"
        data = None if body is None else json.dumps(body).encode()
        started = self.clock()
        expired = threading.Event()
        connection = self.connect(self.host, self.port, timeout=allowed)

        def stop() -> None:
            expired.set()
            with contextlib.suppress(OSError, AttributeError):
                connection.sock.shutdown(socket.SHUT_RDWR)

        timer = threading.Timer(allowed, stop)
        timer.daemon = True
        timer.start()
        late = ApiError(f"{method} {path} did not answer within {allowed:g} seconds")
        try:
            connection.request(method, f"{self.prefix}{path}", body=data, headers=headers)
            sock = getattr(connection, "sock", None)
            if sock is not None:
                sock.settimeout(max(allowed - (self.clock() - started), 0.001))
            reply = connection.getresponse()
            status, text = reply.status, reply.read().decode()
        except TimeoutError:
            raise late from None
        except (http.client.HTTPException, OSError, UnicodeDecodeError):
            if expired.is_set():
                raise late from None
            raise ApiError(f"{method} {path} could not reach Airbyte") from None
        finally:
            timer.cancel()
            connection.close()
        if expired.is_set() or self.clock() - started > allowed:
            raise late
        if status != http.client.OK:
            raise ApiError(f"{method} {path} answered {status}")
        try:
            parsed = json.loads(text)
        except ValueError:
            raise ApiError(f"{method} {path} answered with text that is not JSON") from None
        if not isinstance(parsed, dict):
            raise ApiError(f"{method} {path} answered with JSON that is not an object")
        return parsed


def covers(actual: Any, wanted: Any) -> bool:
    """Whether ``actual`` holds every value of ``wanted``. An object may hold more fields. A list must match item by item, in order, at its length."""
    if isinstance(wanted, dict):
        return isinstance(actual, dict) and all(name in actual and covers(actual[name], value) for name, value in wanted.items())
    if isinstance(wanted, list):
        return isinstance(actual, list) and len(actual) == len(wanted) and all(covers(left, right) for left, right in zip(actual, wanted, strict=True))
    return type(actual) is type(wanted) and actual == wanted


def is_id(value: object) -> bool:
    """Whether a value is an ID in the documented form: the public API's schema gives every ID as ``format: UUID`` (airbyte-platform, api.yaml)."""
    return isinstance(value, str) and ID_FORM.fullmatch(value) is not None


def checked(item: Any, kind: str) -> dict[str, Any]:
    """The item of an answer when it is an object that holds each required key of its kind with the required type, and each ID in the form of a UUID.

    Anything else is refused by kind and key, so no write is ever planned from an answer with a missing or empty ID.
    """
    if not isinstance(item, dict):
        raise ApiError(f"an answer holds a {kind} that is not an object")
    for key, expected in ANSWER_SHAPES[kind].items():
        if key not in item or type(item[key]) is not expected:
            raise ApiError(f"a {kind} in an answer lacks {key} or holds it in the wrong form")
    for key in ID_KEYS[kind]:
        if not is_id(item[key]):
            raise ApiError(f"a {kind} in an answer holds {key} that is empty or not an ID")
    return item


def listing(api: Api, path: str, kind: str) -> list[dict[str, Any]]:
    """Every object of a list call, page by page, each checked against the table of its kind."""
    found: list[dict[str, Any]] = []
    separator = "&" if "?" in path else "?"
    for page in range(MAX_PAGES):
        reply = api.request("GET", f"{path}{separator}limit={PAGE_SIZE}&offset={page * PAGE_SIZE}")
        data = reply.get("data")
        if not isinstance(data, list):
            raise ApiError(f"GET {path} answered with no list of objects")
        found += [checked(item, kind) for item in data]
        if not reply.get("next"):
            return found
    raise ApiError(f"GET {path} has more pages than {MAX_PAGES}")


def written(api: Api, method: str, path: str, body: dict[str, Any], kind: str, expected_id: str | None = None) -> dict[str, Any]:
    """Make one write and return its answer, checked with the same closed table as a read: each required key of the kind, its type, each ID a UUID.

    An answer that fails is an error that names the call, and the step that made it reports no change. An update must answer for the object that it updated.
    Every write of the module goes through this function (``WRITE_CALLS``).
    """
    try:
        item = checked(api.request(method, path, body), kind)
    except ApiError as error:
        raise ApiError(f"{method} {path} answered, and {error}") from None
    if expected_id is not None and item[ID_OF[kind]] != expected_id:
        raise ApiError(f"{method} {path} answered for another {kind} than the one it updated")
    return item


def named(objects: list[dict[str, Any]], name: str, what: str) -> dict[str, Any] | None:
    matches = [item for item in objects if item["name"] == name]
    if len(matches) > 1:
        raise ApiError(f"{len(matches)} {what} objects are named {name}; Airbyte must hold one")
    return matches[0] if matches else None


def check_document(wanted: dict[str, Any]) -> None:
    """Refuse a document that is not in the documented form, that names a key field, or whose configuration already holds a value at a key field."""
    try:
        for part in ("source", "destination"):
            if not all(isinstance(wanted[part][field], dict if field != "name" else str) for field in ("name", "definition", "configuration")):
                raise KeyError(part)
            if any(not isinstance(wanted[part]["definition"].get(field), str) for field in DEFINITION_FIELDS):
                raise KeyError(part)
        if any(field not in wanted["connection"] for field in CONNECTION_FIELDS):
            raise KeyError("connection")
    except (KeyError, TypeError, AttributeError) as error:
        raise ApiError(f"the document does not hold its {error.args[0] if error.args else 'parts'} in the documented form") from None
    for part in ("source", "destination"):
        if "secret_fields" in wanted[part]:
            raise ApiError(f"the {part} names secret_fields; the module holds the one table of the fields that take the key")
        table = CONNECTOR_KEY_FIELDS[part]
        if wanted[part]["definition"]["dockerRepository"] != table["image"]:
            raise ApiError(f"the {part} is not the pinned connector image that the module knows")
        for path in table["fields"]:
            *parents, last = path.split(".")
            node: Any = wanted[part]["configuration"]
            for step in parents:
                node = node.get(step) if isinstance(node, dict) else None
            if not isinstance(node, dict):
                raise ApiError(f"the {part}'s configuration lacks the object that holds a key field")
            if last in node:
                raise ApiError(f"the {part}'s configuration already holds a value at a key field")


def one_workspace(api: Api) -> str:
    workspaces = listing(api, "/workspaces", "workspace")
    if len(workspaces) != 1:
        raise ApiError(f"Airbyte holds {len(workspaces)} workspaces, and the connection needs exactly one")
    return str(workspaces[0]["workspaceId"])


def read_key_files(directory: Path) -> dict[str, str]:
    """The two key files under ``<directory>/airbyte``. A missing or empty file stops the run and is named by its file name, never by a value."""
    values: dict[str, str] = {}
    for name, file_name in KEY_FILES.items():
        try:
            text = (directory / KEY_FOLDER / file_name).read_text().strip()
        except OSError:
            raise ApiError(f"the key file {file_name} cannot be read; the launcher places it before the connection is applied") from None
        if not text:
            raise ApiError(f"the key file {file_name} is empty")
        values[name] = text
    return values


def read_record(path: Path) -> dict[str, str]:
    """The fingerprints that the last runs applied, or nothing when the file is missing or not valid, which sends each key again."""
    try:
        record = json.loads(path.read_text())
    except (OSError, ValueError):
        return {}
    if not isinstance(record, dict) or any(not isinstance(key, str) or not isinstance(value, str) for key, value in record.items()):
        return {}
    return dict(record)


def record_owner() -> tuple[int, int]:
    return os.getuid(), os.getgid()


def write_record(path: Path, record: Mapping[str, str]) -> None:
    """Write the record for its owner only, through a fresh temporary file, so it never inherits the permissions of a file that was already there."""
    try:
        write_private_file(path, json.dumps(dict(record), sort_keys=True) + "\n", replace=True, owner=record_owner())
    except SecretFileError:
        raise ApiError("the record of applied keys cannot be written") from None


def repair_record(path: Path) -> bool:
    """Give an existing record the expected mode and owner and return whether that changed anything."""
    try:
        if path.exists() and access_differs(path, record_owner()):
            set_access(path, record_owner())
            return True
    except (OSError, SecretFileError):
        raise ApiError("the record of applied keys cannot be given its mode and owner") from None
    return False


def fingerprint(actor_id: str, secrets: Mapping[str, str]) -> str:
    """The SHA-256 of an actor's ID and the key's ID. A rotated key has a new ID, a replacement actor has a new ID, and nothing of the secret is in it."""
    return hashlib.sha256(f"{actor_id}\n{secrets[FINGERPRINT_OF]}".encode()).hexdigest()


def inject(configuration: dict[str, Any], fields: Mapping[str, str], secrets: Mapping[str, str]) -> dict[str, Any]:
    """A copy of the configuration with each secret put at its path. The document's own configuration is not changed."""
    filled = copy.deepcopy(configuration)
    for path, name in fields.items():
        *parents, last = path.split(".")
        node = filled
        for step in parents:
            node = node[step]
        node[last] = secrets[name]
    return filled


def differences(left: Any, right: Any, prefix: str = "") -> set[str]:
    """The dotted paths at which two configurations differ: a value that differs, and a key that only one of them holds. A list is one value."""
    if isinstance(left, dict) and isinstance(right, dict):
        found: set[str] = set()
        for name in set(left) | set(right):
            path = f"{prefix}.{name}" if prefix else str(name)
            if name not in left or name not in right:
                found.add(path)
            else:
                found |= differences(left[name], right[name], path)
        return found
    return set() if type(left) is type(right) and left == right else {prefix}


def outgoing_configuration(kind: str, wanted: dict[str, Any], secrets: Mapping[str, str]) -> dict[str, Any]:
    """The configuration that is sent for the source or the destination, once it is shown to differ from the document at exactly the key fields of its table."""
    fields = CONNECTOR_KEY_FIELDS[kind]["fields"]
    sent = inject(wanted["configuration"], fields, secrets)
    if differences(wanted["configuration"], sent) != set(fields):
        raise ApiError(f"the outgoing {kind} configuration differs from the document outside the key fields")
    return sent


@dataclass
class Step:
    """One write of the plan: its label for the report and the call that makes it."""

    label: str
    run: Callable[[Api], None]


def plan(api: Api, wanted: dict[str, Any], secrets: Mapping[str, str], recorded: dict[str, str]) -> list[Step]:
    """Read every object and decide the whole plan, with no write. Raise ``ApiError`` for anything that no update can repair, before any step is made.

    The plan covers the document, the outgoing configurations, the shape of every answer, the identities (a name that two objects hold, a workspace
    count other than one), the definitions, the objects' links and the drift. Every ID that a write needs is read here. A step runs after the steps
    before it and takes the IDs that they made. ``recorded`` is updated by a step after its call succeeds."""
    check_document(wanted)
    if any(name not in secrets or not secrets[name] for name in KEY_FILES):
        raise ApiError("the access key is not available to the module")
    sent = {kind: outgoing_configuration(kind, wanted[kind], secrets) for kind in ("source", "destination")}
    workspace = one_workspace(api)
    definitions = {
        kind: named(
            listing(api, f"/workspaces/{workspace}/definitions/{kind}s", f"{kind}_definition"), wanted[kind]["definition"]["name"], f"{kind} definition"
        )
        for kind in ("source", "destination")
    }
    actors = {kind: named(listing(api, f"/{kind}s?workspaceIds={workspace}", kind), wanted[kind]["name"], kind) for kind in ("source", "destination")}
    connection = named(listing(api, f"/connections?workspaceIds={workspace}", "connection"), wanted["connection"]["name"], "connection")
    steps: list[Step] = []
    ids: dict[str, str] = {}
    for kind in ("source", "destination"):
        steps += plan_definition(workspace, kind, wanted[kind]["definition"], definitions[kind], ids)
    for kind in ("source", "destination"):
        steps += plan_actor(workspace, kind, wanted[kind], sent[kind], actors[kind], ids, secrets, recorded)
    steps += plan_connection(wanted["connection"], connection, ids)
    return steps


def plan_definition(workspace: str, kind: str, wanted: dict[str, Any], current: dict[str, Any] | None, ids: dict[str, str]) -> list[Step]:
    path = f"/workspaces/{workspace}/definitions/{kind}s"
    if current is None:

        def create(api: Api) -> None:
            answer = written(api, "POST", path, {field: wanted[field] for field in DEFINITION_FIELDS}, f"{kind}_definition")
            ids[f"{kind}_definition"] = answer["id"]

        return [Step(f"{kind} definition", create)]
    if current["dockerRepository"] != wanted["dockerRepository"]:
        raise ApiError(f"the {kind} definition {wanted['name']} runs another image than the document's")
    ids[f"{kind}_definition"] = current["id"]
    if current["dockerImageTag"] == wanted["dockerImageTag"]:
        return []
    body = {"name": wanted["name"], "dockerImageTag": wanted["dockerImageTag"]}
    definition_id = current["id"]

    def update_definition(api: Api) -> None:
        written(api, "PUT", f"{path}/{definition_id}", body, f"{kind}_definition", definition_id)

    return [Step(f"{kind} definition", update_definition)]


def plan_actor(
    workspace: str,
    kind: str,
    wanted: dict[str, Any],
    sent: dict[str, Any],
    current: dict[str, Any] | None,
    ids: dict[str, str],
    secrets: Mapping[str, str],
    recorded: dict[str, str],
) -> list[Step]:
    """The step that makes the source or the destination, or none. ``sent`` is the configuration with the key in it. Airbyte answers its secrets as the mask, so
    the answer is compared with the document, which holds none, and the key is sent again when the record's fingerprint of this actor's ID differs."""
    path, id_field = f"/{kind}s", f"{kind}Id"
    if current is None:

        def create(api: Api) -> None:
            body = {"name": wanted["name"], "workspaceId": workspace, "definitionId": ids[f"{kind}_definition"], "configuration": sent}
            ids[kind] = written(api, "POST", path, body, kind)[id_field]
            recorded[kind] = fingerprint(ids[kind], secrets)

        return [Step(kind, create)]
    if f"{kind}_definition" not in ids or current["definitionId"] != ids[f"{kind}_definition"]:
        raise ApiError(f"the {kind} {wanted['name']} belongs to another definition than the pinned one; delete it in Airbyte and run again")
    actor_id = current[id_field]
    ids[kind] = actor_id
    applied = fingerprint(actor_id, secrets)
    if covers(current["configuration"], wanted["configuration"]) and recorded.get(kind) == applied:
        return []

    def update(api: Api) -> None:
        written(api, "PATCH", f"{path}/{actor_id}", {"configuration": sent}, kind, actor_id)
        recorded[kind] = applied

    return [Step(kind, update)]


def plan_connection(wanted: dict[str, Any], current: dict[str, Any] | None, ids: dict[str, str]) -> list[Step]:
    fields = {field: wanted[field] for field in CONNECTION_FIELDS}
    if current is None:

        def create(api: Api) -> None:
            written(api, "POST", "/connections", {**fields, "sourceId": ids["source"], "destinationId": ids["destination"]}, "connection")

        return [Step("connection", create)]
    known = ("source" in ids and "destination" in ids) and (current["sourceId"], current["destinationId"]) == (ids["source"], ids["destination"])
    if not known:
        raise ApiError(
            f"the connection {wanted['name']} joins other objects than the pinned ones; Airbyte cannot move it, and deleting it would lose its sync state"
        )
    if covers(current, fields):
        return []
    connection_id = current["connectionId"]

    def update(api: Api) -> None:
        written(api, "PATCH", f"/connections/{connection_id}", fields, "connection", connection_id)

    return [Step("connection", update)]


def execute(api: Api, steps: list[Step]) -> list[str]:
    """Make each step in order and return the labels of the steps made."""
    for step in steps:
        step.run(api)
    return [step.label for step in steps]


def reconcile(api: Api, wanted: dict[str, Any], secrets: Mapping[str, str], recorded: dict[str, str]) -> list[str]:
    """Make Airbyte hold the document and return what changed, in the order it changed. A second call returns an empty list.

    The whole plan is decided from reads before the first write, so a refusal never follows a write. ``recorded`` holds the fingerprint of the key that each
    object was last given and is updated in place after each successful call.
    """
    return execute(api, plan(api, wanted, secrets, recorded))


def apply_document(api: Api, wanted: dict[str, Any], secrets: Mapping[str, str], record_file: Path) -> list[str]:
    """Reconcile and keep the record of applied keys, and return what changed.

    The record is written after a failed run too, so a source that was given its key is not sent it again. After a successful run the record's mode
    and owner are repaired, and that is reported as ``record file``."""
    recorded = read_record(record_file)
    before = dict(recorded)
    try:
        changes = reconcile(api, wanted, secrets, recorded)
    finally:
        if recorded != before:
            write_record(record_file, recorded)
    if repair_record(record_file):
        changes = [*changes, "record file"]
    return changes


def client_credentials(run: Runner, kubectl: list[str], namespace: str, budget: Budget) -> tuple[str, str]:
    """The administrator's client id and secret from the namespace's auth secret.

    The call runs under the ``timeout`` program with the smaller of its own limit and the time left in the run, and carries kubectl's own request
    timeout. A failure of any kind, a timeout included, names the secret and the namespace and never shows the program's text or a value."""
    left = budget.remaining()
    if left <= 0:
        raise ApiError(f"the secret {AUTH_OBJECT} was not read: the run deadline has passed")
    seconds = max(1, math.ceil(min(KUBECTL_SECONDS, left)))
    arguments = ["timeout", f"--kill-after={KUBECTL_KILL_SECONDS}s", f"{seconds}s", *kubectl]
    code, output, _ = run([*arguments, "get", "secret", AUTH_OBJECT, "--namespace", namespace, "--output", "json", f"--request-timeout={seconds}s"])
    if code in TIMED_OUT:
        raise ApiError(f"reading the secret {AUTH_OBJECT} in {namespace} did not finish within {seconds} seconds")
    if code != 0:
        raise ApiError(f"the secret {AUTH_OBJECT} cannot be read in {namespace}")
    try:
        data = json.loads(output)["data"]
        return base64.b64decode(data[ID_FIELD]).decode(), base64.b64decode(data[KEY_FIELD]).decode()
    except (ValueError, KeyError, TypeError):
        raise ApiError(f"the secret {AUTH_OBJECT} holds no {ID_FIELD} and {KEY_FIELD} yet; Airbyte writes them when it first starts") from None


def access_token(base_url: str, client_id: str, client_secret: str, timeout_seconds: float, budget: Budget | None = None) -> str:
    reply = HttpApi(base_url, None, timeout_seconds=timeout_seconds, budget=budget).request(
        "POST", "/applications/token", {"clientId": client_id, "clientSecret": client_secret}
    )
    token = reply.get("access_token")
    if not isinstance(token, str) or not token:
        raise ApiError("POST /applications/token answered with no access token")
    return token


def main() -> None:
    module = AnsibleModule(
        argument_spec={
            "document": {"type": "dict", "required": True},
            "base_url": {"type": "str", "default": "http://127.0.0.1:8000/api/public/v1"},
            "namespace": {"type": "str", "default": "airbyte"},
            "kubectl": {"type": "list", "elements": "str", "default": ["k3s", "kubectl"]},
            "timeout_seconds": {"type": "int", "default": 600},
            "run_seconds": {"type": "int", "default": 1800},
            "secrets_directory": {"type": "path", "required": True},
            "record_file": {"type": "path", "required": True},
        },
        supports_check_mode=False,
    )
    params = module.params
    budget = Budget(params["run_seconds"])
    try:
        client_id, client_secret = client_credentials(
            lambda arguments: module.run_command(arguments, check_rc=False), params["kubectl"], params["namespace"], budget
        )
        token = access_token(params["base_url"], client_id, client_secret, params["timeout_seconds"], budget)
        changes = apply_document(
            HttpApi(params["base_url"], lambda: token, timeout_seconds=params["timeout_seconds"], budget=budget),
            params["document"],
            read_key_files(Path(params["secrets_directory"])),
            Path(params["record_file"]),
        )
        module.exit_json(changed=bool(changes), changes=changes)
    except ApiError as error:
        module.fail_json(msg=str(error))


if __name__ == "__main__":
    main()
