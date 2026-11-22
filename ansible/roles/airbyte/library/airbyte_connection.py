"""Ansible module: make the raw connection in a running Airbyte match a document, and change nothing when it already does.

The document is what ``scripts.ingestion.connection render`` prints: a pinned definition, a source and a destination, and one connection. The module reads
the instance administrator's client credentials from the namespace's secret, asks Airbyte's public API for a token and then, in this order, makes the
definition of each connector (the pinned version), the source, the destination and the connection match. An object is found by its name, created when it is
missing, and updated when its answer does not hold every value of the document. The platform adds fields of its own to an answer, so only the document's
values are compared. A drifted setting, a schedule that was switched on, a changed connector version and a dropped selected file are repaired.

The access key of the project profile is never in the document: the host reads it from two root-only files and the module puts it into the source and
the destination at the fields the document names (``secret_fields``). Airbyte answers every secret field as the mask ``**********``, so a secret cannot be
compared. The module compares the document's other values and keeps a record file with, for each object, a SHA-256 fingerprint of the key's ID, which
changes when the key is rotated. A key whose fingerprint differs from the record is sent again, and the record is written after the call succeeds. A secret
changed by hand in Airbyte is not seen; the next rotation or a deleted record sends the key again.

A structural drift that no update can repair (an object that belongs to another definition, source or destination) is refused with its name and changed
nowhere: deleting a connection would delete its sync state, and the next sync would copy every file again. A name that two objects hold, or a workspace
count other than one, stops the run before any write. The token, the client credentials, the key and every body of a call stay out of every message: a
failed call names its method, its path and its status.

The module uses only the standard library and runs on the host's Python.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import http.client
import json
import os
import time
import urllib.parse
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.secret_files import SecretFileError, write_private_file

AUTH_OBJECT = "airbyte-auth-secrets"
ID_FIELD = "instance-admin-client-id"
KEY_FIELD = "instance-admin-client-secret"
LOCAL_ADDRESSES = ("127.0.0.1", "localhost")
KEY_FOLDER = "airbyte"
KEY_FILES = {"aws_access_key_id": "aws_access_key_id", "aws_secret_access_key": "aws_secret_access_key"}
FINGERPRINT_OF = "aws_access_key_id"
READ_BYTES = 65536
KUBECTL_SECONDS = 60
PAGE_SIZE = 100
MAX_PAGES = 1000
DEFINITION_FIELDS = ("name", "dockerRepository", "dockerImageTag")
CONNECTION_FIELDS = ("name", "status", "schedule", "namespaceDefinition", "configurations")
Runner = Callable[[list[str]], "tuple[int, str, str]"]


class ApiError(RuntimeError):
    """A call or an object is not usable. The message holds a method, a path, a status or an object's name, and never a token, a secret or a body."""


class Api:
    """What the reconcile step needs: one call that returns the JSON object of the reply."""

    def request(self, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        raise NotImplementedError


class HttpApi(Api):
    """Calls over plain HTTP to Airbyte's local address only, so the bearer token never leaves the host.

    A call has one deadline of ``timeout_seconds`` from its start. The reply is read in pieces, each with the time that is left as its socket timeout, and the
    read stops at the deadline, so a reply that trickles in cannot take longer than the deadline however many pieces it has.
    """

    def __init__(
        self,
        base_url: str,
        token: Callable[[], str] | None,
        *,
        timeout_seconds: float,
        connect: Callable[..., Any] = http.client.HTTPConnection,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        address = urllib.parse.urlsplit(base_url)
        if address.scheme != "http" or address.hostname not in LOCAL_ADDRESSES or address.port is None:
            raise ApiError("the Airbyte address must be http on the host's local address with a port")
        self.host, self.port, self.prefix = address.hostname, address.port, address.path.rstrip("/")
        self.token = token
        self.timeout_seconds = timeout_seconds
        self.connect = connect
        self.clock = clock

    def request(self, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        headers = {"Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if self.token is not None:
            headers["Authorization"] = f"Bearer {self.token()}"
        data = None if body is None else json.dumps(body).encode()
        deadline = self.clock() + self.timeout_seconds
        connection = self.connect(self.host, self.port, timeout=self.timeout_seconds)
        try:
            connection.request(method, f"{self.prefix}{path}", body=data, headers=headers)
            reply = connection.getresponse()
            status, pieces = reply.status, []
            while True:
                remaining = deadline - self.clock()
                if remaining <= 0:
                    raise TimeoutError
                sock = getattr(connection, "sock", None)
                if sock is not None:
                    sock.settimeout(remaining)
                piece = reply.read(READ_BYTES)
                if not piece:
                    break
                pieces.append(piece)
            if self.clock() > deadline:
                raise TimeoutError
            text = b"".join(pieces).decode()
        except TimeoutError:
            raise ApiError(f"{method} {path} did not answer within {self.timeout_seconds:g} seconds") from None
        except (http.client.HTTPException, OSError, UnicodeDecodeError):
            raise ApiError(f"{method} {path} could not reach Airbyte") from None
        finally:
            connection.close()
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


def listing(api: Api, path: str) -> list[dict[str, Any]]:
    """Every object of a list call, page by page."""
    found: list[dict[str, Any]] = []
    separator = "&" if "?" in path else "?"
    for page in range(MAX_PAGES):
        reply = api.request("GET", f"{path}{separator}limit={PAGE_SIZE}&offset={page * PAGE_SIZE}")
        data = reply.get("data")
        if not isinstance(data, list):
            raise ApiError(f"GET {path} answered with no list of objects")
        found += [item for item in data if isinstance(item, dict)]
        if not reply.get("next"):
            return found
    raise ApiError(f"GET {path} has more pages than {MAX_PAGES}")


def named(objects: list[dict[str, Any]], name: str, what: str) -> dict[str, Any] | None:
    matches = [item for item in objects if item.get("name") == name]
    if len(matches) > 1:
        raise ApiError(f"{len(matches)} {what} objects are named {name}; Airbyte must hold one")
    return matches[0] if matches else None


def check_document(wanted: dict[str, Any]) -> None:
    try:
        for part in ("source", "destination"):
            if not all(isinstance(wanted[part][field], dict if field != "name" else str) for field in ("name", "definition", "configuration")):
                raise KeyError(part)
            if any(not isinstance(wanted[part]["definition"].get(field), str) for field in DEFINITION_FIELDS[:3]):
                raise KeyError(part)
        if any(field not in wanted["connection"] for field in CONNECTION_FIELDS):
            raise KeyError("connection")
    except (KeyError, TypeError, AttributeError) as error:
        raise ApiError(f"the document does not hold its {error.args[0] if error.args else 'parts'} in the documented form") from None


def one_workspace(api: Api) -> str:
    workspaces = listing(api, "/workspaces")
    if len(workspaces) != 1 or not isinstance(workspaces[0].get("workspaceId"), str):
        raise ApiError(f"Airbyte holds {len(workspaces)} workspaces, and the connection needs exactly one")
    return str(workspaces[0]["workspaceId"])


def read_key_files(directory: Path) -> dict[str, str]:
    """The two key files under ``<directory>/airbyte``. A missing or empty file stops the run and is named by its file name, never by a value."""
    values: dict[str, str] = {}
    for name, file_name in KEY_FILES.items():
        try:
            text = (directory / KEY_FOLDER / file_name).read_text().strip()
        except OSError:
            raise ApiError(f"the key file {file_name} cannot be read; the deployment places it before the connection is applied") from None
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


def write_record(path: Path, record: Mapping[str, str]) -> None:
    """Write the record for its owner only, through a fresh temporary file, so it never inherits the permissions of a file that was already there."""
    try:
        write_private_file(path, json.dumps(dict(record), sort_keys=True) + "\n", replace=True, owner=(os.getuid(), os.getgid()))
    except SecretFileError:
        raise ApiError("the record of applied keys cannot be written") from None


def fingerprint(actor_id: str, secrets: Mapping[str, str]) -> str:
    """The SHA-256 of an actor's ID and the key's ID. A rotated key has a new ID, a replacement actor has a new ID, and nothing of the secret is in it."""
    return hashlib.sha256(f"{actor_id}\n{secrets[FINGERPRINT_OF]}".encode()).hexdigest()


def inject(configuration: dict[str, Any], fields: dict[str, str], secrets: Mapping[str, str]) -> dict[str, Any]:
    """A copy of the configuration with each secret put at its path. The document's own configuration is not changed."""
    filled = copy.deepcopy(configuration)
    for path, name in fields.items():
        *parents, last = path.split(".")
        node = filled
        for step in parents:
            node = node[step]
        node[last] = secrets[name]
    return filled


def check_secret_fields(part: dict[str, Any], what: str) -> dict[str, str]:
    fields = part.get("secret_fields", {})
    if not isinstance(fields, dict):
        raise ApiError(f"the {what}'s secret_fields are not in the documented form")
    for path, name in fields.items():
        if not isinstance(path, str) or not path or not isinstance(name, str) or name not in KEY_FILES:
            raise ApiError(f"the {what}'s secret_fields name a secret that the module does not read")
        *parents, _ = path.split(".")
        node: Any = part["configuration"]
        for step in parents:
            node = node.get(step) if isinstance(node, dict) else None
        if not isinstance(node, dict):
            raise ApiError(f"the {what}'s secret_fields name a field whose parent is not in its configuration")
    return {str(path): str(name) for path, name in fields.items()}


@dataclass
class Step:
    """One write of the plan: its label for the report and the call that makes it."""

    label: str
    run: Callable[[Api], None]


def plan(api: Api, wanted: dict[str, Any], secrets: Mapping[str, str], recorded: dict[str, str]) -> list[Step]:
    """Read every object and decide the whole plan, with no write. Raise ``ApiError`` for anything that no update can repair, before any step is made.

    The plan covers the identities (a name that two objects hold, a workspace count other than one), the definitions, the objects' links and the drift. A
    step runs after the steps before it and takes the IDs that they made. ``recorded`` is updated by a step after its call succeeds.
    """
    check_document(wanted)
    if any(name not in secrets or not secrets[name] for name in KEY_FILES):
        raise ApiError("the access key is not available to the module")
    for part in ("source", "destination"):
        wanted[part]["secret_fields"] = check_secret_fields(wanted[part], part)
    workspace = one_workspace(api)
    definitions = {
        kind: named(listing(api, f"/workspaces/{workspace}/definitions/{kind}s"), wanted[kind]["definition"]["name"], f"{kind} definition")
        for kind in ("source", "destination")
    }
    actors = {kind: named(listing(api, f"/{kind}s?workspaceIds={workspace}"), wanted[kind]["name"], kind) for kind in ("source", "destination")}
    connection = named(listing(api, f"/connections?workspaceIds={workspace}"), wanted["connection"]["name"], "connection")
    steps: list[Step] = []
    ids: dict[str, str] = {}
    for kind in ("source", "destination"):
        steps += plan_definition(workspace, kind, wanted[kind]["definition"], definitions[kind], ids)
    for kind in ("source", "destination"):
        steps += plan_actor(workspace, kind, wanted[kind], actors[kind], ids, secrets, recorded)
    steps += plan_connection(workspace, wanted["connection"], connection, ids)
    return steps


def plan_definition(workspace: str, kind: str, wanted: dict[str, Any], current: dict[str, Any] | None, ids: dict[str, str]) -> list[Step]:
    path = f"/workspaces/{workspace}/definitions/{kind}s"
    if current is None:

        def create(api: Api) -> None:
            ids[f"{kind}_definition"] = str(api.request("POST", path, {field: wanted[field] for field in DEFINITION_FIELDS})["id"])

        return [Step(f"{kind} definition", create)]
    if current.get("dockerRepository") != wanted["dockerRepository"]:
        raise ApiError(f"the {kind} definition {wanted['name']} runs another image than the document's")
    ids[f"{kind}_definition"] = str(current["id"])
    if current.get("dockerImageTag") == wanted["dockerImageTag"]:
        return []
    body = {"name": wanted["name"], "dockerImageTag": wanted["dockerImageTag"]}
    return [Step(f"{kind} definition", lambda api: api.request("PUT", f"{path}/{current['id']}", body))]


def plan_actor(
    workspace: str, kind: str, wanted: dict[str, Any], current: dict[str, Any] | None, ids: dict[str, str], secrets: Mapping[str, str], recorded: dict[str, str]
) -> list[Step]:
    """The step that makes the source or the destination, or none. The key is put into the configuration that is sent; Airbyte answers its secrets as the mask,
    so the answer is compared with the document, which holds none, and the key is sent again when the record's fingerprint of this actor's ID differs."""
    path, id_field = f"/{kind}s", f"{kind}Id"
    sent = inject(wanted["configuration"], wanted.get("secret_fields", {}), secrets)
    if current is None:

        def create(api: Api) -> None:
            body = {"name": wanted["name"], "workspaceId": workspace, "definitionId": ids[f"{kind}_definition"], "configuration": sent}
            ids[kind] = str(api.request("POST", path, body)[id_field])
            recorded[kind] = fingerprint(ids[kind], secrets)

        return [Step(kind, create)]
    if f"{kind}_definition" not in ids or current.get("definitionId") != ids[f"{kind}_definition"]:
        raise ApiError(f"the {kind} {wanted['name']} belongs to another definition than the pinned one; delete it in Airbyte and run again")
    actor_id = str(current[id_field])
    ids[kind] = actor_id
    applied = fingerprint(actor_id, secrets)
    if covers(current.get("configuration"), wanted["configuration"]) and recorded.get(kind) == applied:
        return []

    def update(api: Api) -> None:
        api.request("PATCH", f"{path}/{actor_id}", {"configuration": sent})
        recorded[kind] = applied

    return [Step(kind, update)]


def plan_connection(workspace: str, wanted: dict[str, Any], current: dict[str, Any] | None, ids: dict[str, str]) -> list[Step]:
    fields = {field: wanted[field] for field in CONNECTION_FIELDS}
    if current is None:

        def create(api: Api) -> None:
            api.request("POST", "/connections", {**fields, "sourceId": ids["source"], "destinationId": ids["destination"]})

        return [Step("connection", create)]
    known = ("source" in ids and "destination" in ids) and (current.get("sourceId"), current.get("destinationId")) == (ids["source"], ids["destination"])
    created_now = any(kind not in ids for kind in ("source", "destination"))
    if created_now or not known:
        raise ApiError(
            f"the connection {wanted['name']} joins other objects than the pinned ones; Airbyte cannot move it, and deleting it would lose its sync state"
        )
    if covers(current, fields):
        return []
    return [Step("connection", lambda api: api.request("PATCH", f"/connections/{current['connectionId']}", fields))]


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
    """Reconcile and keep the record of applied keys. The record is written after a failed run too, so a source that was given its key is not sent it again."""
    recorded = read_record(record_file)
    before = dict(recorded)
    try:
        return reconcile(api, wanted, secrets, recorded)
    finally:
        if recorded != before:
            write_record(record_file, recorded)


def client_credentials(run: Runner, kubectl: list[str], namespace: str) -> tuple[str, str]:
    """The administrator's client id and secret from the namespace's auth secret.

    The call has kubectl's own deadline (``--request-timeout``), and the module's task has a deadline too. A failure of any kind, a timeout included, names the
    secret and the namespace and never shows kubectl's text or a value.
    """
    code, output, _ = run([*kubectl, "get", "secret", AUTH_OBJECT, "--namespace", namespace, "--output", "json", f"--request-timeout={KUBECTL_SECONDS}s"])
    if code != 0:
        raise ApiError(f"the secret {AUTH_OBJECT} cannot be read in {namespace}")
    try:
        data = json.loads(output)["data"]
        return base64.b64decode(data[ID_FIELD]).decode(), base64.b64decode(data[KEY_FIELD]).decode()
    except (ValueError, KeyError, TypeError):
        raise ApiError(f"the secret {AUTH_OBJECT} holds no {ID_FIELD} and {KEY_FIELD} yet; Airbyte writes them when it first starts") from None


def access_token(base_url: str, client_id: str, client_secret: str, timeout_seconds: float) -> str:
    reply = HttpApi(base_url, None, timeout_seconds=timeout_seconds).request(
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
            "secrets_directory": {"type": "path", "required": True},
            "record_file": {"type": "path", "required": True},
        },
        supports_check_mode=False,
    )
    params = module.params
    try:
        client_id, client_secret = client_credentials(lambda arguments: module.run_command(arguments, check_rc=False), params["kubectl"], params["namespace"])
        token = access_token(params["base_url"], client_id, client_secret, params["timeout_seconds"])
        changes = apply_document(
            HttpApi(params["base_url"], lambda: token, timeout_seconds=params["timeout_seconds"]),
            params["document"],
            read_key_files(Path(params["secrets_directory"])),
            Path(params["record_file"]),
        )
        module.exit_json(changed=bool(changes), changes=changes)
    except ApiError as error:
        module.fail_json(msg=str(error))


if __name__ == "__main__":
    main()
