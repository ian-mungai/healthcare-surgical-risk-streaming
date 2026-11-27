"""The Airbyte calls of the record connections, against a stand-in of Airbyte's public API.

The stand-in holds only what this reading of the API needs: the token call, the workspace, the built-in definitions, the sources, destinations and connections
with their lists, creates and updates, and the job calls. It is the test's statement of that reading, which is the one the raw connection's module makes of the
same API, and the real API is proven on the host: a case here shows that the code does what it means to do, not that Airbyte answers as the stand-in does.

Airbyte reads a source's schema when it creates a connection. An S3 source that holds no static key cannot list the bucket, because a pod has no sign-in of
its own on the host, and Airbyte then answers the create with status 500 (the live record load of Nov 26 2026); the stand-in does the same. It masks each
stored key in its answers.

A second call of ``ensure_*`` makes no create and no update, a drifted object is repaired by an update, a field that Airbyte adds to an answer is not drift, a
secret is sent only in the body of a request for the destination or for an S3 source and never shown in an error, and a failed or late call becomes one error
that holds no body.
"""

from __future__ import annotations

import contextlib
import http.client
import io
import json
import pathlib
import re
import socket
import threading
import time
import traceback
import unittest
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from unittest import mock
from urllib.parse import parse_qs, urlsplit

from scripts.ingestion import airbyte_api
from scripts.ingestion import record_definitions as definitions
from scripts.ingestion.airbyte_api import ACTIVE_STATUSES, JOB_STATUSES, TERMINAL_STATUSES, AirbyteError, HttpAirbyteApi, check_api_origin, main
from scripts.ingestion.record_definitions import DESTINATION, SOURCE_FILE, SOURCE_S3, TRACK_LIST, Environment

RUN = "run_20261122_ab12cd34"
ENVIRONMENT = Environment("example-bucket", "us-west-2", "warehouse", "http://nessie.example:19120/api/v2")
ACCESS_ID = "AKIAEXAMPLEEXAMPLE0"
ACCESS_VALUE = "wJalrXUtnFEMIexample0000000000000000000000"
MASK = "**********"
SECRET_FIELDS = ("access_key_id", "secret_access_key", "aws_access_key_id", "aws_secret_access_key")
S3_SOURCE_DEFINITION = "source_definition1"
PREFIX = "/api/public/v1"
ORIGIN = "http://10.42.0.1:8001"
WRITE_METHODS = ("POST", "PUT", "PATCH", "DELETE")
PLATFORM_FIELD = {"platform_added": "a default"}
MARKER = "marker_6f1c2e_never_in_a_message"
ENDLESS_LIMIT = 5000


class StandIn:
    """Airbyte's public API in memory. ``log`` lists each call as its method and path; ``bodies`` keeps each request body."""

    def __init__(self, *, builtin: bool = True) -> None:
        self.log: list[str] = []
        self.bodies: list[tuple[str, dict[str, Any]]] = []
        self.definitions: dict[str, dict[str, dict[str, Any]]] = {"source": {}, "destination": {}}
        if builtin:
            for kind, identifier, connector in (
                ("source", "source_definition1", SOURCE_S3),
                ("source", "source_definition2", SOURCE_FILE),
                ("destination", "destination_definition1", DESTINATION),
            ):
                self.add_definition(kind, identifier, connector.repository, connector.tag)
        self.sources: dict[str, dict[str, Any]] = {}
        self.destinations: dict[str, dict[str, Any]] = {}
        self.connections: dict[str, dict[str, Any]] = {}
        self.jobs: dict[int, dict[str, Any]] = {}
        self.tokens = 0
        self.expire_next_token = False
        self.failures: dict[str, tuple[int, bytes] | type[Exception]] = {}
        self.counters: dict[str, int] = {}
        self.workspaces = [{"workspaceId": "workspace1"}]
        self.replies: dict[tuple[str, int], Any] = {}
        self.endless: set[str] = set()
        self.link = f"{ORIGIN}{PREFIX}/more"
        self.requested: list[tuple[str, int, list[str]]] = []
        self.budgets: list[float] = []
        self.mutations: dict[str, Any] = {}

    def add_definition(self, kind: str, identifier: str, repository: str, tag: str, name: str = "Built-in") -> None:
        """A definition that Airbyte holds already, as its built-in ones: the client never writes one."""
        self.definitions[kind][identifier] = {
            "id": identifier,
            "name": name,
            "dockerRepository": repository,
            "dockerImageTag": tag,
            "platform_added": "a default",
        }

    def ident(self, prefix: str) -> str:
        self.counters[prefix] = self.counters.get(prefix, 0) + 1
        return f"{prefix}{self.counters[prefix]}"

    def send(self, method: str, target: str, payload: bytes | None, headers: dict[str, str]) -> tuple[int, bytes]:
        parts = urlsplit(target)
        path = parts.path.removeprefix(PREFIX)
        query = parse_qs(parts.query)
        body = json.loads(payload) if payload else {}
        label = f"{method} {path.strip('/')}"
        self.log.append(label)
        self.bodies.append((label, body))
        failure = self.failures.get(label)
        if isinstance(failure, tuple):
            return failure
        if failure is not None:
            raise failure()
        if label == "POST applications/token":
            self.tokens += 1
            token = {"access_token": f"token{self.tokens}"}
            if label in self.mutations:
                self.mutations[label](token)
            return 200, json.dumps(token).encode()
        if headers.get("Authorization") != f"Bearer token{self.tokens}" or self.expire_next_token:
            self.expire_next_token = False
            return 401, b'{"message":"expired"}'
        if label == "POST connections" and not self.schema_can_be_read(body["sourceId"]):
            return 500, b'{"message":"Schema discovery failed for source"}'
        answer = self.route(method, path.strip("/").split("/"), query, body)
        if label in self.mutations and answer is not None:
            self.mutations[label](answer)
        return (200, json.dumps(answer).encode()) if answer is not None else (404, b"{}")

    def route(self, method: str, parts: list[str], query: dict[str, list[str]], body: dict[str, Any]) -> Any:
        if parts == ["workspaces"] and method == "GET":
            return {"data": self.workspaces}
        if parts[0] == "workspaces" and len(parts) >= 4 and parts[2] == "definitions":
            return self.definition(method, parts[3].removesuffix("s"), query)
        if parts[0] in ("sources", "destinations"):
            return self.actor(method, parts[0].removesuffix("s"), parts[1:], body, query)
        if parts[0] == "connections":
            return self.connection(method, parts[1:], body)
        if parts[0] == "jobs":
            return self.job(method, parts[1:], query, body)
        return None

    def definition(self, method: str, kind: str, query: dict[str, list[str]]) -> Any:
        store = self.definitions[kind]
        if method == "GET":
            offset, limit = int(query.get("offset", ["0"])[0]), int(query.get("limit", ["100"])[0])
            return {"data": list(store.values())[offset : offset + limit], "next": f"{ORIGIN}{PREFIX}/more" if offset + limit < len(store) else None}
        raise AssertionError(f"{method} of a {kind} definition: the client writes no definition")

    def actor(self, method: str, kind: str, rest: list[str], body: dict[str, Any], query: dict[str, list[str]]) -> Any:
        store = self.sources if kind == "source" else self.destinations
        key = f"{kind}Id"
        if method == "GET":
            data = [self.shown(kind, value) for value in store.values()]
            offset = int(query.get("offset", ["0"])[0])
            self.requested.append((kind, offset, query.get("workspaceIds", [])))
            if (kind, offset) in self.replies:
                reply = self.replies[(kind, offset)]
                return reply(data) if callable(reply) else reply
            limit = int(query.get("limit", ["100"])[0])
            if kind in self.endless:
                return {"data": [], "next": self.link} if offset < ENDLESS_LIMIT else {"data": []}
            more = offset + limit < len(data)
            return {"data": data[offset : offset + limit], **({"next": self.link} if more else {})}
        if method == "POST":
            entry = {**body, key: self.ident(kind)}
            entry["configuration"] = {**body["configuration"], **PLATFORM_FIELD}
            store[entry[key]] = entry
            return entry
        store[rest[0]]["configuration"] = {**body["configuration"], **PLATFORM_FIELD}
        return store[rest[0]]

    def shown(self, kind: str, entry: dict[str, Any]) -> dict[str, Any]:
        def masked(values: dict[str, Any]) -> dict[str, Any]:
            return {name: MASK if name in SECRET_FIELDS else masked(value) if isinstance(value, dict) else value for name, value in values.items()}

        return {**entry, "configuration": masked(entry["configuration"])}

    def schema_can_be_read(self, source_id: str) -> bool:
        """Whether Airbyte's schema discovery of the source can list what it reads: an S3 source needs the static key."""
        source = self.sources[source_id]
        credentials = source["configuration"].get("credentials", {})
        signed_in = credentials.get("aws_access_key_id") == ACCESS_ID and credentials.get("aws_secret_access_key") == ACCESS_VALUE
        return bool(source["definitionId"] != S3_SOURCE_DEFINITION or (credentials.get("auth_type") == "iam_user" and signed_in))

    def connection(self, method: str, rest: list[str], body: dict[str, Any]) -> Any:
        if method == "GET":
            return {"data": list(self.connections.values())}
        if method == "POST":
            entry = {**body, "connectionId": self.ident("connection"), "platform_added": "x"}
            self.connections[entry["connectionId"]] = entry
            return entry
        self.connections[rest[0]].update(body)
        return self.connections[rest[0]]

    def job(self, method: str, rest: list[str], query: dict[str, list[str]], body: dict[str, Any]) -> Any:
        if method == "POST":
            number = len(self.jobs) + 1
            self.jobs[number] = {"jobId": number, "connectionId": body["connectionId"], "jobType": body["jobType"], "status": "running"}
            return dict(self.jobs[number])
        if method == "GET" and not rest:
            return {"data": [job for job in self.jobs.values() if job["connectionId"] == query["connectionId"][0]]}
        if method == "GET":
            return self.jobs[int(rest[0])]
        self.jobs[int(rest[0])]["status"] = "cancelled"
        return self.jobs[int(rest[0])]


class Api(HttpAirbyteApi):
    def __init__(self, stand_in: StandIn) -> None:
        super().__init__(ORIGIN, "client", "client_value", ACCESS_ID, ACCESS_VALUE)
        self.stand_in = stand_in

    def send(self, method: str, target: str, payload: bytes | None, headers: dict[str, str], budget: float) -> tuple[int, bytes]:
        self.stand_in.budgets.append(budget)
        return self.stand_in.send(method, target, payload, headers)


def prepared() -> tuple[Api, StandIn]:
    stand_in = StandIn()
    return Api(stand_in), stand_in


def ensure_everything(api: Api, run: str = RUN) -> dict[str, str]:
    """The writes of a load in its order: the load checks the three definitions before its first copy and so before any of these writes."""
    api.check_definitions()
    api.ensure_destination(definitions.destination_configuration(ENVIRONMENT, definitions.branch_name(run)))
    ids = {}
    for table in definitions.TABLES:
        api.ensure_source(table.name, definitions.source_configuration(table, run, ENVIRONMENT, "ab" * 32))
        ids[table.name] = api.ensure_connection(table.name, definitions.connection_configuration(table))
    return ids


def writes(stand_in: StandIn) -> list[str]:
    return [label for label in stand_in.log if label.split(" ")[0] in WRITE_METHODS and label not in ("POST applications/token", "POST jobs")]


class EnsureTest(unittest.TestCase):
    def test_the_first_call_creates_the_destination_each_source_and_each_connection_on_the_built_in_definitions_and_no_definition(self) -> None:
        api, stand_in = prepared()
        ids = ensure_everything(api)
        self.assertEqual(["clinical_data", "lab_data", "track_list"], sorted(ids))
        self.assertEqual((3, 1, 3), (len(stand_in.sources), len(stand_in.destinations), len(stand_in.connections)))
        self.assertEqual([], [label for label in stand_in.log if "definitions" in label and not label.startswith("GET ")])
        self.assertEqual((2, 1), (len(stand_in.definitions["source"]), len(stand_in.definitions["destination"])), "no definition was added")
        self.assertEqual(
            {"clinical_data": "source_definition1", "lab_data": "source_definition1", "track_list": "source_definition2"},
            {table: stand_in.sources[f"source{index}"]["definitionId"] for index, table in enumerate(("clinical_data", "lab_data", "track_list"), start=1)},
        )
        self.assertEqual({"destination_definition1"}, {entry["definitionId"] for entry in stand_in.destinations.values()})

    def test_a_second_call_makes_no_write_although_airbyte_added_fields_to_its_answers(self) -> None:
        api, stand_in = prepared()
        ensure_everything(api)
        stand_in.log.clear()
        ensure_everything(api)
        self.assertEqual([], writes(stand_in))

    def test_a_new_run_changes_only_the_read_prefix_of_each_csv_source_and_the_branch_of_the_destination(self) -> None:
        api, stand_in = prepared()
        ensure_everything(api)
        stand_in.log.clear()
        ensure_everything(api, "run_next")
        self.assertEqual(["PATCH destinations/destination1", "PATCH sources/source1", "PATCH sources/source2"], writes(stand_in))
        self.assertEqual("load_run_next", api.destination_branch_now())

    def test_a_drifted_connection_and_a_drifted_source_are_repaired(self) -> None:
        api, stand_in = prepared()
        ids = ensure_everything(api)
        stand_in.connections[ids["lab_data"]]["schedule"] = {"scheduleType": "basic"}
        stand_in.connections[ids["lab_data"]]["status"] = "inactive"
        stand_in.sources["source1"]["configuration"]["streams"][0]["format"]["inference_type"] = "Primitive Types Only"
        stand_in.log.clear()
        ensure_everything(api)
        self.assertEqual(["PATCH sources/source1", f"PATCH connections/{ids['lab_data']}"], writes(stand_in))
        self.assertEqual(
            ({"scheduleType": "manual"}, "active"), (stand_in.connections[ids["lab_data"]]["schedule"], stand_in.connections[ids["lab_data"]]["status"])
        )
        self.assertEqual("None", stand_in.sources["source1"]["configuration"]["streams"][0]["format"]["inference_type"])

    def test_a_connection_with_another_sync_mode_is_repaired_to_the_full_refresh_that_overwrites(self) -> None:
        api, stand_in = prepared()
        ids = ensure_everything(api)
        stream = stand_in.connections[ids["clinical_data"]]["configurations"]["streams"][0]
        self.assertEqual("full_refresh_overwrite", stream["syncMode"])
        stream["syncMode"] = "full_refresh_append"
        stand_in.log.clear()
        ensure_everything(api)
        self.assertEqual([f"PATCH connections/{ids['clinical_data']}"], writes(stand_in))
        self.assertEqual("full_refresh_overwrite", stand_in.connections[ids["clinical_data"]]["configurations"]["streams"][0]["syncMode"])

    def test_a_definition_at_another_version_is_refused_before_any_write_and_the_message_names_no_value(self) -> None:
        for kind, identifier, connector in (
            ("source", "source_definition1", SOURCE_S3),
            ("source", "source_definition2", SOURCE_FILE),
            ("destination", "destination_definition1", DESTINATION),
        ):
            for tag in ("0.0.1", "latest", "", f"{connector.tag}.1"):
                with self.subTest(kind=kind, image=connector.repository, tag=tag):
                    api, stand_in = prepared()
                    stand_in.definitions[kind][identifier]["dockerImageTag"] = tag
                    with self.assertRaises(AirbyteError) as raised:
                        ensure_everything(api)
                    self.assertEqual([], writes(stand_in), "a refusal comes before the first write of any kind")
                    message = str(raised.exception)
                    self.assertIn(f"{kind} definition", message)
                    for value in (connector.repository, connector.tag, tag, identifier):
                        if value:
                            self.assertNotIn(value, message)

    def test_the_check_of_the_definitions_makes_only_reads_and_checks_all_three_before_it_answers(self) -> None:
        api, stand_in = prepared()
        api.check_definitions()
        self.assertEqual([], [label for label in stand_in.log if not label.startswith("GET ") and label != "POST applications/token"])
        for kind in ("sources", "destinations"):
            self.assertTrue(any(label.startswith(f"GET workspaces/workspace1/definitions/{kind}") for label in stand_in.log), kind)
        for kind, identifier, connector in (
            ("source", "source_definition1", SOURCE_S3),
            ("source", "source_definition2", SOURCE_FILE),
            ("destination", "destination_definition1", DESTINATION),
        ):
            with self.subTest(kind=kind, image=connector.repository):
                api, stand_in = prepared()
                stand_in.definitions[kind][identifier]["dockerImageTag"] = "0.0.1"
                with self.assertRaises(AirbyteError):
                    api.check_definitions()
                self.assertEqual([], writes(stand_in))

    def test_no_definition_or_two_definitions_of_a_pinned_image_are_refused_whatever_their_names_and_tags(self) -> None:
        for kind, identifier, connector in (
            ("source", "source_definition1", SOURCE_S3),
            ("source", "source_definition2", SOURCE_FILE),
            ("destination", "destination_definition1", DESTINATION),
        ):
            with self.subTest(kind=kind, image=connector.repository, case="none"):
                api, stand_in = prepared()
                del stand_in.definitions[kind][identifier]
                with self.assertRaises(AirbyteError) as raised:
                    ensure_everything(api)
                self.assertNotIn(connector.repository, str(raised.exception))
                self.assertEqual([], writes(stand_in), "a refusal comes before the first write of any kind")
            for second_tag, name in ((connector.tag, "Built-in"), (connector.tag, "vitaldb S3 source (pinned)"), ("0.0.1", "older")):
                with self.subTest(kind=kind, image=connector.repository, case=f"two: {second_tag} {name}"):
                    api, stand_in = prepared()
                    stand_in.add_definition(kind, "definition_extra", connector.repository, second_tag, name)
                    with self.assertRaises(AirbyteError) as raised:
                        ensure_everything(api)
                    self.assertNotIn(connector.repository, str(raised.exception))
                    self.assertEqual([], writes(stand_in), "a refusal comes before the first write of any kind")

    def test_only_the_exact_image_matches_so_a_similar_image_is_neither_taken_nor_counted(self) -> None:
        api, stand_in = prepared()
        for index, repository in enumerate(
            (
                "airbyte/source-s3-data-lake",
                "airbyte/source-s3-v3",
                "other/airbyte/source-s3",
                "airbyte/source-s3 ",
                "AIRBYTE/SOURCE-S3",
                "airbyte/source-file-secure",
            )
        ):
            stand_in.add_definition("source", f"similar{index}", repository, SOURCE_S3.tag)
        stand_in.add_definition("destination", "similar_destination", "airbyte/destination-s3", DESTINATION.tag)
        ids = ensure_everything(api)
        self.assertEqual(3, len(ids))
        self.assertEqual({"destination_definition1"}, {entry["definitionId"] for entry in stand_in.destinations.values()})

    def test_the_definition_is_found_on_a_later_page_of_the_listing(self) -> None:
        api, stand_in = prepared()
        built_in = dict(stand_in.definitions["source"])
        stand_in.definitions["source"].clear()
        for index in range(250):
            stand_in.add_definition("source", f"other{index}", f"other/source-{index}", "1.0.0")
        stand_in.definitions["source"].update(built_in)
        ids = ensure_everything(api)
        self.assertEqual(3, len(ids))

    def test_a_run_and_a_second_run_never_write_a_definition(self) -> None:
        api, stand_in = prepared()
        ensure_everything(api)
        ensure_everything(api, "run_next")
        self.assertEqual([], [label for label in stand_in.log if "definitions" in label and not label.startswith("GET ")])
        self.assertTrue(any(label.startswith("GET workspaces/workspace1/definitions/sources") for label in stand_in.log))

    def test_the_connections_hold_no_schedule_and_the_destination_branch_is_the_load_branch(self) -> None:
        api, stand_in = prepared()
        ensure_everything(api)
        self.assertEqual([{"scheduleType": "manual"}], [connection["schedule"] for connection in stand_in.connections.values()][:1])
        self.assertEqual({"manual"}, {connection["schedule"]["scheduleType"] for connection in stand_in.connections.values()})
        self.assertEqual({"raw_records"}, {connection["namespaceFormat"] for connection in stand_in.connections.values()})
        self.assertEqual(f"load_{RUN}", api.destination_branch_now())

    def test_an_object_that_belongs_to_another_definition_or_joins_other_objects_is_refused_not_deleted(self) -> None:
        api, stand_in = prepared()
        ids = ensure_everything(api)
        stand_in.sources["source1"]["definitionId"] = "other"
        with self.assertRaises(AirbyteError):
            ensure_everything(api)
        stand_in.sources["source1"]["definitionId"] = "source_definition1"
        stand_in.connections[ids["lab_data"]]["sourceId"] = "other"
        with self.assertRaises(AirbyteError):
            ensure_everything(api)
        self.assertEqual(3, len(stand_in.connections))

    def test_two_objects_with_one_name_stop_the_run_before_any_write(self) -> None:
        api, stand_in = prepared()
        ensure_everything(api)
        stand_in.sources["source9"] = {**stand_in.sources["source1"], "sourceId": "source9"}
        stand_in.log.clear()
        with self.assertRaises(AirbyteError):
            ensure_everything(api)
        self.assertEqual([], writes(stand_in))


class SecretTest(unittest.TestCase):
    def holds_the_key_where_it_belongs(self, stand_in: StandIn, verb: str) -> None:
        """Each request body holds the key exactly when it makes or changes the destination or one of the two S3 sources."""
        keyed = {"destination1", "source1", "source2"}
        for label, body in stand_in.bodies:
            if verb == "POST":
                wanted = label == "POST destinations" or (label == "POST sources" and body["definitionId"] == S3_SOURCE_DEFINITION)
            else:
                wanted = label.startswith("PATCH ") and label.rpartition("/")[2] in keyed
            self.assertEqual(wanted, ACCESS_VALUE in json.dumps(body), label)
            self.assertEqual(wanted, ACCESS_ID in json.dumps(body), label)

    def test_the_destination_and_the_two_S3_sources_get_the_access_key_in_their_requests_and_no_other_request_does(self) -> None:
        api, stand_in = prepared()
        ensure_everything(api)
        self.holds_the_key_where_it_belongs(stand_in, "POST")
        self.assertEqual(ACCESS_VALUE, stand_in.destinations["destination1"]["configuration"]["secret_access_key"])
        for name in ("source1", "source2"):
            self.assertEqual(
                {"auth_type": "iam_user", "aws_access_key_id": ACCESS_ID, "aws_secret_access_key": ACCESS_VALUE},
                stand_in.sources[name]["configuration"]["credentials"],
            )
        self.assertNotIn(ACCESS_VALUE, json.dumps([stand_in.sources["source3"], stand_in.connections]))

    def test_after_a_new_run_and_a_repair_only_the_destination_and_the_S3_source_requests_hold_the_access_key(self) -> None:
        api, stand_in = prepared()
        ids = ensure_everything(api)
        stand_in.connections[ids["lab_data"]]["status"] = "inactive"
        stand_in.sources["source1"]["configuration"]["bucket"] = "other"
        stand_in.bodies.clear()
        ensure_everything(api, "run_next")
        patched = [label for label, _ in stand_in.bodies if label.startswith("PATCH ")]
        for name in ("destinations/destination1", "sources/source1", "sources/source2"):
            self.assertIn(f"PATCH {name}", patched)
        self.holds_the_key_where_it_belongs(stand_in, "PATCH")

    def test_the_S3_sources_that_an_earlier_load_made_with_the_pods_credentials_are_given_the_key_before_their_connection_is_made(self) -> None:
        api, stand_in = prepared()
        ensure_everything(api)
        for name in ("source1", "source2"):
            stand_in.sources[name]["configuration"]["credentials"] = {"auth_type": "default_credentials"}
        stand_in.connections.clear()
        stand_in.log.clear()
        ensure_everything(api)
        self.assertEqual(["PATCH sources/source1", "POST connections", "PATCH sources/source2", "POST connections", "POST connections"], writes(stand_in))

    def test_a_masked_key_in_a_stored_S3_source_is_not_a_difference(self) -> None:
        api, stand_in = prepared()
        ensure_everything(api)
        stand_in.log.clear()
        ensure_everything(api)
        self.assertEqual([], [label for label in writes(stand_in) if "sources" in label])

    def test_no_error_of_a_source_request_holds_a_secret_or_a_response_body(self) -> None:
        api, stand_in = prepared()
        stand_in.failures["POST sources"] = (500, json.dumps({"message": f"bad {ACCESS_VALUE}"}).encode())
        with self.assertRaises(AirbyteError) as raised:
            ensure_everything(api)
        for hidden in (ACCESS_VALUE, ACCESS_ID, "bad", "client_value"):
            self.assertNotIn(hidden, str(raised.exception))
        self.assertIn("POST sources", str(raised.exception))

    def test_a_masked_secret_in_the_stored_destination_is_not_a_difference(self) -> None:
        api, stand_in = prepared()
        ensure_everything(api)
        stand_in.log.clear()
        ensure_everything(api)
        self.assertNotIn("PATCH destinations/destination1", stand_in.log)

    def test_no_error_holds_a_secret_or_a_response_body(self) -> None:
        api, stand_in = prepared()
        stand_in.failures["POST destinations"] = (500, json.dumps({"message": f"bad {ACCESS_VALUE}"}).encode())
        with self.assertRaises(AirbyteError) as raised:
            ensure_everything(api)
        for hidden in (ACCESS_VALUE, ACCESS_ID, "bad", "client_value"):
            self.assertNotIn(hidden, str(raised.exception))
        self.assertIn("POST destinations", str(raised.exception))


class TokenTest(unittest.TestCase):
    def test_one_token_serves_every_call_and_the_first_call_is_the_token_call(self) -> None:
        api, stand_in = prepared()
        ensure_everything(api)
        self.assertEqual(1, stand_in.tokens)
        self.assertEqual("POST applications/token", stand_in.log[0])
        self.assertEqual({"client_id": "client", "client_secret": "client_value"}, stand_in.bodies[0][1])

    def test_an_expired_token_is_renewed_once_and_the_call_is_repeated_once(self) -> None:
        api, stand_in = prepared()
        api.running_jobs("connection1")
        stand_in.expire_next_token = True
        stand_in.log.clear()
        api.running_jobs("connection1")
        self.assertEqual(["GET jobs", "POST applications/token", "GET jobs"], stand_in.log)

    def test_a_token_that_is_refused_twice_stops_with_an_error(self) -> None:
        api, stand_in = prepared()
        api.running_jobs("connection1")
        stand_in.failures["GET jobs"] = (401, b"{}")
        with self.assertRaises(AirbyteError):
            api.running_jobs("connection1")


PLANTED = "tokenvalue_5b7e91"
MALFORMED_TOKENS = {
    "a trailing line feed": f"{PLANTED}\n",
    "a trailing carriage return and line feed": f"{PLANTED}\r\n",
    "a space inside": f"{PLANTED} more",
    "a leading space": f" {PLANTED}",
    "a NUL byte": f"{PLANTED}\x00",
    "a tab": f"{PLANTED}\t",
    "a letter outside ASCII": f"{PLANTED}\u00e9",
    "an equals sign before other characters": f"{PLANTED}=x",
    "an opening bracket": f"{PLANTED}[",
    "a second line": f"{PLANTED}\nInjected: yes",
}
VALID_TOKENS = ("abc", "A-b.c_d~e+f/g", "abc=", "abc==", PLANTED, "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2ln")


def whole_text(error: BaseException) -> str:
    """The message, the traceback with its causes and contexts, and the arguments of the error."""
    return "".join(traceback.format_exception(error)) + repr(error.args)


def raised_text(call: Callable[[], object]) -> tuple[AirbyteError, str]:
    """The error that the call raises and its whole text, formatted inside the handler: ``assertRaises`` clears the traceback before the test can read it."""
    try:
        call()
    except AirbyteError as error:
        return error, whole_text(error)
    raise AssertionError("the call raised no AirbyteError")


class TokenFormTest(unittest.TestCase):
    """A token that is not of the bearer form (RFC 6750 b64token, as the Ansible client reads it) is refused before it is cached or put in a header."""

    def refused(self, token: str) -> tuple[Api, StandIn, AirbyteError, str]:
        api, stand_in = prepared()
        stand_in.mutations["POST applications/token"] = lambda answer: answer.update({"access_token": token})
        error, text = raised_text(lambda: api.running_jobs("connection1"))
        return api, stand_in, error, text

    def assert_text_is_clean(self, text: str) -> None:
        self.assertIn("renew_token", text, "the text holds the traceback, which names the function that refused the token")
        self.assertNotIn(PLANTED, text)

    def test_a_malformed_token_is_refused_with_a_fixed_message_that_holds_no_part_of_it(self) -> None:
        messages = set()
        for what, token in MALFORMED_TOKENS.items():
            with self.subTest(what):
                _, _, error, text = self.refused(token)
                self.assert_text_is_clean(text)
                self.assertIn("POST applications/token", str(error))
                messages.add(str(error))
        self.assertEqual(1, len(messages), "the message is one fixed text, whatever the token held")

    def test_a_malformed_token_is_not_cached_and_no_call_follows_the_token_call(self) -> None:
        self.assertGreaterEqual(len(MALFORMED_TOKENS), 10)
        for what, token in MALFORMED_TOKENS.items():
            with self.subTest(what):
                api, stand_in, _, _ = self.refused(token)
                self.assertIsNone(api.token)
                self.assertEqual(["POST applications/token"], stand_in.log)

    def test_a_malformed_token_that_comes_with_a_renewal_is_refused_the_same_way(self) -> None:
        api, stand_in = prepared()
        api.running_jobs("connection1")
        stand_in.expire_next_token = True
        stand_in.mutations["POST applications/token"] = lambda answer: answer.update({"access_token": f"{PLANTED}\n"})
        _, text = raised_text(lambda: api.running_jobs("connection1"))
        self.assert_text_is_clean(text)
        self.assertEqual(["GET jobs", "GET jobs", "POST applications/token"], stand_in.log[-3:])

    def test_control_each_character_that_the_form_allows_is_accepted(self) -> None:
        self.assertGreaterEqual(len(VALID_TOKENS), 6)
        for token in VALID_TOKENS:
            with self.subTest(token):
                api, stand_in = prepared()
                stand_in.mutations["POST applications/token"] = lambda answer, token=token: answer.update({"access_token": token})
                api.renew_token(1.0)
                self.assertEqual(token, api.token)

    def test_control_the_text_check_sees_a_token_in_a_message_in_a_cause_and_in_arguments(self) -> None:
        in_message = AirbyteError(f"bad {PLANTED}")
        in_cause = AirbyteError("fixed")
        in_cause.__cause__ = ValueError(PLANTED)
        in_arguments = AirbyteError("fixed", PLANTED)
        for error in (in_message, in_cause, in_arguments):
            self.assertIn(PLANTED, whole_text(error))
        self.assertNotIn(PLANTED, whole_text(AirbyteError("fixed")))

    def test_the_form_is_the_one_that_the_ansible_client_holds(self) -> None:
        source = (pathlib.Path(__file__).resolve().parents[2] / "ansible/roles/airbyte/library/airbyte_connection.py").read_text(encoding="utf-8")
        found = re.search(r'^TOKEN_FORM = re\.compile\(r"(.*)"\)$', source, re.MULTILINE)
        self.assertIsNotNone(found, "the Ansible client's TOKEN_FORM line was not found")
        self.assertEqual(found.group(1) if found else None, airbyte_api.TOKEN_FORM.pattern)

    def test_the_whole_client_over_http_refuses_a_token_with_a_line_break_before_it_goes_into_a_header(self) -> None:
        seen: list[str | None] = []

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_POST(self) -> None:  # noqa: N802
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                body = json.dumps({"access_token": f"{PLANTED}\n"}).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self) -> None:  # noqa: N802
                seen.append(self.headers.get("Authorization"))
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

            def log_message(self, *args: Any) -> None:
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            api = HttpAirbyteApi(ORIGIN, "client", "client_value", ACCESS_ID, ACCESS_VALUE)
            api.host, api.port = "127.0.0.1", server.server_address[1]
            _, text = raised_text(lambda: api.call("GET", "/workspaces", timeout=5.0))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
        self.assert_text_is_clean(text)
        self.assertEqual([], seen)
        self.assertIsNone(api.token)


class FindConnectionsTest(unittest.TestCase):
    def test_before_anything_exists_none_is_found_and_nothing_is_written(self) -> None:
        api, stand_in = prepared()
        self.assertEqual({}, api.find_connection_ids())
        self.assertEqual([], writes(stand_in))

    def test_after_the_first_load_each_table_s_connection_is_found_by_its_name_and_nothing_is_written(self) -> None:
        api, stand_in = prepared()
        ids = ensure_everything(api)
        stand_in.log.clear()
        other, _ = prepared()
        other.stand_in = stand_in
        self.assertEqual(ids, other.find_connection_ids())
        self.assertEqual([], writes(stand_in))

    def test_a_connection_that_is_not_one_of_the_three_is_not_listed_and_a_duplicate_name_is_refused(self) -> None:
        api, stand_in = prepared()
        ensure_everything(api)
        stand_in.connections["connection9"] = {"connectionId": "connection9", "name": "someone else's connection"}
        self.assertEqual(3, len(api.find_connection_ids()))
        stand_in.connections["connection8"] = {**stand_in.connections["connection1"], "connectionId": "connection8"}
        with self.assertRaises(AirbyteError):
            api.find_connection_ids()


class SyncTest(unittest.TestCase):
    def test_a_sync_is_started_polled_and_cancelled_by_its_job_ID(self) -> None:
        api, stand_in = prepared()
        ids = ensure_everything(api)
        self.assertEqual([], api.running_jobs(ids["lab_data"]))
        job = api.start_sync(ids["lab_data"])
        self.assertEqual("running", api.job_status(job))
        self.assertEqual([job], api.running_jobs(ids["lab_data"]))
        self.assertEqual({"connectionId": ids["lab_data"], "jobType": "sync"}, [body for label, body in stand_in.bodies if label == "POST jobs"][0])
        api.cancel_job(job)
        self.assertEqual("cancelled", api.job_status(job))
        self.assertEqual([], api.running_jobs(ids["lab_data"]))

    def test_only_jobs_of_the_connection_count_as_running(self) -> None:
        api, stand_in = prepared()
        ids = ensure_everything(api)
        api.start_sync(ids["lab_data"])
        self.assertEqual([], api.running_jobs(ids["clinical_data"]))


class StartTest(unittest.TestCase):
    """The job ID is all that the start of a sync needs from its answer, because the ID is what lets the caller record the job and cancel it."""

    def test_a_started_job_whose_answer_has_no_readable_status_is_still_returned_by_its_ID_and_its_status_is_read_later(self) -> None:
        for name, status in (("null", None), ("missing", ...), ("an object", {MARKER: MARKER}), ("a text outside the set", MARKER)):
            with self.subTest(name):
                api, stand_in = prepared()
                ids = ensure_everything(api)

                def corrupt(answer: dict[str, Any], status: Any = status) -> None:
                    if status is ...:
                        answer.pop("status")
                    else:
                        answer["status"] = status

                stand_in.mutations["POST jobs"] = corrupt
                job = api.start_sync(ids["lab_data"])
                self.assertEqual(1, job)
                stand_in.mutations.clear()
                self.assertEqual("running", api.job_status(job))

    def test_a_reply_with_no_job_ID_is_refused_with_a_message_that_says_nothing_can_be_cancelled_and_holds_no_answer(self) -> None:
        for name, corrupt in (
            ("missing", lambda answer: answer.pop("jobId")),
            ("null", lambda answer: answer.__setitem__("jobId", None)),
            ("a text", lambda answer: answer.__setitem__("jobId", MARKER)),
            ("a boolean", lambda answer: answer.__setitem__("jobId", True)),
        ):
            with self.subTest(name):
                api, stand_in = prepared()
                ids = ensure_everything(api)
                stand_in.mutations["POST jobs"] = corrupt
                with self.assertRaises(AirbyteError) as raised:
                    api.start_sync(ids["lab_data"])
                self.assertIn("cannot cancel", str(raised.exception))
                self.assertIn("jobId", str(raised.exception))
                self.assertNotIn(MARKER, str(raised.exception))


class PagingTest(unittest.TestCase):
    """A list is paged by the client's own offset: it starts at 0, adds the page size after each page, keeps its route and filters and ends when a reply has
    no ``next``. No ``next`` link is parsed: only whether there is one matters."""

    def destination_offsets(self, stand_in: StandIn) -> list[int]:
        return [offset for kind, offset, _ in stand_in.requested if kind == "destination"]

    def behind_an_empty_first_page(self, following: Any) -> tuple[Api, StandIn]:
        """Everything is ensured, then the first destination page is empty and says ``following``; the destination is on the page at offset 100."""
        api, stand_in = prepared()
        ensure_everything(api)
        stand_in.replies[("destination", 0)] = {"data": [], "next": following}
        stand_in.replies[("destination", 100)] = lambda data: {"data": data}
        stand_in.requested.clear()
        stand_in.log.clear()
        return api, stand_in

    def test_an_empty_page_with_a_next_moves_to_the_next_page_and_the_object_behind_it_is_found(self) -> None:
        api, stand_in = self.behind_an_empty_first_page(f"{ORIGIN}{PREFIX}/destinations?workspaceIds=workspace1&limit=100&offset=100")
        ensure_everything(api)
        self.assertEqual([], writes(stand_in), "the destination behind the empty page was found, and none was made again")
        self.assertEqual([0, 100], self.destination_offsets(stand_in))

    def test_each_request_keeps_the_route_and_the_filters_whatever_the_next_says(self) -> None:
        api, stand_in = self.behind_an_empty_first_page("http://8.8.8.8:80/other/route?workspaceIds=other&limit=1&offset=100")
        ensure_everything(api)
        self.assertEqual(
            [("destination", 0, ["workspace1"]), ("destination", 100, ["workspace1"])], [item for item in stand_in.requested if item[0] == "destination"]
        )

    def test_a_next_of_any_form_is_never_parsed_the_next_page_is_the_one_after_the_page_size(self) -> None:
        for name, following in (
            ("a text", MARKER),
            ("no offset", f"{ORIGIN}{PREFIX}/destinations?workspaceIds=workspace1&limit=100"),
            ("a text offset", f"{ORIGIN}{PREFIX}/destinations?offset={MARKER}"),
            ("a blank first offset before a second", f"{ORIGIN}{PREFIX}/destinations?offset=&offset=200"),
            ("an offset split by a line break", f"{ORIGIN}{PREFIX}/destinations?offset=2\n00"),
            ("an offset split by an encoded line break", f"{ORIGIN}{PREFIX}/destinations?offset=1%0A00"),
            ("two offsets", f"{ORIGIN}{PREFIX}/destinations?offset=100&offset=200"),
            ("an offset that is the one just read", f"{ORIGIN}{PREFIX}/destinations?offset=0"),
            ("an offset that goes back", f"{ORIGIN}{PREFIX}/destinations?offset=-100"),
            ("an offset far ahead", f"{ORIGIN}{PREFIX}/destinations?offset=900"),
            ("an offset that is too long", f"{ORIGIN}{PREFIX}/destinations?offset=99999999999"),
            ("an address that cannot be read", "http://[::1"),
        ):
            with self.subTest(name):
                api, stand_in = self.behind_an_empty_first_page(following)
                ensure_everything(api)
                self.assertEqual([], writes(stand_in))
                self.assertEqual([0, 100], self.destination_offsets(stand_in))

    def test_a_list_that_has_a_next_for_ever_is_refused_at_the_page_cap_and_each_offset_is_one_page_size_further(self) -> None:
        api, stand_in = prepared()
        ensure_everything(api)
        stand_in.endless = {"destination"}
        stand_in.requested.clear()
        with mock.patch.object(airbyte_api, "MAX_PAGES", 5), self.assertRaises(AirbyteError) as raised:
            ensure_everything(api)
        self.assertIn("more pages than 5", str(raised.exception))
        self.assertEqual([0, 100, 200, 300, 400], self.destination_offsets(stand_in))

    def test_a_list_in_pages_of_one_is_read_to_its_end_by_the_clients_own_offsets_and_stops_at_the_reply_with_no_next(self) -> None:
        api, stand_in = prepared()
        ensure_everything(api)
        stand_in.requested.clear()
        stand_in.log.clear()
        with mock.patch.object(airbyte_api, "PAGE_SIZE", 1):
            ensure_everything(api)
        self.assertEqual([], writes(stand_in))
        self.assertEqual([0], self.destination_offsets(stand_in), "one destination is one page")
        self.assertEqual([0, 1, 2], [offset for kind, offset, _ in stand_in.requested if kind == "source"][:3])

    def test_a_reply_with_no_next_or_an_empty_next_ends_the_list_at_its_first_page(self) -> None:
        for name, extra in (("no next", {}), ("an empty next", {"next": ""}), ("a null next", {"next": None})):
            with self.subTest(name):
                api, stand_in = prepared()
                ensure_everything(api)
                stand_in.replies[("destination", 0)] = lambda data, extra=extra: {"data": data, **extra}
                stand_in.requested.clear()
                stand_in.log.clear()
                ensure_everything(api)
                self.assertEqual([0], self.destination_offsets(stand_in))


class FailureTest(unittest.TestCase):
    def test_a_refused_call_a_dropped_connection_and_a_deadline_are_each_one_error_that_names_the_call(self) -> None:
        for name, failure in (("refused", (500, b"{}")), ("dropped", ConnectionResetError), ("late", TimeoutError)):
            with self.subTest(name):
                api, stand_in = prepared()
                stand_in.failures["GET workspaces"] = failure
                with self.assertRaises(AirbyteError) as raised:
                    api.ensure_destination(definitions.destination_configuration(ENVIRONMENT, definitions.branch_name(RUN)))
                self.assertIn("GET workspaces", str(raised.exception))

    def test_an_answer_that_is_not_an_object_is_refused(self) -> None:
        api, stand_in = prepared()
        stand_in.failures["GET workspaces"] = (200, b"[1]")
        with self.assertRaises(AirbyteError):
            ensure_everything(api)

    def test_a_workspace_list_that_does_not_hold_exactly_one_workspace_is_refused(self) -> None:
        for workspaces in ([], [{"workspaceId": "a"}, {"workspaceId": "b"}]):
            with self.subTest(count=len(workspaces)):
                api, stand_in = prepared()
                stand_in.workspaces = workspaces
                with self.assertRaises(AirbyteError):
                    ensure_everything(api)

    def test_a_table_that_was_never_ensured_is_refused(self) -> None:
        api, _ = prepared()
        with self.assertRaises(AirbyteError):
            api.ensure_connection(TRACK_LIST.name, definitions.connection_configuration(TRACK_LIST))
        with self.assertRaises(AirbyteError):
            api.destination_branch_now()

    def test_a_list_with_more_pages_is_read_to_its_end(self) -> None:
        api, stand_in = prepared()
        ensure_everything(api)
        stand_in.log.clear()
        with mock.patch.object(airbyte_api, "PAGE_SIZE", 1):
            ensure_everything(api)
        self.assertEqual([], writes(stand_in))
        self.assertIn("GET sources", stand_in.log)
        self.assertGreater(stand_in.log.count("GET sources"), 3, "each list was read in pages")


class AddressTest(unittest.TestCase):
    """The address is where both credentials go, so only a plain ``http://<private IPv4 literal>:<port>`` is accepted: no name, no userinfo, no path."""

    REFUSED = (
        ("a host name", "http://airbyte.example:8001"),
        ("localhost by name", "http://localhost:8001"),
        ("a public address", "http://8.8.8.8:8001"),
        ("a public address that looks private", "http://10.42.0.1.example:8001"),
        ("loopback", "http://127.0.0.1:8001"),
        ("the unspecified address", "http://0.0.0.0:8001"),
        ("a multicast address", "http://224.0.0.1:8001"),
        ("userinfo", "http://user:pass@10.42.0.1:8001"),
        ("userinfo with an at sign in the host part", "http://10.42.0.1@8.8.8.8:8001"),
        ("no port", "http://10.42.0.1"),
        ("port 0", "http://10.42.0.1:0"),
        ("a port over 65535", "http://10.42.0.1:70000"),
        ("a path", "http://10.42.0.1:8001/api"),
        ("a slash after the port", "http://10.42.0.1:8001/"),
        ("an empty query", "http://10.42.0.1:8001?"),
        ("an empty fragment", "http://10.42.0.1:8001#"),
        ("a line end after the port", "http://10.42.0.1:8001\n"),
        ("a port with leading zeros", "http://10.42.0.1:08001"),
        ("a port with a sign", "http://10.42.0.1:+8001"),
        ("an address in the 172 block outside the private range", "http://172.32.0.1:8001"),
        ("a query", "http://10.42.0.1:8001?a=1"),
        ("a fragment", "http://10.42.0.1:8001#x"),
        ("an IPv6 literal", "http://[fd00::1]:8001"),
        ("https", "https://10.42.0.1:8001"),
        ("no scheme", "10.42.0.1:8001"),
        ("an empty text", ""),
        ("leading zeros", "http://010.042.000.001:8001"),
        ("a space", "http://10.42.0.1 :8001"),
        ("a decimal address", "http://170524673:8001"),
    )
    ACCEPTED = ("http://10.42.0.1:8001", "http://172.16.4.1:8001", "http://192.168.1.1:80", "http://169.254.10.2:8001")

    def test_each_address_that_could_send_a_credential_elsewhere_is_refused(self) -> None:
        for name, address in self.REFUSED:
            with self.subTest(name), self.assertRaises(AirbyteError):
                HttpAirbyteApi(address, "client", "client_value", ACCESS_ID, ACCESS_VALUE)
            with self.subTest(f"{name} by the check"), self.assertRaises(AirbyteError):
                check_api_origin(address)

    def test_a_private_or_link_local_IPv4_literal_with_a_port_is_accepted_as_a_host_and_a_port(self) -> None:
        for address in self.ACCEPTED:
            with self.subTest(address):
                host, port = check_api_origin(address)
                api = HttpAirbyteApi(address, "client", "client_value", ACCESS_ID, ACCESS_VALUE)
                self.assertEqual((host, port), (api.host, api.port))
        self.assertEqual(("10.42.0.1", 8001), check_api_origin("http://10.42.0.1:8001"))

    def test_the_messages_name_the_rule_and_never_the_address(self) -> None:
        with self.assertRaises(AirbyteError) as raised:
            check_api_origin("http://user:hunter2@8.8.8.8:8001")
        self.assertNotIn("hunter2", str(raised.exception))
        self.assertNotIn("8.8.8.8", str(raised.exception))


def first(answer: Any) -> dict[str, Any]:
    entry: dict[str, Any] = answer["data"][0]
    return entry


def whole_answer(answer: Any) -> dict[str, Any]:
    whole: dict[str, Any] = answer
    return whole


def in_configuration(answer: Any) -> dict[str, Any]:
    configuration: dict[str, Any] = answer["data"][0]["configuration"]
    return configuration


def started(api: Api, stand_in: StandIn) -> None:
    ids = ensure_everything(api)
    api.start_sync(ids["lab_data"])


def read_everything(api: Api) -> None:
    ensure_everything(api)


def read_jobs(api: Api) -> None:
    api.running_jobs(api.connection_ids.get("lab_data", "connection1"))


def read_branch(api: Api) -> None:
    api.destination_branch_now()


def read_status(api: Api) -> None:
    api.job_status(1)


def start_one(api: Api) -> None:
    api.start_sync("connection1")


def nothing(api: Api, stand_in: StandIn) -> None:
    return None


def built(api: Api, stand_in: StandIn) -> None:
    ensure_everything(api)


DEFINITIONS = "workspaces/workspace1/definitions"
# (name, label of the call, the object of the answer that holds the field, the field, the setup before the call, the call)
FIELD_CASES = (
    ("token", "POST applications/token", whole_answer, "access_token", nothing, read_jobs),
    ("workspace", "GET workspaces", first, "workspaceId", nothing, read_everything),
    ("source definition id", f"GET {DEFINITIONS}/sources", first, "id", built, read_everything),
    ("source definition repository", f"GET {DEFINITIONS}/sources", first, "dockerRepository", built, read_everything),
    ("source definition tag", f"GET {DEFINITIONS}/sources", first, "dockerImageTag", built, read_everything),
    ("destination definition id", f"GET {DEFINITIONS}/destinations", first, "id", built, read_everything),
    ("destination definition repository", f"GET {DEFINITIONS}/destinations", first, "dockerRepository", built, read_everything),
    ("destination definition tag", f"GET {DEFINITIONS}/destinations", first, "dockerImageTag", built, read_everything),
    ("source id", "GET sources", first, "sourceId", built, read_everything),
    ("source definition of a source", "GET sources", first, "definitionId", built, read_everything),
    ("source configuration", "GET sources", first, "configuration", built, read_everything),
    ("destination id", "GET destinations", first, "destinationId", built, read_everything),
    ("destination definition of the destination", "GET destinations", first, "definitionId", built, read_everything),
    ("destination configuration", "GET destinations", first, "configuration", built, read_everything),
    ("destination branch", "GET destinations", in_configuration, "main_branch_name", built, read_branch),
    ("created source", "POST sources", whole_answer, "sourceId", nothing, read_everything),
    ("created destination", "POST destinations", whole_answer, "destinationId", nothing, read_everything),
    ("connection id", "GET connections", first, "connectionId", built, read_everything),
    ("connection source", "GET connections", first, "sourceId", built, read_everything),
    ("connection destination", "GET connections", first, "destinationId", built, read_everything),
    ("created connection", "POST connections", whole_answer, "connectionId", nothing, read_everything),
    ("job id in the list", "GET jobs", first, "jobId", started, read_jobs),
    ("job status in the list", "GET jobs", first, "status", started, read_jobs),
    ("job connection in the list", "GET jobs", first, "connectionId", started, read_jobs),
    ("job type in the list", "GET jobs", first, "jobType", started, read_jobs),
    ("job id of a started sync", "POST jobs", whole_answer, "jobId", built, start_one),
    ("job id of a read job", "GET jobs/1", whole_answer, "jobId", started, read_status),
    ("job status of a read job", "GET jobs/1", whole_answer, "status", started, read_status),
)


def run_corrupted(case: tuple[Any, ...], corrupt: Any) -> tuple[Api, StandIn, AirbyteError]:
    _, label, locate, field, setup, call = case
    api, stand_in = prepared()
    setup(api, stand_in)
    stand_in.log.clear()
    stand_in.mutations[label] = lambda answer: corrupt(locate(answer), field)
    try:
        call(api)
    except AirbyteError as error:
        return api, stand_in, error
    raise AssertionError(f"{case[0]}: the corrupted answer was accepted")


class FieldTest(unittest.TestCase):
    """Every field of every answer that the load reads is checked: a missing or wrong field refuses the call, and no message holds the marker."""

    def test_a_missing_field_and_a_field_of_the_wrong_type_each_refuse_the_call_and_no_message_holds_the_marker(self) -> None:
        corruptions = {
            "missing": lambda entry, field: entry.pop(field),
            "an object": lambda entry, field: entry.__setitem__(field, {MARKER: MARKER}),
            "a list": lambda entry, field: entry.__setitem__(field, [MARKER]),
            "a boolean": lambda entry, field: entry.__setitem__(field, True),
            "a number": lambda entry, field: entry.__setitem__(field, 17),
            "an empty text": lambda entry, field: entry.__setitem__(field, ""),
            "null": lambda entry, field: entry.__setitem__(field, None),
        }
        for case in FIELD_CASES:
            for how, corrupt in corruptions.items():
                if case[3] in ("jobId",) and how == "a number":
                    continue
                if case[3] == "configuration" and how == "an object":
                    continue
                with self.subTest(f"{case[0]}: {how}"):
                    _, _, error = run_corrupted(case, corrupt)
                    self.assertNotIn(MARKER, str(error))
                    self.assertIn(case[3] if case[3] != "access_token" else "token", str(error))

    def test_a_status_outside_the_closed_set_refuses_the_call_and_the_message_does_not_hold_it(self) -> None:
        for case in FIELD_CASES:
            if case[3] != "status":
                continue
            with self.subTest(case[0]):
                _, _, error = run_corrupted(case, lambda entry, field: entry.__setitem__(field, MARKER))
                self.assertNotIn(MARKER, str(error))

    def test_the_closed_set_is_the_documented_one_and_active_and_terminal_make_it_whole(self) -> None:
        self.assertEqual({"pending", "queued", "running", "incomplete", "failed", "succeeded", "cancelled"}, set(JOB_STATUSES))
        self.assertEqual(set(JOB_STATUSES), set(ACTIVE_STATUSES) | set(TERMINAL_STATUSES))
        self.assertEqual(set(), set(ACTIVE_STATUSES) & set(TERMINAL_STATUSES))

    def test_each_documented_status_is_read_and_only_the_active_ones_count_as_running(self) -> None:
        for status in JOB_STATUSES:
            with self.subTest(status):
                api, stand_in = prepared()
                ids = ensure_everything(api)
                job = api.start_sync(ids["lab_data"])
                stand_in.jobs[job]["status"] = status
                self.assertEqual(status, api.job_status(job))
                self.assertEqual([job] if status in ACTIVE_STATUSES else [], api.running_jobs(ids["lab_data"]))

    def test_a_job_entry_that_is_not_an_object_or_of_another_connection_or_type_refuses_the_list(self) -> None:
        for name, entry in (
            ("a number", 5),
            ("a text", MARKER),
            ("a list", [MARKER]),
            ("another connection", {"jobId": 9, "connectionId": MARKER, "jobType": "sync", "status": "running"}),
            ("another type", {"jobId": 9, "connectionId": "connection1", "jobType": "reset", "status": "running"}),
        ):
            with self.subTest(name):
                api, stand_in = prepared()
                stand_in.mutations["GET jobs"] = lambda answer, entry=entry: answer["data"].append(entry)
                with self.assertRaises(AirbyteError) as raised:
                    api.running_jobs("connection1")
                self.assertNotIn(MARKER, str(raised.exception))

    def test_a_job_that_is_not_the_one_that_was_asked_for_refuses_the_read(self) -> None:
        api, stand_in = prepared()
        ids = ensure_everything(api)
        job = api.start_sync(ids["lab_data"])
        stand_in.mutations[f"GET jobs/{job}"] = lambda answer: answer.__setitem__("jobId", job + 1)
        with self.assertRaises(AirbyteError):
            api.job_status(job)

    def test_a_list_page_with_an_entry_that_is_not_an_object_or_a_next_that_is_not_text_refuses_the_list_of_every_kind(self) -> None:
        labels = ("GET workspaces", f"GET {DEFINITIONS}/sources", f"GET {DEFINITIONS}/destinations", "GET sources", "GET destinations", "GET connections")
        for label in labels:
            for name, corrupt in (
                ("an entry that is a text", lambda answer: answer["data"].append(MARKER)),
                ("a next that is a number", lambda answer: answer.__setitem__("next", 7)),
                ("data that is an object", lambda answer: answer.__setitem__("data", {MARKER: MARKER})),
                ("data that is missing", lambda answer: answer.pop("data")),
            ):
                with self.subTest(f"{label}: {name}"):
                    api, stand_in = prepared()
                    if label != "GET workspaces":
                        ensure_everything(api)
                    stand_in.mutations[label] = corrupt
                    with self.assertRaises(AirbyteError) as raised:
                        ensure_everything(api)
                    self.assertNotIn(MARKER, str(raised.exception))


class DeadlineTest(unittest.TestCase):
    """The deadline of an exchange is one elapsed time, whatever the server does, and the call that has no time left makes no request."""

    def test_a_call_with_no_time_left_makes_no_request(self) -> None:
        api, stand_in = prepared()
        with self.assertRaises(AirbyteError):
            api.call("GET", "/jobs/1", timeout=0.0)
        self.assertEqual([], stand_in.log)

    def test_every_request_of_a_call_is_given_what_is_left_of_the_smaller_of_the_two_limits(self) -> None:
        api, stand_in = prepared()
        api.timeout_seconds = 60.0
        started_at = time.monotonic()
        api.call("GET", "/workspaces", timeout=5.0)
        self.assertEqual(2, len(stand_in.budgets))
        self.assertTrue(all(0 < budget <= 5.0 for budget in stand_in.budgets), stand_in.budgets)
        self.assertEqual(sorted(stand_in.budgets, reverse=True), stand_in.budgets)
        self.assertLess(time.monotonic() - started_at, 1.0)

    def test_the_clients_own_limit_bounds_a_call_that_is_given_more_time(self) -> None:
        api, stand_in = prepared()
        api.timeout_seconds = 3.0
        api.call("GET", "/workspaces", timeout=500.0)
        self.assertTrue(all(0 < budget <= 3.0 for budget in stand_in.budgets), stand_in.budgets)

    def test_an_expired_token_renewal_and_repeat_stay_inside_one_deadline(self) -> None:
        api, stand_in = prepared()
        api.call("GET", "/workspaces", timeout=4.0)
        stand_in.expire_next_token = True
        stand_in.budgets.clear()
        api.call("GET", "/workspaces", timeout=4.0)
        self.assertEqual(3, len(stand_in.budgets))
        self.assertTrue(all(0 < budget <= 4.0 for budget in stand_in.budgets), stand_in.budgets)

    def serve(self, behaviour: str) -> tuple[int, threading.Thread, threading.Event, socket.socket]:
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listener.settimeout(0.2)
        stop = threading.Event()

        def run() -> None:
            clients: list[socket.socket] = []
            try:
                while not stop.is_set():
                    try:
                        client, _ = listener.accept()
                    except OSError:
                        continue
                    clients.append(client)
                    client.settimeout(0.1)
                    with contextlib.suppress(OSError):
                        client.recv(65536)
                    if behaviour == "silent":
                        continue
                    for byte in b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n" + b"x" * 1000:
                        if stop.is_set():
                            break
                        with contextlib.suppress(OSError):
                            client.sendall(bytes([byte]))
                        time.sleep(0.05)
            finally:
                for client in clients:
                    client.close()

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        return listener.getsockname()[1], thread, stop, listener

    def check(self, behaviour: str) -> None:
        port, thread, stop, listener = self.serve(behaviour)
        try:
            api, _ = prepared()
            api.host, api.port = "127.0.0.1", port
            started_at = time.monotonic()
            with self.assertRaises(TimeoutError):
                HttpAirbyteApi.send(api, "GET", "/api/public/v1/workspaces", None, {}, 0.5)
            elapsed = time.monotonic() - started_at
            self.assertLess(elapsed, 1.5, f"the exchange ended after {elapsed:.2f} s")
            self.assertGreaterEqual(elapsed, 0.4)
        finally:
            stop.set()
            thread.join(timeout=5)
            listener.close()
            self.assertFalse(thread.is_alive())

    def test_a_server_that_never_answers_ends_the_exchange_at_the_deadline(self) -> None:
        self.check("silent")

    def test_a_server_that_trickles_one_byte_at_a_time_ends_the_exchange_at_the_deadline(self) -> None:
        self.check("trickle")

    def test_the_deadline_is_one_error_that_names_the_call_through_the_whole_client(self) -> None:
        port, thread, stop, listener = self.serve("trickle")
        try:
            api = HttpAirbyteApi(ORIGIN, "client", "client_value", ACCESS_ID, ACCESS_VALUE)
            api.host, api.port = "127.0.0.1", port
            started_at = time.monotonic()
            with self.assertRaises(AirbyteError) as raised:
                api.exchange("POST", "/applications/token", {}, 0.5, authorized=False)
            self.assertLess(time.monotonic() - started_at, 1.5)
            self.assertIn("POST applications/token", str(raised.exception))
            self.assertIn("deadline", str(raised.exception))
        finally:
            stop.set()
            thread.join(timeout=5)
            listener.close()


class ReplyServer:
    """A local server that sends one reply of a kind: ``close`` (``Connection: close``, no length), ``length`` (``Content-Length``) or ``chunked``.

    ``gap`` is the seconds between two bytes of the body: 0 sends the body at once, in blocks, and any other value trickles it one byte at a time.
    """

    HEADS = {
        "close": b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n",
        "chunked": b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n",
        "chunked_extension": b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
    }

    def __init__(self, kind: str, size: int, gap: float, stall_after: int | None = None) -> None:
        self.kind, self.size, self.gap, self.stall_after = kind, size, gap, stall_after
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(1)
        self.listener.settimeout(0.2)
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    @property
    def port(self) -> int:
        return int(self.listener.getsockname()[1])

    def pieces(self) -> list[bytes]:
        if self.kind == "chunked_extension":
            return [b"1;", *([b"e"] * self.size), b"\r\nx\r\n0\r\n\r\n"]
        body = b"x" * self.size
        blocks = [body[start : start + 1 if self.gap else start + 40_000] for start in range(0, self.size, 1 if self.gap else 40_000)]
        if self.kind == "chunked":
            return [f"{len(block):x}\r\n".encode() + block + b"\r\n" for block in blocks] + [b"0\r\n\r\n"]
        return blocks

    def run(self) -> None:
        clients: list[socket.socket] = []
        head = self.HEADS.get(self.kind, f"HTTP/1.1 200 OK\r\nContent-Length: {self.size}\r\n\r\n".encode())
        try:
            while not self.stop.is_set():
                try:
                    client, _ = self.listener.accept()
                except OSError:
                    continue
                clients.append(client)
                client.settimeout(0.5)
                with contextlib.suppress(OSError):
                    client.recv(65536)
                try:
                    client.sendall(head)
                    for number, piece in enumerate(self.pieces()):
                        if self.stop.is_set():
                            break
                        if self.stall_after is not None and number >= self.stall_after:
                            self.stop.wait(30)
                            break
                        client.sendall(piece)
                        if self.gap:
                            time.sleep(self.gap)
                except OSError:
                    continue
                if self.kind in ("close", "chunked_extension"):
                    client.close()
        finally:
            for client in clients:
                client.close()

    def close(self) -> None:
        self.stop.set()
        self.thread.join(timeout=5)
        self.listener.close()


class BodyDeadlineTest(unittest.TestCase):
    """A body is read in bounded reads and the clock is read after each, so the exchange ends at its deadline whatever the reply's framing says.

    Measured on this machine against a 0.5 s budget with a body of 200 bytes at one byte every 0.02 s, five runs for each kind: ``Connection: close`` ended at
    0.501 to 0.503 s, ``Content-Length`` at 0.501 to 0.503 s and chunked at 0.501 to 0.502 s. The margin that the tests allow is 0.25 s.
    """

    BUDGET = 0.5
    MARGIN = 0.25

    def api(self, server: ReplyServer) -> HttpAirbyteApi:
        api = HttpAirbyteApi(ORIGIN, "client", "client_value", ACCESS_ID, ACCESS_VALUE)
        api.host, api.port = "127.0.0.1", server.port
        return api

    def test_a_body_that_trickles_ends_within_the_budget_and_a_small_margin_for_each_kind_of_reply(self) -> None:
        for kind in ("close", "length", "chunked"):
            with self.subTest(kind):
                server = ReplyServer(kind, 200, 0.02)
                try:
                    started_at = time.monotonic()
                    with self.assertRaises(TimeoutError):
                        self.api(server).send("GET", "/api/public/v1/workspaces", None, {}, self.BUDGET)
                    elapsed = time.monotonic() - started_at
                finally:
                    server.close()
                self.assertGreaterEqual(elapsed, self.BUDGET - 0.05)
                self.assertLess(elapsed, self.BUDGET + self.MARGIN, f"{kind}: the exchange ended after {elapsed:.3f} s")

    def test_a_chunk_extension_that_trickles_under_connection_close_ends_at_the_deadline_although_one_read_is_waiting_for_its_line(self) -> None:
        server = ReplyServer("chunked_extension", 200, 0.02)
        try:
            started_at = time.monotonic()
            with self.assertRaises(TimeoutError):
                self.api(server).send("GET", "/api/public/v1/workspaces", None, {}, self.BUDGET)
            elapsed = time.monotonic() - started_at
        finally:
            server.close()
        self.assertGreaterEqual(elapsed, self.BUDGET - 0.05)
        self.assertLess(elapsed, self.BUDGET + self.MARGIN, f"the exchange ended after {elapsed:.3f} s")

    def test_control_a_chunked_close_reply_with_a_short_extension_arrives_whole(self) -> None:
        server = ReplyServer("chunked_extension", 5, 0)
        try:
            self.assertEqual((200, b"x"), self.api(server).send("GET", "/api/public/v1/workspaces", None, {}, 5.0))
        finally:
            server.close()

    def test_a_body_that_stalls_part_way_ends_at_the_deadline_and_not_a_whole_socket_timeout_later_for_each_kind_of_reply(self) -> None:
        for kind in ("close", "length", "chunked"):
            with self.subTest(kind):
                server = ReplyServer(kind, 200, 0.02, stall_after=20)
                try:
                    started_at = time.monotonic()
                    with self.assertRaises(TimeoutError):
                        self.api(server).send("GET", "/api/public/v1/workspaces", None, {}, self.BUDGET)
                    elapsed = time.monotonic() - started_at
                finally:
                    server.close()
                self.assertGreaterEqual(elapsed, self.BUDGET - 0.05)
                self.assertLess(elapsed, self.BUDGET + self.MARGIN, f"{kind}: the exchange ended after {elapsed:.3f} s")

    def test_a_body_that_is_larger_than_one_read_arrives_whole_for_each_kind_of_reply(self) -> None:
        for kind in ("close", "length", "chunked"):
            for size in (0, 5, 65_536, 300_000):
                with self.subTest(f"{kind} {size}"):
                    server = ReplyServer(kind, size, 0)
                    try:
                        status, body = self.api(server).send("GET", "/api/public/v1/workspaces", None, {}, 5.0)
                    finally:
                        server.close()
                    self.assertEqual((200, b"x" * size), (status, body))

    def test_the_whole_client_names_the_call_and_the_deadline_when_a_close_reply_trickles(self) -> None:
        server = ReplyServer("close", 200, 0.02)
        try:
            started_at = time.monotonic()
            with self.assertRaises(AirbyteError) as raised:
                self.api(server).exchange("POST", "/applications/token", {}, self.BUDGET, authorized=False)
            elapsed = time.monotonic() - started_at
        finally:
            server.close()
        self.assertLess(elapsed, self.BUDGET + self.MARGIN)
        self.assertIn("POST applications/token", str(raised.exception))
        self.assertIn("deadline", str(raised.exception))

    def late_clock(self, trigger: str) -> tuple[Any, Any]:
        """A clock that jumps 10 s ahead once ``trigger`` has happened: ``headers`` (``getresponse`` returned) or ``end`` (the read that found the end of the
        body returned). Each of the two clock checks of the body loop is then the only thing that can end the exchange."""
        real = time.monotonic
        late = [False]
        original_read1, original_get = http.client.HTTPResponse.read1, http.client.HTTPConnection.getresponse

        def read1(response: http.client.HTTPResponse, *arguments: Any) -> bytes:
            block = original_read1(response, *arguments)
            if trigger == "end" and not block:
                late[0] = True
            return block

        def getresponse(connection: http.client.HTTPConnection) -> http.client.HTTPResponse:
            reply = original_get(connection)
            if trigger == "headers":
                late[0] = True
            return reply

        def clock() -> float:
            return real() + (10.0 if late[0] else 0.0)

        return clock, (read1, getresponse)

    def test_a_reply_whose_headers_took_all_the_time_is_a_timeout_before_the_first_read_of_its_body(self) -> None:
        clock, (read1, getresponse) = self.late_clock("headers")
        server = ReplyServer("length", 100, 0)
        try:
            with (
                mock.patch("scripts.ingestion.airbyte_api.time.monotonic", clock),
                mock.patch.object(http.client.HTTPConnection, "getresponse", getresponse),
                self.assertRaises(TimeoutError),
            ):
                self.api(server).send("GET", "/api/public/v1/workspaces", None, {}, 5.0)
        finally:
            server.close()

    def test_a_body_whose_last_read_came_back_after_the_deadline_is_a_timeout_although_the_whole_body_arrived(self) -> None:
        clock, (read1, getresponse) = self.late_clock("end")
        server = ReplyServer("length", 100, 0)
        try:
            with (
                mock.patch("scripts.ingestion.airbyte_api.time.monotonic", clock),
                mock.patch.object(http.client.HTTPResponse, "read1", read1),
                self.assertRaises(TimeoutError),
            ):
                self.api(server).send("GET", "/api/public/v1/workspaces", None, {}, 5.0)
        finally:
            server.close()

    def test_control_the_same_reply_with_a_clock_that_never_jumps_is_returned(self) -> None:
        clock, (read1, getresponse) = self.late_clock("never")
        server = ReplyServer("length", 100, 0)
        try:
            with mock.patch("scripts.ingestion.airbyte_api.time.monotonic", clock), mock.patch.object(http.client.HTTPResponse, "read1", read1):
                self.assertEqual((200, b"x" * 100), self.api(server).send("GET", "/api/public/v1/workspaces", None, {}, 5.0))
        finally:
            server.close()

    def test_a_body_that_is_cut_off_at_the_cap_is_returned_at_the_cap(self) -> None:
        server = ReplyServer("length", 300_000, 0)
        try:
            with mock.patch.object(airbyte_api, "BODY_LIMIT_BYTES", 100_000):
                status, body = self.api(server).send("GET", "/api/public/v1/workspaces", None, {}, 5.0)
        finally:
            server.close()
        self.assertEqual((200, 100_000), (status, len(body)))


class CheckCommandTest(unittest.TestCase):
    def run_main(self, address: str) -> tuple[int, str]:
        error = io.StringIO()
        with contextlib.redirect_stderr(error):
            code = main(["check", "--address", address])
        return code, error.getvalue()

    def test_the_command_accepts_what_the_client_accepts_and_refuses_what_it_refuses_without_naming_the_address(self) -> None:
        for address in AddressTest.ACCEPTED:
            with self.subTest(address):
                self.assertEqual((0, ""), self.run_main(address))
        for name, address in AddressTest.REFUSED:
            with self.subTest(name):
                code, message = self.run_main(address)
                self.assertEqual(1, code)
                self.assertNotIn(address or "\0", message)


class RedirectTest(unittest.TestCase):
    """A redirect would send the credentials, or the bearer token, to the place it names, so it is refused and followed nowhere."""

    def test_a_redirect_of_any_call_is_refused_and_no_second_request_is_made(self) -> None:
        for status in (301, 302, 303, 307, 308):
            with self.subTest(status):
                api, stand_in = prepared()
                api.running_jobs("connection1")
                stand_in.failures["GET jobs"] = (status, b"")
                stand_in.log.clear()
                with self.assertRaises(AirbyteError) as raised:
                    api.running_jobs("connection1")
                self.assertIn("redirect", str(raised.exception))
                self.assertEqual(["GET jobs"], stand_in.log)

    def test_a_redirect_of_the_token_call_is_refused_before_any_credential_is_sent_again(self) -> None:
        api, stand_in = prepared()
        stand_in.failures["POST applications/token"] = (307, b"")
        with self.assertRaises(AirbyteError) as raised:
            api.running_jobs("connection1")
        self.assertIn("redirect", str(raised.exception))
        self.assertEqual(["POST applications/token"], stand_in.log)

    def test_every_request_goes_to_the_one_origin_of_the_client(self) -> None:
        api, _ = prepared()
        self.assertEqual(("10.42.0.1", 8001), (api.host, api.port))


if __name__ == "__main__":
    unittest.main()
