"""The Airbyte calls of the record connections, against a stand-in of Airbyte's public API.

The stand-in holds only what this reading of the API needs: the token call, the workspace, the custom definitions, the sources, destinations and connections
with their lists, creates and updates, and the job calls. It is the test's statement of that reading, which is the one the raw connection's module makes of the
same API, and the real API is proven on the host: a case here shows that the code does what it means to do, not that Airbyte answers as the stand-in does.

A second call of ``ensure_*`` makes no create and no update, a drifted object is repaired by an update, a field that Airbyte adds to an answer is not drift, a
secret is sent only in the body of a destination request and never shown in an error, and a failed or late call becomes one error that holds no body.
"""

from __future__ import annotations

import json
import unittest
from typing import Any
from urllib.parse import parse_qs, urlsplit

from scripts.ingestion import record_definitions as definitions
from scripts.ingestion.airbyte_api import AirbyteError, HttpAirbyteApi, check_api_origin
from scripts.ingestion.record_definitions import DESTINATION, SOURCE_FILE, SOURCE_S3, TRACK_LIST, Environment

RUN = "run_20261122_ab12cd34"
ENVIRONMENT = Environment("example-bucket", "us-west-2", "warehouse", "http://nessie.example:19120/api/v2")
ACCESS_ID = "AKIAEXAMPLEEXAMPLE0"
ACCESS_VALUE = "wJalrXUtnFEMIexample0000000000000000000000"
MASK = "**********"
SECRET_FIELDS = ("access_key_id", "secret_access_key")
PREFIX = "/api/public/v1"
ORIGIN = "http://10.42.0.1:8001"
WRITE_METHODS = ("POST", "PUT", "PATCH", "DELETE")
PLATFORM_FIELD = {"platform_added": "a default"}


class StandIn:
    """Airbyte's public API in memory. ``log`` lists each call as its method and path; ``bodies`` keeps each request body."""

    def __init__(self) -> None:
        self.log: list[str] = []
        self.bodies: list[tuple[str, dict[str, Any]]] = []
        self.definitions: dict[str, dict[str, dict[str, Any]]] = {"source": {}, "destination": {}}
        self.sources: dict[str, dict[str, Any]] = {}
        self.destinations: dict[str, dict[str, Any]] = {}
        self.connections: dict[str, dict[str, Any]] = {}
        self.jobs: dict[int, dict[str, Any]] = {}
        self.tokens = 0
        self.expire_next_token = False
        self.failures: dict[str, tuple[int, bytes] | type[Exception]] = {}
        self.counters: dict[str, int] = {}
        self.workspaces = [{"workspaceId": "workspace1"}]
        self.page_size: int | None = None

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
            return 200, json.dumps({"access_token": f"token{self.tokens}"}).encode()
        if headers.get("Authorization") != f"Bearer token{self.tokens}" or self.expire_next_token:
            self.expire_next_token = False
            return 401, b'{"message":"expired"}'
        answer = self.route(method, path.strip("/").split("/"), query, body)
        return (200, json.dumps(answer).encode()) if answer is not None else (404, b"{}")

    def route(self, method: str, parts: list[str], query: dict[str, list[str]], body: dict[str, Any]) -> Any:
        if parts == ["workspaces"] and method == "GET":
            return {"data": self.workspaces}
        if parts[0] == "workspaces" and len(parts) >= 4 and parts[2] == "definitions":
            return self.definition(method, parts[3].removesuffix("s"), parts[4:], body)
        if parts[0] in ("sources", "destinations"):
            return self.actor(method, parts[0].removesuffix("s"), parts[1:], body, query)
        if parts[0] == "connections":
            return self.connection(method, parts[1:], body)
        if parts[0] == "jobs":
            return self.job(method, parts[1:], query, body)
        return None

    def definition(self, method: str, kind: str, rest: list[str], body: dict[str, Any]) -> Any:
        store = self.definitions[kind]
        if method == "GET":
            return {"data": list(store.values())}
        if method == "POST":
            entry = {**body, "id": self.ident(f"{kind}_definition")}
            store[entry["id"]] = entry
            return entry
        store[rest[0]].update(body)
        return store[rest[0]]

    def actor(self, method: str, kind: str, rest: list[str], body: dict[str, Any], query: dict[str, list[str]]) -> Any:
        store = self.sources if kind == "source" else self.destinations
        key = f"{kind}Id"
        if method == "GET":
            data = [self.shown(kind, value) for value in store.values()]
            if self.page_size is None:
                return {"data": data}
            offset = int(query.get("offset", ["0"])[0])
            return {"data": data[offset : offset + self.page_size], "next": "more" if offset + self.page_size < len(data) else ""}
        if method == "POST":
            entry = {**body, key: self.ident(kind)}
            entry["configuration"] = {**body["configuration"], **PLATFORM_FIELD}
            store[entry[key]] = entry
            return entry
        store[rest[0]]["configuration"] = {**body["configuration"], **PLATFORM_FIELD}
        return store[rest[0]]

    def shown(self, kind: str, entry: dict[str, Any]) -> dict[str, Any]:
        if kind == "source":
            return entry
        return {**entry, "configuration": {name: MASK if name in SECRET_FIELDS else value for name, value in entry["configuration"].items()}}

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
            return self.jobs[number]
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

    def send(self, method: str, target: str, payload: bytes | None, headers: dict[str, str]) -> tuple[int, bytes]:
        return self.stand_in.send(method, target, payload, headers)


def prepared() -> tuple[Api, StandIn]:
    stand_in = StandIn()
    return Api(stand_in), stand_in


def ensure_everything(api: Api, run: str = RUN) -> dict[str, str]:
    api.ensure_destination(definitions.destination_configuration(ENVIRONMENT, definitions.branch_name(run)))
    ids = {}
    for table in definitions.TABLES:
        api.ensure_source(table.name, definitions.source_configuration(table, run, ENVIRONMENT, "ab" * 32))
        ids[table.name] = api.ensure_connection(table.name, definitions.connection_configuration(table))
    return ids


def writes(stand_in: StandIn) -> list[str]:
    return [label for label in stand_in.log if label.split(" ")[0] in WRITE_METHODS and label not in ("POST applications/token", "POST jobs")]


class EnsureTest(unittest.TestCase):
    def test_the_first_call_creates_each_definition_the_destination_each_source_and_each_connection(self) -> None:
        api, stand_in = prepared()
        ids = ensure_everything(api)
        self.assertEqual(["clinical_data", "lab_data", "track_list"], sorted(ids))
        self.assertEqual((3, 1, 3), (len(stand_in.sources), len(stand_in.destinations), len(stand_in.connections)))
        images = {f"{entry['dockerRepository']}:{entry['dockerImageTag']}" for kind in stand_in.definitions.values() for entry in kind.values()}
        self.assertEqual({SOURCE_S3.image, SOURCE_FILE.image, DESTINATION.image}, images)

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

    def test_a_definition_at_another_version_is_moved_to_the_pinned_one(self) -> None:
        api, stand_in = prepared()
        ensure_everything(api)
        for kind in stand_in.definitions.values():
            for entry in kind.values():
                entry["dockerImageTag"] = "0.0.1"
        stand_in.log.clear()
        ensure_everything(api)
        self.assertEqual(3, len([label for label in writes(stand_in) if label.startswith("PUT")]))
        tags = {entry["dockerImageTag"] for kind in stand_in.definitions.values() for entry in kind.values()}
        self.assertEqual({SOURCE_S3.tag, SOURCE_FILE.tag, DESTINATION.tag}, tags)

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
    def test_the_destination_gets_the_access_key_in_its_request_and_the_other_requests_get_none(self) -> None:
        api, stand_in = prepared()
        ensure_everything(api)
        for label, body in stand_in.bodies:
            self.assertEqual(label == "POST destinations", ACCESS_VALUE in json.dumps(body), label)
        self.assertEqual(ACCESS_VALUE, stand_in.destinations["destination1"]["configuration"]["secret_access_key"])

    def test_after_a_new_run_and_a_repair_only_the_destination_requests_hold_the_access_key(self) -> None:
        api, stand_in = prepared()
        ids = ensure_everything(api)
        stand_in.connections[ids["lab_data"]]["status"] = "inactive"
        stand_in.sources["source1"]["configuration"]["bucket"] = "other"
        stand_in.bodies.clear()
        ensure_everything(api, "run_next")
        self.assertIn("PATCH destinations/destination1", [label for label, _ in stand_in.bodies])
        for label, body in stand_in.bodies:
            self.assertEqual(label.startswith("PATCH destinations"), ACCESS_VALUE in json.dumps(body), label)

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
        self.assertEqual({"clientId": "client", "clientSecret": "client_value"}, stand_in.bodies[0][1])

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
        stand_in.page_size = 1
        stand_in.log.clear()
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
    ACCEPTED = ("http://10.42.0.1:8001", "http://172.16.4.1:8001", "http://192.168.1.1:80", "http://169.254.10.2:8001", "http://10.42.0.1:8001/")

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
