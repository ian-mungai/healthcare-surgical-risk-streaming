"""The Airbyte calls of the record connections, against a stand-in of Airbyte's configuration API.

The stand-in holds only what this reading of the API needs: the token call, the definitions, sources, destinations and connections with their lists, creates
and updates, the schema discovery, and the sync, job and cancel calls. It is the test's statement of that reading, and the real API is proven on the host: a
case here shows that the code does what it means to do, not that Airbyte answers as the stand-in does.

A second call of ``ensure_*`` makes no create and no update, a drifted object is repaired by an update, a secret is sent only in a request body and never shown
in an error, and a failed or late call becomes one error that holds no body.
"""

from __future__ import annotations

import json
import unittest
from typing import Any

from scripts.ingestion import record_definitions as definitions
from scripts.ingestion.airbyte_api import AirbyteError, HttpAirbyteApi
from scripts.ingestion.record_definitions import CSV_TABLES, DESTINATION, SOURCE_S3, TRACK_LIST, Environment

RUN = "run_20261122_ab12cd34"
ENVIRONMENT = Environment("example-bucket", "us-west-2", "warehouse", "http://nessie.example:19120/api/v2")
ACCESS_ID = "AKIAEXAMPLEEXAMPLE0"
ACCESS_VALUE = "wJalrXUtnFEMIexample0000000000000000000000"
MASK = "**********"
SECRET_FIELDS = ("access_key_id", "secret_access_key")


class StandIn:
    """Airbyte's configuration API in memory. ``log`` lists each call as its path; ``bodies`` keeps each request body for the cases that read one."""

    def __init__(self) -> None:
        self.log: list[str] = []
        self.bodies: list[tuple[str, dict[str, Any]]] = []
        self.definitions: dict[str, dict[str, Any]] = {}
        self.sources: dict[str, dict[str, Any]] = {}
        self.destinations: dict[str, dict[str, Any]] = {}
        self.connections: dict[str, dict[str, Any]] = {}
        self.jobs: dict[int, dict[str, Any]] = {}
        self.tokens = 0
        self.expire_next_token = False
        self.failures: dict[str, tuple[int, bytes] | type[Exception]] = {}
        self.counters: dict[str, int] = {}

    def ident(self, prefix: str) -> str:
        self.counters[prefix] = self.counters.get(prefix, 0) + 1
        return f"{prefix}{self.counters[prefix]}"

    def send(self, method: str, target: str, payload: bytes | None, headers: dict[str, str]) -> tuple[int, bytes]:
        path = target.removeprefix("/api/v1/")
        body = json.loads(payload) if payload else {}
        self.log.append(path)
        self.bodies.append((path, body))
        failure = self.failures.get(path)
        if isinstance(failure, tuple):
            return failure
        if failure is not None:
            raise failure()
        if path == "applications/token":
            self.tokens += 1
            return 200, json.dumps({"access_token": f"token{self.tokens}"}).encode()
        if headers.get("Authorization") != f"Bearer token{self.tokens}" or self.expire_next_token:
            self.expire_next_token = False
            return 401, b'{"message":"expired"}'
        handler = getattr(self, path.replace("/", "_"), None)
        if handler is None:
            return 404, b"{}"
        return 200, json.dumps(handler(body)).encode()

    def workspaces_list(self, body: dict[str, Any]) -> dict[str, Any]:
        return {"workspaces": [{"workspaceId": "workspace1"}]}

    def definitions_list(self, kind: str) -> dict[str, Any]:
        return {f"{kind}Definitions": [value for value in self.definitions.values() if value["kind"] == kind]}

    def definition_create(self, kind: str, body: dict[str, Any]) -> dict[str, Any]:
        entry = {**body[f"{kind}Definition"], "kind": kind, f"{kind}DefinitionId": self.ident(f"{kind}_definition")}
        self.definitions[entry[f"{kind}DefinitionId"]] = entry
        return entry

    def definition_update(self, kind: str, body: dict[str, Any]) -> dict[str, Any]:
        entry = self.definitions[body[f"{kind}DefinitionId"]]
        entry["dockerImageTag"] = body["dockerImageTag"]
        return entry

    def source_definitions_list_for_workspace(self, body: dict[str, Any]) -> dict[str, Any]:
        return self.definitions_list("source")

    def source_definitions_create_custom(self, body: dict[str, Any]) -> dict[str, Any]:
        return self.definition_create("source", body)

    def source_definitions_update(self, body: dict[str, Any]) -> dict[str, Any]:
        return self.definition_update("source", body)

    def destination_definitions_list_for_workspace(self, body: dict[str, Any]) -> dict[str, Any]:
        return self.definitions_list("destination")

    def destination_definitions_create_custom(self, body: dict[str, Any]) -> dict[str, Any]:
        return self.definition_create("destination", body)

    def destination_definitions_update(self, body: dict[str, Any]) -> dict[str, Any]:
        return self.definition_update("destination", body)

    def masked(self, configuration: dict[str, Any]) -> dict[str, Any]:
        return {name: MASK if name in SECRET_FIELDS else value for name, value in configuration.items()}

    def sources_list(self, body: dict[str, Any]) -> dict[str, Any]:
        return {"sources": list(self.sources.values())}

    def sources_create(self, body: dict[str, Any]) -> dict[str, Any]:
        entry = {**body, "sourceId": self.ident("source")}
        self.sources[entry["sourceId"]] = entry
        return entry

    def sources_update(self, body: dict[str, Any]) -> dict[str, Any]:
        self.sources[body["sourceId"]].update(body)
        return self.sources[body["sourceId"]]

    def sources_discover_schema(self, body: dict[str, Any]) -> dict[str, Any]:
        name = self.sources[body["sourceId"]]["name"].removeprefix("vitaldb_").removesuffix("_source")
        return {
            "catalog": {
                "streams": [{"stream": {"name": name, "supportedSyncModes": ["full_refresh"]}, "config": {"syncMode": "full_refresh", "selected": False}}]
            },
            "catalogId": "catalog1",
        }

    def destinations_list(self, body: dict[str, Any]) -> dict[str, Any]:
        return {"destinations": [{**value, "connectionConfiguration": self.masked(value["connectionConfiguration"])} for value in self.destinations.values()]}

    def destinations_create(self, body: dict[str, Any]) -> dict[str, Any]:
        entry = {**body, "destinationId": self.ident("destination")}
        self.destinations[entry["destinationId"]] = entry
        return entry

    def destinations_update(self, body: dict[str, Any]) -> dict[str, Any]:
        self.destinations[body["destinationId"]].update(body)
        return self.destinations[body["destinationId"]]

    def connections_list(self, body: dict[str, Any]) -> dict[str, Any]:
        return {"connections": list(self.connections.values())}

    def connections_create(self, body: dict[str, Any]) -> dict[str, Any]:
        entry = {**body, "connectionId": self.ident("connection")}
        self.connections[entry["connectionId"]] = entry
        return entry

    def connections_update(self, body: dict[str, Any]) -> dict[str, Any]:
        self.connections[body["connectionId"]].update(body)
        return self.connections[body["connectionId"]]

    def connections_sync(self, body: dict[str, Any]) -> dict[str, Any]:
        job = len(self.jobs) + 1
        self.jobs[job] = {"id": job, "configId": body["connectionId"], "status": "running"}
        return {"job": self.jobs[job]}

    def jobs_list(self, body: dict[str, Any]) -> dict[str, Any]:
        return {"jobs": [{"job": job} for job in self.jobs.values() if job["configId"] == body["configId"]]}

    def jobs_get(self, body: dict[str, Any]) -> dict[str, Any]:
        return {"job": self.jobs[body["id"]]}

    def jobs_cancel(self, body: dict[str, Any]) -> dict[str, Any]:
        self.jobs[body["id"]]["status"] = "cancelled"
        return {"job": self.jobs[body["id"]]}


class Api(HttpAirbyteApi):
    def __init__(self, stand_in: StandIn) -> None:
        super().__init__("http://airbyte.example:8001", "client", "client_secret", ACCESS_ID, ACCESS_VALUE)
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
        api.ensure_source(table.name, definitions.source_configuration(table, run, ENVIRONMENT))
        ids[table.name] = api.ensure_connection(table.name, definitions.connection_configuration(table))
    return ids


WRITES = ("create", "update", "discover")


def writes(stand_in: StandIn) -> list[str]:
    return [path for path in stand_in.log if any(word in path for word in WRITES)]


class EnsureTest(unittest.TestCase):
    def test_the_first_call_creates_each_definition_the_destination_each_source_and_each_connection(self) -> None:
        api, stand_in = prepared()
        ids = ensure_everything(api)
        self.assertEqual(3, len(stand_in.connections))
        self.assertEqual(["clinical_data", "lab_data", "track_list"], sorted(ids))
        images = {f"{entry['dockerRepository']}:{entry['dockerImageTag']}" for entry in stand_in.definitions.values()}
        self.assertEqual({SOURCE_S3.image, definitions.SOURCE_FILE.image, DESTINATION.image}, images)
        self.assertEqual(1, len(stand_in.destinations))
        self.assertEqual(3, len(stand_in.sources))

    def test_a_second_call_makes_no_create_no_update_and_no_discovery(self) -> None:
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
        updated = [path for path in stand_in.log if "update" in path]
        self.assertEqual(["destinations/update", "sources/update", "sources/update"], updated)
        self.assertEqual([], [path for path in stand_in.log if "create" in path or "discover" in path])
        self.assertEqual("load_run_next", api.destination_branch_now())

    def test_a_drifted_connection_is_repaired_and_a_drifted_source_is_repaired(self) -> None:
        api, stand_in = prepared()
        ids = ensure_everything(api)
        stand_in.connections[ids["lab_data"]]["scheduleType"] = "basic"
        stand_in.connections[ids["lab_data"]]["status"] = "inactive"
        stand_in.sources["source1"]["connectionConfiguration"]["streams"][0]["format"]["inference_type"] = "Primitive Types Only"
        stand_in.log.clear()
        ensure_everything(api)
        self.assertEqual(["connections/update"], [path for path in stand_in.log if "update" in path][-1:])
        self.assertEqual("manual", stand_in.connections[ids["lab_data"]]["scheduleType"])
        self.assertEqual("active", stand_in.connections[ids["lab_data"]]["status"])
        self.assertEqual("None", stand_in.sources["source1"]["connectionConfiguration"]["streams"][0]["format"]["inference_type"])

    def test_a_connection_with_another_sync_mode_is_repaired_to_the_full_refresh_that_overwrites(self) -> None:
        api, stand_in = prepared()
        ids = ensure_everything(api)
        config = stand_in.connections[ids["clinical_data"]]["syncCatalog"]["streams"][0]["config"]
        self.assertEqual(("full_refresh", "overwrite", True), (config["syncMode"], config["destinationSyncMode"], config["selected"]))
        config["destinationSyncMode"] = "append"
        stand_in.log.clear()
        ensure_everything(api)
        self.assertIn("connections/update", stand_in.log)
        self.assertEqual("overwrite", stand_in.connections[ids["clinical_data"]]["syncCatalog"]["streams"][0]["config"]["destinationSyncMode"])

    def test_a_definition_at_another_version_is_moved_to_the_pinned_one(self) -> None:
        api, stand_in = prepared()
        ensure_everything(api)
        for entry in stand_in.definitions.values():
            entry["dockerImageTag"] = "0.0.1"
        stand_in.log.clear()
        ensure_everything(api)
        self.assertEqual(3, len([path for path in stand_in.log if path.endswith("definitions/update")]))
        self.assertEqual({SOURCE_S3.tag, definitions.SOURCE_FILE.tag, DESTINATION.tag}, {entry["dockerImageTag"] for entry in stand_in.definitions.values()})

    def test_the_connections_hold_no_schedule_and_the_destination_branch_is_the_load_branch(self) -> None:
        api, stand_in = prepared()
        ensure_everything(api)
        self.assertEqual({"manual"}, {connection["scheduleType"] for connection in stand_in.connections.values()})
        self.assertEqual({"raw_records"}, {connection["namespaceFormat"] for connection in stand_in.connections.values()})
        self.assertEqual(f"load_{RUN}", api.destination_branch_now())
        self.assertNotEqual("main", api.destination_branch_now())


class SecretTest(unittest.TestCase):
    def test_the_destination_gets_the_access_key_in_its_request_and_the_other_requests_get_none(self) -> None:
        api, stand_in = prepared()
        ensure_everything(api)
        for path, body in stand_in.bodies:
            holds = ACCESS_VALUE in json.dumps(body)
            self.assertEqual(path == "destinations/create", holds, path)
        self.assertEqual(ACCESS_VALUE, stand_in.destinations["destination1"]["connectionConfiguration"]["secret_access_key"])

    def test_after_a_new_run_and_a_repair_only_the_destination_requests_hold_the_access_key(self) -> None:
        api, stand_in = prepared()
        ids = ensure_everything(api)
        stand_in.connections[ids["lab_data"]]["status"] = "inactive"
        stand_in.sources["source1"]["connectionConfiguration"]["bucket"] = "other"
        stand_in.bodies.clear()
        ensure_everything(api, "run_next")
        self.assertIn("destinations/update", [path for path, _ in stand_in.bodies])
        for path, body in stand_in.bodies:
            self.assertEqual(path == "destinations/update", ACCESS_VALUE in json.dumps(body), path)

    def test_a_masked_secret_in_the_stored_destination_is_not_a_difference(self) -> None:
        api, stand_in = prepared()
        ensure_everything(api)
        stand_in.log.clear()
        ensure_everything(api)
        self.assertNotIn("destinations/update", stand_in.log)

    def test_no_error_holds_a_secret_or_a_response_body(self) -> None:
        api, stand_in = prepared()
        stand_in.failures["destinations/create"] = (500, json.dumps({"message": f"bad {ACCESS_VALUE}"}).encode())
        with self.assertRaises(AirbyteError) as raised:
            ensure_everything(api)
        self.assertNotIn(ACCESS_VALUE, str(raised.exception))
        self.assertNotIn(ACCESS_ID, str(raised.exception))
        self.assertNotIn("bad", str(raised.exception))
        self.assertIn("destinations/create", str(raised.exception))


class TokenTest(unittest.TestCase):
    def test_one_token_serves_every_call_and_the_first_call_is_the_token_call(self) -> None:
        api, stand_in = prepared()
        ensure_everything(api)
        self.assertEqual(1, stand_in.tokens)
        self.assertEqual("applications/token", stand_in.log[0])

    def test_an_expired_token_is_renewed_once_and_the_call_is_repeated_once(self) -> None:
        api, stand_in = prepared()
        api.running_jobs("connection1")
        stand_in.expire_next_token = True
        stand_in.log.clear()
        api.running_jobs("connection1")
        self.assertEqual(["jobs/list", "applications/token", "jobs/list"], stand_in.log)

    def test_a_token_that_is_refused_twice_stops_with_an_error(self) -> None:
        api, stand_in = prepared()
        api.running_jobs("connection1")
        stand_in.failures["jobs/list"] = (401, b"{}")
        with self.assertRaises(AirbyteError):
            api.running_jobs("connection1")


class SyncTest(unittest.TestCase):
    def test_a_sync_is_started_polled_and_cancelled_by_its_job_ID(self) -> None:
        api, stand_in = prepared()
        ids = ensure_everything(api)
        self.assertEqual([], api.running_jobs(ids["lab_data"]))
        job = api.start_sync(ids["lab_data"])
        self.assertEqual("running", api.job_status(job))
        self.assertEqual([job], api.running_jobs(ids["lab_data"]))
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
                stand_in.failures["workspaces/list"] = failure
                with self.assertRaises(AirbyteError) as raised:
                    api.ensure_destination(definitions.destination_configuration(ENVIRONMENT, definitions.branch_name(RUN)))
                self.assertIn("workspaces/list", str(raised.exception))

    def test_an_answer_that_is_not_an_object_is_refused(self) -> None:
        api, stand_in = prepared()
        stand_in.failures["workspaces/list"] = (200, b"[1]")
        with self.assertRaises(AirbyteError):
            ensure_everything(api)

    def test_a_workspace_list_that_does_not_hold_exactly_one_workspace_is_refused(self) -> None:
        api, stand_in = prepared()
        stand_in.failures["workspaces/list"] = (200, b'{"workspaces": []}')
        with self.assertRaises(AirbyteError):
            ensure_everything(api)

    def test_the_connection_ID_of_a_table_that_was_never_ensured_is_refused(self) -> None:
        api, _ = prepared()
        with self.assertRaises(AirbyteError):
            api.ensure_connection(TRACK_LIST.name, definitions.connection_configuration(TRACK_LIST))
        with self.assertRaises(AirbyteError):
            api.destination_branch_now()


class AddressTest(unittest.TestCase):
    def test_an_address_that_is_not_a_plain_http_URL_is_refused(self) -> None:
        for address in ("https://airbyte.example", "airbyte.example:8001", "http://", "http://a?b=1"):
            with self.subTest(address), self.assertRaises(AirbyteError):
                HttpAirbyteApi(address, "client", "secret", ACCESS_ID, ACCESS_VALUE)


class ConnectionsOfTableTest(unittest.TestCase):
    def test_the_tables_have_one_connection_each_and_it_stays_the_same_across_calls(self) -> None:
        api, stand_in = prepared()
        first = ensure_everything(api)
        second = ensure_everything(api, "run_next")
        self.assertEqual(first, second)
        self.assertEqual(len(CSV_TABLES) + 1, len(first))


if __name__ == "__main__":
    unittest.main()
