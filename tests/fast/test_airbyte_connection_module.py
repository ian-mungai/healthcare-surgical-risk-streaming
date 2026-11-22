"""The step that applies the raw connection to a running Airbyte: it makes what exists match the document, and a second run changes nothing.

A fake of Airbyte's public API stands in for the server. It keeps the objects, adds fields of its own to every answer and keeps a log of each write, so these
cases show what a real server shows: a second run writes nothing, a drifted object is repaired and only that one, and a refusal names the call and never a
value. Real calls over HTTP run in the acceptance test and in the host's live batch.
"""

from __future__ import annotations

import base64
import copy
import http.client
import importlib.util
import json
import sys
import unittest
from types import ModuleType
from typing import Any
from urllib.parse import parse_qs, urlparse

from scripts.ingestion.connection import desired_state, full_release_selection
from tests.fast.ansible_support import ROLES

MODULE = ROLES / "airbyte" / "library" / "airbyte_connection.py"
WORKSPACE = "11111111-1111-1111-1111-111111111111"
HIDDEN_ONE = "client-id-that-must-not-leak"
HIDDEN_TWO = "client-secret-that-must-not-leak"
HIDDEN_THREE = "token-that-must-not-leak"


def load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("airbyte_connection", MODULE)
    if spec is None or spec.loader is None:
        raise ImportError("the module cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules["airbyte_connection"] = module
    spec.loader.exec_module(module)
    return module


MODULE_UNDER_TEST = load_module()


def document() -> dict[str, Any]:
    return desired_state(full_release_selection(), bucket="imungai-example-project", region="us-west-2", source_tag="4.17.1", destination_tag="1.9.11")


class FakeAirbyte:
    """Airbyte's public API as far as the module uses it. Every answer carries fields that the document does not hold."""

    def __init__(self, workspaces: int = 1) -> None:
        self.workspaces = [WORKSPACE, "22222222-2222-2222-2222-222222222222"][:workspaces]
        self.objects: dict[str, dict[str, dict[str, Any]]] = {
            "source_definitions": {},
            "destination_definitions": {},
            "sources": {},
            "destinations": {},
            "connections": {},
        }
        self.writes: list[str] = []
        self.calls: list[str] = []
        self.counter = 0
        self.fail_on: str | None = None

    def new_id(self) -> str:
        self.counter += 1
        return f"00000000-0000-0000-0000-{self.counter:012d}"

    def request(self, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        self.calls.append(f"{method} {path}")
        if self.fail_on and self.fail_on in f"{method} {path}":
            raise MODULE_UNDER_TEST.ApiError(f"{method} {path} answered 500")
        url = urlparse(path)
        parts = url.path.strip("/").split("/")
        offset = int(parse_qs(url.query).get("offset", ["0"])[0])
        if method == "GET":
            return self.read(parts, offset)
        self.writes.append(f"{method} {url.path}")
        return self.write(method, parts, copy.deepcopy(body or {}))

    def listing(self, kind: str, offset: int) -> dict[str, Any]:
        rows = [copy.deepcopy(row) for row in self.objects[kind].values()]
        page = rows[offset : offset + 2]
        return {"data": page, "next": "more" if offset + 2 < len(rows) else None}

    def read(self, parts: list[str], offset: int) -> dict[str, Any]:
        if parts == ["workspaces"]:
            return {"data": [{"workspaceId": identifier, "name": "w", "dataResidency": "auto"} for identifier in self.workspaces]}
        if parts[0] == "workspaces" and parts[2:3] == ["definitions"]:
            return self.listing(f"{parts[3][:-1]}_definitions", offset)
        return self.listing(parts[0], offset)

    def write(self, method: str, parts: list[str], body: dict[str, Any]) -> dict[str, Any]:
        if parts[0] == "workspaces":
            kind, identifier = f"{parts[3][:-1]}_definitions", parts[4] if len(parts) > 4 else None
            record = self.objects[kind].get(identifier or "", {}) | body | {"id": identifier or self.new_id(), "documentationUrl": "https://example.invalid"}
            self.objects[kind][record["id"]] = record
            return record
        kind = parts[0]
        key = {"sources": "sourceId", "destinations": "destinationId", "connections": "connectionId"}[kind]
        identifier = parts[1] if len(parts) > 1 else self.new_id()
        record = self.objects[kind].get(identifier, {"workspaceId": WORKSPACE}) | body | {key: identifier, "createdAt": 1}
        if kind == "sources":
            record |= {"sourceType": "s3", "configuration": self.merged(self.objects[kind].get(identifier), body)}
        if kind == "destinations":
            record |= {"destinationType": "s3", "configuration": self.merged(self.objects[kind].get(identifier), body)}
        if kind == "connections":
            record |= {"tags": [], "schedule": body.get("schedule", record.get("schedule", {"scheduleType": "manual"}))}
        self.objects[kind][identifier] = record
        return record

    @staticmethod
    def merged(stored: dict[str, Any] | None, body: dict[str, Any]) -> dict[str, Any]:
        configuration: dict[str, Any] = copy.deepcopy(body.get("configuration", (stored or {}).get("configuration", {})))
        configuration["a_default_of_the_platform"] = True
        return configuration

    def only(self, kind: str) -> dict[str, Any]:
        (record,) = self.objects[kind].values()
        return record


def apply(api: FakeAirbyte, wanted: dict[str, Any] | None = None) -> list[str]:
    changes: list[str] = MODULE_UNDER_TEST.reconcile(api, wanted or document())
    return changes


class CreateAndRepeatTest(unittest.TestCase):
    def test_on_an_empty_airbyte_every_object_is_created_once_definitions_first_and_the_connection_last(self) -> None:
        api = FakeAirbyte()
        changes = apply(api)
        self.assertEqual(["source definition", "destination definition", "source", "destination", "connection"], changes)
        self.assertEqual(
            [
                f"POST /workspaces/{WORKSPACE}/definitions/sources",
                f"POST /workspaces/{WORKSPACE}/definitions/destinations",
                "POST /sources",
                "POST /destinations",
                "POST /connections",
            ],
            api.writes,
        )
        connected = api.only("connections")
        self.assertEqual((api.only("sources")["sourceId"], api.only("destinations")["destinationId"]), (connected["sourceId"], connected["destinationId"]))
        self.assertEqual(api.only("source_definitions")["id"], api.only("sources")["definitionId"])
        self.assertEqual(api.only("destination_definitions")["id"], api.only("destinations")["definitionId"])

    def test_a_second_run_changes_nothing_and_writes_nothing_although_the_platform_adds_fields_of_its_own(self) -> None:
        api = FakeAirbyte()
        apply(api)
        api.writes.clear()
        self.assertEqual([], apply(api))
        self.assertEqual([], api.writes)

    def test_a_third_run_after_a_repair_changes_nothing(self) -> None:
        api = FakeAirbyte()
        apply(api)
        api.only("connections")["schedule"] = {"scheduleType": "cron", "cronExpression": "0 0 * * * ?"}
        apply(api)
        api.writes.clear()
        self.assertEqual([], apply(api))

    def test_many_objects_in_pages_are_all_seen(self) -> None:
        api = FakeAirbyte()
        apply(api)
        for index in range(5):
            api.objects["sources"][f"other{index}"] = {"sourceId": f"other{index}", "name": f"other_{index}", "workspaceId": WORKSPACE, "configuration": {}}
        api.writes.clear()
        self.assertEqual([], apply(api))
        self.assertEqual([], api.writes)


class DriftTest(unittest.TestCase):
    def repaired(self, damage: Any, expected: list[str]) -> FakeAirbyte:
        api = FakeAirbyte()
        apply(api)
        damage(api)
        api.writes.clear()
        self.assertEqual(expected, apply(api))
        api.writes.clear()
        self.assertEqual([], apply(api), "the repair must hold")
        return api

    def test_a_changed_source_connector_version_is_repaired_by_updating_the_definition(self) -> None:
        def damage(api: FakeAirbyte) -> None:
            api.only("source_definitions")["dockerImageTag"] = "4.17.2"

        api = FakeAirbyte()
        apply(api)
        damage(api)
        api.writes.clear()
        self.assertEqual(["source definition"], apply(api))
        self.assertEqual(1, len(api.writes))
        self.assertTrue(api.writes[0].startswith("PUT /workspaces/"))
        self.assertEqual("4.17.1", api.only("source_definitions")["dockerImageTag"])

    def test_a_changed_destination_connector_version_is_repaired(self) -> None:
        self.repaired(lambda api: api.only("destination_definitions").update(dockerImageTag="latest"), ["destination definition"])

    def test_a_dropped_selected_file_is_repaired(self) -> None:
        def damage(api: FakeAirbyte) -> None:
            api.only("sources")["configuration"]["streams"][0]["globs"].pop()

        api = self.repaired(damage, ["source"])
        self.assertEqual(6390, len(api.only("sources")["configuration"]["streams"][0]["globs"]))

    def test_an_added_file_is_repaired_because_the_selection_is_the_pins_and_nothing_else(self) -> None:
        def damage(api: FakeAirbyte) -> None:
            api.only("sources")["configuration"]["streams"][0]["globs"].append("vitaldb/1.0.0/SHA256SUMS.txt")

        api = self.repaired(damage, ["source"])
        self.assertEqual(6390, len(api.only("sources")["configuration"]["streams"][0]["globs"]))

    def test_a_source_that_replicates_records_is_repaired_to_copy_raw_files(self) -> None:
        def damage(api: FakeAirbyte) -> None:
            api.only("sources")["configuration"]["delivery_method"] = {"delivery_type": "use_records_transfer"}

        api = self.repaired(damage, ["source"])
        self.assertEqual("use_file_transfer", api.only("sources")["configuration"]["delivery_method"]["delivery_type"])

    def test_a_destination_that_writes_another_prefix_is_repaired(self) -> None:
        api = self.repaired(lambda api: api.only("destinations")["configuration"].update(s3_bucket_path="warehouse"), ["destination"])
        self.assertEqual("landing", api.only("destinations")["configuration"]["s3_bucket_path"])

    def test_a_schedule_that_was_switched_on_is_switched_off(self) -> None:
        api = self.repaired(lambda api: api.only("connections").update(schedule={"scheduleType": "cron", "cronExpression": "0 0 * * * ?"}), ["connection"])
        self.assertEqual("manual", api.only("connections")["schedule"]["scheduleType"])

    def test_a_connection_that_was_switched_off_is_switched_on(self) -> None:
        api = self.repaired(lambda api: api.only("connections").update(status="inactive"), ["connection"])
        self.assertEqual("active", api.only("connections")["status"])

    def test_a_stream_that_stopped_copying_files_is_repaired(self) -> None:
        def damage(api: FakeAirbyte) -> None:
            api.only("connections")["configurations"]["streams"][0]["includeFiles"] = False

        api = self.repaired(damage, ["connection"])
        self.assertTrue(api.only("connections")["configurations"]["streams"][0]["includeFiles"])

    def test_a_stream_that_replaced_instead_of_appending_is_repaired(self) -> None:
        def damage(api: FakeAirbyte) -> None:
            api.only("connections")["configurations"]["streams"][0]["syncMode"] = "full_refresh_overwrite"

        self.repaired(damage, ["connection"])

    def test_a_deleted_connection_is_created_again(self) -> None:
        api = self.repaired(lambda api: api.objects["connections"].clear(), ["connection"])
        self.assertEqual(1, len(api.objects["connections"]))

    def test_a_deleted_source_is_created_again_with_the_connection_that_went_with_it(self) -> None:
        api = FakeAirbyte()
        apply(api)
        api.objects["sources"].clear()
        api.objects["connections"].clear()
        self.assertEqual(["source", "connection"], apply(api))
        self.assertEqual(api.only("sources")["sourceId"], api.only("connections")["sourceId"])


class RefusalTest(unittest.TestCase):
    def test_two_objects_of_one_name_are_refused_before_any_write(self) -> None:
        api = FakeAirbyte()
        apply(api)
        twin = copy.deepcopy(api.only("sources"))
        twin["sourceId"] = "twin"
        api.objects["sources"]["twin"] = twin
        api.writes.clear()
        with self.assertRaises(MODULE_UNDER_TEST.ApiError) as raised:
            apply(api)
        self.assertIn("vitaldb_release", str(raised.exception))
        self.assertEqual([], api.writes)

    def test_zero_or_two_workspaces_are_refused_because_the_target_is_not_certain(self) -> None:
        for count in (0, 2):
            with self.subTest(count=count), self.assertRaises(MODULE_UNDER_TEST.ApiError):
                apply(FakeAirbyte(workspaces=count))

    def test_a_structural_drift_that_no_update_can_repair_is_refused_and_nothing_is_written(self) -> None:
        def move_connection(api: FakeAirbyte) -> None:
            api.only("connections")["sourceId"] = "other"

        def move_source(api: FakeAirbyte) -> None:
            api.only("sources")["definitionId"] = "other"

        def move_destination(api: FakeAirbyte) -> None:
            api.only("destinations")["definitionId"] = "other"

        def swap_image(api: FakeAirbyte) -> None:
            api.only("source_definitions")["dockerRepository"] = "someone/else"

        for name, damage in {"connection": move_connection, "source": move_source, "destination": move_destination, "image": swap_image}.items():
            with self.subTest(name):
                api = FakeAirbyte()
                apply(api)
                damage(api)
                api.writes.clear()
                with self.assertRaises(MODULE_UNDER_TEST.ApiError):
                    apply(api)
                self.assertEqual([], api.writes)

    def test_a_failed_call_stops_the_run_and_names_the_call(self) -> None:
        api = FakeAirbyte()
        api.fail_on = "POST /destinations"
        with self.assertRaises(MODULE_UNDER_TEST.ApiError) as raised:
            apply(api)
        self.assertIn("POST /destinations", str(raised.exception))
        self.assertEqual(0, len(api.objects["connections"]))

    def test_a_document_without_its_parts_is_refused_before_any_call(self) -> None:
        api = FakeAirbyte()
        wanted = document()
        del wanted["connection"]
        with self.assertRaises(MODULE_UNDER_TEST.ApiError):
            apply(api, wanted)
        self.assertEqual([], api.calls)


class CoverTest(unittest.TestCase):
    def test_what_the_document_holds_must_be_in_the_answer_and_extra_fields_of_the_platform_do_not_matter(self) -> None:
        covers = MODULE_UNDER_TEST.covers
        self.assertTrue(covers({"a": 1, "b": {"c": [1, 2], "d": 3}}, {"b": {"c": [1, 2]}}))
        self.assertFalse(covers({"a": 1}, {"a": 2}))
        self.assertFalse(covers({"a": 1}, {"b": 1}))
        self.assertFalse(covers({"a": [1, 2, 3]}, {"a": [1, 2]}), "a list must match whole")
        self.assertFalse(covers({"a": [1, 2]}, {"a": [2, 1]}), "a list keeps its order")
        self.assertFalse(covers({"a": True}, {"a": 1}), "True is not 1")
        self.assertFalse(covers({"a": 1.0}, {"a": True}))


class CredentialsTest(unittest.TestCase):
    def encoded(self, value: str) -> str:
        return base64.b64encode(value.encode()).decode()

    def run_with(self, code: int, output: str, error: str = "") -> tuple[str, str]:
        calls: list[list[str]] = []

        def run(arguments: list[str]) -> tuple[int, str, str]:
            calls.append(arguments)
            return code, output, error

        result: tuple[str, str] = MODULE_UNDER_TEST.client_credentials(run, ["k3s", "kubectl"], "airbyte")
        self.assertEqual("get", calls[0][2])
        return result

    def test_the_client_id_and_secret_are_read_from_the_namespaces_auth_secret(self) -> None:
        data = {"data": {"instance-admin-client-id": self.encoded(HIDDEN_ONE), "instance-admin-client-secret": self.encoded(HIDDEN_TWO)}}
        self.assertEqual((HIDDEN_ONE, HIDDEN_TWO), self.run_with(0, json.dumps(data)))

    def test_a_missing_key_or_a_failed_read_stops_without_showing_a_value(self) -> None:
        for name, (code, output, error) in {
            "no keys yet": (0, json.dumps({"data": {"instance-admin-password": self.encoded("pw-that-must-not-leak")}}), ""),
            "no secret": (1, "", f"NotFound {HIDDEN_TWO}"),
            "not json": (0, f"not json {HIDDEN_TWO}", ""),
        }.items():
            with self.subTest(name), self.assertRaises(MODULE_UNDER_TEST.ApiError) as raised:
                self.run_with(code, output, error)
            for value in (HIDDEN_TWO, "pw-that-must-not-leak"):
                self.assertNotIn(value, str(raised.exception))


class HttpTest(unittest.TestCase):
    class Connection:
        def __init__(self, status: int, text: str, failure: Exception | None, log: list[Any]) -> None:
            self.status, self.text, self.failure, self.log = status, text, failure, log
            self.closed = False

        def request(self, method: str, url: str, body: bytes | None, headers: dict[str, str]) -> None:
            self.log.append((method, url, body, headers))
            if isinstance(self.failure, OSError):
                raise self.failure

        def getresponse(self) -> HttpTest.Connection:
            if self.failure:
                raise self.failure
            return self

        def read(self) -> bytes:
            return self.text.encode()

        def close(self) -> None:
            self.closed = True

    def api(self, status: int = 200, text: str = "{}", failure: Exception | None = None, base: str = "http://127.0.0.1:8000/api/public/v1") -> Any:
        log: list[Any] = []
        made: list[HttpTest.Connection] = []

        def connect(host: str, port: int, timeout: float) -> HttpTest.Connection:
            log.append((host, port, timeout))
            made.append(HttpTest.Connection(status, text, failure, log))
            return made[-1]

        client = MODULE_UNDER_TEST.HttpApi(base, lambda: HIDDEN_THREE, timeout_seconds=7, connect=connect)
        client.log, client.made = log, made
        return client

    def test_a_call_goes_to_the_local_address_with_the_bearer_token_a_deadline_and_the_json_body(self) -> None:
        client = self.api(text='{"data": []}')
        self.assertEqual({"data": []}, client.request("POST", "/sources", {"a": 1}))
        self.assertEqual(("127.0.0.1", 8000, 7), client.log[0])
        method, url, body, headers = client.log[1]
        self.assertEqual(("POST", "/api/public/v1/sources"), (method, url))
        self.assertEqual(f"Bearer {HIDDEN_THREE}", headers["Authorization"])
        self.assertEqual({"a": 1}, json.loads(body))
        self.assertTrue(client.made[0].closed)

    def test_an_address_that_is_not_local_is_refused_so_the_token_never_leaves_the_host(self) -> None:
        for base in ("http://example.invalid:8000/api", "https://127.0.0.1:8000/api", "http://127.0.0.1/api", "http://10.42.0.1:8000/api"):
            with self.subTest(base=base), self.assertRaises(MODULE_UNDER_TEST.ApiError):
                self.api(base=base)

    def test_a_refusal_names_the_call_and_the_status_and_never_the_token_or_the_body(self) -> None:
        with self.assertRaises(MODULE_UNDER_TEST.ApiError) as raised:
            self.api(status=400, text=f"echo {HIDDEN_TWO}").request("POST", "/sources", {"hidden": HIDDEN_TWO})
        self.assertIn("POST /sources", str(raised.exception))
        self.assertIn("400", str(raised.exception))
        for value in (HIDDEN_THREE, HIDDEN_TWO):
            self.assertNotIn(value, str(raised.exception))

    def test_a_timeout_and_a_connection_failure_name_the_call_and_close_the_connection(self) -> None:
        for failure in (TimeoutError("slow"), ConnectionRefusedError("refused"), http.client.RemoteDisconnected("gone")):
            with self.subTest(type(failure).__name__):
                client = self.api(failure=failure)
                with self.assertRaises(MODULE_UNDER_TEST.ApiError) as raised:
                    client.request("GET", "/workspaces")
                self.assertIn("GET /workspaces", str(raised.exception))
                self.assertTrue(client.made[0].closed)

    def test_a_reply_that_is_not_a_json_object_is_refused(self) -> None:
        for text in ("[]", "not json", ""):
            with self.subTest(text=text), self.assertRaises(MODULE_UNDER_TEST.ApiError):
                self.api(text=text).request("GET", "/workspaces")


if __name__ == "__main__":
    unittest.main()
