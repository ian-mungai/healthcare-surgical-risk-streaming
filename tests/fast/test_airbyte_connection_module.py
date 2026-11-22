"""The step that applies the raw connection to a running Airbyte: it makes what exists match the document, and a second run changes nothing.

A fake of Airbyte's public API stands in for the server. It keeps the objects, adds fields of its own to every answer and keeps a log of each write, so these
cases show what a real server shows: a second run writes nothing, a drifted object is repaired and only that one, and a refusal names the call and never a
value. Real calls over HTTP run in the acceptance test and in the host's live batch.
"""

from __future__ import annotations

import base64
import contextlib
import copy
import hashlib
import http.client
import importlib.util
import json
import os
import re
import socket
import socketserver
import sys
import tempfile
import threading
import time
import unittest
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest import mock
from urllib.parse import parse_qs, urlparse

from scripts.ingestion.connection import desired_state, full_release_selection
from tests.fast.ansible_support import ROLES, shared_module

MODULE = ROLES / "airbyte" / "library" / "airbyte_connection.py"
WORKSPACE = "11111111-1111-1111-1111-111111111111"
HIDDEN_ONE = "client-id-that-must-not-leak"
HIDDEN_TWO = "client-secret-that-must-not-leak"
HIDDEN_THREE = "token-that-must-not-leak"
KEY_ID = "AKIAEXAMPLEKEYID0001"
KEY_VALUE = "key-value-that-must-not-leak-0001"
OTHER_KEY_ID = "AKIAEXAMPLEKEYID0002"
OTHER_KEY_VALUE = "key-value-that-must-not-leak-0002"
KEYS = {"aws_access_key_id": KEY_ID, "aws_secret_access_key": KEY_VALUE}
MASK = "**********"
MASKED_FIELDS = {"aws_access_key_id", "aws_secret_access_key", "access_key_id", "secret_access_key"}


def load_module() -> ModuleType:
    shared_module("secret_files")
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
        self.damage: tuple[str, str | None, str, Any] | None = None
        self.bodies: list[tuple[str, str, dict[str, Any]]] = []
        self.keys: dict[str, str] = dict(KEYS)
        self.recorded: dict[str, str] = {}

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
        self.bodies.append((method, url.path, copy.deepcopy(body or {})))
        return self.write(method, parts, copy.deepcopy(body or {}))

    KIND_NAMES = {
        "source_definitions": "source_definition",
        "destination_definitions": "destination_definition",
        "sources": "source",
        "destinations": "destination",
        "connections": "connection",
    }

    def damaged(self, kind: str, rows: list[Any]) -> list[Any]:
        """The rows of one kind as a faulty server would answer them, when a test has planted a fault for that kind."""
        if self.damage is None or self.damage[0] != kind:
            return rows
        _, key, how, expected = self.damage
        out: list[Any] = []
        for row in rows:
            if how == "not an object":
                out.append("not an object")
                continue
            name = str(key)
            if how == "missing":
                row.pop(name, None)
            elif how == "empty":
                row[name] = ""
            elif how == "not a uuid":
                row[name] = "an-id-that-is-text-but-not-a-uuid"
            else:
                row[name] = "a text" if expected is not str else 5
            out.append(row)
        return out

    def listing(self, kind: str, offset: int) -> dict[str, Any]:
        rows = self.damaged(self.KIND_NAMES[kind], [self.masked(copy.deepcopy(row)) for row in self.objects[kind].values()])
        page = rows[offset : offset + 2]
        return {"data": page, "next": "more" if offset + 2 < len(rows) else None}

    def read(self, parts: list[str], offset: int) -> dict[str, Any]:
        if parts == ["workspaces"]:
            rows = [{"workspaceId": identifier, "name": "w", "dataResidency": "auto"} for identifier in self.workspaces]
            return {"data": self.damaged("workspace", rows)}
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

    @classmethod
    def masked(cls, node: Any) -> Any:
        """The answer of the API: every secret field of a configuration comes back as the mask and never as its value."""
        if isinstance(node, dict):
            return {name: MASK if name in MASKED_FIELDS else cls.masked(value) for name, value in node.items()}
        if isinstance(node, list):
            return [cls.masked(value) for value in node]
        return node

    @staticmethod
    def merged(stored: dict[str, Any] | None, body: dict[str, Any]) -> dict[str, Any]:
        configuration: dict[str, Any] = copy.deepcopy(body.get("configuration", (stored or {}).get("configuration", {})))
        configuration["a_default_of_the_platform"] = True
        return configuration

    def only(self, kind: str) -> dict[str, Any]:
        (record,) = self.objects[kind].values()
        return record


def apply(api: FakeAirbyte, wanted: dict[str, Any] | None = None) -> list[str]:
    changes: list[str] = MODULE_UNDER_TEST.reconcile(api, wanted or document(), api.keys, api.recorded)
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
            api.objects["sources"][f"4444444{index}-4444-4444-4444-444444444444"] = {
                "sourceId": f"4444444{index}-4444-4444-4444-444444444444",
                "name": f"other_{index}",
                "definitionId": "44444444-4444-4444-4444-444444444444",
                "workspaceId": WORKSPACE,
                "configuration": {},
            }
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
        twin["sourceId"] = "77777777-7777-7777-7777-777777777777"
        api.objects["sources"]["77777777-7777-7777-7777-777777777777"] = twin
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
            api.only("connections")["sourceId"] = "66666666-6666-6666-6666-666666666666"

        def move_source(api: FakeAirbyte) -> None:
            api.only("sources")["definitionId"] = "66666666-6666-6666-6666-666666666666"

        def move_destination(api: FakeAirbyte) -> None:
            api.only("destinations")["definitionId"] = "66666666-6666-6666-6666-666666666666"

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


def stored_values(api: FakeAirbyte, kind: str) -> dict[str, Any]:
    """The secret fields that the fake keeps for its one source or destination."""
    configuration = api.only(kind)["configuration"]
    return {name: value for name, value in {**configuration, **configuration.get("credentials", {})}.items() if name in MASKED_FIELDS}


class KeyInjectionTest(unittest.TestCase):
    def test_the_key_is_added_at_apply_time_to_the_source_and_the_destination_and_the_document_holds_none(self) -> None:
        wanted = document()
        self.assertNotIn(KEY_VALUE, json.dumps(wanted))
        api = FakeAirbyte()
        apply(api, wanted)
        self.assertEqual({"aws_access_key_id": KEY_ID, "aws_secret_access_key": KEY_VALUE}, stored_values(api, "sources"))
        self.assertEqual({"access_key_id": KEY_ID, "secret_access_key": KEY_VALUE}, stored_values(api, "destinations"))
        self.assertEqual("iam_user", api.only("sources")["configuration"]["credentials"]["auth_type"])
        self.assertNotIn(KEY_VALUE, json.dumps(wanted), "the document is not changed by the apply")

    def test_the_key_goes_only_in_the_body_of_a_source_and_a_destination_call(self) -> None:
        api = FakeAirbyte()
        apply(api)
        carrying = sorted({f"{method} {path}" for method, path, body in api.bodies if KEY_VALUE in json.dumps(body)})
        self.assertEqual(["POST /destinations", "POST /sources"], carrying)

    def test_a_second_run_changes_nothing_although_the_api_answers_every_secret_as_the_mask(self) -> None:
        api = FakeAirbyte()
        apply(api)
        self.assertEqual(MASK, api.masked(api.only("sources"))["configuration"]["credentials"]["aws_secret_access_key"])
        api.writes.clear()
        self.assertEqual([], apply(api))
        self.assertEqual([], api.writes)

    def test_a_rotated_key_is_sent_to_both_objects_once_and_then_the_run_is_stable(self) -> None:
        api = FakeAirbyte()
        apply(api)
        api.keys = {"aws_access_key_id": OTHER_KEY_ID, "aws_secret_access_key": OTHER_KEY_VALUE}
        api.writes.clear()
        self.assertEqual(["source", "destination"], apply(api))
        self.assertEqual({"aws_access_key_id": OTHER_KEY_ID, "aws_secret_access_key": OTHER_KEY_VALUE}, stored_values(api, "sources"))
        self.assertEqual({"access_key_id": OTHER_KEY_ID, "secret_access_key": OTHER_KEY_VALUE}, stored_values(api, "destinations"))
        api.writes.clear()
        self.assertEqual([], apply(api))

    def test_a_lost_record_sends_the_key_again_once(self) -> None:
        api = FakeAirbyte()
        apply(api)
        api.recorded.clear()
        self.assertEqual(["source", "destination"], apply(api))
        self.assertEqual([], apply(api))

    def test_a_repair_of_another_setting_keeps_the_key_in_the_object(self) -> None:
        api = FakeAirbyte()
        apply(api)
        api.only("sources")["configuration"]["streams"][0]["globs"].pop()
        self.assertEqual(["source"], apply(api))
        self.assertEqual({"aws_access_key_id": KEY_ID, "aws_secret_access_key": KEY_VALUE}, stored_values(api, "sources"))

    def test_the_record_holds_a_fingerprint_of_the_key_id_and_never_a_value(self) -> None:
        api = FakeAirbyte()
        apply(api)
        text = json.dumps(api.recorded)
        for value in (KEY_ID, KEY_VALUE):
            self.assertNotIn(value, text)
        self.assertEqual({"source", "destination"}, set(api.recorded))
        for fingerprint in api.recorded.values():
            self.assertRegex(fingerprint, r"^[0-9a-f]{64}$")
        expected = {
            "source": hashlib.sha256(f"{api.only('sources')['sourceId']}\n{KEY_ID}".encode()).hexdigest(),
            "destination": hashlib.sha256(f"{api.only('destinations')['destinationId']}\n{KEY_ID}".encode()).hexdigest(),
        }
        self.assertEqual(expected, api.recorded)

    def test_no_message_of_a_failed_run_holds_a_key_value(self) -> None:
        for failing in ("POST /sources", "POST /destinations", "PATCH /sources"):
            with self.subTest(failing=failing):
                api = FakeAirbyte()
                if failing.startswith("PATCH"):
                    apply(api)
                    api.keys = {"aws_access_key_id": OTHER_KEY_ID, "aws_secret_access_key": OTHER_KEY_VALUE}
                api.fail_on = failing
                with self.assertRaises(MODULE_UNDER_TEST.ApiError) as raised:
                    apply(api)
                for value in (KEY_ID, KEY_VALUE, OTHER_KEY_ID, OTHER_KEY_VALUE):
                    self.assertNotIn(value, str(raised.exception))

    def test_a_failed_write_does_not_record_the_key_as_applied_so_the_next_run_sends_it(self) -> None:
        api = FakeAirbyte()
        apply(api)
        api.keys = {"aws_access_key_id": OTHER_KEY_ID, "aws_secret_access_key": OTHER_KEY_VALUE}
        api.fail_on = "PATCH /sources"
        with self.assertRaises(MODULE_UNDER_TEST.ApiError):
            apply(api)
        api.fail_on = None
        self.assertEqual(["source", "destination"], apply(api))
        self.assertEqual([], apply(api))

    def refused_before_any_call(self, wanted: dict[str, Any], api: FakeAirbyte | None = None) -> None:
        api = api or FakeAirbyte()
        with self.assertRaises(MODULE_UNDER_TEST.ApiError) as raised:
            apply(api, wanted)
        self.assertEqual([], api.calls)
        for value in (KEY_ID, KEY_VALUE):
            self.assertNotIn(value, str(raised.exception))

    def test_a_document_that_maps_the_secret_to_an_ordinary_field_is_refused_before_any_call(self) -> None:
        wanted = document()
        wanted["destination"]["secret_fields"] = {"s3_bucket_path": "aws_secret_access_key"}
        self.refused_before_any_call(wanted)

    def test_a_document_with_secret_fields_in_any_form_is_refused_before_any_call(self) -> None:
        for part in ("source", "destination"):
            for fields in ({}, {"credentials.aws_access_key_id": "aws_access_key_id"}, {"": "aws_access_key_id"}, "x", None):
                with self.subTest(part=part, fields=fields):
                    wanted = document()
                    wanted[part]["secret_fields"] = fields
                    self.refused_before_any_call(wanted)

    def test_a_configuration_that_already_has_a_value_where_the_key_goes_is_refused_before_any_call(self) -> None:
        for part, parent, name in (("source", "credentials", "aws_access_key_id"), ("source", "credentials", "aws_secret_access_key")):
            with self.subTest(part=part, name=name):
                wanted = document()
                wanted[part]["configuration"][parent][name] = "a value that the document must not hold"
                self.refused_before_any_call(wanted)
        for name in ("access_key_id", "secret_access_key"):
            with self.subTest(part="destination", name=name):
                wanted = document()
                wanted["destination"]["configuration"][name] = "a value that the document must not hold"
                self.refused_before_any_call(wanted)

    def test_a_source_without_its_credentials_object_is_refused_before_any_call(self) -> None:
        wanted = document()
        del wanted["source"]["configuration"]["credentials"]
        self.refused_before_any_call(wanted)

    def test_a_connector_that_is_not_one_of_the_two_pinned_images_is_refused_before_any_call(self) -> None:
        for part, image in (("source", "someone/else"), ("source", "airbyte/destination-s3"), ("destination", "airbyte/source-s3"), ("destination", "")):
            with self.subTest(part=part, image=image):
                wanted = document()
                wanted[part]["definition"]["dockerRepository"] = image
                self.refused_before_any_call(wanted)

    def test_the_outgoing_configuration_may_differ_from_the_document_at_the_table_paths_only(self) -> None:
        def evil(configuration: dict[str, Any], fields: Any, secrets: Any) -> dict[str, Any]:
            filled = copy.deepcopy(configuration)
            filled["s3_bucket_path"] = secrets["aws_secret_access_key"]
            return filled

        api = FakeAirbyte()
        with mock.patch.object(MODULE_UNDER_TEST, "inject", evil), self.assertRaises(MODULE_UNDER_TEST.ApiError) as raised:
            apply(api)
        self.assertEqual([], api.calls)
        self.assertNotIn(KEY_VALUE, str(raised.exception))

    def test_the_table_names_for_each_pinned_connector_exactly_the_two_paths_that_take_the_key(self) -> None:
        table = MODULE_UNDER_TEST.CONNECTOR_KEY_FIELDS
        self.assertEqual({"source", "destination"}, set(table))
        self.assertEqual(
            ("airbyte/source-s3", {"credentials.aws_access_key_id": "aws_access_key_id", "credentials.aws_secret_access_key": "aws_secret_access_key"}),
            (table["source"]["image"], table["source"]["fields"]),
        )
        self.assertEqual(
            ("airbyte/destination-s3", {"access_key_id": "aws_access_key_id", "secret_access_key": "aws_secret_access_key"}),
            (table["destination"]["image"], table["destination"]["fields"]),
        )

    def test_a_missing_key_value_is_refused_before_any_call(self) -> None:
        api = FakeAirbyte()
        api.keys = {"aws_access_key_id": KEY_ID}
        self.refused_before_any_call(document(), api)


class KeyFilesTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="airbyte_key_files_")
        self.addCleanup(folder.cleanup)
        self.folder = Path(folder.name)
        (self.folder / "airbyte").mkdir()

    def write(self, name: str, text: str) -> None:
        (self.folder / "airbyte" / name).write_text(text)

    def test_the_two_files_are_read_and_a_trailing_newline_is_dropped(self) -> None:
        self.write("aws_access_key_id", KEY_ID + "\n")
        self.write("aws_secret_access_key", KEY_VALUE)
        self.assertEqual(KEYS, MODULE_UNDER_TEST.read_key_files(self.folder))

    def test_a_missing_or_empty_file_stops_and_names_the_file_and_never_a_value(self) -> None:
        self.write("aws_access_key_id", KEY_ID)
        with self.assertRaises(MODULE_UNDER_TEST.ApiError) as raised:
            MODULE_UNDER_TEST.read_key_files(self.folder)
        self.assertIn("aws_secret_access_key", str(raised.exception))
        self.write("aws_secret_access_key", "\n")
        with self.assertRaises(MODULE_UNDER_TEST.ApiError) as raised:
            MODULE_UNDER_TEST.read_key_files(self.folder)
        self.assertNotIn(KEY_ID, str(raised.exception))

    def test_the_record_is_written_for_its_owner_only_and_read_back(self) -> None:
        path = self.folder / "record.json"
        MODULE_UNDER_TEST.write_record(path, {"source": "a" * 64})
        self.assertEqual(0o400, path.stat().st_mode & 0o777)
        self.assertEqual({"source": "a" * 64}, MODULE_UNDER_TEST.read_record(path))
        self.assertEqual({}, MODULE_UNDER_TEST.read_record(self.folder / "none.json"))
        path.chmod(0o600)
        path.write_text("not json")
        self.assertEqual({}, MODULE_UNDER_TEST.read_record(path))

    def test_a_failure_after_the_source_keeps_its_record_and_the_next_run_sends_only_what_is_missing(self) -> None:
        api = FakeAirbyte()
        path = self.folder / "record.json"
        api.fail_on = "POST /destinations"
        with self.assertRaises(MODULE_UNDER_TEST.ApiError):
            MODULE_UNDER_TEST.apply_document(api, document(), KEYS, path)
        self.assertEqual({"source"}, set(MODULE_UNDER_TEST.read_record(path)))
        api.fail_on = None
        self.assertEqual(["destination", "connection"], MODULE_UNDER_TEST.apply_document(api, document(), KEYS, path))
        api.writes.clear()
        self.assertEqual([], MODULE_UNDER_TEST.apply_document(api, document(), KEYS, path))
        self.assertEqual([], api.writes)
        self.assertEqual(0o400, path.stat().st_mode & 0o777)
        for value in (KEY_ID, KEY_VALUE):
            self.assertNotIn(value, path.read_text())

    def test_the_module_never_logs_a_body_or_a_value(self) -> None:
        text = MODULE.read_text()
        for forbidden in ("module.log", "module.debug", "module.warn", "no_log=False"):
            self.assertNotIn(forbidden, text)
        self.assertIsNone(re.search(r"(?<![A-Za-z_])print\(", text))


class PlanTest(unittest.TestCase):
    """The run reads every object and decides the whole plan before it writes: a refusal never follows a write."""

    def build(self) -> FakeAirbyte:
        api = FakeAirbyte()
        apply(api)
        api.writes.clear()
        api.calls.clear()
        return api

    def refused_without_a_write(self, api: FakeAirbyte) -> None:
        with self.assertRaises(MODULE_UNDER_TEST.ApiError):
            apply(api)
        self.assertEqual([], api.writes)

    def test_a_drifted_version_and_a_wrongly_linked_connection_give_no_write_call(self) -> None:
        api = self.build()
        api.only("source_definitions")["dockerImageTag"] = "latest"
        api.only("connections")["sourceId"] = "55555555-5555-5555-5555-555555555555"
        self.refused_without_a_write(api)
        self.assertEqual("latest", api.only("source_definitions")["dockerImageTag"], "the drifted version is not touched when the run is refused")

    def test_a_drifted_source_and_a_destination_of_another_definition_give_no_write_call(self) -> None:
        api = self.build()
        api.only("sources")["configuration"]["streams"][0]["globs"].pop()
        api.only("destinations")["definitionId"] = "66666666-6666-6666-6666-666666666666"
        self.refused_without_a_write(api)

    def test_a_rotated_key_and_a_wrongly_linked_connection_give_no_write_call(self) -> None:
        api = self.build()
        api.keys = {"aws_access_key_id": OTHER_KEY_ID, "aws_secret_access_key": OTHER_KEY_VALUE}
        api.only("connections")["destinationId"] = "55555555-5555-5555-5555-555555555555"
        self.refused_without_a_write(api)

    def test_a_missing_definition_with_a_connection_that_cannot_follow_gives_no_write_call(self) -> None:
        api = self.build()
        api.objects["destination_definitions"].clear()
        self.refused_without_a_write(api)

    def test_a_missing_source_with_a_connection_left_over_gives_no_write_call(self) -> None:
        api = self.build()
        api.objects["sources"].clear()
        self.refused_without_a_write(api)

    def test_two_objects_of_one_name_and_a_drift_give_no_write_call(self) -> None:
        api = self.build()
        api.only("connections")["schedule"] = {"scheduleType": "cron", "cronExpression": "0 0 * * * ?"}
        twin = copy.deepcopy(api.only("destinations"))
        twin["destinationId"] = "77777777-7777-7777-7777-777777777777"
        api.objects["destinations"]["77777777-7777-7777-7777-777777777777"] = twin
        self.refused_without_a_write(api)

    def test_planning_reads_everything_and_writes_nothing_and_the_plan_names_each_step_in_order(self) -> None:
        api = self.build()
        api.only("source_definitions")["dockerImageTag"] = "latest"
        api.only("connections")["schedule"] = {"scheduleType": "cron", "cronExpression": "0 0 * * * ?"}
        steps = MODULE_UNDER_TEST.plan(api, document(), api.keys, api.recorded)
        self.assertEqual(["source definition", "connection"], [step.label for step in steps])
        self.assertEqual([], api.writes)
        self.assertTrue(all(call.startswith("GET ") for call in api.calls))

    def test_a_clean_plan_applies_in_order_and_a_second_plan_is_empty(self) -> None:
        api = FakeAirbyte()
        steps = MODULE_UNDER_TEST.plan(api, document(), api.keys, api.recorded)
        self.assertEqual(["source definition", "destination definition", "source", "destination", "connection"], [step.label for step in steps])
        self.assertEqual([], api.writes)


class ActorIdentityTest(unittest.TestCase):
    def test_the_fingerprint_binds_the_key_to_the_actor_it_was_sent_to(self) -> None:
        one = MODULE_UNDER_TEST.fingerprint("actor-one", KEYS)
        self.assertNotEqual(one, MODULE_UNDER_TEST.fingerprint("actor-two", KEYS))
        self.assertNotEqual(one, MODULE_UNDER_TEST.fingerprint("actor-one", {**KEYS, "aws_access_key_id": OTHER_KEY_ID}))
        self.assertRegex(one, r"^[0-9a-f]{64}$")

    def test_a_replacement_source_of_the_same_name_gets_the_key_again(self) -> None:
        api = FakeAirbyte()
        apply(api)
        old = api.only("sources")
        replacement = copy.deepcopy(old)
        replacement["sourceId"] = "99999999-9999-9999-9999-999999999999"
        for name in ("aws_access_key_id", "aws_secret_access_key"):
            replacement["configuration"]["credentials"].pop(name)
        api.objects["sources"] = {replacement["sourceId"]: replacement}
        api.only("connections")["sourceId"] = replacement["sourceId"]
        api.writes.clear()
        self.assertEqual(["source"], apply(api))
        self.assertEqual({"aws_access_key_id": KEY_ID, "aws_secret_access_key": KEY_VALUE}, stored_values(api, "sources"))
        api.writes.clear()
        self.assertEqual([], apply(api))

    def test_a_replacement_destination_of_the_same_name_gets_the_key_again(self) -> None:
        api = FakeAirbyte()
        apply(api)
        replacement = copy.deepcopy(api.only("destinations"))
        replacement["destinationId"] = "88888888-8888-8888-8888-888888888888"
        for name in ("access_key_id", "secret_access_key"):
            replacement["configuration"].pop(name)
        api.objects["destinations"] = {replacement["destinationId"]: replacement}
        api.only("connections")["destinationId"] = replacement["destinationId"]
        self.assertEqual(["destination"], apply(api))
        self.assertEqual({"access_key_id": KEY_ID, "secret_access_key": KEY_VALUE}, stored_values(api, "destinations"))


class RecordFileTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="airbyte_record_file_")
        self.addCleanup(folder.cleanup)
        self.folder = Path(folder.name)

    def test_a_record_does_not_inherit_the_permissions_of_a_file_that_was_already_there(self) -> None:
        path = self.folder / "record.json"
        for name in (".record.json.new", ".record.json.deadbeef.new"):
            planted = self.folder / name
            planted.write_text("planted")
            planted.chmod(0o666)
        MODULE_UNDER_TEST.write_record(path, {"source": "a" * 64})
        self.assertEqual(0o400, path.stat().st_mode & 0o777)
        self.assertFalse((self.folder / ".record.json.deadbeef.new").exists(), "a leftover of this module's temporary name is removed")
        self.assertEqual("planted", (self.folder / ".record.json.new").read_text(), "a file of another name is left alone")
        self.assertNotIn("planted", path.read_text())

    def test_a_record_that_exists_with_broader_permissions_is_replaced_by_an_owner_only_file(self) -> None:
        path = self.folder / "record.json"
        path.write_text("{}")
        path.chmod(0o666)
        MODULE_UNDER_TEST.write_record(path, {"source": "b" * 64})
        self.assertEqual(0o400, path.stat().st_mode & 0o777)
        self.assertEqual({"source": "b" * 64}, MODULE_UNDER_TEST.read_record(path))

    def test_a_write_cut_short_leaves_the_old_record_whole(self) -> None:
        path = self.folder / "record.json"
        MODULE_UNDER_TEST.write_record(path, {"source": "a" * 64})
        with mock.patch("ansible.module_utils.secret_files.os.fsync", side_effect=KeyboardInterrupt), self.assertRaises(KeyboardInterrupt):
            MODULE_UNDER_TEST.write_record(path, {"source": "c" * 64})
        self.assertEqual({"source": "a" * 64}, MODULE_UNDER_TEST.read_record(path))


class DeadlineTest(unittest.TestCase):
    def call(self, code: int, *, budget: Any = None) -> tuple[list[list[str]], str]:
        calls: list[list[str]] = []

        def run(arguments: list[str]) -> tuple[int, str, str]:
            calls.append(arguments)
            return code, "", "text of the program"

        message = ""
        try:
            MODULE_UNDER_TEST.client_credentials(run, ["k3s", "kubectl"], "airbyte", budget or MODULE_UNDER_TEST.Budget(1000))
        except MODULE_UNDER_TEST.ApiError as error:
            message = str(error)
        return calls, message

    def test_the_kubectl_call_runs_under_a_timeout_program_with_the_time_left_and_its_own_request_timeout(self) -> None:
        (arguments,), _ = self.call(1)
        self.assertEqual("timeout", arguments[0])
        self.assertEqual(f"{MODULE_UNDER_TEST.KUBECTL_SECONDS}s", arguments[2])
        self.assertIn(f"--request-timeout={MODULE_UNDER_TEST.KUBECTL_SECONDS}s", arguments)

    def test_the_timeout_is_the_smaller_of_its_own_limit_and_the_time_left_in_the_run(self) -> None:
        (arguments,), _ = self.call(1, budget=MODULE_UNDER_TEST.Budget(7.2))
        self.assertEqual("8s", arguments[2], "the time left rounds up to whole seconds")
        self.assertIn("--request-timeout=8s", arguments)

    def test_a_run_with_no_time_left_makes_no_call(self) -> None:
        calls, message = self.call(1, budget=MODULE_UNDER_TEST.Budget(0))
        self.assertEqual([], calls)
        self.assertIn("run deadline", message)

    def test_a_call_that_the_timeout_program_stopped_is_a_named_error_without_text(self) -> None:
        for code in (124, 137):
            with self.subTest(code=code):
                _, message = self.call(code)
                self.assertIn("did not finish within", message)
                self.assertIn("airbyte-auth-secrets", message)
                self.assertNotIn("text of the program", message)

    def test_any_other_failure_is_a_named_error_without_the_programs_text(self) -> None:
        _, message = self.call(1)
        self.assertIn("airbyte-auth-secrets", message)
        self.assertNotIn("text of the program", message)

    def test_the_module_reads_the_credentials_through_that_call_with_its_run_budget_and_the_task_has_a_deadline_too(self) -> None:
        text = MODULE.read_text()
        self.assertIn("client_credentials(", text)
        self.assertNotIn("subprocess", text)
        tasks = (ROLES / "airbyte" / "tasks" / "connection.yml").read_text()
        self.assertIn("airbyte_connection_run_seconds | int + 60", tasks)


class AnswerShapeTest(unittest.TestCase):
    """Every item of every answer is checked on read against a closed table of required keys and types, so a write never meets a missing ID."""

    KINDS = ("workspace", "source_definition", "destination_definition", "source", "destination", "connection")

    def drifted(self) -> FakeAirbyte:
        api = FakeAirbyte()
        apply(api)
        api.only("source_definitions")["dockerImageTag"] = "latest"
        api.only("sources")["configuration"]["streams"][0]["globs"].pop()
        api.only("connections")["schedule"] = {"scheduleType": "cron", "cronExpression": "0 0 * * * ?"}
        api.keys = {"aws_access_key_id": OTHER_KEY_ID, "aws_secret_access_key": OTHER_KEY_VALUE}
        api.writes.clear()
        return api

    def test_the_table_names_every_kind_and_the_keys_that_a_write_needs(self) -> None:
        shapes = MODULE_UNDER_TEST.ANSWER_SHAPES
        self.assertEqual(set(self.KINDS), set(shapes))
        for kind, needed in {
            "workspace": {"workspaceId"},
            "source_definition": {"id", "name", "dockerRepository", "dockerImageTag"},
            "destination_definition": {"id", "name", "dockerRepository", "dockerImageTag"},
            "source": {"sourceId", "name", "definitionId", "configuration"},
            "destination": {"destinationId", "name", "definitionId", "configuration"},
            "connection": {"connectionId", "name", "sourceId", "destinationId", "status", "schedule", "configurations"},
        }.items():
            self.assertLessEqual(needed, set(shapes[kind]), kind)

    def test_each_required_key_of_each_kind_missing_or_mistyped_is_refused_and_nothing_is_written(self) -> None:
        for kind in self.KINDS:
            for key, expected in MODULE_UNDER_TEST.ANSWER_SHAPES[kind].items():
                for damage in ("missing", "mistyped"):
                    with self.subTest(kind=kind, key=key, damage=damage):
                        api = self.drifted()
                        api.damage = (kind, key, damage, expected)
                        with self.assertRaises(MODULE_UNDER_TEST.ApiError) as raised:
                            apply(api)
                        self.assertEqual([], api.writes)
                        self.assertNotIn(KEY_VALUE, str(raised.exception))

    def test_the_table_names_the_ids_of_each_kind_and_they_are_documented_as_uuids(self) -> None:
        """The public API's schema (airbyte-platform, api.yaml) gives every ID of these kinds as ``format: UUID``."""
        self.assertEqual(
            {
                "workspace": {"workspaceId"},
                "source_definition": {"id"},
                "destination_definition": {"id"},
                "source": {"sourceId", "definitionId"},
                "destination": {"destinationId", "definitionId"},
                "connection": {"connectionId", "sourceId", "destinationId"},
            },
            {kind: set(keys) for kind, keys in MODULE_UNDER_TEST.ID_KEYS.items()},
        )
        for kind, keys in MODULE_UNDER_TEST.ID_KEYS.items():
            self.assertLessEqual(set(keys), set(MODULE_UNDER_TEST.ANSWER_SHAPES[kind]), kind)

    def test_each_id_of_each_kind_empty_or_not_a_uuid_is_refused_and_nothing_is_written(self) -> None:
        for kind, keys in MODULE_UNDER_TEST.ID_KEYS.items():
            for key in keys:
                for damage in ("empty", "not a uuid"):
                    with self.subTest(kind=kind, key=key, damage=damage):
                        api = self.drifted()
                        api.damage = (kind, key, damage, str)
                        with self.assertRaises(MODULE_UNDER_TEST.ApiError) as raised:
                            apply(api)
                        self.assertEqual([], api.writes)
                        self.assertIn(key, str(raised.exception))

    def test_an_id_in_the_form_of_a_uuid_in_either_case_is_accepted(self) -> None:
        self.assertTrue(MODULE_UNDER_TEST.is_id("0b0d1d3a-4d9b-4a5e-8b0c-1f2e3d4c5b6a"))
        self.assertTrue(MODULE_UNDER_TEST.is_id("0B0D1D3A-4D9B-4A5E-8B0C-1F2E3D4C5B6A"))
        for text in ("", " ", "x", "0b0d1d3a4d9b4a5e8b0c1f2e3d4c5b6a", "0b0d1d3a-4d9b-4a5e-8b0c-1f2e3d4c5b6", "0b0d1d3a-4d9b-4a5e-8b0c-1f2e3d4c5b6a\n", "../a"):
            self.assertFalse(MODULE_UNDER_TEST.is_id(text), repr(text))

    def test_the_id_in_the_answer_of_a_create_is_held_to_the_same_form(self) -> None:
        for answer in ({"sourceId": ""}, {"sourceId": "not-a-uuid"}, {"sourceId": 5}, {}):
            with self.subTest(answer=answer), self.assertRaises(MODULE_UNDER_TEST.ApiError):
                MODULE_UNDER_TEST.created_id(answer, "sourceId")

    def test_an_item_that_is_not_an_object_is_refused_and_nothing_is_written(self) -> None:
        api = self.drifted()
        api.damage = ("connection", None, "not an object", None)
        with self.assertRaises(MODULE_UNDER_TEST.ApiError):
            apply(api)
        self.assertEqual([], api.writes)

    def test_the_refusal_names_the_kind_and_the_key_and_no_value(self) -> None:
        api = self.drifted()
        api.damage = ("connection", "connectionId", "missing", str)
        with self.assertRaises(MODULE_UNDER_TEST.ApiError) as raised:
            apply(api)
        self.assertIn("connection", str(raised.exception))
        self.assertIn("connectionId", str(raised.exception))

    def test_an_answer_of_a_create_without_its_id_is_a_named_error(self) -> None:
        api = FakeAirbyte()
        real = api.write

        def without_id(method: str, parts: list[str], body: dict[str, Any]) -> dict[str, Any]:
            record = real(method, parts, body)
            return {name: value for name, value in record.items() if name not in ("sourceId", "id")}

        with mock.patch.object(api, "write", without_id), self.assertRaises(MODULE_UNDER_TEST.ApiError) as raised:
            apply(api)
        self.assertIn("answer", str(raised.exception))


class RecordPermissionTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="airbyte_record_permission_")
        self.addCleanup(folder.cleanup)
        self.path = Path(folder.name) / "record.json"

    def test_an_unchanged_run_repairs_a_record_with_broader_permissions_and_reports_it_once(self) -> None:
        api = FakeAirbyte()
        MODULE_UNDER_TEST.apply_document(api, document(), KEYS, self.path)
        self.path.chmod(0o666)
        api.writes.clear()
        self.assertEqual(["record file"], MODULE_UNDER_TEST.apply_document(api, document(), KEYS, self.path))
        self.assertEqual(0o400, self.path.stat().st_mode & 0o777)
        self.assertEqual([], api.writes)
        self.assertEqual([], MODULE_UNDER_TEST.apply_document(api, document(), KEYS, self.path))

    def test_a_record_with_another_owner_group_is_repaired_too(self) -> None:
        other = [group for group in os.getgroups() if group != os.getgid()]
        if not other:
            self.skipTest("the test user belongs to one group only")
        api = FakeAirbyte()
        MODULE_UNDER_TEST.apply_document(api, document(), KEYS, self.path)
        os.chown(self.path, -1, other[0])
        self.assertEqual(["record file"], MODULE_UNDER_TEST.apply_document(api, document(), KEYS, self.path))
        self.assertEqual(os.getgid(), self.path.stat().st_gid)

    def test_a_run_that_writes_the_record_anew_reports_no_extra_change(self) -> None:
        api = FakeAirbyte()
        self.assertEqual(
            ["source definition", "destination definition", "source", "destination", "connection"],
            MODULE_UNDER_TEST.apply_document(api, document(), KEYS, self.path),
        )


class SocketDeadlineTest(unittest.TestCase):
    """The deadline of an exchange holds on real local sockets: a timer shuts the socket down at the deadline, whatever the server does."""

    MARGIN = 0.25

    def serve(self, behave: Callable[[socket.socket], None]) -> str:
        class Handler(socketserver.BaseRequestHandler):
            def handle(self) -> None:
                self.request.recv(65536)
                with contextlib.suppress(OSError):
                    behave(self.request)

        server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
        thread.start()

        def stop() -> None:
            server.shutdown()
            server.server_close()
            thread.join(5)

        self.addCleanup(stop)
        return f"http://127.0.0.1:{server.server_address[1]}/api"

    @staticmethod
    def answer(body: bytes = b"{}") -> bytes:
        return b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body

    def timed(self, base: str, deadline: float) -> tuple[float, str]:
        client = MODULE_UNDER_TEST.HttpApi(base, lambda: "t", timeout_seconds=deadline)
        started = time.monotonic()
        with self.assertRaises(MODULE_UNDER_TEST.ApiError) as raised:
            client.request("GET", "/workspaces")
        return time.monotonic() - started, str(raised.exception)

    def test_control_a_quick_answer_comes_back(self) -> None:
        base = self.serve(lambda sock: sock.sendall(self.answer(b'{"data": []}')))
        self.assertEqual({"data": []}, MODULE_UNDER_TEST.HttpApi(base, lambda: "t", timeout_seconds=1).request("GET", "/workspaces"))

    def test_slow_headers_end_at_the_deadline(self) -> None:
        def slow(sock: socket.socket) -> None:
            time.sleep(1.2)
            sock.sendall(self.answer())

        elapsed, message = self.timed(self.serve(slow), 0.25)
        self.assertLess(elapsed, 0.25 + self.MARGIN)
        self.assertIn("did not answer within 0.25 seconds", message)

    def test_a_slow_body_ends_at_the_deadline(self) -> None:
        def slow(sock: socket.socket) -> None:
            sock.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n" + b"x" * 10)
            time.sleep(1.5)

        elapsed, message = self.timed(self.serve(slow), 0.25)
        self.assertLess(elapsed, 0.25 + self.MARGIN)
        self.assertIn("did not answer within 0.25 seconds", message)

    def test_a_trickle_of_one_byte_at_a_time_ends_at_the_deadline(self) -> None:
        def trickle(sock: socket.socket) -> None:
            sock.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 100000\r\n\r\n")
            for _ in range(200):
                sock.sendall(b"x")
                time.sleep(0.02)

        elapsed, message = self.timed(self.serve(trickle), 0.25)
        self.assertLess(elapsed, 0.25 + self.MARGIN)
        self.assertIn("did not answer within", message)

    def test_a_server_that_sends_the_headers_slowly_a_byte_at_a_time_ends_at_the_deadline(self) -> None:
        def trickle(sock: socket.socket) -> None:
            for byte in b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}":
                sock.sendall(bytes([byte]))
                time.sleep(0.03)

        elapsed, _ = self.timed(self.serve(trickle), 0.25)
        self.assertLess(elapsed, 0.25 + self.MARGIN)

    def test_a_server_that_accepts_and_never_answers_ends_at_the_deadline(self) -> None:
        elapsed, _ = self.timed(self.serve(lambda sock: time.sleep(2)), 0.25)
        self.assertLess(elapsed, 0.25 + self.MARGIN)

    def test_the_total_of_all_exchanges_is_bounded_by_the_run_budget(self) -> None:
        base = self.serve(lambda sock: sock.sendall(self.answer()))
        budget = MODULE_UNDER_TEST.Budget(0.4)
        client = MODULE_UNDER_TEST.HttpApi(base, lambda: "t", timeout_seconds=5, budget=budget)
        client.request("GET", "/a")
        time.sleep(0.5)
        with self.assertRaises(MODULE_UNDER_TEST.ApiError) as raised:
            client.request("GET", "/b")
        self.assertIn("run deadline", str(raised.exception))

    def test_an_exchange_gets_the_smaller_of_its_own_deadline_and_the_time_left_in_the_run(self) -> None:
        def slow(sock: socket.socket) -> None:
            time.sleep(2)

        base = self.serve(slow)
        client = MODULE_UNDER_TEST.HttpApi(base, lambda: "t", timeout_seconds=5, budget=MODULE_UNDER_TEST.Budget(0.3))
        started = time.monotonic()
        with self.assertRaises(MODULE_UNDER_TEST.ApiError):
            client.request("GET", "/a")
        self.assertLess(time.monotonic() - started, 0.3 + self.MARGIN)


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

        result: tuple[str, str] = MODULE_UNDER_TEST.client_credentials(run, ["k3s", "kubectl"], "airbyte", MODULE_UNDER_TEST.Budget(1000))
        self.assertEqual("get", calls[0][calls[0].index("kubectl") + 1])
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


class Clock:
    """The time that a test lets pass: each read of a reply advances it."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class HttpTest(unittest.TestCase):
    class Socket:
        def __init__(self) -> None:
            self.timeouts: list[float] = []

        def settimeout(self, value: float) -> None:
            self.timeouts.append(value)

    class Connection:
        def __init__(self, status: int, text: str, failure: Exception | None, log: list[Any], clock: Clock, header_seconds: float, read_seconds: float) -> None:
            self.status, self.text, self.failure, self.log = status, text, failure, log
            self.clock, self.header_seconds, self.read_seconds = clock, header_seconds, read_seconds
            self.closed = False
            self.sock = HttpTest.Socket()
            self.sent = 0

        def request(self, method: str, url: str, body: bytes | None, headers: dict[str, str]) -> None:
            self.log.append((method, url, body, headers))
            if isinstance(self.failure, OSError):
                raise self.failure

        def getresponse(self) -> HttpTest.Connection:
            if self.failure:
                raise self.failure
            self.clock.now += self.header_seconds
            return self

        def read(self, amount: int | None = None) -> bytes:
            data = self.text.encode()[self.sent :] if amount is None else self.text.encode()[self.sent : self.sent + amount]
            self.sent += len(data)
            self.clock.now += self.read_seconds
            return data

        def close(self) -> None:
            self.closed = True

    def api(
        self,
        status: int = 200,
        text: str = "{}",
        failure: Exception | None = None,
        base: str = "http://127.0.0.1:8000/api/public/v1",
        *,
        header_seconds: float = 0.0,
        read_seconds: float = 0.0,
        limit: float = 7,
    ) -> Any:
        log: list[Any] = []
        made: list[HttpTest.Connection] = []
        clock = Clock()

        def connect(host: str, port: int, timeout: float) -> HttpTest.Connection:
            log.append((host, port, timeout))
            made.append(HttpTest.Connection(status, text, failure, log, clock, header_seconds, read_seconds))
            return made[-1]

        client = MODULE_UNDER_TEST.HttpApi(base, lambda: HIDDEN_THREE, timeout_seconds=limit, connect=connect, clock=clock)
        client.log, client.made = log, made
        return client

    def test_an_exchange_that_ended_after_its_deadline_is_a_timeout_even_if_the_answer_came(self) -> None:
        client = self.api(text='{"data": []}', read_seconds=1.0, limit=0.5)
        with self.assertRaises(MODULE_UNDER_TEST.ApiError) as raised:
            client.request("GET", "/workspaces")
        self.assertIn("did not answer within 0.5 seconds", str(raised.exception))

    def test_a_reply_in_many_quick_pieces_is_put_together_whole(self) -> None:
        body = {"data": [{"name": "n" * 50} for _ in range(5000)]}
        client = self.api(text=json.dumps(body), read_seconds=0.001, limit=5)
        self.assertEqual(body, client.request("GET", "/sources"))

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
