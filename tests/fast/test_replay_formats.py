"""The Avro formats of the stream: their files, the topic and registry subject of each, and the match between each schema and the payload the replay builds.

Failure modes covered here:

- A schema field and the payload field of the same event drift apart, so the message cannot be encoded or a field is dropped without notice.
- A field of the wrong type is declared, such as a time as a double, so a case time loses its exactness.
- Two formats share a topic or a subject, or a format has no schema file, so a registration overwrites another format.
- The compatibility rule is weaker than the guarantee that a new reader can read every older message that Kafka still holds.

The registry's own acceptance and refusal of a change is proved against a real registry by ``tests/e2e/stack_replay.py``.
"""

from __future__ import annotations

import json
import unittest
from typing import Any

from replay.events.formats import COMPATIBILITY_RULE, FORMATS, KIND_FORMAT, schema_of
from replay.events.schedule import admitted_occurrences
from replay.events.stream import occurrence_events, outgoing
from tests.fast.replay_support import RUN_ID, boundary_recordings, boundary_schedule

AVRO_PYTHON_TYPES: dict[str, type | tuple[type, ...]] = {"string": str, "int": int, "long": int, "double": float, "boolean": bool}


def conforms(value: Any, avro_type: Any) -> bool:
    if isinstance(avro_type, str):
        expected = AVRO_PYTHON_TYPES[avro_type]
        return isinstance(value, expected) and not (avro_type != "boolean" and isinstance(value, bool))
    if avro_type["type"] == "array":
        return isinstance(value, list) and all(conforms(item, avro_type["items"]) for item in value)
    if avro_type["type"] == "enum":
        return value in avro_type["symbols"]
    raise AssertionError(f"a type this test does not know: {avro_type}")


class Schemas(unittest.TestCase):
    def test_each_format_has_a_record_schema_of_its_own_name_in_one_namespace(self) -> None:
        for key, format_ in FORMATS.items():
            with self.subTest(format=key):
                schema = schema_of(key)
                self.assertEqual("record", schema["type"])
                self.assertEqual("surgical.stream", schema["namespace"])
                self.assertTrue(schema["name"])
                self.assertEqual(len(schema["fields"]), len({field["name"] for field in schema["fields"]}))
                self.assertEqual(f"{format_.topic}-value", format_.subject)

    def test_no_two_formats_share_a_topic_a_subject_or_a_record_name(self) -> None:
        self.assertEqual(5, len(FORMATS))
        for attribute in ("topic", "subject"):
            values = [getattr(format_, attribute) for format_ in FORMATS.values()]
            self.assertEqual(len(values), len(set(values)), attribute)
        names = [schema_of(key)["name"] for key in FORMATS]
        self.assertEqual(len(names), len(set(names)))

    def test_every_kind_the_replay_sends_has_a_format_and_the_five_formats_are_the_five_the_slice_names(self) -> None:
        self.assertEqual({"numeric", "waveform", "string", "final_score", "case_end"}, set(KIND_FORMAT))
        self.assertEqual({"numeric", "waveform", "string_event", "case_lifecycle", "feature_ready"}, set(FORMATS))
        self.assertEqual(KIND_FORMAT["final_score"], KIND_FORMAT["case_end"])

    def test_the_payload_of_each_kind_has_exactly_the_fields_and_the_types_of_its_schema(self) -> None:
        recordings = boundary_recordings()
        schedule = boundary_schedule(rooms=2, window_us=40_000_000)
        seen = set()
        for scheduled in admitted_occurrences(schedule):
            recording = recordings[scheduled.occurrence.case_id]
            for event in occurrence_events(recording):
                payload = outgoing(RUN_ID, scheduled, recording, event).payload
                schema = schema_of(KIND_FORMAT[event.kind])
                seen.add(event.kind)
                with self.subTest(kind=event.kind, event=payload["event_id"]):
                    self.assertEqual({field["name"] for field in schema["fields"]}, set(payload))
                    for field in schema["fields"]:
                        self.assertTrue(conforms(payload[field["name"]], field["type"]), f"{field['name']} = {payload[field['name']]!r}")
        self.assertEqual(set(KIND_FORMAT), seen)

    def test_the_times_and_the_counts_are_integers_so_that_no_time_loses_its_exactness(self) -> None:
        for key in FORMATS:
            fields = {field["name"]: field["type"] for field in schema_of(key)["fields"]}
            for name, avro_type in fields.items():
                if name.endswith("_us") or name in ("room", "case_id", "discriminator"):
                    self.assertIn(avro_type, ("int", "long"), f"{key}.{name}")

    def test_the_registry_checks_each_change_against_the_formats_earlier_versions(self) -> None:
        self.assertEqual("BACKWARD_TRANSITIVE", COMPATIBILITY_RULE)

    def test_the_schema_files_are_plain_json_with_a_document_for_every_field(self) -> None:
        for key in FORMATS:
            schema = json.loads(json.dumps(schema_of(key)))
            self.assertTrue(schema.get("doc"), key)
            for field in schema["fields"]:
                self.assertTrue(field.get("doc"), f"{key}.{field['name']}")
