"""Closed inputs and quiet diagnostics of the ingestion checks: a file with an unknown, missing or mistyped field is refused, and no message echoes a value.

The files are the selection, the pairing records, the check result, the manifest reference and the manifest. Each refusal has a well-formed control.
"""

from __future__ import annotations

import functools
import json
import operator
import os
import stat
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.ingestion import selection
from scripts.ingestion.check import (
    CHECKED_FILE_FIELDS,
    FINDING_FIELDS,
    RESULT_FIELDS,
    CheckResult,
    ChecksumListError,
    PinnedListError,
    ResultFormatError,
)
from scripts.ingestion.cli import InputError, read_input, write_private
from scripts.ingestion.closed import ClosedInputError
from scripts.ingestion.gate import GateRefused
from scripts.ingestion.landing import PAIRING_FIELDS, PairingError, load_pairing_records
from scripts.ingestion.manifest import (
    MANIFEST_FIELDS,
    MANIFEST_LIST_FIELDS,
    MANIFEST_SELECTION_FIELDS,
    REFERENCE_FIELDS,
    ManifestError,
    ManifestRef,
    PublishRefused,
    publish_manifest,
    read_manifest,
)
from scripts.ingestion.notice import FIELD_NAMES, Cause, ClosedError, Code, Notice, Subject, render
from scripts.ingestion.selection import CASE_FILES_FIELDS, LIST_PIN_FIELDS, SELECTION_FIELDS, SELECTION_OPTIONAL, Selection, SelectionError
from scripts.ingestion.store import StoreError
from scripts.sample import candidates, publisher
from scripts.sample.candidates import CREDIT_FIELDS, FILE_PIN_FIELDS, RELEASE_FIELDS, RELEASE_FILE, SOURCE_FIELDS, TRACK_LIST_FIELDS, CandidateError
from scripts.sample.publisher import TRACK_LIST_COLUMNS, PublisherError
from tests.fast.ingestion_support import SOURCE, SOURCE_PREFIX, digest, landed_set, list_text_for, pin_the_synthetic_list, synthetic_pin, tracked_pin

SELECTION: dict[str, Any] = {
    "name": "probe_set",
    "release": "1.0.0",
    "source_bucket": "physionet-open",
    "source_prefix": SOURCE_PREFIX,
    "measurement_period": "2016-08 to 2017-06",
    "checksum_list": {"key": f"{SOURCE_PREFIX}SHA256SUMS.txt", "sha256": digest(b"list")},
    "files": ["clinical_data.csv"],
    "case_files": {"pattern": "vital_files/{:04d}.vital", "first": 1, "last": 2},
}


def edited(document: dict[str, Any], change: Callable[[dict[str, Any]], None]) -> str:
    copy = json.loads(json.dumps(document))
    change(copy)
    return json.dumps(copy)


def case_files_set(**fields: object) -> Callable[[dict[str, Any]], None]:
    def change(document: dict[str, Any]) -> None:
        document["case_files"].update(fields)

    return change


def setUpModule() -> None:
    pin_the_synthetic_list()


class SelectionInputTest(unittest.TestCase):
    def setUp(self) -> None:
        self.enterContext(synthetic_pin(b"list"))

    def test_control_the_complete_selection_loads(self) -> None:
        self.assertEqual(3, len(Selection.from_json(json.dumps(SELECTION)).files))

    def test_an_unknown_missing_or_mistyped_field_is_refused(self) -> None:
        changes: list[Callable[[dict[str, Any]], None]] = [
            lambda document: document.update(fiels=["a.csv"]),
            lambda document: document["checksum_list"].update(extra="x"),
            lambda document: document["case_files"].update(extra="x"),
            lambda document: document.pop("measurement_period"),
            lambda document: document["checksum_list"].pop("key"),
            lambda document: document.update(files="clinical_data.csv"),
            lambda document: document.update(release=1),
            lambda document: document["case_files"].update(first="1"),
            lambda document: document["case_files"].update(first=True),
            lambda document: document.update(files=[1]),
            lambda document: document.update(files=["clinical_data.csv", None]),
        ]
        for number, change in enumerate(changes):
            with self.subTest(change=number), self.assertRaises(SelectionError):
                Selection.from_json(edited(SELECTION, change))

    def test_a_message_never_echoes_a_value_of_the_file(self) -> None:
        for change in (lambda document: document["case_files"].update(first="private-value-1"), lambda document: document.update(**{"private-value-2": 1})):
            with self.assertRaises(SelectionError) as raised:
                Selection.from_json(edited(SELECTION, change))
            self.assertNotIn("private-value", str(raised.exception))

    def test_a_case_files_pattern_that_does_not_format_is_refused_without_echoing_it(self) -> None:
        for pattern in ("{private-value}", "{}{}", "{:04d"):
            document = json.loads(json.dumps(SELECTION))
            document["case_files"]["pattern"] = pattern
            with self.subTest(pattern=pattern), self.assertRaises(SelectionError) as raised:
                Selection.from_json(json.dumps(document))
            self.assertNotIn("private-value", str(raised.exception))


class SelectionEchoTest(unittest.TestCase):
    def setUp(self) -> None:
        self.enterContext(synthetic_pin(b"list"))

    MARKER = "private-value"

    def refused(self, change: Callable[[dict[str, Any]], None]) -> str:
        with self.assertRaises(SelectionError) as raised:
            Selection.from_json(edited(SELECTION, change))
        self.assertNotIn(self.MARKER, str(raised.exception))
        return str(raised.exception)

    def test_a_path_that_is_not_relative_is_named_by_its_position_and_never_by_its_value(self) -> None:
        message = self.refused(lambda document: document.update(files=["clinical_data.csv", f"{self.MARKER}/../x.csv"]))
        self.assertIn("files[2]", message)
        self.assertNotIn("x.csv", message)

    def test_a_case_format_with_an_attribute_or_an_index_is_refused_as_an_invalid_format(self) -> None:
        for pattern in (f"{{0.{self.MARKER}}}", f"{{0[{self.MARKER}]}}", f"{{:{self.MARKER}}}", "{:04d}{:04d}", "{:04d}.vital{", "x", "{:4d}", "{!r:04d}"):
            with self.subTest(pattern=pattern):
                message = self.refused(case_files_set(pattern=pattern))
                self.assertIn("case_files", message)

    def test_control_the_known_case_format_expands(self) -> None:
        for pattern, first in (("vital_files/{:04d}.vital", "vital_files/0001.vital"), ("{:06d}.vital", "000001.vital")):
            with self.subTest(pattern=pattern):
                document = json.loads(json.dumps(SELECTION))
                document["case_files"]["pattern"] = pattern
                self.assertIn(first, Selection.from_json(json.dumps(document)).files)

    def test_a_range_that_is_reversed_or_negative_is_refused(self) -> None:
        for first, last in ((2, 1), (6388, 1), (-1, 2)):
            with self.subTest(first=first, last=last):
                self.refused(case_files_set(first=first, last=last))


def synthetic_result() -> str:
    """The result file of a passed check of the synthetic release, under the pin of that release's list."""
    with synthetic_pin(list_text_for(SOURCE).encode()):
        return landed_set().check().to_json()


class RepeatedFieldTest(unittest.TestCase):
    def setUp(self) -> None:
        self.enterContext(synthetic_pin(b"list"))

    MARKER = "private-value"

    def test_a_field_named_twice_is_refused_whatever_the_file_and_never_echoed(self) -> None:
        selection = json.dumps(SELECTION)
        cases: list[tuple[str, Callable[[str], object], type[Exception]]] = [
            (
                selection.replace('"files": ["clinical_data.csv"]', '"files": ["a.csv", "b.csv"], "files": ["clinical_data.csv"]'),
                Selection.from_json,
                SelectionError,
            ),
            (selection.replace('"first": 1', '"first": 1, "first": 2'), Selection.from_json, SelectionError),
            (selection.replace('"release": "1.0.0"', f'"release": "1.0.0", "{self.MARKER}": 1, "{self.MARKER}": 2'), Selection.from_json, SelectionError),
            ('[{"source_key": "a", "source_key": "b", "landed_key": "c", "size": 1}]', load_pairing_records, PairingError),
            (synthetic_result().replace('"findings"', '"findings": [], "findings"', 1), CheckResult.from_json, ResultFormatError),
            ('{"bucket": "b", "bucket": "c", "key": "k", "version_id": "v", "sha256": "s"}', ManifestRef.from_json, ManifestError),
        ]
        for number, (text, load, error) in enumerate(cases):
            with self.subTest(case=number), self.assertRaises(error) as raised:
                load(text)
            self.assertNotIn(self.MARKER, str(raised.exception))

    def test_control_the_same_files_without_a_repeated_field_load(self) -> None:
        self.assertEqual(3, len(Selection.from_json(json.dumps(SELECTION)).files))
        self.assertEqual(1, len(load_pairing_records('[{"source_key": "a", "landed_key": "c", "size": 1}]')))


class PairingInputTest(unittest.TestCase):
    RECORD = {"source_key": "a/b", "landed_key": "landing/x/b", "size": 3}

    def test_a_record_with_an_unknown_field_is_refused(self) -> None:
        self.assertEqual(1, len(load_pairing_records(json.dumps([self.RECORD]))))
        with self.assertRaises(PairingError) as raised:
            load_pairing_records(json.dumps([{**self.RECORD, "synced": "private-value"}]))
        self.assertNotIn("private-value", str(raised.exception))
        with self.assertRaises(PairingError):
            load_pairing_records(json.dumps([{**self.RECORD, "size": True}]))


class ResultInputTest(unittest.TestCase):
    def document(self) -> dict[str, Any]:
        document: dict[str, Any] = json.loads(landed_set().check().to_json())
        return document

    def test_control_a_result_loads_and_writes_back_the_same_text(self) -> None:
        text = landed_set().check().to_json()
        self.assertEqual(text, CheckResult.from_json(text).to_json())

    def test_an_unknown_missing_or_mistyped_field_is_refused(self) -> None:
        changes: list[Callable[[dict[str, Any]], None]] = [
            lambda document: document.update(note="x"),
            lambda document: document.pop("list_sha256"),
            lambda document: document["files"][0].update(extra="x"),
            lambda document: document["files"][0].pop("version_id"),
            lambda document: document["files"][0].update(size="3"),
            lambda document: document["files"][0].update(size=True),
            lambda document: document["files"][0].update(version_id=7),
            lambda document: document.update(file_count="3"),
            lambda document: document.update(findings=[{"position": 1, "cause": "missing", "source_key": "", "extra": 1}]),
            lambda document: document.update(findings=[{"position": 1, "cause": "private-value", "source_key": ""}]),
            lambda document: document.update(findings=[{"position": "1", "cause": "missing", "source_key": ""}]),
            lambda document: document.update(files={}),
        ]
        for number, change in enumerate(changes):
            with self.subTest(change=number), self.assertRaises(ResultFormatError):
                CheckResult.from_json(edited(self.document(), change))

    def test_a_result_names_its_format_and_one_of_an_earlier_check_is_refused_with_the_advice_to_rerun_check(self) -> None:
        self.assertEqual("check_result_3", self.document().get("format"))
        earlier = self.document()
        earlier.pop("format", None)
        with self.assertRaisesRegex(ResultFormatError, "rerun check"):
            CheckResult.from_json(json.dumps(earlier))
        with self.assertRaisesRegex(ResultFormatError, "rerun check"):
            CheckResult.from_json(json.dumps(earlier | {"format": "check_result_1"}))
        with self.assertRaisesRegex(ResultFormatError, "rerun check"):
            CheckResult.from_json(json.dumps(earlier | {"format": "check_result_2"}))

    def test_text_that_is_not_json_is_refused_without_echoing_it(self) -> None:
        with self.assertRaises(ResultFormatError) as raised:
            CheckResult.from_json("private-value")
        self.assertNotIn("private-value", str(raised.exception))


class ReferenceInputTest(unittest.TestCase):
    REFERENCE = {"bucket": "example-bucket", "key": f"manifests/probe_set/{digest(b'm')}.json", "version_id": "v1", "sha256": digest(b"m")}

    def test_control_a_reference_loads(self) -> None:
        self.assertEqual(ManifestRef(**self.REFERENCE), ManifestRef.from_json(json.dumps(self.REFERENCE)))

    def test_an_unknown_missing_or_mistyped_field_is_refused(self) -> None:
        for change in (lambda d: d.update(note="x"), lambda d: d.pop("version_id"), lambda d: d.update(version_id=1), lambda d: d.update(sha256=None)):
            with self.subTest(), self.assertRaises(ManifestError):
                ManifestRef.from_json(edited(self.REFERENCE, change))
        with self.assertRaises(ManifestError):
            ManifestRef.from_json("[]")


class CommandErrorTest(unittest.TestCase):
    def test_every_error_that_an_input_can_raise_is_reported_and_none_is_a_traceback(self) -> None:
        raised: tuple[type[Exception], ...] = (SelectionError, PairingError, ResultFormatError, ManifestError, InputError, ChecksumListError, PinnedListError)
        raised += (PublishRefused, GateRefused, StoreError, ClosedInputError, PublisherError, CandidateError)
        self.assertEqual([], [error.__name__ for error in raised if not issubclass(error, ClosedError)])
        with self.assertRaises(TypeError):
            SelectionError("private-value")  # type: ignore[arg-type]


class NoticeTest(unittest.TestCase):
    def test_a_notice_takes_only_the_closed_values_its_text_names(self) -> None:
        self.assertEqual("vitaldb/1.0.0/a.csv: missing", render(Notice(Code.FINDING_KEY, key="vitaldb/1.0.0/a.csv", cause=Cause.MISSING)))
        wrong: list[Callable[[], object]] = [
            lambda: Notice(Code.FINDING_KEY, key="private value", cause=Cause.MISSING),
            lambda: Notice(Code.FINDING_KEY, key="private-value", cause=Cause.MISSING),
            lambda: Notice(Code.FINDING_POSITION, position="1", cause=Cause.MISSING),  # type: ignore[arg-type]
            lambda: Notice(Code.FINDING_POSITION, position=True, cause=Cause.MISSING),
            lambda: Notice(Code.OBJECT_FIELDS, subject=Subject.SELECTION, fields=("private_value",)),
            lambda: Notice(Code.CHECK_PASSED, count=1, total=1, digest="private-value"),
            lambda: Notice(Code.NOT_PASSED, count=1),
            lambda: Notice(Code.CANNOT_READ),
            lambda: Notice("private-value"),  # type: ignore[arg-type]
        ]
        for number, make in enumerate(wrong):
            with self.subTest(number), self.assertRaises(TypeError):
                make()


class TrackedPinTest(unittest.TestCase):
    def setUp(self) -> None:
        self.enterContext(tracked_pin())

    def test_a_tracked_pin_that_is_not_valid_stops_every_full_release(self) -> None:
        self.addCleanup(selection.pinned_values.cache_clear)
        with tempfile.TemporaryDirectory(prefix="ingestion_pin_") as folder:
            pin = json.loads(RELEASE_FILE.read_text())
            pin["checksum_list"]["key"] = f"{SOURCE_PREFIX}OTHER.txt"
            broken = Path(folder) / "release.json"
            broken.write_text(json.dumps(pin))
            selection.pinned_values.cache_clear()
            with mock.patch.object(selection, "RELEASE_FILE", broken), self.assertRaisesRegex(SelectionError, "tracked release pin is not valid"):
                selection.pinned_release()


class FrozenAuthorityTest(unittest.TestCase):
    """A field set that closes an input, and a manifest a reader holds, cannot be changed by the code that holds them."""

    def test_the_field_sets_of_the_closed_inputs_cannot_be_changed(self) -> None:
        field_sets: tuple[Any, ...] = (CHECKED_FILE_FIELDS, RELEASE_FIELDS, SOURCE_FIELDS, FILE_PIN_FIELDS, TRACK_LIST_FIELDS, CREDIT_FIELDS)
        columns: Any = TRACK_LIST_COLUMNS
        edits: list[Callable[[], object]] = [functools.partial(operator.setitem, fields, "size_bytes", object) for fields in field_sets]
        edits.append(functools.partial(operator.iadd, columns, ["extra"]))
        for number, edit in enumerate(edits):
            with self.subTest(edit=number), self.assertRaises((TypeError, AttributeError)):
                edit()

    def test_every_other_module_level_authority_value_cannot_be_changed(self) -> None:
        mappings: tuple[Any, ...] = (
            SELECTION_FIELDS,
            SELECTION_OPTIONAL,
            LIST_PIN_FIELDS,
            CASE_FILES_FIELDS,
            PAIRING_FIELDS,
            RESULT_FIELDS,
            FINDING_FIELDS,
            REFERENCE_FIELDS,
            MANIFEST_FIELDS,
            MANIFEST_SELECTION_FIELDS,
            MANIFEST_LIST_FIELDS,
            candidates.SOURCE_VALUES,
            candidates.TABLE_SUBJECTS,
            publisher.KEY_READS,
            publisher.URL_READS,
            publisher.MISSING_PROGRAM,
        )
        edits: list[Callable[[], object]] = [functools.partial(operator.setitem, mapping, "size_bytes", object) for mapping in mappings]
        slot: Any = candidates.SLOTS[0]
        names: Any = FIELD_NAMES
        files: Any = selection.full_release_files()
        edits += [
            lambda: vars(slot).update(rule="another rule"),
            lambda: setattr(slot, "rule", "another rule"),
            lambda: names.add("private_value"),
            lambda: files.append("private-value.csv"),
            lambda: setattr(Code, "NOT_JSON", "private-value"),
            lambda: setattr(Cause, "MISSING", "private-value"),
        ]
        for number, edit in enumerate(edits):
            with self.subTest(edit=number), self.assertRaises((TypeError, AttributeError)):
                edit()
        self.assertNotIn("private_value", FIELD_NAMES)
        self.assertNotIn("another rule", [item.rule for item in candidates.SLOTS])

    def test_the_selection_and_list_of_a_read_manifest_cannot_be_changed(self) -> None:
        landed = landed_set()
        manifest = read_manifest(landed.store, publish_manifest(landed.store, landed.selection, landed.check()))
        mappings: tuple[Any, ...] = (manifest.selection, manifest.checksum_list)
        for mapping in mappings:
            with self.assertRaises(TypeError):
                mapping["sha256"] = "0" * 64


class FileHelperTest(unittest.TestCase):
    def test_a_file_that_cannot_be_read_is_named_by_its_role_only(self) -> None:
        with tempfile.TemporaryDirectory(prefix="ingestion_inputs_") as folder:
            readable = Path(folder) / "present.json"
            readable.write_text("{}")
            self.assertEqual("{}", read_input(readable, Subject.SELECTION))
            missing = Path(folder) / "private-folder" / "private-value.json"
            with self.assertRaises(InputError) as raised:
                read_input(missing, Subject.SELECTION)
            self.assertEqual("cannot read the selection", str(raised.exception))
            self.assertNotIn(folder, str(raised.exception))

    def test_a_result_or_reference_is_written_for_its_owner_only_is_never_replaced_and_a_failed_write_names_no_path(self) -> None:
        with tempfile.TemporaryDirectory(prefix="ingestion_inputs_") as folder:
            target = Path(folder) / "reference.json"
            write_private(target, "one\n")
            self.assertEqual(0o600, stat.S_IMODE(os.stat(target).st_mode))
            with self.assertRaises(ClosedInputError):
                write_private(target, "two\n")
            self.assertEqual("one\n", target.read_text())
            blocked = target / "inside" / "x.json"
            with self.assertRaises(ClosedInputError) as raised:
                write_private(blocked, "x")
            self.assertNotIn(folder, str(raised.exception))


if __name__ == "__main__":
    unittest.main()
