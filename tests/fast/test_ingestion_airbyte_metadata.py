"""The pairing records are made from the metadata records that Airbyte writes for each copied file, and the check refuses a pairing that is not true.

Airbyte names a landed key from the sync that wrote it, so the key of a file is read from its metadata record. The form of that record and of the key is the
documented one, which the host's fixed-sample copy confirms. These cases show that a wrong reading of the form can never pass the check: a path that names no
landed object, a file copied twice and a file with no record each fail it. The store is the stand-in store of the ingestion tests.
"""

from __future__ import annotations

import contextlib
import dataclasses
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.ingestion import cli
from scripts.ingestion.landing import PairingError, PairingRecord, pairings_from_metadata
from tests.fast.ingestion_support import SOURCE_PREFIX, Landed, landed_set, pin_the_synthetic_list
from tests.fast.memory_store import MemoryStore

CONFIGURATION = {"AWS_PROFILE": "example_profile", "AWS_REGION": "us-west-2", "S3_BUCKET": "example-bucket"}
MARKER = "private-value"


def setUpModule() -> None:
    pin_the_synthetic_list()


def record(path: str, key: str, size: int, *, joined: bool = False) -> str:
    """One metadata line, in the form of Airbyte's documentation: a folder and a file name, a byte count and the path of the landed file."""
    folder, _, name = path.rpartition("/")
    data = {"file_name": path if joined else name, "bytes": size, "source_uri": "s3://example", "_airbyte_file_path": "/" + key.removeprefix("landing/")}
    if not joined:
        data["folder"] = folder
    return json.dumps({"_airbyte_raw_id": "x", "_airbyte_data": data})


class ParseTest(unittest.TestCase):
    def test_a_record_pairs_its_source_key_its_landed_key_and_its_size(self) -> None:
        text = record("vital_files/0001.vital", "landing/Files/s1/vital_files/0001.vital", 12) + "\n"
        self.assertEqual(
            (PairingRecord(f"{SOURCE_PREFIX}vital_files/0001.vital", "landing/Files/s1/vital_files/0001.vital", 12),),
            pairings_from_metadata([text], SOURCE_PREFIX),
        )

    def test_a_file_name_that_holds_its_folder_gives_the_same_source_key(self) -> None:
        folded = record("vital_files/0001.vital", "landing/Files/s1/a", 12, joined=True)
        split = record("vital_files/0001.vital", "landing/Files/s1/a", 12)
        self.assertEqual(pairings_from_metadata([folded], SOURCE_PREFIX), pairings_from_metadata([split], SOURCE_PREFIX))

    def test_a_file_with_no_folder_and_records_of_several_files_keep_their_order(self) -> None:
        lines = [
            record("clinical_data.csv", "landing/Files/s1/clinical_data.csv", 5),
            record("vital_files/0002.vital", "landing/Files/s1/vital_files/0002.vital", 9),
        ]
        found = pairings_from_metadata(["\n".join(lines) + "\n"], SOURCE_PREFIX)
        self.assertEqual([f"{SOURCE_PREFIX}clinical_data.csv", f"{SOURCE_PREFIX}vital_files/0002.vital"], [item.source_key for item in found])

    def test_the_records_of_several_metadata_files_join_and_an_empty_file_adds_none(self) -> None:
        first = record("vital_files/0001.vital", "landing/Files/s1/vital_files/0001.vital", 1) + "\n"
        second = record("vital_files/0002.vital", "landing/Files/s2/vital_files/0002.vital", 2) + "\n"
        self.assertEqual(2, len(pairings_from_metadata([first, "", second], SOURCE_PREFIX)))

    def test_a_line_that_is_not_a_record_is_refused_and_names_the_line_and_no_value(self) -> None:
        good = record("vital_files/0001.vital", "landing/Files/s1/vital_files/0001.vital", 1)
        bad_lines = [
            "not json " + MARKER,
            "[]",
            json.dumps({"_airbyte_data": {"file_name": MARKER}}),
            json.dumps({"_airbyte_data": {"file_name": "a.vital", "bytes": True, "_airbyte_file_path": "/x"}}),
            json.dumps({"_airbyte_data": {"file_name": "a.vital", "bytes": -1, "_airbyte_file_path": "/x"}}),
            json.dumps({"_airbyte_data": {"file_name": "../a.vital", "bytes": 1, "_airbyte_file_path": "/x"}}),
            json.dumps({"_airbyte_data": {"file_name": "a.vital", "bytes": 1, "_airbyte_file_path": ""}}),
            json.dumps({"_airbyte_data": {"file_name": MARKER + " a", "bytes": 1, "_airbyte_file_path": "/x"}}),
        ]
        for bad in bad_lines:
            with self.subTest(bad=bad[:40]), self.assertRaises(PairingError) as raised:
                pairings_from_metadata([good + "\n" + bad + "\n"], SOURCE_PREFIX)
            self.assertIn("line 2", str(raised.exception))
            self.assertNotIn(MARKER, str(raised.exception))


class BoundaryTest(unittest.TestCase):
    """A metadata path is a path below the landing prefix, and a landed file belongs to one source."""

    def refused(self, path: str) -> str:
        line = json.dumps({"_airbyte_data": {"file_name": "a.vital", "bytes": 1, "_airbyte_file_path": path}})
        good = record("vital_files/0001.vital", "landing/Files/s1/vital_files/0001.vital", 1)
        with self.assertRaises(PairingError) as raised:
            pairings_from_metadata([good + "\n" + line + "\n"], SOURCE_PREFIX)
        self.assertIn("line 2", str(raised.exception))
        self.assertNotIn(path, str(raised.exception))
        return str(raised.exception)

    def test_a_file_path_that_leaves_the_landing_prefix_or_is_not_a_plain_path_is_refused(self) -> None:
        for path in ("../x", "/../x", "/a/../b", "/a/./b", "//a/b", "/a//b", "/", "/a/b/", "/a\\b", "/a/..", "/./a", "/a/b\x00c"):
            with self.subTest(path=path):
                self.refused(path)

    def test_a_plain_path_with_dots_and_spaces_in_a_name_is_accepted(self) -> None:
        for path in ("/Files/2026_11_22_1763800000000_/vital_files/0001.vital", "Files/s1/a.b.c", "/Files/s 1/a-b_c.vital", "/.hidden/a..b"):
            with self.subTest(path=path):
                line = json.dumps({"_airbyte_data": {"file_name": "0001.vital", "folder": "vital_files", "bytes": 1, "_airbyte_file_path": path}})
                found = pairings_from_metadata([line], SOURCE_PREFIX)
                self.assertEqual("landing/" + path.lstrip("/"), found[0].landed_key)

    def test_two_sources_pointing_at_one_landed_file_are_refused_in_the_pairing_step(self) -> None:
        one = record("vital_files/0001.vital", "landing/Files/s1/shared", 1)
        two = record("vital_files/0002.vital", "landing/Files/s1/shared", 1)
        with self.assertRaises(PairingError) as raised:
            pairings_from_metadata([one + "\n", two + "\n"], SOURCE_PREFIX)
        self.assertIn("record 2", str(raised.exception))
        self.assertNotIn("shared", str(raised.exception))

    def test_a_source_that_has_two_records_is_left_to_the_check_as_a_duplicate_pairing(self) -> None:
        one = record("vital_files/0001.vital", "landing/Files/s1/a", 1)
        two = record("vital_files/0001.vital", "landing/Files/s2/a", 1)
        self.assertEqual(2, len(pairings_from_metadata([one + "\n", two + "\n"], SOURCE_PREFIX)))


class CommandTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="ingestion_metadata_")
        self.addCleanup(folder.cleanup)
        self.folder = Path(folder.name)
        self.landed = landed_set()
        self.store: MemoryStore = self.landed.store

    def metadata_lines(self, landed: Landed, *, skip: tuple[str, ...] = (), shift: str = "") -> str:
        return "".join(record(path, key + shift, landed.sizes[path]) + "\n" for path, key in landed.keys.items() if path not in skip)

    def put_metadata(self, text: str, name: str = "landing/meta/vitaldb_files/part_0.jsonl") -> None:
        self.store.put_object(name, text.encode())

    def run_main(self, *arguments: str) -> tuple[int, str, str]:
        output, errors = io.StringIO(), io.StringIO()
        with (
            mock.patch.object(cli, "load_configuration", return_value=CONFIGURATION),
            mock.patch.object(cli, "S3Store", return_value=self.store),
            contextlib.redirect_stdout(output),
            contextlib.redirect_stderr(errors),
        ):
            code = cli.main(list(arguments))
        return code, output.getvalue(), errors.getvalue()

    def pairings(self) -> tuple[int, str, str]:
        return self.run_main("pairings", "--pairings", str(self.folder / "pairings.json"))

    def check(self) -> tuple[int, str, str]:
        (self.folder / "selection.json").write_text(
            json.dumps(
                {
                    "name": self.landed.selection.name,
                    "release": self.landed.selection.release,
                    "source_bucket": self.landed.selection.source_bucket,
                    "source_prefix": self.landed.selection.source_prefix,
                    "measurement_period": self.landed.selection.measurement_period,
                    "checksum_list": {"key": self.landed.selection.checksum_list_key, "sha256": self.landed.selection.checksum_list_sha256},
                    "files": list(self.landed.selection.files),
                }
            )
        )
        (self.folder / "SHA256SUMS.txt").write_bytes(self.landed.list_bytes)
        return self.run_main(
            "check",
            "--selection",
            str(self.folder / "selection.json"),
            "--checksum-list",
            str(self.folder / "SHA256SUMS.txt"),
            "--pairings",
            str(self.folder / "pairings.json"),
            "--result",
            str(self.folder / "result.json"),
        )

    def test_control_the_pairings_of_a_complete_copy_pass_the_check(self) -> None:
        self.put_metadata(self.metadata_lines(self.landed))
        code, output, errors = self.pairings()
        self.assertEqual((0, ""), (code, errors), output)
        self.assertIn("3 pairing records", output)
        self.assertEqual(0, self.check()[0])
        self.assertEqual(0o600, (self.folder / "pairings.json").stat().st_mode & 0o777)

    def test_only_the_metadata_files_are_read_and_no_payload(self) -> None:
        self.put_metadata(self.metadata_lines(self.landed))
        self.store.reads.clear()
        self.pairings()
        self.assertEqual({"landing/meta/vitaldb_files/part_0.jsonl"}, {read.key for read in self.store.reads})

    def test_a_path_that_names_no_landed_object_fails_the_check_so_a_wrong_reading_of_the_form_never_passes(self) -> None:
        self.put_metadata(self.metadata_lines(self.landed, shift="_elsewhere"))
        self.assertEqual(0, self.pairings()[0])
        code, _, errors = self.check()
        self.assertEqual(1, code)
        self.assertIn("landed_object_missing", errors)

    def test_a_file_with_no_metadata_record_fails_the_check_as_missing(self) -> None:
        self.put_metadata(self.metadata_lines(self.landed, skip=("vital_files/0002.vital",)))
        self.assertEqual(0, self.pairings()[0])
        code, _, errors = self.check()
        self.assertEqual(1, code)
        self.assertIn("vitaldb/1.0.0/vital_files/0002.vital: missing", errors)

    def test_a_file_copied_twice_fails_the_check_as_a_duplicate_pairing(self) -> None:
        text = self.metadata_lines(self.landed)
        self.put_metadata(text)
        self.put_metadata(text, "landing/meta/vitaldb_files/part_1.jsonl")
        self.assertEqual(0, self.pairings()[0])
        code, _, errors = self.check()
        self.assertEqual(1, code)
        self.assertIn("duplicate_pairing", errors)

    def test_a_listed_key_outside_the_landing_prefix_is_refused_before_it_is_read_and_no_pairing_file_is_written(self) -> None:
        self.put_metadata(self.metadata_lines(self.landed))
        for key in (
            "outside/probe.jsonl",
            "landing/../outside/probe.jsonl",
            "landing//x.jsonl",
            "/landing/x.jsonl",
            "landing/a/../../b.jsonl",
            "landing/./x.jsonl",
        ):
            with self.subTest(key=key):
                self.store.put_object(key, b"x")
                version = self.store.list_versions(key)[0]
                self.store.reads.clear()
                real = self.store.list_versions
                with mock.patch.object(self.store, "list_versions", lambda prefix, real=real, version=version: [*real(prefix), version]):
                    code, _, errors = self.pairings()
                self.assertEqual(1, code)
                self.assertIn("outside the landing prefix", errors)
                self.assertNotIn(key, errors)
                self.assertEqual([], [read for read in self.store.reads if read.key == key])
                self.assertFalse((self.folder / "pairings.json").exists())
                self.store.history.pop(key)

    def test_a_metadata_path_with_a_dot_dot_writes_no_pairing_file(self) -> None:
        self.put_metadata(record("vital_files/0001.vital", "landing/Files/../../outside/a", 1) + "\n")
        code, _, errors = self.pairings()
        self.assertEqual(1, code)
        self.assertIn("line 1", errors)
        self.assertFalse((self.folder / "pairings.json").exists())

    def test_two_sources_that_name_one_landed_file_stop_the_command_and_write_no_pairing_file(self) -> None:
        text = record("vital_files/0001.vital", "landing/Files/s1/shared", 1) + "\n" + record("vital_files/0002.vital", "landing/Files/s1/shared", 1) + "\n"
        self.put_metadata(text)
        code, _, errors = self.pairings()
        self.assertEqual(1, code)
        self.assertIn("record 2", errors)
        self.assertFalse((self.folder / "pairings.json").exists())

    def test_a_metadata_file_that_is_not_valid_stops_the_command_and_writes_no_pairing_file(self) -> None:
        self.put_metadata(f"not json {MARKER}\n")
        code, _, errors = self.pairings()
        self.assertEqual(1, code)
        self.assertIn("line 1", errors)
        self.assertNotIn(MARKER, errors)
        self.assertFalse((self.folder / "pairings.json").exists())

    def test_no_metadata_file_stops_the_command_instead_of_writing_an_empty_list(self) -> None:
        code, _, errors = self.pairings()
        self.assertEqual(1, code)
        self.assertIn("no metadata file", errors)
        self.assertFalse((self.folder / "pairings.json").exists())

    def test_an_older_version_and_a_deleted_metadata_file_are_not_read(self) -> None:
        self.put_metadata(self.metadata_lines(self.landed))
        self.put_metadata("older text that is not a record\n", "landing/meta/vitaldb_files/part_1.jsonl")
        self.put_metadata(self.metadata_lines(self.landed), "landing/meta/vitaldb_files/part_1.jsonl")
        self.store.put_delete_marker("landing/meta/vitaldb_files/part_2.jsonl")
        self.store.put_object("landing/meta/vitaldb_files/part_1.jsonl", self.metadata_lines(self.landed).encode())
        self.store.reads.clear()
        self.assertEqual(0, self.pairings()[0])
        read = [(item.key, item.version_id) for item in self.store.reads]
        current = {
            key: [item.version.version_id for item in stored if item.version.is_latest and not item.version.is_delete_marker]
            for key, stored in self.store.history.items()
        }
        self.assertNotIn("landing/meta/vitaldb_files/part_2.jsonl", [key for key, _ in read])
        self.assertTrue(all(version in current[key] for key, version in read))

    def test_a_rerun_writes_the_same_file(self) -> None:
        self.put_metadata(self.metadata_lines(self.landed))
        self.pairings()
        first = (self.folder / "pairings.json").read_text()
        self.pairings()
        self.assertEqual(first, (self.folder / "pairings.json").read_text())
        self.assertEqual(3, len(json.loads(first)))
        self.assertEqual({"source_key", "landed_key", "size"}, set(json.loads(first)[0]))
        self.assertEqual(
            [
                dataclasses.asdict(item)
                for item in sorted(pairings_from_metadata([self.metadata_lines(self.landed)], SOURCE_PREFIX), key=lambda item: item.source_key)
            ],
            json.loads(first),
        )


if __name__ == "__main__":
    unittest.main()
