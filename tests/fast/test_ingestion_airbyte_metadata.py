"""The pairing records are made from the metadata records that Airbyte writes for each copied file, and the check refuses a pairing that is not true.

Airbyte names a landed key from the sync that wrote it, so the key of a file is read from its metadata record. The form is the one that the pinned connectors
write: the S3 source (``source-s3``, ``stream_reader.upload``) gives the folder and the name of the object's whole key, and the S3 destination
(``destination-s3``, ``FileChunkTask``) gives the whole landed key, which starts with its bucket path (``landing``) and holds a folder of its own sync. A
fixed-sample copy on the host that was read in another form paired no selected file. These cases also show that a wrong reading can never pass the check: a
path that names no landed object, a file copied twice and a file with no record each fail it. The store is the stand-in store of the ingestion tests.

Every record also names its sync (``_airbyte_meta.sync_id``, the number of Airbyte's job). A sync that was stopped can have committed records before Airbyte
saved its checkpoint, so the next sync copies those files again and two syncs hold a record of one file; a file whose checkpoint was saved is not copied
again, so only the stopped sync holds its record. For each file, the records of one sync are taken: a sync that the caller names as succeeded before any
other, and among those the latest. Two records of one file in that sync are still two.
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


def of_sync(data: object, sync: object = 1) -> str:
    """One metadata line with the data and the sync that wrote it."""
    return json.dumps({"_airbyte_raw_id": "x", "_airbyte_meta": {"sync_id": sync, "changes": []}, "_airbyte_data": data})


def record(path: str, key: str, size: int, *, joined: bool = False, sync: int = 1) -> str:
    """One metadata line as the pinned connectors write it: the folder and the name of the object's whole key, a byte count and the whole landed key."""
    folder, _, name = (SOURCE_PREFIX + path).rpartition("/")
    data = {
        "file_name": SOURCE_PREFIX + path if joined else name,
        "bytes": size,
        "source_uri": f"s3://example/{SOURCE_PREFIX}{path}",
        "_airbyte_file_path": key,
    }
    if not joined:
        data["folder"] = folder
    return of_sync(data, sync)


class ParseTest(unittest.TestCase):
    def test_the_record_of_the_pinned_connectors_gives_the_object_key_once_and_the_landed_key_once(self) -> None:
        landed = "landing/vitaldb_files/2026_01_02_1767312000000_/vitaldb/1.0.0/vital_files/0001.vital"
        line = json.dumps(
            {
                "_airbyte_raw_id": "x",
                "_airbyte_meta": {"sync_id": 7, "changes": []},
                "_airbyte_data": {
                    "folder": "vitaldb/1.0.0/vital_files",
                    "file_name": "0001.vital",
                    "bytes": 12,
                    "source_uri": "s3://physionet-open/vitaldb/1.0.0/vital_files/0001.vital",
                    "updated_at": "2026-01-02T00:00:00.000000Z",
                    "_airbyte_file_path": landed,
                },
            }
        )
        self.assertEqual((PairingRecord("vitaldb/1.0.0/vital_files/0001.vital", landed, 12),), pairings_from_metadata([line], SOURCE_PREFIX))

    def test_a_record_whose_folder_is_not_under_the_release_path_is_refused(self) -> None:
        for folder in ("vital_files", "", "other/1.0.0/vital_files", "vitaldb/1.0.0x"):
            with self.subTest(folder=folder), self.assertRaises(PairingError):
                data = {"folder": folder, "file_name": "0001.vital", "bytes": 1, "_airbyte_file_path": "landing/s1/a"}
                pairings_from_metadata([of_sync(data)], SOURCE_PREFIX)

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
            of_sync({"file_name": MARKER}),
            of_sync({"file_name": "vitaldb/1.0.0/a.vital", "bytes": True, "_airbyte_file_path": "landing/x"}),
            of_sync({"file_name": "vitaldb/1.0.0/a.vital", "bytes": -1, "_airbyte_file_path": "landing/x"}),
            of_sync({"file_name": "vitaldb/1.0.0/../a.vital", "bytes": 1, "_airbyte_file_path": "landing/x"}),
            of_sync({"file_name": "vitaldb/1.0.0/a.vital", "bytes": 1, "_airbyte_file_path": ""}),
            of_sync({"file_name": "vitaldb/1.0.0/" + MARKER + " a", "bytes": 1, "_airbyte_file_path": "landing/x"}),
        ]
        for bad in bad_lines:
            with self.subTest(bad=bad[:40]), self.assertRaises(PairingError) as raised:
                pairings_from_metadata([good + "\n" + bad + "\n"], SOURCE_PREFIX)
            self.assertIn("line 2", str(raised.exception))
            self.assertNotIn(MARKER, str(raised.exception))


class SyncTest(unittest.TestCase):
    """For each source key the records of one sync are taken: a named succeeded sync before any other, then the latest."""

    def found(self, lines: list[str], succeeded: tuple[int, ...] = ()) -> list[tuple[str, str]]:
        records = pairings_from_metadata(["\n".join(lines) + "\n"], SOURCE_PREFIX, succeeded)
        return [(item.source_key.removeprefix(SOURCE_PREFIX), item.landed_key) for item in records]

    def line(self, name: str, folder: str, sync: int) -> str:
        return record(f"vital_files/{name}.vital", f"landing/{folder}/{name}", 1, sync=sync)

    def test_a_file_that_a_stopped_sync_and_the_retry_both_recorded_takes_the_retrys_record_and_one_only_the_stopped_sync_recorded_is_kept(self) -> None:
        lines = [self.line("0002", "retry", 12), self.line("0001", "stopped", 11), self.line("0002", "stopped", 11)]
        expected = [("vital_files/0002.vital", "landing/retry/0002"), ("vital_files/0001.vital", "landing/stopped/0001")]
        for succeeded in ((), (12,)):
            with self.subTest(succeeded=succeeded):
                self.assertEqual(expected, self.found(lines, succeeded))

    def test_the_latest_sync_is_the_one_of_the_higher_number_and_not_the_later_line(self) -> None:
        self.assertEqual([("vital_files/0001.vital", "landing/ten/0001")], self.found([self.line("0001", "ten", 10), self.line("0001", "nine", 9)]))

    def test_a_succeeded_sync_wins_over_a_later_sync_that_is_not_named_as_succeeded(self) -> None:
        lines = [self.line("0001", "succeeded", 2), self.line("0001", "later", 3)]
        self.assertEqual([("vital_files/0001.vital", "landing/succeeded/0001")], self.found(lines, (2,)))
        self.assertEqual([("vital_files/0001.vital", "landing/later/0001")], self.found(lines))

    def test_the_latest_of_two_succeeded_syncs_wins_and_a_succeeded_sync_that_wrote_no_record_is_no_fault(self) -> None:
        lines = [self.line("0001", "first", 2), self.line("0001", "second", 4)]
        self.assertEqual([("vital_files/0001.vital", "landing/second/0001")], self.found(lines, (2, 4, 5)))

    def test_two_records_of_one_file_in_the_sync_that_wins_are_both_kept_beside_an_older_sync(self) -> None:
        lines = [self.line("0001", "older", 1), self.line("0001", "one", 2), self.line("0001", "two", 2)]
        self.assertEqual(["landing/one/0001", "landing/two/0001"], [landed for _, landed in self.found(lines, (2,))])

    def test_succeeded_syncs_of_which_none_wrote_a_record_are_refused(self) -> None:
        with self.assertRaises(PairingError) as raised:
            self.found([self.line("0001", "first", 2)], (3, 4))
        self.assertIn("--succeeded-sync", str(raised.exception))

    def test_a_record_that_names_no_sync_as_a_whole_number_of_zero_or_more_is_refused(self) -> None:
        data = {"file_name": f"{SOURCE_PREFIX}vital_files/0001.vital", "bytes": 1, "_airbyte_file_path": "landing/s1/a"}
        lines = [json.dumps({"_airbyte_data": data}), json.dumps({"_airbyte_meta": [], "_airbyte_data": data})]
        lines += [of_sync(data, wrong) for wrong in ("1", True, -1, 1.0, None)]
        for line in lines:
            with self.subTest(line=line[:60]), self.assertRaises(PairingError) as raised:
                pairings_from_metadata([line], SOURCE_PREFIX)
            self.assertIn("line 1", str(raised.exception))


class BoundaryTest(unittest.TestCase):
    """A metadata path is a path below the landing prefix, and a landed file belongs to one source."""

    def refused(self, path: str) -> str:
        line = of_sync({"file_name": f"{SOURCE_PREFIX}a.vital", "bytes": 1, "_airbyte_file_path": path})
        good = record("vital_files/0001.vital", "landing/Files/s1/vital_files/0001.vital", 1)
        with self.assertRaises(PairingError) as raised:
            pairings_from_metadata([good + "\n" + line + "\n"], SOURCE_PREFIX)
        self.assertIn("line 2", str(raised.exception))
        self.assertNotIn(path, str(raised.exception))
        return str(raised.exception)

    def test_a_file_path_that_leaves_the_landing_prefix_or_is_not_a_plain_path_is_refused(self) -> None:
        for path in (
            "../x",
            "/../x",
            "landing/a/../b",
            "landing/a/./b",
            "//landing/b",
            "landing//b",
            "/",
            "landing/a/b/",
            "landing/a\\b",
            "landing/a/..",
            "/./landing/a",
            "landing/a/b\x00c",
        ):
            with self.subTest(path=path):
                self.refused(path)

    def test_a_file_path_that_does_not_start_at_the_landing_prefix_is_refused(self) -> None:
        for path in ("/Files/s1/a.vital", "Files/s1/a.vital", "landing", "landing/", "landingx/s1/a", "outside/landing/a"):
            with self.subTest(path=path):
                self.refused(path)

    def test_a_plain_path_with_dots_and_spaces_in_a_name_is_accepted(self) -> None:
        for path in ("/landing/files/2026_11_22_1763800000000_/vital_files/0001.vital", "landing/s1/a.b.c", "landing/s 1/a-b_c.vital", "landing/.hidden/a..b"):
            with self.subTest(path=path):
                line = of_sync({"file_name": "0001.vital", "folder": f"{SOURCE_PREFIX}vital_files", "bytes": 1, "_airbyte_file_path": path})
                found = pairings_from_metadata([line], SOURCE_PREFIX)
                self.assertEqual(path.removeprefix("/"), found[0].landed_key)

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

    def pairings(self, *succeeded: str) -> tuple[int, str, str]:
        named = [argument for number in succeeded for argument in ("--succeeded-sync", number)]
        return self.run_main("pairings", "--pairings", str(self.folder / "pairings.json"), *named)

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

    def stopped_sync_metadata(self, sync: int) -> str:
        """The records that a stopped sync committed for two files, each naming a landed key of its own."""
        paths = list(self.landed.keys)[:2]
        return "".join(record(path, f"landing/stopped/{SOURCE_PREFIX}{path}", self.landed.sizes[path], sync=sync) + "\n" for path in paths)

    def test_records_that_a_stopped_sync_committed_do_not_block_the_retry_that_copied_the_files_again(self) -> None:
        self.put_metadata(self.stopped_sync_metadata(1), "landing/stopped_0.jsonl")
        self.put_metadata("".join(record(path, key, self.landed.sizes[path], sync=2) + "\n" for path, key in self.landed.keys.items()))
        for succeeded in ((), ("2",), ("2", "3")):
            with self.subTest(succeeded=succeeded):
                for name in ("pairings.json", "result.json"):
                    (self.folder / name).unlink(missing_ok=True)
                code, output, errors = self.pairings(*succeeded)
                self.assertEqual((0, ""), (code, errors), output)
                self.assertIn("3 pairing records", output)
                checked, _, check_errors = self.check()
                self.assertEqual(0, checked, check_errors)

    def test_a_succeeded_sync_that_wrote_no_record_stops_the_command_and_writes_no_pairing_file(self) -> None:
        self.put_metadata(self.metadata_lines(self.landed))
        code, _, errors = self.pairings("9")
        self.assertEqual(1, code)
        self.assertIn("--succeeded-sync", errors)
        self.assertFalse((self.folder / "pairings.json").exists())

    def test_a_succeeded_sync_that_is_not_a_whole_number_is_a_usage_error_that_shows_no_value(self) -> None:
        self.put_metadata(self.metadata_lines(self.landed))
        with self.assertRaises(SystemExit):
            self.pairings(MARKER)
        self.assertFalse((self.folder / "pairings.json").exists())

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
