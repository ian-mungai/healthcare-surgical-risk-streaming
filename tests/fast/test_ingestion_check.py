"""The checksum check: which cause fails it, that a passing control accompanies each cause and that a payload is only ever streamed.

Each case plants one fault in the stand-in store and expects the check to fail for that cause alone, with the clean set as its control.
The stand-in's limits are in ``memory_store.py``. The landed key of a source key is the rule of ``scripts.ingestion.landing``, which the
tests reach through that function and so keep passing when the rule is replaced.
"""

from __future__ import annotations

import json
import unittest
from dataclasses import replace

from scripts.ingestion.check import ChecksumListError, PinnedListError, parse_checksum_list, run_check
from scripts.ingestion.landing import LANDING_PREFIX, PairingError, PairingRecord, load_pairing_records
from scripts.ingestion.selection import Selection, SelectionError
from tests.fast.ingestion_support import SOURCE, SOURCE_PREFIX, Landed, digest, landed_set, list_text_for, selection_for
from tests.fast.memory_store import BLOCK_SIZE


def causes(landed: Landed) -> dict[str, str]:
    return {finding.source_key: finding.cause for finding in landed.check().findings}


class CheckCausesTest(unittest.TestCase):
    def test_control_a_complete_unchanged_set_passes_and_binds_each_version(self) -> None:
        landed = landed_set()
        result = landed.check()
        self.assertTrue(result.passed)
        self.assertEqual((), result.findings)
        self.assertEqual({SOURCE_PREFIX + path for path in SOURCE}, {file.source_key for file in result.files})
        for file in result.files:
            path = file.source_key.removeprefix(SOURCE_PREFIX)
            self.assertEqual(landed.versions[path].version_id, file.version_id)
            self.assertEqual(landed.key_for(file.source_key), file.key)
            self.assertEqual(digest(SOURCE[path]), file.sha256)
            self.assertEqual(len(SOURCE[path]), file.size)
            self.assertEqual(landed.versions[path].last_modified, file.retrieved_utc)

    def test_a_planted_changed_file_fails_for_the_checksum_and_names_only_that_file(self) -> None:
        changed = "vital_files/0002.vital"
        landed = landed_set(landed={changed: SOURCE[changed] + b" truncated or extended"})
        result = landed.check()
        self.assertFalse(result.passed)
        self.assertEqual({SOURCE_PREFIX + changed: "checksum_differs"}, {finding.source_key: finding.cause for finding in result.findings})
        self.assertEqual({SOURCE_PREFIX + path for path in SOURCE if path != changed}, {file.source_key for file in result.files})

    def test_a_planted_missing_file_fails_for_missing_and_names_only_that_file(self) -> None:
        landed = landed_set(skip=("vital_files/0001.vital",))
        self.assertEqual({SOURCE_PREFIX + "vital_files/0001.vital": "missing"}, causes(landed))

    def test_a_current_delete_marker_means_the_landed_object_is_missing(self) -> None:
        landed = landed_set()
        landed.store.put_delete_marker(landed.key_for(SOURCE_PREFIX + "clinical_data.csv"))
        self.assertEqual({SOURCE_PREFIX + "clinical_data.csv": "landed_object_missing"}, causes(landed))

    def test_a_selected_file_that_the_checksum_list_does_not_name_fails_for_that_cause(self) -> None:
        listed = {path: data for path, data in SOURCE.items() if path != "clinical_data.csv"}
        landed = landed_set()
        list_bytes = list_text_for(listed).encode()
        selection = replace(landed.selection, checksum_list_sha256=digest(list_bytes))
        result = run_check(landed.store, selection, list_bytes, landed.pairings())
        self.assertEqual({SOURCE_PREFIX + "clinical_data.csv": "not_in_checksum_list"}, {finding.source_key: finding.cause for finding in result.findings})
        self.assertEqual([], landed.store.payload_reads(landed.key_for(SOURCE_PREFIX + "clinical_data.csv")))

    def test_an_object_with_a_null_version_id_fails_for_that_cause(self) -> None:
        landed = landed_set()
        unversioned = landed_set()
        unversioned.store.versioned = False
        unversioned.store.history.clear()
        for path, data in SOURCE.items():
            unversioned.land(path, data)
        self.assertEqual({}, causes(landed))
        self.assertEqual({SOURCE_PREFIX + path: "null_version_id" for path in SOURCE}, causes(unversioned))

    def test_every_fault_is_reported_in_one_check(self) -> None:
        landed = landed_set(landed={"vital_files/0002.vital": b"changed"}, skip=("clinical_data.csv",))
        self.assertEqual(
            {SOURCE_PREFIX + "vital_files/0002.vital": "checksum_differs", SOURCE_PREFIX + "clinical_data.csv": "missing"},
            causes(landed),
        )

    def test_a_message_names_the_key_and_the_cause(self) -> None:
        landed = landed_set(skip=("clinical_data.csv",))
        (finding,) = landed.check().findings
        self.assertIn(SOURCE_PREFIX + "clinical_data.csv", finding.message)
        self.assertIn("missing", finding.message)


class PairingTest(unittest.TestCase):
    """The landed key of a file is not a function of its source key: a record of the test states each pairing, and the records' form is the rule here."""

    def check_with(self, landed: Landed, records: list[PairingRecord]) -> dict[str, str]:
        return {finding.source_key: finding.cause for finding in run_check(landed.store, landed.selection, landed.list_bytes, records).findings}

    def test_control_each_file_is_found_at_the_key_its_record_names(self) -> None:
        landed = landed_set()
        self.assertEqual(3, len({key.split("/")[1] for key in landed.keys.values()}))
        result = landed.check()
        self.assertTrue(result.passed)
        self.assertEqual({landed.keys[path] for path in SOURCE}, {file.key for file in result.files})

    def test_a_file_with_no_record_is_missing_even_when_an_object_sits_at_the_old_rule_key(self) -> None:
        landed = landed_set()
        unpaired = SOURCE_PREFIX + "clinical_data.csv"
        records = [record for record in landed.pairings() if record.source_key != unpaired]
        landed.store.put_object(LANDING_PREFIX + unpaired, SOURCE["clinical_data.csv"])
        self.assertEqual({unpaired: "missing"}, self.check_with(landed, records))
        self.assertEqual({}, self.check_with(landed, landed.pairings()))

    def test_two_records_for_one_source_file_are_refused_and_nothing_of_it_is_read(self) -> None:
        landed = landed_set()
        source_key = SOURCE_PREFIX + "vital_files/0001.vital"
        landed.store.put_object("landing/sync_99/" + source_key, SOURCE["vital_files/0001.vital"])
        second = PairingRecord(source_key, "landing/sync_99/" + source_key, len(SOURCE["vital_files/0001.vital"]))
        landed.store.reads.clear()
        self.assertEqual({source_key: "duplicate_pairing"}, self.check_with(landed, [*landed.pairings(), second]))
        self.assertEqual([], landed.store.payload_reads(landed.key_for(source_key)))
        self.assertEqual([], landed.store.payload_reads("landing/sync_99/"))
        self.assertEqual({}, self.check_with(landed, landed.pairings()))

    def test_a_record_whose_landed_key_does_not_exist_is_refused(self) -> None:
        landed = landed_set()
        source_key = SOURCE_PREFIX + "vital_files/0002.vital"
        records = [
            PairingRecord(source_key, "landing/sync_77/gone", record.size) if record.source_key == source_key else record for record in landed.pairings()
        ]
        self.assertEqual({source_key: "landed_object_missing"}, self.check_with(landed, records))

    def test_a_record_whose_size_differs_from_the_landed_object_is_refused_before_it_is_read(self) -> None:
        landed = landed_set()
        source_key = SOURCE_PREFIX + "vital_files/0002.vital"
        records = [replace(record, size=record.size + 1) if record.source_key == source_key else record for record in landed.pairings()]
        landed.store.reads.clear()
        self.assertEqual({source_key: "pairing_size_differs"}, self.check_with(landed, records))
        self.assertEqual([], landed.store.payload_reads(landed.key_for(source_key)))

    def test_a_record_for_a_file_that_is_not_selected_is_ignored(self) -> None:
        landed = landed_set()
        extra = PairingRecord(SOURCE_PREFIX + "vital_files/9999.vital", "landing/sync_99/none", 1)
        self.assertEqual({}, self.check_with(landed, [*landed.pairings(), extra, extra]))

    def test_the_records_file_names_each_pairing_and_refuses_anything_else(self) -> None:
        text = json.dumps([{"source_key": "a/b", "landed_key": "landing/x/b", "size": 3}])
        self.assertEqual((PairingRecord("a/b", "landing/x/b", 3),), load_pairing_records(text))
        for bad in (
            "{}",
            "[1]",
            json.dumps([{"source_key": "a", "landed_key": "b"}]),
            json.dumps([{"source_key": "a", "landed_key": "b", "size": -1}]),
            json.dumps([{"source_key": "", "landed_key": "b", "size": 1}]),
            json.dumps([{"source_key": "a", "landed_key": "b", "size": "1"}]),
            "not json",
        ):
            with self.subTest(bad=bad), self.assertRaises(PairingError):
                load_pairing_records(bad)


class NewVersionTest(unittest.TestCase):
    def test_a_new_landed_version_of_a_verified_key_is_a_new_check_of_that_version(self) -> None:
        landed = landed_set()
        first = landed.check()
        path = "vital_files/0001.vital"
        landed.land(path, b"another copy with other bytes")
        self.assertEqual({SOURCE_PREFIX + path: "checksum_differs"}, causes(landed))
        landed.land(path, SOURCE[path])
        second = landed.check()
        self.assertTrue(second.passed)
        self.assertNotEqual(first.result_sha256, second.result_sha256)
        self.assertEqual(landed.versions[path].version_id, next(file.version_id for file in second.files if file.source_key.endswith(path)))

    def test_a_rerun_over_the_same_versions_gives_the_same_result_and_writes_nothing(self) -> None:
        landed = landed_set()
        written = list(landed.store.writes)
        first = landed.check()
        second = landed.check()
        self.assertEqual(first.to_json(), second.to_json())
        self.assertEqual(written, landed.store.writes)


class StreamingTest(unittest.TestCase):
    def test_a_payload_is_read_in_blocks_of_the_exact_latest_version_and_never_whole(self) -> None:
        path = "vital_files/0001.vital"
        landed = landed_set()
        landed.land(path, b"a short-lived version that the check must not read")
        landed.land(path, SOURCE[path])
        result = landed.check()
        self.assertTrue(result.passed)
        reads = landed.store.payload_reads(landed.key_for(SOURCE_PREFIX + path))
        self.assertEqual([("iter_version", landed.versions[path].version_id)], [(read.method, read.version_id) for read in reads])
        self.assertEqual(BLOCK_SIZE, landed.store.largest_block)
        self.assertGreater(len(SOURCE[path]), BLOCK_SIZE * 2)

    def test_the_check_reads_only_the_selected_keys(self) -> None:
        landed = landed_set()
        landed.store.put_object(LANDING_PREFIX + "vitaldb/1.0.0/vital_files/9999.vital", b"not selected")
        landed.check()
        self.assertEqual([], landed.store.payload_reads(LANDING_PREFIX + "vitaldb/1.0.0/vital_files/9999.vital"))


class PinnedListTest(unittest.TestCase):
    def test_control_the_list_with_its_pinned_digest_is_used(self) -> None:
        landed = landed_set()
        self.assertTrue(landed.check().passed)

    def test_a_list_that_differs_from_its_pinned_digest_stops_the_check_before_any_read(self) -> None:
        landed = landed_set()
        tampered = landed.list_bytes.replace(digest(SOURCE["clinical_data.csv"]).encode(), digest(b"other").encode())
        with self.assertRaisesRegex(PinnedListError, "SHA-256"):
            run_check(landed.store, landed.selection, tampered, landed.pairings())
        self.assertEqual([], landed.store.reads)

    def test_the_list_must_be_well_formed(self) -> None:
        good = digest(b"x")
        self.assertEqual({"a.csv": good}, parse_checksum_list(f"{good} a.csv\n".encode()))
        for text in (f"{good}  a.csv\n", f"{good[:-1]} a.csv\n", f"{good.upper()} a.csv\n", f"{good} a.csv\r\n", f"{good} a.csv\n{good} a.csv\n", f"{good}\n"):
            with self.subTest(text=text), self.assertRaises(ChecksumListError):
                parse_checksum_list(text.encode())


class SelectionTest(unittest.TestCase):
    def text(self, **changes: object) -> str:
        values: dict[str, object] = {
            "name": "probe_set",
            "release": "1.0.0",
            "source_bucket": "example-publisher",
            "source_prefix": SOURCE_PREFIX,
            "measurement_period": "2016-08 to 2017-06",
            "checksum_list": {"key": f"{SOURCE_PREFIX}SHA256SUMS.txt", "sha256": digest(b"list")},
            "files": ["clinical_data.csv"],
        }
        return json.dumps(values | changes)

    def test_a_selection_is_an_input_with_explicit_files_or_a_numbered_range(self) -> None:
        explicit = Selection.from_json(self.text())
        self.assertEqual(("clinical_data.csv",), explicit.files)
        ranged = Selection.from_json(self.text(files=["clinical_data.csv"], case_files={"pattern": "vital_files/{:04d}.vital", "first": 1, "last": 3}))
        self.assertEqual(("clinical_data.csv", "vital_files/0001.vital", "vital_files/0002.vital", "vital_files/0003.vital"), ranged.files)
        self.assertNotEqual(explicit.keys_sha256, ranged.keys_sha256)

    def test_a_selection_that_cannot_name_one_set_is_refused(self) -> None:
        refused: list[dict[str, object]] = [
            {"files": []},
            {"files": ["a.csv", "a.csv"]},
            {"files": ["/a.csv"]},
            {"files": ["../a.csv"]},
            {"name": "has space"},
            {"release": ""},
        ]
        for changes in refused:
            with self.subTest(changes=changes), self.assertRaises(SelectionError):
                Selection.from_json(self.text(**changes))


class SelectionHelperTest(unittest.TestCase):
    def test_the_digest_of_the_keys_does_not_depend_on_their_order(self) -> None:
        one = selection_for(["b.csv", "a.csv"], b"list")
        two = selection_for(["a.csv", "b.csv"], b"list")
        self.assertEqual(one.keys_sha256, two.keys_sha256)


if __name__ == "__main__":
    unittest.main()
