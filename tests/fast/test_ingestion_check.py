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
from scripts.ingestion.landing import LANDING_PREFIX, landed_key
from scripts.ingestion.selection import Selection, SelectionError
from tests.fast.ingestion_support import SOURCE, SOURCE_PREFIX, Landed, digest, landed_set, list_text_for, selection_for
from tests.fast.memory_store import BLOCK_SIZE


def causes(landed: Landed) -> dict[str, str]:
    return {finding.source_key: finding.cause for finding in run_check(landed.store, landed.selection, landed.list_bytes).findings}


class LandingRuleTest(unittest.TestCase):
    def test_the_rule_of_the_tests_puts_the_source_key_under_the_landing_prefix(self) -> None:
        self.assertEqual("landing/vitaldb/1.0.0/vital_files/0001.vital", landed_key("vitaldb/1.0.0/vital_files/0001.vital"))
        self.assertEqual("landing/", LANDING_PREFIX)


class CheckCausesTest(unittest.TestCase):
    def test_control_a_complete_unchanged_set_passes_and_binds_each_version(self) -> None:
        landed = landed_set()
        result = run_check(landed.store, landed.selection, landed.list_bytes)
        self.assertTrue(result.passed)
        self.assertEqual((), result.findings)
        self.assertEqual({SOURCE_PREFIX + path for path in SOURCE}, {file.source_key for file in result.files})
        for file in result.files:
            path = file.source_key.removeprefix(SOURCE_PREFIX)
            self.assertEqual(landed.versions[path].version_id, file.version_id)
            self.assertEqual(landed_key(file.source_key), file.key)
            self.assertEqual(digest(SOURCE[path]), file.sha256)
            self.assertEqual(len(SOURCE[path]), file.size)
            self.assertEqual(landed.versions[path].last_modified, file.retrieved_utc)

    def test_a_planted_changed_file_fails_for_the_checksum_and_names_only_that_file(self) -> None:
        changed = "vital_files/0002.vital"
        landed = landed_set(landed={changed: SOURCE[changed] + b" truncated or extended"})
        result = run_check(landed.store, landed.selection, landed.list_bytes)
        self.assertFalse(result.passed)
        self.assertEqual({SOURCE_PREFIX + changed: "checksum_differs"}, {finding.source_key: finding.cause for finding in result.findings})
        self.assertEqual({SOURCE_PREFIX + path for path in SOURCE if path != changed}, {file.source_key for file in result.files})

    def test_a_planted_missing_file_fails_for_missing_and_names_only_that_file(self) -> None:
        landed = landed_set(skip=("vital_files/0001.vital",))
        self.assertEqual({SOURCE_PREFIX + "vital_files/0001.vital": "missing"}, causes(landed))

    def test_a_current_delete_marker_is_missing(self) -> None:
        landed = landed_set()
        landed.store.put_delete_marker(landed_key(SOURCE_PREFIX + "clinical_data.csv"))
        self.assertEqual({SOURCE_PREFIX + "clinical_data.csv": "missing"}, causes(landed))

    def test_a_selected_file_that_the_checksum_list_does_not_name_fails_for_that_cause(self) -> None:
        listed = {path: data for path, data in SOURCE.items() if path != "clinical_data.csv"}
        landed = landed_set()
        list_bytes = list_text_for(listed).encode()
        selection = replace(landed.selection, checksum_list_sha256=digest(list_bytes))
        result = run_check(landed.store, selection, list_bytes)
        self.assertEqual({SOURCE_PREFIX + "clinical_data.csv": "not_in_checksum_list"}, {finding.source_key: finding.cause for finding in result.findings})
        self.assertEqual([], landed.store.payload_reads(landed_key(SOURCE_PREFIX + "clinical_data.csv")))

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
        (finding,) = run_check(landed.store, landed.selection, landed.list_bytes).findings
        self.assertIn(SOURCE_PREFIX + "clinical_data.csv", finding.message)
        self.assertIn("missing", finding.message)


class NewVersionTest(unittest.TestCase):
    def test_a_new_landed_version_of_a_verified_key_is_a_new_check_of_that_version(self) -> None:
        landed = landed_set()
        first = run_check(landed.store, landed.selection, landed.list_bytes)
        path = "vital_files/0001.vital"
        landed.land(path, b"another copy with other bytes")
        self.assertEqual({SOURCE_PREFIX + path: "checksum_differs"}, causes(landed))
        landed.land(path, SOURCE[path])
        second = run_check(landed.store, landed.selection, landed.list_bytes)
        self.assertTrue(second.passed)
        self.assertNotEqual(first.result_sha256, second.result_sha256)
        self.assertEqual(landed.versions[path].version_id, next(file.version_id for file in second.files if file.source_key.endswith(path)))

    def test_a_rerun_over_the_same_versions_gives_the_same_result_and_writes_nothing(self) -> None:
        landed = landed_set()
        written = list(landed.store.writes)
        first = run_check(landed.store, landed.selection, landed.list_bytes)
        second = run_check(landed.store, landed.selection, landed.list_bytes)
        self.assertEqual(first.to_json(), second.to_json())
        self.assertEqual(written, landed.store.writes)


class StreamingTest(unittest.TestCase):
    def test_a_payload_is_read_in_blocks_of_the_exact_latest_version_and_never_whole(self) -> None:
        path = "vital_files/0001.vital"
        landed = landed_set()
        landed.land(path, b"a short-lived version that the check must not read")
        landed.land(path, SOURCE[path])
        result = run_check(landed.store, landed.selection, landed.list_bytes)
        self.assertTrue(result.passed)
        reads = landed.store.payload_reads(landed_key(SOURCE_PREFIX + path))
        self.assertEqual([("iter_version", landed.versions[path].version_id)], [(read.method, read.version_id) for read in reads])
        self.assertEqual(BLOCK_SIZE, landed.store.largest_block)
        self.assertGreater(len(SOURCE[path]), BLOCK_SIZE * 2)

    def test_the_check_reads_only_the_selected_keys(self) -> None:
        landed = landed_set()
        landed.store.put_object(LANDING_PREFIX + "vitaldb/1.0.0/vital_files/9999.vital", b"not selected")
        run_check(landed.store, landed.selection, landed.list_bytes)
        self.assertEqual([], landed.store.payload_reads(LANDING_PREFIX + "vitaldb/1.0.0/vital_files/9999.vital"))


class PinnedListTest(unittest.TestCase):
    def test_control_the_list_with_its_pinned_digest_is_used(self) -> None:
        landed = landed_set()
        self.assertTrue(run_check(landed.store, landed.selection, landed.list_bytes).passed)

    def test_a_list_that_differs_from_its_pinned_digest_stops_the_check_before_any_read(self) -> None:
        landed = landed_set()
        tampered = landed.list_bytes.replace(digest(SOURCE["clinical_data.csv"]).encode(), digest(b"other").encode())
        with self.assertRaisesRegex(PinnedListError, "SHA-256"):
            run_check(landed.store, landed.selection, tampered)
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
