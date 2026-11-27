"""The inputs of the Bronze job: the gate runs before any payload is read, and a file is read by the version that its manifest binds.

Failure modes covered here:

- A substituted or missing object version is read, because the gate was left out or ran after the reads, so a replaced file becomes Bronze rows.
- A file whose bytes differ from the SHA-256 of its manifest entry is decoded.
- A case that the manifest does not hold is skipped, so a table is short with no word of it.
- A reader takes the newest version of a key where the manifest binds another.

The decoder, the structural check and the tables are proved by ``tests/e2e/bronze_decode.py``; the store here is the stand-in of the ingestion tests.
"""

from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from scripts.ingestion.gate import GateRefused
from scripts.ingestion.manifest import ManifestRef, publish_manifest
from spark.bronze.inputs import CaseFile, InputError, StoreSpec, fetch_case, verify_inputs
from tests.fast.ingestion_support import SOURCE, SOURCE_PREFIX, Landed, landed_set, list_text_for, pin_the_synthetic_list, synthetic_pin

CASE_ONE = SOURCE["vital_files/0001.vital"]
CASE_TWO = SOURCE["vital_files/0002.vital"]


def setUpModule() -> None:
    pin_the_synthetic_list()


def published(landed: Landed) -> ManifestRef:
    return publish_manifest(landed.store, landed.selection, landed.check())


class Gate(unittest.TestCase):
    def test_control_the_bound_versions_pass_and_no_case_file_is_read_by_the_gate(self) -> None:
        landed = landed_set()
        reference = published(landed)
        landed.store.reads.clear()
        inputs = verify_inputs(landed.store, reference, None)
        self.assertEqual([1, 2], [file.case_id for file in inputs.cases])
        self.assertEqual(landed.versions["vital_files/0001.vital"].version_id, inputs.cases[0].version_id)
        self.assertEqual([], landed.store.payload_reads("landing/sync_01/"))
        self.assertEqual([], landed.store.payload_reads("landing/sync_00/"))

    def test_the_wanted_cases_are_the_ones_returned_once_each(self) -> None:
        landed = landed_set()
        inputs = verify_inputs(landed.store, published(landed), [2, 2])
        self.assertEqual([2], [file.case_id for file in inputs.cases])
        self.assertEqual(SOURCE_PREFIX + "clinical_data.csv", inputs.clinical.source_key)

    def test_a_substituted_version_is_refused_before_any_payload_is_read(self) -> None:
        landed = landed_set()
        reference = published(landed)
        landed.land("vital_files/0001.vital", b"another recording")
        landed.store.reads.clear()
        with self.assertRaises(GateRefused):
            verify_inputs(landed.store, reference, [1])
        self.assertEqual([], [read for read in landed.store.reads if read.key.startswith("landing/")])

    def test_a_missing_version_is_refused_before_any_payload_is_read(self) -> None:
        landed = landed_set()
        reference = published(landed)
        landed.store.put_delete_marker(landed.keys["vital_files/0002.vital"])
        landed.store.reads.clear()
        with self.assertRaises(GateRefused):
            verify_inputs(landed.store, reference, [1, 2])
        self.assertEqual([], [read for read in landed.store.reads if read.key.startswith("landing/")])

    def test_a_case_that_the_manifest_does_not_hold_is_refused_by_number(self) -> None:
        landed = landed_set()
        with self.assertRaises(InputError) as caught:
            verify_inputs(landed.store, published(landed), [1, 9])
        self.assertIn("case 9", str(caught.exception))

    def test_a_manifest_with_no_clinical_table_is_refused(self) -> None:
        source = {"vital_files/0001.vital": CASE_ONE}
        with synthetic_pin(list_text_for(source).encode()):
            landed = landed_set(source)
            with self.assertRaises(InputError) as caught:
                verify_inputs(landed.store, published(landed), None)
        self.assertIn("clinical table", str(caught.exception))


class Fetch(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="bronze_inputs_")
        self.addCleanup(folder.cleanup)
        self.folder = Path(folder.name)
        self.landed = landed_set()
        self.inputs = verify_inputs(self.landed.store, published(self.landed), None)

    def test_control_the_bound_version_is_written_to_the_scratch_folder(self) -> None:
        path = fetch_case(self.landed.store, self.inputs.cases[0], self.folder)
        self.assertEqual((self.folder / "0001.vital", CASE_ONE), (path, path.read_bytes()))

    def test_a_version_whose_bytes_differ_from_the_manifest_sha256_is_refused(self) -> None:
        wrong = replace(self.inputs.cases[0], sha256="0" * 64)
        with self.assertRaises(InputError) as caught:
            fetch_case(self.landed.store, wrong, self.folder)
        self.assertIn("SHA-256", str(caught.exception))

    def test_a_version_that_no_longer_exists_is_refused_by_case(self) -> None:
        gone = CaseFile(1, self.inputs.cases[0].key, "no-such-version", len(CASE_ONE), self.inputs.cases[0].sha256, "2026-01-01T00:00:00Z")
        with self.assertRaises(InputError) as caught:
            fetch_case(self.landed.store, gone, self.folder)
        self.assertIn("case 1", str(caught.exception))

    def test_the_file_is_read_by_its_bound_version_when_a_newer_one_exists(self) -> None:
        bound = self.inputs.cases[0]
        self.landed.land("vital_files/0001.vital", b"a newer version")
        self.landed.store.reads.clear()
        path = fetch_case(self.landed.store, bound, self.folder)
        self.assertEqual(CASE_ONE, path.read_bytes())
        self.assertEqual([bound.version_id], [read.version_id for read in self.landed.store.reads])


class ReadLogChoice(unittest.TestCase):
    """The driver makes a read log only when the selected store carries a log path: the folder store. The S3 store has none."""

    def test_a_folder_store_with_a_log_path_makes_that_log(self) -> None:
        self.assertEqual(Path("/run/reads.log"), StoreSpec("folder", "bucket", "/landed", "", "/run/reads.log").log_to_create)

    def test_a_folder_store_without_a_log_path_makes_none(self) -> None:
        self.assertIsNone(StoreSpec("folder", "bucket", "/landed", "", "").log_to_create)

    def test_an_s3_store_makes_none_even_when_a_log_path_was_given_to_the_job(self) -> None:
        self.assertIsNone(StoreSpec("s3", "bucket", "", "us-west-2").log_to_create)
        self.assertIsNone(StoreSpec("s3", "bucket", "", "us-west-2", "/run/reads.log").log_to_create)


class DecodeCreatesTheLogOnlyForTheFolderStore(unittest.TestCase):
    def test_the_job_asks_the_selected_store_and_not_its_own_argument(self) -> None:
        source = Path("spark/jobs/bronze_decode.py").read_text()
        self.assertIn("spec.log_to_create", source)
        self.assertNotIn("if arguments.read_log:", source)


if __name__ == "__main__":
    unittest.main()
