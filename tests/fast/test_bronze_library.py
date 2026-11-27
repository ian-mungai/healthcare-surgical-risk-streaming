"""The decoder's own functions, driven with a stand-in for the publisher's library at its boundary (``tests/fast/fake_vitaldb.py``).

The stand-in is a port of the library's reading, so these tests run the decoder's production code: the check of a case (``case_problem``) and the rows of a
case (``case_rows``). Each of the two guards is shown to be needed by a test that fails when it is bypassed.

Failure modes covered here:

- The decoder asks the library for tracks by a name that the library also matches as the ending of another track's name, so a float track ``HR`` brings in the
  four-byte integer track ``D/HR``, whose values the library cannot unpack, and a valid file is refused in either pass mode and for either format code.
- The numeric tracks of the four-byte integer formats are sent through the library and not read by the walk, or are read twice.
- The count check is bypassed: the library silently skips a record and the track has fewer rows than the file holds, with no refusal.
- A track that the library selected without being asked is dropped silently instead of refused.

The real library is run by the acceptance test ``tests/e2e/bronze_decode.py``.
"""

from __future__ import annotations

import gzip
import importlib
import tempfile
import unittest
from pathlib import Path
from typing import Any

from tests.fast.fake_vitaldb import FakeVitalFile, installed
from tests.fast.vital_support import START, device_packet, numeric_record, text_record, track_packet, vital, waveform_record

CASEEND = 10
CASEEND_US = CASEEND * 1_000_000
MODES = ("grouped", "per_track")
library: Any
rows_module: Any


def setUpModule() -> None:
    global library, rows_module
    unittest.enterModuleContext(installed())
    library = importlib.import_module("spark.bronze.library")
    rows_module = importlib.import_module("spark.bronze.rows")


class Files(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name)
        self.count = 0
        FakeVitalFile.loads.clear()
        FakeVitalFile.errors.clear()
        FakeVitalFile.drop_records = {}
        FakeVitalFile.shift_times = {}
        FakeVitalFile.hidden_tracks = set()
        self.lineage = rows_module.Lineage("v0001", "ab" * 32)

    def file(self, *packets: bytes) -> Path:
        self.count += 1
        path = self.root / f"case{self.count}.vital"
        path.write_bytes(gzip.compress(vital(*packets)))
        return path

    def numeric(self, path: Path, mode: str) -> list[tuple[str, int, float]]:
        return [(row[1], row[2], row[5]) for row in library.case_rows(path, 7, "numeric", self.lineage, mode)]

    def problem(self, path: Path, mode: str = "grouped") -> Any:
        return library.case_problem(path, 7, CASEEND_US, mode)


def ends(tid: int, fmt: int, last: float) -> list[bytes]:
    """A first record at the start of the recording and a last one at the clinical end, so that the recording starts and ends where the case does."""
    return [numeric_record(tid, fmt, 1 if fmt != 1 else 1.0, dt=START), numeric_record(tid, fmt, last, dt=START + CASEEND)]


class FourByteIntegers(Files):
    def reviewers_file(self, fmt: int, value: int) -> Path:
        """A float track ``HR`` with no device and an integer track ``D/HR`` of the format under test, whose name ends with ``HR``."""
        return self.file(
            device_packet(1, "D", "D"),
            track_packet(1, name="HR"),
            track_packet(2, name="HR", fmt=fmt, did=1),
            *ends(1, 1, 2.5),
            numeric_record(2, fmt, 0 if fmt == 8 else -5, dt=START),
            numeric_record(2, fmt, value, dt=START + CASEEND),
        )

    def test_a_float_track_and_an_integer_track_whose_name_ends_with_it_are_both_decoded_in_both_modes_for_both_formats(self) -> None:
        for fmt, value in ((7, 2147483647), (8, 4294967295)):
            for mode in MODES:
                with self.subTest(fmt=fmt, mode=mode):
                    path = self.reviewers_file(fmt, value)
                    self.assertIsNone(self.problem(path, mode))
                    found = self.numeric(path, mode)
                    self.assertEqual(
                        sorted([("HR", 0, 1.0), ("HR", CASEEND_US, 2.5), ("D/HR", 0, 0.0 if fmt == 8 else -5.0), ("D/HR", CASEEND_US, float(value))]),
                        sorted(found),
                    )

    def test_three_tracks_of_mixed_formats_whose_names_end_alike_are_each_decoded_from_their_own_records(self) -> None:
        path = self.file(
            device_packet(1, "D", "D"),
            device_packet(2, "E", "E"),
            track_packet(1, name="HR"),
            track_packet(2, name="HR", fmt=7, did=1),
            track_packet(3, name="HR", fmt=8, did=2),
            numeric_record(1, 1, 1.0, dt=START),
            numeric_record(2, 7, -2147483648, dt=START),
            numeric_record(3, 8, 4294967295, dt=START),
            numeric_record(1, 1, 3.0, dt=START + CASEEND),
            numeric_record(2, 7, 2147483647, dt=START + CASEEND),
            numeric_record(3, 8, 0, dt=START + CASEEND),
        )
        for mode in MODES:
            with self.subTest(mode=mode):
                self.assertIsNone(self.problem(path, mode))
                found = self.numeric(path, mode)
                expected = [
                    ("HR", 0, 1.0),
                    ("HR", CASEEND_US, 3.0),
                    ("D/HR", 0, -2147483648.0),
                    ("D/HR", CASEEND_US, 2147483647.0),
                    ("E/HR", 0, 4294967295.0),
                    ("E/HR", CASEEND_US, 0.0),
                ]
                self.assertEqual(sorted(expected), sorted(found))

    def test_a_name_that_is_the_ending_of_another_tracks_whole_name_selects_only_its_own_track(self) -> None:
        path = self.file(
            device_packet(1, "X", "XD"),
            device_packet(2, "D", "D"),
            track_packet(1, name="HR", fmt=7, did=1),
            track_packet(2, name="HR", fmt=1, did=2),
            numeric_record(1, 7, 5, dt=START),
            numeric_record(1, 7, 6, dt=START + CASEEND),
            numeric_record(2, 1, 1.5, dt=START),
            numeric_record(2, 1, 2.5, dt=START + CASEEND),
        )
        for mode in MODES:
            with self.subTest(mode=mode):
                self.assertIsNone(self.problem(path, mode))
                self.assertEqual(
                    sorted([("XD/HR", 0, 5.0), ("XD/HR", CASEEND_US, 6.0), ("D/HR", 0, 1.5), ("D/HR", CASEEND_US, 2.5)]), sorted(self.numeric(path, mode))
                )

    def test_the_library_is_never_asked_to_decode_a_four_byte_integer_track(self) -> None:
        path = self.reviewers_file(7, 2147483647)
        FakeVitalFile.loads.clear()
        self.numeric(path, "grouped")
        with_records = [names for _, names, header_only in FakeVitalFile.loads if not header_only]
        self.assertEqual([["HR"]], with_records)

    def test_every_read_with_records_names_whole_tracks_only(self) -> None:
        path = self.reviewers_file(8, 4294967295)
        for mode in MODES:
            FakeVitalFile.loads.clear()
            self.problem(path, mode)
            self.numeric(path, mode)
            for _, names, header_only in FakeVitalFile.loads:
                if not header_only:
                    self.assertTrue(names, "a read with records names the tracks it wants")
                    self.assertTrue(all(name in ("HR", "D/HR") for name in names or []), names)

    def test_a_track_that_the_library_selected_without_being_asked_for_is_refused_and_not_dropped(self) -> None:
        path = self.file(
            device_packet(1, "D", "D"),
            track_packet(1, name="HR"),
            track_packet(2, name="HR", did=1),
            *ends(1, 1, 2.5),
            *ends(2, 1, 3.5),
        )
        original = library.load

        def selecting_everything(target: Path, names: Any = None, *, header_only: bool = False) -> Any:
            return original(target, None, header_only=header_only)

        library.load = selecting_everything
        try:
            with self.assertRaises(rows_module.StructureError) as caught:
                library.library_group(path, 7, ["HR"])
        finally:
            library.load = original
        self.assertIn("case 7", str(caught.exception))
        self.assertIn("D/HR", str(caught.exception))


class Guards(Files):
    def valid(self) -> Path:
        return self.file(
            device_packet(1, "D", "D"),
            track_packet(1, name="HR", did=1),
            track_packet(2, name="EVENT", kind=5, fmt=1),
            track_packet(3, name="WAVE", kind=1, fmt=6, did=1, rate=2.0),
            numeric_record(1, 1, 1.0, dt=START),
            numeric_record(1, 1, 2.0, dt=START + 5),
            numeric_record(1, 1, 3.0, dt=START + CASEEND),
            text_record(2, "start", dt=START),
            text_record(2, "end", dt=START + CASEEND),
            waveform_record(3, 6, [1, 2, 3], dt=START),
        )

    def test_control_a_valid_file_passes_the_check_in_both_modes(self) -> None:
        path = self.valid()
        for mode in MODES:
            self.assertIsNone(self.problem(path, mode))

    def test_a_record_that_the_library_skips_is_found_by_the_count_check_naming_the_track_and_both_counts(self) -> None:
        path = self.valid()
        FakeVitalFile.drop_records = {"D/HR": 2}
        for mode in MODES:
            found = self.problem(path, mode)
            self.assertIn("case 7", found or "")
            self.assertIn("D/HR", found or "")
            self.assertIn("3 records in the file and the library returned 2", found or "")

    def test_a_record_that_the_library_reads_at_another_time_is_found_by_the_time_check(self) -> None:
        path = self.valid()
        FakeVitalFile.shift_times = {"D/HR": 2}
        found = self.problem(path)
        self.assertIn("D/HR", found or "")
        self.assertIn("another time", found or "")

    def test_a_skipped_text_record_is_found_by_the_count_check(self) -> None:
        path = self.valid()
        FakeVitalFile.drop_records = {"EVENT": 1}
        self.assertIn("EVENT", self.problem(path) or "")

    def test_a_skipped_waveform_record_is_found_by_the_count_check(self) -> None:
        path = self.valid()
        FakeVitalFile.drop_records = {"D/WAVE": 1}
        self.assertIn("D/WAVE", self.problem(path) or "")

    def test_a_track_that_the_file_declares_and_the_library_does_not_read_is_refused_by_name(self) -> None:
        path = self.valid()
        FakeVitalFile.hidden_tracks = {"EVENT"}
        found = self.problem(path)
        self.assertIn("case 7", found or "")
        self.assertIn("EVENT", found or "")
        self.assertIn("did not read it", found or "")

    def test_the_rows_of_the_valid_file_are_the_same_in_both_modes(self) -> None:
        path = self.valid()
        for kind in ("tracks", "numeric", "string", "waveform"):
            grouped = list(library.case_rows(path, 7, kind, self.lineage, "grouped"))
            single = list(library.case_rows(path, 7, kind, self.lineage, "per_track"))
            self.assertEqual(grouped, single, kind)
            self.assertTrue(grouped, kind)


if __name__ == "__main__":
    unittest.main()
