"""The packet walk of a case file: what it refuses before any decode, what it counts and the values of the tracks that the library cannot read.

Failure modes covered here:

- A record packet whose declared lengths do not fit its packet (the info block, a numeric value, a text, a waveform's samples) keeps the gzip framing and the
  declared track length, so the library skips it and the reading disappears from the tables with no word of it.
- A record names a track that the file does not declare (before or after the declaration), so the library skips it.
- A track of a type that Bronze does not carry, a numeric or waveform track with a sample format that the file format does not define, or a waveform track
  of a four-byte integer format that the library cannot read, passes and fails after the branch exists.
- The counts that the walk makes differ from the records of a track (two tracks with one name are counted as one, as the library merges them), so the check
  of what the library returned compares the wrong number.
- A four-byte integer value (format codes 7 and 8) is read as eight bytes, or as a signed value when it is unsigned, or the extreme values change.
- The library's records of a track differ from the file's in number or in time, or the library read a track that the walk did not find, and no one is told.

The library's own reading is proved against the published recordings by the acceptance test ``tests/e2e/bronze_decode.py``.
"""

from __future__ import annotations

import gzip
import tempfile
import unittest
from pathlib import Path

from spark.bronze.structure import Walk, records_problem, tracks_problem, walk_file
from tests.fast.vital_support import (
    START,
    device_packet,
    numeric_record,
    packet,
    record_packet,
    text_record,
    track_packet,
    vital,
    waveform_record,
)


class Files(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name)
        self.count = 0

    def walk(self, *packets: bytes, values_for: tuple[str, ...] = ()) -> Walk:
        self.count += 1
        path = self.root / f"case{self.count}.vital"
        path.write_bytes(gzip.compress(vital(*packets)))
        return walk_file(path, values_for)

    def problem(self, *packets: bytes) -> str:
        return self.walk(*packets).problem or ""


class Records(Files):
    def test_control_a_file_of_whole_records_has_no_problem(self) -> None:
        walked = self.walk(
            track_packet(1),
            track_packet(2, kind=5, fmt=0),
            track_packet(3, kind=1, fmt=6, rate=2.0),
            record_packet(1),
            text_record(2, "note"),
            waveform_record(3, 6, [1, 2, 3]),
        )
        self.assertIsNone(walked.problem)

    def test_a_numeric_record_whose_info_block_is_one_byte_longer_is_refused(self) -> None:
        for infolen in (11, 12, 14):
            self.assertIn("value", self.problem(track_packet(1), record_packet(1), record_packet(1, infolen=infolen), record_packet(1)), infolen)

    def test_an_info_block_shorter_than_its_time_and_track_is_refused(self) -> None:
        for infolen in (0, 9):
            self.assertIn("info block", self.problem(track_packet(1), record_packet(1, infolen=infolen)), infolen)

    def test_an_info_block_that_runs_past_its_packet_is_refused(self) -> None:
        self.assertIn("info block", self.problem(track_packet(1), record_packet(1, payload=b"", infolen=40)))

    def test_a_numeric_value_of_another_length_than_its_format_is_refused(self) -> None:
        for fmt, size in ((1, 4), (2, 8), (3, 1), (4, 1), (5, 2), (6, 2), (7, 4), (8, 4)):
            for length in (0, size - 1, size + 1):
                found = self.problem(track_packet(1, fmt=fmt), record_packet(1, payload=bytes(length)))
                self.assertIn("value", found, (fmt, length))
            self.assertEqual("", self.problem(track_packet(1, fmt=fmt), record_packet(1, payload=bytes(size))), fmt)

    def test_a_text_that_runs_past_its_packet_or_a_text_record_that_is_too_short_is_refused(self) -> None:
        self.assertIn("text", self.problem(track_packet(1, kind=5, fmt=0), record_packet(1, payload=b"\x00\x00\x00\x00\xff\x00\x00\x00ab")))
        self.assertIn("text", self.problem(track_packet(1, kind=5, fmt=0), record_packet(1, payload=b"\x00\x00\x00")))
        self.assertEqual("", self.problem(track_packet(1, kind=5, fmt=0), text_record(1, ""), text_record(1, "a longer text")))

    def test_waveform_samples_that_run_past_their_packet_are_refused(self) -> None:
        whole = waveform_record(1, 6, [1, 2, 3])
        self.assertEqual("", self.problem(track_packet(1, kind=1, fmt=6, rate=2.0), whole))
        short = record_packet(1, payload=b"\x04\x00\x00\x00" + bytes(6))
        self.assertIn("samples", self.problem(track_packet(1, kind=1, fmt=6, rate=2.0), short))
        self.assertIn("samples", self.problem(track_packet(1, kind=1, fmt=6, rate=2.0), record_packet(1, payload=b"\x01")))

    def test_a_record_that_names_a_track_the_file_does_not_declare_is_refused(self) -> None:
        self.assertIn("declare", self.problem(track_packet(1), record_packet(2)))

    def test_a_record_before_the_declaration_of_its_track_is_refused(self) -> None:
        self.assertIn("declare", self.problem(record_packet(1), track_packet(1)))


class Tracks(Files):
    def test_a_track_of_a_type_that_bronze_does_not_carry_is_refused(self) -> None:
        for kind in (0, 3, 4, 6):
            self.assertIn("type", self.problem(track_packet(1, kind=kind)), kind)

    def test_a_numeric_or_waveform_track_with_an_undefined_sample_format_is_refused(self) -> None:
        for kind in (1, 2):
            for fmt in (0, 9, 255):
                self.assertIn("format", self.problem(track_packet(1, kind=kind, fmt=fmt, rate=1.0)), (kind, fmt))

    def test_a_text_track_keeps_any_format_code(self) -> None:
        for fmt in (0, 1, 9):
            self.assertEqual("", self.problem(track_packet(1, kind=5, fmt=fmt), text_record(1, "x")), fmt)

    def test_a_waveform_track_of_a_four_byte_integer_format_is_refused_because_the_library_cannot_read_it(self) -> None:
        for fmt in (7, 8):
            self.assertIn("four-byte", self.problem(track_packet(1, kind=1, fmt=fmt, rate=1.0)), fmt)
        self.assertEqual("", self.problem(track_packet(1, kind=1, fmt=6, rate=1.0)))


class Counts(Files):
    def test_each_track_has_the_count_of_its_records_and_the_times_of_numeric_and_text_ones(self) -> None:
        walked = self.walk(
            track_packet(1),
            track_packet(2, kind=5, fmt=0),
            track_packet(3, kind=1, fmt=6, rate=2.0),
            numeric_record(1, 1, 5.0, dt=START + 1.0),
            numeric_record(1, 1, 6.0, dt=START + 3.0),
            text_record(2, "a", dt=START + 2.0),
            waveform_record(3, 6, [1, 2], dt=START),
            waveform_record(3, 6, [3], dt=START + 1.0),
        )
        self.assertEqual({"T1": 2, "T2": 1, "T3": 2}, {name: track.count for name, track in walked.tracks.items()})
        self.assertEqual([START + 1.0, START + 3.0], list(walked.tracks["T1"].times))
        self.assertEqual([START + 2.0], list(walked.tracks["T2"].times))
        self.assertEqual(0, len(walked.tracks["T3"].times))
        self.assertEqual({"T1": (2, 1), "T2": (5, 0), "T3": (1, 6)}, {name: (track.track_type, track.fmt) for name, track in walked.tracks.items()})

    def test_a_track_is_named_by_its_device_and_its_own_name_as_the_library_names_it(self) -> None:
        walked = self.walk(
            device_packet(1, "TypeA", "Solar8000"),
            device_packet(2, "OnlyType", ""),
            track_packet(1, name="HR", did=1),
            track_packet(2, name="RR", did=2),
            track_packet(3, name="EVENT"),
            track_packet(4, name="LOST", did=9),
        )
        self.assertEqual(["EVENT", "LOST", "OnlyType/RR", "Solar8000/HR"], sorted(walked.tracks))

    def test_two_tracks_with_one_name_are_one_track_with_their_records_in_file_order(self) -> None:
        walked = self.walk(
            track_packet(1, name="HR"),
            track_packet(2, name="HR"),
            numeric_record(1, 1, 60.0, dt=START + 1.0),
            numeric_record(2, 1, 70.0, dt=START + 1.0),
            numeric_record(1, 1, 61.0, dt=START + 2.0),
        )
        self.assertEqual({"HR": 3}, {name: track.count for name, track in walked.tracks.items()})
        self.assertEqual([START + 1.0, START + 1.0, START + 2.0], list(walked.tracks["HR"].times))

    def test_two_tracks_with_one_name_and_another_type_are_refused(self) -> None:
        self.assertIn("one name", self.problem(track_packet(1, name="HR"), track_packet(2, name="HR", kind=5, fmt=0)))

    def test_a_declared_length_adds_up_over_the_tracks_that_share_a_name(self) -> None:
        one = len(record_packet(1))
        walked = self.walk(
            track_packet(1, reclen=2 * one, name="HR"),
            track_packet(2, reclen=one, name="HR"),
            record_packet(1),
            record_packet(2),
            record_packet(1),
        )
        self.assertIsNone(walked.problem)


class Values(Files):
    def test_the_values_of_four_byte_integer_tracks_are_read_with_fixed_width_little_endian_formats(self) -> None:
        signed = [-2147483648, -1, 0, 1, 2147483647]
        unsigned = [0, 1, 2147483648, 4294967294, 4294967295]
        walked = self.walk(
            track_packet(1, name="I32", fmt=7),
            track_packet(2, name="U32", fmt=8),
            track_packet(3, name="F32"),
            *(numeric_record(1, 7, value, dt=START + number) for number, value in enumerate(signed)),
            *(numeric_record(2, 8, value, dt=START + number) for number, value in enumerate(unsigned)),
            numeric_record(3, 1, 2.5, dt=START),
            values_for=("I32", "U32"),
        )
        self.assertIsNone(walked.problem)
        self.assertEqual([(START + number, float(value)) for number, value in enumerate(signed)], walked.tracks["I32"].values)
        self.assertEqual([(START + number, float(value)) for number, value in enumerate(unsigned)], walked.tracks["U32"].values)
        self.assertIsNone(walked.tracks["F32"].values)

    def test_a_track_that_is_not_asked_for_has_no_values(self) -> None:
        walked = self.walk(track_packet(1, name="I32", fmt=7), numeric_record(1, 7, 5, dt=START))
        self.assertIsNone(walked.tracks["I32"].values)

    def test_the_values_follow_the_order_of_the_file_across_two_tracks_of_one_name(self) -> None:
        walked = self.walk(
            track_packet(1, name="X", fmt=7),
            track_packet(2, name="X", fmt=7),
            numeric_record(1, 7, 1, dt=START),
            numeric_record(2, 7, 2, dt=START),
            numeric_record(1, 7, 3, dt=START + 1.0),
            values_for=("X",),
        )
        self.assertEqual([1.0, 2.0, 3.0], [value for _, value in walked.tracks["X"].values or []])


class Library(unittest.TestCase):
    def test_the_records_of_the_library_equal_the_walk_when_the_count_and_the_times_are_the_same(self) -> None:
        self.assertIsNone(records_problem(7, "HR", 3, [1.0, 2.0, 3.0], 3, [1.0, 2.0, 3.0]))
        self.assertIsNone(records_problem(7, "WAVE", 5, None, 5, None))

    def test_a_different_count_is_refused_naming_the_case_the_track_and_both_counts(self) -> None:
        found = records_problem(7, "Solar8000/HR", 3, [1.0, 2.0, 3.0], 2, [1.0, 3.0]) or ""
        for part in ("case 7", "Solar8000/HR", "3", "2"):
            self.assertIn(part, found)
        self.assertIn("records", records_problem(7, "WAVE", 4, None, 3, None) or "")

    def test_the_same_count_with_another_time_is_refused(self) -> None:
        self.assertIn("time", records_problem(7, "HR", 3, [1.0, 2.0, 3.0], 3, [1.0, 2.0, 4.0]) or "")

    def test_a_track_that_one_side_does_not_hold_is_refused_by_name(self) -> None:
        self.assertIsNone(tracks_problem(7, ["A", "B"], ["B", "A"]))
        self.assertIn("B", tracks_problem(7, ["A", "B"], ["A"]) or "")
        self.assertIn("C", tracks_problem(7, ["A"], ["A", "C"]) or "")


class Frames(Files):
    def test_a_packet_of_an_unknown_type_is_skipped_as_the_library_skips_it(self) -> None:
        self.assertIsNone(self.walk(track_packet(1), packet(77, b"abc"), record_packet(1)).problem)


if __name__ == "__main__":
    unittest.main()
