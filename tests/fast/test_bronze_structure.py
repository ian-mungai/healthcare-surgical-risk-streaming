"""Structural corruption: what Bronze refuses and what it keeps.

Failure modes covered here:

- A file that is cut short, has a bad checksum inside its gzip stream or is no gzip file at all is read part way and its first tracks are kept, because the
  publisher's library ends a read at the first error and still returns what it had.
- A file that is not a case file (no ``VITA`` mark) is taken for an empty recording.
- A recording with no track, a track of a type that Bronze does not carry, a start other than the anonymised start or an end far from the clinical end of the
  case is decoded, so its times are wrong or its tail is missing without anyone saying so.
- The rule widens until it refuses a published value (a missing value, an odd unit) or a recording that ends a second before its clinical end, which is how
  every published recording ends.

The library's own reading is proved against the published recordings by the acceptance test ``tests/e2e/bronze_decode.py``.
"""

from __future__ import annotations

import gzip
import struct
import tempfile
import unittest
from pathlib import Path

from spark.bronze.structure import END_TOLERANCE_SECONDS, file_problem, recording_problem

START = 4102444800.0


def written(folder: Path, name: str, data: bytes) -> Path:
    path = folder / name
    path.write_bytes(data)
    return path


def text(value: str) -> bytes:
    data = value.encode()
    return struct.pack("<I", len(data)) + data


def packet(kind: int, body: bytes) -> bytes:
    return bytes([kind]) + struct.pack("<I", len(body)) + body


def track_packet(tid: int, reclen: int | None = None) -> bytes:
    """A track-information packet of a numeric track; ``reclen`` None leaves the field out, as the files of the fixed sample do."""
    body = struct.pack("<HBB", tid, 2, 1) + text(f"T{tid}") + text("unit") + struct.pack("<ffIfddBI", 0.0, 1.0, 0, 0.0, 1.0, 0.0, 0, 0)
    return packet(0, body if reclen is None else body + struct.pack("<I", reclen))


def record_packet(tid: int, value: float = 1.0) -> bytes:
    return packet(1, struct.pack("<HdH", 10, START, tid) + struct.pack("<f", value))


def vital(*packets: bytes, header: bytes = bytes(10)) -> bytes:
    """The uncompressed payload of a case file: the mark, the version, the header and the packets."""
    return b"VITA" + struct.pack("<IH", 3, len(header)) + header + b"".join(packets)


class Container(unittest.TestCase):
    def setUp(self) -> None:
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name)
        self.payload = vital(track_packet(1), track_packet(2), *(record_packet(1 + number % 2) for number in range(4000)))
        self.good = gzip.compress(self.payload)

    def problem(self, name: str, data: bytes) -> str | None:
        return file_problem(written(self.root, name, data))

    def test_a_complete_file_passes(self) -> None:
        self.assertIsNone(self.problem("ok.vital", self.good))

    def test_a_file_cut_short_inside_its_gzip_stream_is_refused(self) -> None:
        for cut in (len(self.good) - 1, len(self.good) // 2, 12):
            self.assertIn("complete gzip", self.problem(f"cut{cut}.vital", self.good[:cut]) or "", cut)

    def test_a_flipped_byte_inside_the_stream_is_refused(self) -> None:
        damaged = bytearray(self.good)
        damaged[len(damaged) // 2] ^= 0xFF
        self.assertIsNotNone(self.problem("flip.vital", bytes(damaged)))

    def test_a_file_that_is_not_gzip_or_is_empty_is_refused(self) -> None:
        self.assertIsNotNone(self.problem("text.vital", b"this is not a case file"))
        self.assertIsNotNone(self.problem("empty.vital", b""))

    def test_a_gzip_file_without_the_mark_is_refused(self) -> None:
        self.assertIn("VITA", self.problem("mark.vital", gzip.compress(b"NOPE" + bytes(2000))) or "")
        self.assertIn("VITA", self.problem("none.vital", gzip.compress(b"")) or "")

    def test_a_payload_cut_and_compressed_again_inside_a_packet_is_refused(self) -> None:
        for lost in (1, 4, 5, 11, 16, 20):
            self.assertIn("cut short", self.problem(f"payload{lost}.vital", gzip.compress(self.payload[:-lost])) or "", lost)

    def test_a_cut_inside_the_header_or_a_packet_head_is_refused(self) -> None:
        start = len(vital())
        for cut in (6, 9, 12, start + 3, start + 4):
            self.assertIsNotNone(self.problem(f"head{cut}.vital", gzip.compress(self.payload[:cut])), cut)

    def test_a_record_packet_too_short_to_name_its_track_is_refused(self) -> None:
        self.assertIn("record", self.problem("short.vital", gzip.compress(vital(track_packet(1), packet(1, b"\x00\x01")))) or "")

    def test_a_track_whose_records_are_not_as_long_as_it_declares_is_refused(self) -> None:
        one = len(record_packet(1))
        whole = vital(track_packet(1, reclen=3 * one), *(record_packet(1) for _ in range(3)))
        self.assertIsNone(self.problem("declared.vital", gzip.compress(whole)))
        short = vital(track_packet(1, reclen=3 * one), *(record_packet(1) for _ in range(2)))
        self.assertIn("track", self.problem("short_track.vital", gzip.compress(short)) or "")
        long = vital(track_packet(1, reclen=3 * one), *(record_packet(1) for _ in range(4)))
        self.assertIn("track", self.problem("long_track.vital", gzip.compress(long)) or "")

    def test_a_track_that_declares_no_length_or_no_record_is_kept(self) -> None:
        self.assertIsNone(self.problem("none_declared.vital", gzip.compress(vital(track_packet(1), track_packet(2, reclen=0), record_packet(1)))))

    def test_a_file_that_is_missing_is_refused_without_a_trace_of_its_path(self) -> None:
        message = file_problem(self.root / "none.vital")
        self.assertIsNotNone(message)
        self.assertNotIn(str(self.root), message or "")


def problem(**changes: object) -> str | None:
    facts: dict[str, object] = {
        "loaded": True,
        "track_types": [1, 2, 2, 5],
        "dtstart": START,
        "dtend": START + 3600.4,
        "caseend_us": 3_600_000_000,
        "anonymised_start": START,
        "tolerance_s": END_TOLERANCE_SECONDS,
    }
    return recording_problem(7, **(facts | changes))  # type: ignore[arg-type]


class Recording(unittest.TestCase):
    def test_a_recording_that_ends_within_the_tolerance_of_its_clinical_end_passes(self) -> None:
        for end in (3600.0, 3600.4, 3598.0, 3602.0, 3599.2):
            self.assertIsNone(problem(dtend=START + end), end)

    def test_an_end_just_past_the_tolerance_is_refused_on_both_sides(self) -> None:
        for end in (3602.5, 3597.5, 1800.0):
            self.assertIn("end", problem(dtend=START + end) or "", end)

    def test_the_library_failing_to_read_the_file_is_refused(self) -> None:
        self.assertIn("read", problem(loaded=False) or "")

    def test_a_file_with_no_track_is_refused(self) -> None:
        self.assertIn("no track", problem(track_types=[]) or "")

    def test_a_track_type_that_is_not_carried_is_refused(self) -> None:
        for kind in (0, 3, 4, 6):
            self.assertIn("type", problem(track_types=[2, kind]) or "", kind)

    def test_a_start_other_than_the_anonymised_start_is_refused(self) -> None:
        self.assertIn("start", problem(dtstart=START + 1.0) or "")

    def test_each_message_names_the_case(self) -> None:
        cases: list[dict[str, object]] = [{"loaded": False}, {"track_types": []}, {"track_types": [9]}, {"dtstart": 1.0}, {"dtend": START + 1.0}]
        for changes in cases:
            self.assertIn("case 7", problem(**changes) or "", changes)


if __name__ == "__main__":
    unittest.main()
