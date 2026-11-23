"""The stand-in for the publisher's library follows the installed library: the same synthetic files give the same records through both.

The stand-in (``tests/fast/fake_vitaldb.py``) is a port of ``VitalFile.load_vital`` made by reading the library, so it can drift from it. ``Parity`` reads each
synthetic file through the real library and through the stand-in and requires the same answer: the return value, the times, the devices and each track's
description and records. It needs the real library, which only the Spark image holds, so it is skipped in the tools environment; the acceptance
test of the decoder runs it in the Spark image and refuses a skip. ``StandIn`` states what the stand-in does at the two places where it once differed (a reset
command, and the seek over a track that was not selected), and runs everywhere.

Failure modes covered here:

- A reset command leaves the records that came before it, so a track shows records that the real library has dropped.
- The stand-in reads the records of a track that was not selected, or of another track that lies inside the length that the unselected track declared, where
  the real library seeks over them and does not read them.
- Any other difference between the port and the library: a name matched differently, a record skipped differently, a limit applied to a packed file.
"""

from __future__ import annotations

import contextlib
import gzip
import importlib
import io
import struct
import tempfile
import unittest
from pathlib import Path
from typing import Any

from tests.fast.fake_vitaldb import FakeVitalFile
from tests.fast.vital_support import START, device_packet, numeric_record, packet, record_packet, text_record, track_packet, vital, waveform_record

RECORD = len(record_packet(1))
EXACT_ONLY = "exact"


def real_library() -> Any:
    """The installed library, or ``None`` when it is not installed (the stand-in of another test module is not the library)."""
    try:
        module = importlib.import_module("vitaldb")
    except ImportError:
        return None
    return module if getattr(module, "__file__", None) else None


class Exact:
    """Names that match a track only by its whole name (the object that the decoder passes), whose iteration is empty."""

    def __init__(self, names: list[str]) -> None:
        self.names = frozenset(names)

    def __contains__(self, name: object) -> bool:
        return name in self.names

    def __len__(self) -> int:
        return len(self.names)

    def __iter__(self) -> Any:
        return iter(())


def reset_packet() -> bytes:
    return packet(6, bytes([6]))


def packed_header() -> bytes:
    """A header of 27 bytes: the version words, the start and end times, and the packed flag."""
    return bytes(10) + struct.pack("<dd", START, START + 20.0) + b"\x01"


def files() -> dict[str, bytes]:
    """The synthetic files by name, each as its uncompressed payload (built with the header that it needs)."""
    event = track_packet(1, name="EVENT", kind=5, fmt=0)
    cases: dict[str, bytes] = {
        "a reset command between two events": vital(event, text_record(1, "before", dt=START), reset_packet(), text_record(1, "after", dt=START + 1.0)),
        "a reset command and no event before it": vital(event, reset_packet(), text_record(1, "after", dt=START + 1.0)),
        "a reset command and an event track with a device": vital(
            device_packet(1, "T", "D"), track_packet(1, name="EVENT", kind=5, fmt=0, did=1), text_record(1, "a"), reset_packet(), text_record(1, "b")
        ),
        "a track that was not selected, declaring its length, with whole records": vital(
            track_packet(1, reclen=2 * RECORD, name="X"),
            track_packet(2, reclen=2 * RECORD, name="Y"),
            numeric_record(1, 1, 1.0, dt=START),
            numeric_record(1, 1, 2.0, dt=START + 1.0),
            numeric_record(2, 1, 3.0, dt=START),
            numeric_record(2, 1, 4.0, dt=START + 1.0),
        ),
        "a track that was not selected, declaring its length, with the records of another track inside it": vital(
            track_packet(1, reclen=2 * RECORD, name="X"),
            track_packet(2, name="Y"),
            numeric_record(1, 1, 1.0, dt=START),
            numeric_record(2, 1, 3.0, dt=START),
            numeric_record(1, 1, 2.0, dt=START + 1.0),
            numeric_record(2, 1, 4.0, dt=START + 1.0),
        ),
        "a track that was not selected, declaring no length": vital(
            track_packet(1, name="X"),
            track_packet(2, name="Y"),
            numeric_record(1, 1, 1.0, dt=START),
            numeric_record(2, 1, 3.0, dt=START),
            numeric_record(1, 1, 2.0, dt=START + 1.0),
            numeric_record(2, 1, 4.0, dt=START + 1.0),
        ),
        "names that end alike": vital(
            device_packet(1, "T", "D"),
            track_packet(1, name="HR"),
            track_packet(2, name="HR", did=1),
            numeric_record(1, 1, 1.0),
            numeric_record(2, 1, 2.0),
        ),
        "a four-byte integer track": vital(track_packet(1, name="I", fmt=7), numeric_record(1, 7, 5, dt=START), numeric_record(1, 1, 1.0)),
        "a four-byte integer track after a float track": vital(
            track_packet(1, name="F"), track_packet(2, name="I", fmt=8), numeric_record(1, 1, 1.0), numeric_record(2, 8, 5)
        ),
        "text with an invalid byte": vital(
            track_packet(1, name="E", kind=5, fmt=0),
            record_packet(1, payload=struct.pack("<I", 0) + struct.pack("<I", 5) + b"ab\xffcd"),
        ),
        "a waveform and a numeric record": vital(
            track_packet(1, name="W", kind=1, fmt=6, rate=2.0),
            track_packet(2, name="N"),
            waveform_record(1, 6, [1, 2, 3]),
            numeric_record(2, 1, 9.5),
        ),
        "a record of a track that is not declared": vital(track_packet(1, name="N"), numeric_record(1, 1, 1.0), record_packet(9)),
        "a numeric record that is too short": vital(track_packet(1, name="N"), numeric_record(1, 1, 1.0), record_packet(1, payload=bytes(2))),
        "a packet over one million bytes": vital(
            track_packet(1, name="N"), numeric_record(1, 1, 1.0), packet(77, bytes(1_000_001)), numeric_record(1, 1, 2.0, dt=START + 1.0)
        ),
    }
    cases["a packet over one million bytes in a packed file"] = vital(
        track_packet(1, reclen=2 * RECORD, name="N"),
        numeric_record(1, 1, 1.0),
        packet(77, bytes(1_000_001)),
        numeric_record(1, 1, 2.0, dt=START + 1.0),
        header=packed_header(),
    )
    cases["a packed file with declared lengths"] = vital(
        track_packet(1, reclen=2 * RECORD, name="A"),
        track_packet(2, reclen=2 * RECORD, name="B"),
        numeric_record(1, 1, 1.0),
        numeric_record(1, 1, 2.0, dt=START + 1.0),
        numeric_record(2, 1, 3.0),
        numeric_record(2, 1, 4.0, dt=START + 1.0),
        header=packed_header(),
    )
    return cases


SELECTIONS: list[tuple[str, Any]] = [
    ("all tracks", None),
    ("the track X by a plain list", ["X"]),
    ("the track Y by a plain list", ["Y"]),
    ("the track HR by a plain list", ["HR"]),
    ("the track D/HR by a plain list", ["D/HR"]),
    ("the track A by a plain list", ["A"]),
    ("the track N by whole name", EXACT_ONLY),
]


def read(library_class: Any, path: Path, names: Any, header_only: bool) -> dict[str, Any]:
    """The answer of one read: the return value, the times, the devices and, for each track, its description and every record."""
    file = library_class()
    file.dtstart = 0
    file.dgmt = 0
    with contextlib.redirect_stdout(io.StringIO()):
        returned = file.load_vital(str(path), track_names=names, header_only=header_only)
    tracks = {}
    for name, track in sorted(file.trks.items()):
        records = []
        for record in track.recs:
            value = record["val"]
            records.append((record["dt"], (value.dtype.char, bytes(value.tobytes())) if hasattr(value, "tobytes") else value))
        tracks[name] = (track.type, track.fmt, track.unit, track.srate, track.gain, track.offset, track.name, track.dname, records)
    return {
        "returned": returned,
        "dtstart": file.dtstart,
        "dtend": file.dtend,
        "packed": file.packed,
        "devices": {name: (device.type, device.port) for name, device in sorted(file.devs.items())},
        "tracks": tracks,
    }


class Folder(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name)

    def written(self, name: str, payload: bytes) -> Path:
        path = self.root / (name.replace(" ", "_") + ".vital")
        path.write_bytes(gzip.compress(payload))
        return path


class StandIn(Folder):
    def records(self, payload: bytes, names: Any = None) -> dict[str, list[Any]]:
        answer = read(FakeVitalFile, self.written("case", payload), names, False)
        return {name: [value for _, value in track[8]] for name, track in answer["tracks"].items()}

    def test_a_reset_command_leaves_only_the_events_that_come_after_it(self) -> None:
        cases = files()
        self.assertEqual({"EVENT": ["after"]}, self.records(cases["a reset command between two events"]))
        self.assertEqual({"EVENT": ["after"]}, self.records(cases["a reset command and no event before it"]))

    def test_a_reset_command_clears_only_the_track_named_event_without_a_device(self) -> None:
        records = self.records(files()["a reset command and an event track with a device"])
        self.assertEqual({"D/EVENT": ["a", "b"]}, records)

    def test_a_track_that_was_not_selected_and_declared_its_length_is_seeked_over_whole(self) -> None:
        records = self.records(files()["a track that was not selected, declaring its length, with whole records"], ["Y"])
        self.assertEqual({"Y": [3.0, 4.0]}, records)

    def test_the_records_of_another_track_inside_that_length_are_lost(self) -> None:
        records = self.records(files()["a track that was not selected, declaring its length, with the records of another track inside it"], ["Y"])
        self.assertEqual({"Y": []}, records)

    def test_a_track_that_declared_no_length_loses_nothing(self) -> None:
        records = self.records(files()["a track that was not selected, declaring no length"], ["Y"])
        self.assertEqual({"Y": [3.0, 4.0]}, records)

    def test_a_packed_file_may_hold_a_packet_over_the_limit_and_a_plain_file_ends_at_it(self) -> None:
        cases = files()
        self.assertEqual({"N": [1.0]}, self.records(cases["a packet over one million bytes"]))
        self.assertEqual({"N": [1.0, 2.0]}, self.records(cases["a packet over one million bytes in a packed file"]))

    def test_a_waveform_record_extends_the_end_of_the_recording_by_its_length_over_its_rate(self) -> None:
        payload = files()["a waveform and a numeric record"]
        answer = read(FakeVitalFile, self.written("case", payload), None, False)
        self.assertEqual((START, START + 1.5), (answer["dtstart"], answer["dtend"]))

    def test_a_waveform_track_with_no_rate_or_a_record_that_does_not_fit_does_not_extend_the_end(self) -> None:
        no_rate = vital(track_packet(1, name="W", kind=1, fmt=6, rate=0.0), waveform_record(1, 6, [1, 2, 3]))
        answer = read(FakeVitalFile, self.written("norate", no_rate), None, False)
        self.assertEqual((True, START), (answer["returned"], answer["dtend"]))
        short = vital(track_packet(1, name="W", kind=1, fmt=6, rate=2.0), record_packet(1, payload=struct.pack("<I", 9) + bytes(4)))
        self.assertEqual(START, read(FakeVitalFile, self.written("short", short), None, False)["dtend"])

    def test_a_header_only_read_does_not_extend_the_end_by_a_waveform_record(self) -> None:
        payload = files()["a waveform and a numeric record"]
        self.assertEqual(START, read(FakeVitalFile, self.written("header", payload), None, True)["dtend"])

    def test_a_four_byte_integer_value_ends_the_read_with_false_and_keeps_what_was_read_before_it(self) -> None:
        answer = read(FakeVitalFile, self.written("case", files()["a four-byte integer track after a float track"]), None, False)
        self.assertFalse(answer["returned"])
        self.assertEqual([1.0], [value for _, value in answer["tracks"]["F"][8]])


@unittest.skipUnless(real_library() is not None, "the publisher's library is installed only in the Spark image")
class Parity(Folder):
    def test_every_synthetic_file_and_selection_gives_the_same_answer_through_the_library_and_the_stand_in(self) -> None:
        real = real_library().VitalFile
        checked = 0
        for name, payload in files().items():
            path = self.written(name, payload)
            for label, selection in SELECTIONS:
                for header_only in (False, True):
                    names = Exact(["N"]) if selection == EXACT_ONLY else selection
                    with self.subTest(file=name, selection=label, header_only=header_only):
                        self.assertEqual(read(real, path, names, header_only), read(FakeVitalFile, path, names, header_only))
                    checked += 1
        self.assertEqual(len(files()) * len(SELECTIONS) * 2, checked)

    def test_the_two_documented_differences_hold_in_the_real_library(self) -> None:
        real = real_library().VitalFile
        cases = files()
        reset = read(real, self.written("reset", cases["a reset command between two events"]), None, False)
        self.assertEqual(["after"], [value for _, value in reset["tracks"]["EVENT"][8]])
        inside = read(
            real,
            self.written("inside", cases["a track that was not selected, declaring its length, with the records of another track inside it"]),
            ["Y"],
            False,
        )
        self.assertEqual([], inside["tracks"]["Y"][8])


if __name__ == "__main__":
    unittest.main()
