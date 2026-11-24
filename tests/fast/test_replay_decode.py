"""The replay's decoder keeps the sample format of each waveform track, from the same field of the publisher's library that Bronze reads.

The decoder runs with the stand-in for the publisher's library (``tests/fast/fake_vitaldb.py``), so these tests run its production code.

Failure modes covered here:

- The decoder drops the sample format, so a waveform second reaches the stream with no way to tell a gap marker from a reading or a float from an integer.
- The decoder reads the format from another field than Bronze does, so the stream and Silver decode one track by two formats.
- The decoder changes the raw samples, so a gap marker of an integer format is no longer the raw value that the format's marker names.

The stand-in gives the four-byte integer formats (7 and 8) a type character that the decoder reads as another dtype than the real library's, so these tests run
formats 1 to 6. The real library is run by the acceptance test ``tests/e2e/stack_replay.py``.
"""

from __future__ import annotations

import gzip
import importlib
import tempfile
import types
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from replay.events.recording import CaseTimes
from tests.fast.fake_vitaldb import FakeVitalFile, installed
from tests.fast.vital_support import START, track_packet, vital, waveform_record

TIMES = CaseTimes(7, 0, 1_000_000, 2_000_000)
decoder: Any
rows_module: Any


class PathFile(FakeVitalFile):
    """The stand-in as the decoder calls it: the real library reads the file when it is made from a path."""

    def __init__(self, path: str) -> None:
        super().__init__()
        self.load_vital(path)


def setUpModule() -> None:
    global decoder, rows_module
    unittest.enterModuleContext(installed())
    decoder = importlib.import_module("replay.service.decode")
    unittest.enterModuleContext(mock.patch.object(decoder, "vitaldb", types.SimpleNamespace(VitalFile=PathFile)))
    rows_module = importlib.import_module("spark.bronze.rows")


class SampleFormat(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name)

    def case(self, sample_format: int, samples: list[float]) -> Path:
        path = self.root / f"case{sample_format}.vital"
        path.write_bytes(
            gzip.compress(vital(track_packet(1, name="WAVE", kind=1, fmt=sample_format, rate=2.0), waveform_record(1, sample_format, samples, dt=START)))
        )
        return path

    def test_each_track_keeps_the_format_that_bronze_reads_from_the_same_track(self) -> None:
        for sample_format in range(1, 7):
            path = self.case(sample_format, [1, 2, 3])
            decoded = decoder.decode_case(path, TIMES).waveforms[0]
            bronze = rows_module.track_info("WAVE", PathFile(str(path)).trks["WAVE"], None)
            with self.subTest(sample_format=sample_format):
                self.assertEqual(sample_format, decoded.sample_format)
                self.assertEqual(bronze.sample_format, decoded.sample_format)

    def test_the_raw_samples_of_an_integer_format_reach_the_event_model_unchanged_with_the_gap_marker_as_the_raw_value(self) -> None:
        path = self.case(6, [65535, 100, 65535])
        decoded = decoder.decode_case(path, TIMES).waveforms[0]
        self.assertEqual([65535.0, 100.0, 65535.0], [float(sample) for sample in decoded.records[0].samples])


class PacketStart(unittest.TestCase):
    def test_a_packet_start_reaches_the_event_model_as_the_float_the_file_holds_with_no_rounding(self) -> None:
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        start = START + 940.0 - 2.0**-21
        path = Path(folder.name) / "case.vital"
        packets = (waveform_record(1, 2, [9, 9, 9, 9], dt=START), waveform_record(1, 2, [1, 2, 3, 4], dt=start))
        path.write_bytes(gzip.compress(vital(track_packet(1, name="WAVE", kind=1, fmt=2, rate=4.0), *packets)))
        decoded = decoder.decode_case(path, CaseTimes(7, 0, 940_000_000, 941_000_000)).waveforms[0]
        self.assertEqual([START, start], [record.start_s for record in decoded.records])
        self.assertNotEqual(START + round((start - START) * 1_000_000) / 1_000_000, decoded.records[1].start_s)
