"""The Bronze rows: the stable keys, the discriminator of readings that share a case, track and time, and the second of each waveform sample.

Failure modes covered here:

- Two readings of one track and one case time get the same key, so one of them is lost or counted twice.
- The discriminator follows the sorted order or the value instead of the order of the file.
- A waveform sample on a second boundary, a record that straddles a second or a second with no sample lands in the wrong second or gets an empty row.
- A published value changes on its way into a row: a missing value, a negative zero or a text with spaces.
- A row has a different number of fields or other types than its table declares, so Spark reads it into the wrong column or fails in the middle of a write.
- A key is reused by two rows of a table, or a table's key names a column that does not exist.

The boundary cases are the hand-built recordings of ``tests/fixtures/boundary/replay_cases.json``. The waveform seconds are compared with the manifest's own
derivation (``reference_seconds``), which cuts the seconds sample by sample with its own code. The decoder with the publisher's library and the tables in
Iceberg are proved by the acceptance test ``tests/e2e/bronze_decode.py``.
"""

from __future__ import annotations

import math
import re
import unittest
from array import array
from collections import Counter
from types import SimpleNamespace

from replay.events.manifest import reference_seconds
from spark.bronze.rows import (
    Lineage,
    StructureError,
    TrackInfo,
    numeric_rows,
    string_rows,
    track_info,
    track_row,
    waveform_rows,
)
from spark.bronze.tables import NUMERIC, STRINGS, TABLES, TRACKS, WAVEFORM, TableSpec
from tests.fast.replay_support import boundary_recordings

LINEAGE = Lineage("v0001", "ab" * 32)
PYTHON_TYPES = {"bigint": int, "int": int, "string": str, "double": float}


def keys_of(table: TableSpec, rows: list[tuple[object, ...]]) -> list[tuple[object, ...]]:
    names = [name for name, _ in table.columns]
    positions = [names.index(name) for name in table.key]
    return [tuple(row[position] for position in positions) for row in rows]


def conforms(table: TableSpec, row: tuple[object, ...]) -> bool:
    """A row has one value for each column, each of the column's Python type; a double column holds a float and never an int."""
    if len(row) != len(table.columns):
        return False
    for value, (_, kind) in zip(row, table.columns, strict=True):
        if kind == "array<double>":
            if not isinstance(value, list) or any(type(item) is not float for item in value):
                return False
        elif type(value) is not PYTHON_TYPES[kind]:
            return False
    return True


class Tables(unittest.TestCase):
    def test_each_key_names_columns_of_its_table(self) -> None:
        for table in TABLES:
            names = [name for name, _ in table.columns]
            self.assertEqual(len(names), len(set(names)), table.name)
            self.assertTrue(set(table.key) <= set(names), table.name)

    def test_the_declared_keys_are_the_ones_of_the_design(self) -> None:
        self.assertEqual(("case_id", "track", "case_time_us", "source_occurrence"), NUMERIC.key)
        self.assertEqual(("case_id", "track", "case_second"), WAVEFORM.key)
        self.assertEqual(("case_id", "track"), TRACKS.key)
        self.assertEqual(("case_id", "track", "case_time_us", "source_occurrence"), STRINGS.key)

    def test_the_ddl_names_every_column_once_in_order(self) -> None:
        for table in TABLES:
            pairs = [part.split(" ", 1) for part in re.split(r", (?![^<]*>)", table.ddl)]
            self.assertEqual([(name, kind) for name, kind in table.columns], [(name, kind) for name, kind in pairs], table.name)

    def test_the_table_names_are_in_the_bronze_namespace(self) -> None:
        self.assertEqual(
            ["lake.bronze.numeric_observations", "lake.bronze.waveform_seconds", "lake.bronze.tracks", "lake.bronze.string_events"],
            [table.full_name for table in (NUMERIC, WAVEFORM, TRACKS, STRINGS)],
        )


class NumericRows(unittest.TestCase):
    def test_readings_that_share_a_time_are_kept_apart_in_the_order_of_the_file(self) -> None:
        readings = [(0, 1.0, 60.0), (1_500_000, 2.5, 62.0), (1_500_000, 2.5, 61.0), (1_500_000, 2.5, 63.0), (7_250_000, 3.0, 64.0)]
        rows = list(numeric_rows(9001, "Test/HR", readings, LINEAGE))
        self.assertEqual([0, 0, 1, 2, 0], [row[3] for row in rows])
        self.assertEqual([60.0, 62.0, 61.0, 63.0, 64.0], [row[5] for row in rows])
        self.assertEqual(len(rows), len(set(keys_of(NUMERIC, rows))))

    def test_the_same_time_on_another_track_starts_again_at_zero(self) -> None:
        first = list(numeric_rows(1, "A", [(5, 5.0, 1.0), (5, 5.0, 2.0)], LINEAGE))
        second = list(numeric_rows(1, "B", [(5, 5.0, 3.0)], LINEAGE))
        self.assertEqual([0, 1, 0], [row[3] for row in first + second])
        self.assertEqual(3, len(set(keys_of(NUMERIC, first + second))))

    def test_a_published_value_is_kept_as_it_is(self) -> None:
        readings = [(0, 0.0, math.nan), (1, 1.0, -0.0), (2, 2.0, math.inf), (3, 3.0, 5e-324), (4, 4.0, 0.1 + 0.2)]
        values = [row[5] for row in numeric_rows(1, "T", readings, LINEAGE)]
        self.assertTrue(math.isnan(values[0]))
        self.assertEqual(math.copysign(1.0, -0.0), math.copysign(1.0, values[1]))
        self.assertEqual([math.inf, 5e-324, 0.1 + 0.2], values[2:])

    def test_a_row_has_the_columns_and_types_of_its_table(self) -> None:
        for row in numeric_rows(1, "T", [(0, 0.5, 2.0)], LINEAGE):
            self.assertTrue(conforms(NUMERIC, row))
            self.assertEqual((1, "T", 0, 0, 0.5, 2.0, "v0001", "ab" * 32), row)

    def test_a_track_with_no_reading_has_no_row(self) -> None:
        self.assertEqual([], list(numeric_rows(1, "T", [], LINEAGE)))

    def test_the_rows_of_the_boundary_cases_have_the_discriminators_of_the_stream(self) -> None:
        for case, recording in boundary_recordings().items():
            for track in recording.numeric:
                readings = [(time, time / 1e6, value) for time, value in track.readings]
                rows = list(numeric_rows(case, track.name, readings, LINEAGE))
                seen: Counter[int] = Counter()
                expected = []
                for time, _ in track.readings:
                    expected.append(seen[time])
                    seen[time] += 1
                self.assertEqual(expected, [row[3] for row in rows])
                self.assertEqual([time for time, _ in track.readings], [row[2] for row in rows])


class StringRows(unittest.TestCase):
    def test_each_event_keeps_its_time_text_and_source_id(self) -> None:
        events = [(0, 0.0, "Case started"), (1_500_000, 1.5, "Test text"), (1_500_000, 1.5, "Second text at the same time"), (13_000_000, 13.0, "")]
        rows = list(string_rows(9001, "Test/EVENT", events, LINEAGE))
        self.assertEqual([0, 0, 1, 0], [row[3] for row in rows])
        self.assertEqual(["Case started", "Test text", "Second text at the same time", ""], [row[5] for row in rows])
        self.assertEqual(["9001/Test/EVENT/0", "9001/Test/EVENT/1", "9001/Test/EVENT/2", "9001/Test/EVENT/3"], [row[6] for row in rows])
        self.assertTrue(all(conforms(STRINGS, row) for row in rows))
        self.assertEqual(len(rows), len(set(keys_of(STRINGS, rows))))

    def test_the_source_id_is_the_one_the_replay_sends(self) -> None:
        recording = boundary_recordings()[9001]
        for track in recording.strings:
            rows = list(string_rows(9001, track.name, [(time, 0.0, text) for time, text in track.readings], LINEAGE))
            self.assertEqual([f"9001/{track.name}/{position}" for position in range(len(track.readings))], [row[6] for row in rows])


ORIGIN = 4102444800.0
RATE_ERRORS = (0.0, -1.0, math.nan, math.inf)


def rows_of(rate: float, records: list[tuple[float, list[float]]], case: int = 1) -> list[tuple[object, ...]]:
    """The rows of a waveform track whose records start at the given seconds from the anonymised start and whose samples are the given values."""
    return list(waveform_rows(case, "Test/WAVE", rate, [(ORIGIN + start, array("d", samples)) for start, samples in records], ORIGIN, LINEAGE))


class WaveformRows(unittest.TestCase):
    def test_a_record_that_straddles_a_second_is_cut_at_the_boundary(self) -> None:
        rows = rows_of(4.0, [(0.75, [1, 2, 3, 4, 5])])
        self.assertEqual([(0, 750_000, [1.0]), (1, 0, [2.0, 3.0, 4.0, 5.0])], [(row[2], row[3], row[4]) for row in rows])

    def test_a_sample_on_a_second_boundary_opens_the_next_second(self) -> None:
        rows = rows_of(4.0, [(0.0, [1, 2, 3, 4, 5])])
        self.assertEqual([(0, [1.0, 2.0, 3.0, 4.0]), (1, [5.0])], [(row[2], row[4]) for row in rows])

    def test_a_sample_that_lies_before_a_boundary_by_less_than_half_a_microsecond_stays_in_the_second_before(self) -> None:
        samples = [float(number) for number in range(1000)]
        rows = rows_of(500.0, [(0.9999996, samples)])
        self.assertEqual([1, 500, 499], [len(row[4]) for row in rows])  # type: ignore[arg-type]
        self.assertEqual([(0, 999_999), (1, 1999), (2, 1999)], [(row[2], row[3]) for row in rows])

    def test_a_second_with_no_sample_has_no_row(self) -> None:
        rows = rows_of(2.0, [(0.0, [1, 2]), (3.0, [3, 4])])
        self.assertEqual([0, 3], [row[2] for row in rows])

    def test_the_samples_of_one_second_follow_the_order_of_the_records(self) -> None:
        rows = rows_of(4.0, [(0.0, [1, 2]), (0.5, [3, 4]), (1.0, [5])])
        self.assertEqual([[1.0, 2.0, 3.0, 4.0], [5.0]], [row[4] for row in rows])

    def test_the_offset_of_a_second_is_that_of_its_first_sample_in_the_order_of_the_records(self) -> None:
        rows = rows_of(4.0, [(0.5, [1]), (0.25, [2])])
        self.assertEqual([(0, 500_000, [1.0, 2.0])], [(row[2], row[3], row[4]) for row in rows])

    def test_a_row_has_the_columns_and_types_of_its_table(self) -> None:
        rows = rows_of(4.0, [(0.0, [65535, 0, 7])], case=7)
        self.assertTrue(all(conforms(WAVEFORM, row) for row in rows))
        self.assertEqual((7, "Test/WAVE", 0, 0, [65535.0, 0.0, 7.0], "v0001", "ab" * 32), rows[0])

    def test_a_double_and_a_float_sample_are_kept_exactly(self) -> None:
        records = [(ORIGIN, array("f", [0.1, -0.0])), (ORIGIN + 1.0, array("d", [0.1, math.nan]))]
        rows = list(waveform_rows(1, "T", 2.0, records, ORIGIN, LINEAGE))
        self.assertEqual(array("f", [0.1])[0], rows[0][4][0])
        self.assertEqual(math.copysign(1.0, -0.0), math.copysign(1.0, rows[0][4][1]))
        self.assertEqual(0.1, rows[1][4][0])
        self.assertTrue(math.isnan(rows[1][4][1]))

    def test_the_boundary_cases_equal_the_manifest_derivation_in_seconds_and_samples_and_within_a_microsecond_in_offsets(self) -> None:
        checked = 0
        for case, recording in boundary_recordings().items():
            for track in recording.waveforms:
                reference = reference_seconds(track)
                records = [(ORIGIN + record.start_us / 1_000_000, record.samples) for record in track.records]
                rows = list(waveform_rows(case, track.name, track.sample_rate, records, ORIGIN, LINEAGE))
                self.assertEqual(sorted(reference), [row[2] for row in rows])
                for row in rows:
                    offset, samples = reference[row[2]]
                    self.assertEqual(samples, row[4])
                    self.assertLessEqual(abs(offset - row[3]), 1)
                self.assertEqual(len(rows), len(set(keys_of(WAVEFORM, rows))))
                checked += 1
        self.assertGreaterEqual(checked, 2)

    def test_a_rate_that_is_not_a_positive_number_is_refused_by_case_and_track(self) -> None:
        for rate in RATE_ERRORS:
            with self.assertRaises(StructureError) as caught:
                rows_of(rate, [(0.0, [1])], case=5)
            self.assertIn("case 5", str(caught.exception))
            self.assertIn("Test/WAVE", str(caught.exception))

    def test_a_start_that_is_not_a_number_is_refused_by_case_and_track(self) -> None:
        with self.assertRaises(StructureError) as caught:
            list(waveform_rows(5, "Test/WAVE", 4.0, [(math.nan, array("d", [1.0]))], ORIGIN, LINEAGE))
        self.assertIn("case 5", str(caught.exception))


def fake_track(**fields: object) -> SimpleNamespace:
    defaults = {
        "name": "ECG_II",
        "type": 1,
        "fmt": 6,
        "unit": "mV",
        "srate": 500.0,
        "mindisp": -1.0,
        "maxdisp": 2.0,
        "col": 7,
        "montype": 3,
        "gain": 0.01,
        "offset": -5.0,
        "dname": "SNUADC",
    }
    return SimpleNamespace(**(defaults | fields))


class TrackRows(unittest.TestCase):
    def test_a_track_keeps_every_published_field(self) -> None:
        device = SimpleNamespace(name="SNUADC", type="SNUADC-type", port="COM1")
        info = track_info("SNUADC/ECG_II", fake_track(), device)
        self.assertEqual(
            TrackInfo("SNUADC/ECG_II", "SNUADC", "ECG_II", "SNUADC-type", "COM1", "waveform", 1, 6, "uint16", "mV", 500.0, 0.01, -5.0, -1.0, 2.0, 7, 3), info
        )
        row = track_row(565, info, LINEAGE)
        self.assertTrue(conforms(TRACKS, row))
        self.assertEqual((565, "SNUADC/ECG_II"), row[:2])
        self.assertEqual(("v0001", "ab" * 32), row[-2:])

    def test_each_sample_format_of_the_file_format_has_its_published_type_name(self) -> None:
        names = {1: "float32", 2: "float64", 3: "int8", 4: "uint8", 5: "int16", 6: "uint16", 7: "int32", 8: "uint32"}
        for fmt, name in names.items():
            info = track_info("X", fake_track(fmt=fmt), None)
            self.assertEqual((fmt, name), (info.sample_format, info.sample_type))

    def test_a_format_with_no_sample_type_keeps_its_code_and_has_no_name(self) -> None:
        for fmt in (0, 9, 255):
            info = track_info("N", fake_track(type=2, fmt=fmt), None)
            self.assertEqual((fmt, ""), (info.sample_format, info.sample_type))
            self.assertTrue(conforms(TRACKS, track_row(1, info, LINEAGE)))

    def test_a_text_track_is_text_whatever_its_format_code_is(self) -> None:
        for fmt in (0, 1, 6, 9):
            info = track_info("Solar8000/GAS2_AGENT", fake_track(type=5, fmt=fmt), None)
            self.assertEqual(("string", fmt, "text"), (info.kind, info.sample_format, info.sample_type), fmt)
            self.assertEqual(fmt, track_row(565, info, LINEAGE)[8])

    def test_the_rate_the_gain_and_the_offset_of_a_track_are_in_its_row_as_published(self) -> None:
        info = track_info("X", fake_track(srate=62.5, gain=0.1, offset=-20.0), None)
        row = dict(zip((name for name, _ in TRACKS.columns), track_row(3, info, LINEAGE), strict=True))
        self.assertEqual((62.5, 0.1, -20.0, "uint16", 6), (row["sample_rate"], row["gain"], row["value_offset"], row["sample_type"], row["sample_format"]))

    def test_a_track_with_no_device_has_empty_device_fields(self) -> None:
        info = track_info("EVENT", fake_track(name="EVENT", type=5, fmt=0, unit="", srate=0.0, dname=""), None)
        self.assertEqual(("", "", "", "string"), (info.device, info.device_type, info.device_port, info.kind))

    def test_numbers_that_the_library_gives_as_integers_become_floats(self) -> None:
        info = track_info("X", fake_track(srate=0, gain=0, offset=0, mindisp=0, maxdisp=0), None)
        row = track_row(1, info, LINEAGE)
        self.assertTrue(conforms(TRACKS, row))

    def test_a_track_type_that_the_design_does_not_carry_is_refused(self) -> None:
        with self.assertRaises(StructureError) as caught:
            track_info("X", fake_track(type=4), None)
        self.assertIn("X", str(caught.exception))

    def test_the_rows_of_one_case_have_one_key_for_each_track(self) -> None:
        infos = [track_info(f"D/T{number}", fake_track(name=f"T{number}"), None) for number in range(3)]
        rows = [track_row(1, info, LINEAGE) for info in infos]
        self.assertEqual(3, len(set(keys_of(TRACKS, rows))))


if __name__ == "__main__":
    unittest.main()
