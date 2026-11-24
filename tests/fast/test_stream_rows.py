"""The rows that cross Spark's edge: the decoded input event into the keyed operator, the records out of it and the lake tables they are merged into.

Failure modes covered here:

- An event or a record loses or changes a field on its way through a Spark row, for one of its kinds.
- A column of a lake table has no field in its record, or a record field has no column, so a merge or a read fails or drops a value.
- The columns of the declared Spark schema and the columns of the rows differ, so the operator fails on its first output.
- The merge of a table is not keyed on the record's stable ID, so a repeated batch adds a second row.
"""

from __future__ import annotations

import dataclasses
import re
import unittest

from spark.streaming.model import DecisionRecord, ErrorRecord, FeatureRecord, LedgerRecord, Record
from spark.streaming.rows import (
    INPUT_COLUMNS,
    OUTPUT_COLUMNS,
    TABLES,
    event_from_row,
    event_row,
    record_from_row,
    record_row,
    table_values,
)
from spark.streaming.sinks import create_sql, merge_sql
from tests.fast.stream_support import RUN, events_for, synthetic_recording

RECORDS: list[Record] = [
    FeatureRecord("a|0|1.0|60|v", RUN, 0, "1.0", 1, "v", 60, 70, True, '{"features":{}}', False, ("waveform_invalid_second", "x")),
    FeatureRecord("a|0|1.0|120|v", RUN, 0, "1.0", 1, "v", 120, 130, False, "{}", True, ()),
    DecisionRecord("e#0:5", "e", RUN, 1, "1.0", "numeric", "t", 5, 6, 7, 1, "late_accepted", 0, 5),
    LedgerRecord("a|0|1.0|ledger", RUN, 0, "1.0", 1, '{"final_seen":true}'),
    ErrorRecord("a|0|1.0|60|v|error", RUN, 0, "1.0", 60, "the sample rate is not a positive whole number"),
]


class Events(unittest.TestCase):
    def test_every_event_of_a_case_survives_a_spark_row(self) -> None:
        events = events_for(synthetic_recording(caseend_s=60, opend_s=50))
        self.assertEqual({"numeric", "waveform", "string", "op_start", "final_score", "case_end"}, {event.kind for event in events})
        for event in events:
            self.assertEqual(repr(event), repr(event_from_row(event_row(event))))

    def test_the_input_columns_are_the_columns_of_an_event_row(self) -> None:
        row = event_row(events_for(synthetic_recording(caseend_s=60, opend_s=50))[0])
        self.assertEqual([name for name, _ in INPUT_COLUMNS], list(row))

    def test_a_waveform_row_keeps_the_sample_format_of_its_track_as_an_integer_column(self) -> None:
        events = events_for(synthetic_recording(caseend_s=60, opend_s=50, arterial_format=5, arterial_gain=0.01, arterial_offset=382.68))
        formats = {event.track: event_row(event)["sample_format"] for event in events if event.kind == "waveform"}
        self.assertEqual({"SNUADC/ART": 5, "SNUADC/PLETH": 5}, formats)
        self.assertEqual(("sample_format", "int"), next(column for column in INPUT_COLUMNS if column[0] == "sample_format"))
        self.assertEqual(5, event_from_row(event_row(next(event for event in events if event.kind == "waveform"))).sample_format)


class Records(unittest.TestCase):
    def test_every_record_survives_an_output_row(self) -> None:
        for record in RECORDS:
            with self.subTest(record=record):
                self.assertEqual(record, record_from_row(record_row(record)))

    def test_the_output_columns_are_the_columns_of_every_output_row(self) -> None:
        for record in RECORDS:
            self.assertEqual([name for name, _ in OUTPUT_COLUMNS], list(record_row(record)))


class Tables(unittest.TestCase):
    def test_each_table_has_one_column_for_each_field_of_its_record(self) -> None:
        for record_type, spec in TABLES.items():
            with self.subTest(table=spec.name):
                fields = [field.name for field in dataclasses.fields(record_type)]
                self.assertEqual(fields, [name for name, _ in spec.columns])
                record = next(record for record in RECORDS if isinstance(record, record_type))
                self.assertEqual(len(fields), len(table_values(record)))

    def test_a_merge_is_keyed_on_the_stable_id_and_adds_only_what_is_new(self) -> None:
        for spec in TABLES.values():
            sql = merge_sql(spec)
            self.assertIn(f"ON t.{spec.key} = s.{spec.key}", sql)
            self.assertIn("WHEN NOT MATCHED THEN INSERT *", sql)
            self.assertNotIn("UPDATE", sql)
            self.assertNotIn("DELETE", sql)
            self.assertEqual({"feature_id", "decision_id", "ledger_id", "error_id"}, {s.key for s in TABLES.values()})

    def test_a_table_is_created_once_with_the_columns_of_its_record(self) -> None:
        for spec in TABLES.values():
            sql = create_sql(spec)
            self.assertTrue(sql.startswith("CREATE TABLE IF NOT EXISTS "))
            for name, ddl in spec.columns:
                self.assertRegex(sql, rf"\b{re.escape(name)} {re.escape(ddl)}[,)]")

    def test_the_tables_are_in_the_feature_and_stream_namespaces_of_the_lake_catalog(self) -> None:
        self.assertEqual(
            {"lake.feature.stream_features", "lake.stream.event_decisions", "lake.stream.occurrence_ledger", "lake.stream.feature_errors"},
            {spec.name for spec in TABLES.values()},
        )


if __name__ == "__main__":
    unittest.main()
