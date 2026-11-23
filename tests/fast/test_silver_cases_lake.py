"""How the Spark steps of the Silver cases job decide to write a table, on a stand-in for the Spark session.

A stand-in cannot show that Spark does what the calls ask: the acceptance run does. These tests hold the decisions of the job: a table that already holds the
rows is not written again, a table with the same columns is overwritten in place, any other table is replaced, and a table with other columns is never read
as if it had the expected ones.

Failure modes covered here:

- A rerun that writes a new snapshot of an unchanged table.
- A replace of an existing table that has the same columns, which stalls in Spark with the Nessie catalog.
- A property that is lost or never set, so the never-feature marking is missing.
- An inherited measure or cutoff table with other columns, read as if it were the frozen one.
"""

from __future__ import annotations

import unittest
from collections import Counter
from typing import Any

from spark.jobs.silver_cases import CUTOFF_COLUMNS, Lake
from spark.silver_cases import CutoffRow, SilverCasesError


class Count:
    def __init__(self, value: int) -> None:
        self.value = value

    def count(self) -> int:
        return self.value


class Writer:
    def __init__(self, log: list[str], table: str) -> None:
        self.log = log
        self.table = table

    def tableProperty(self, key: str, value: str) -> Writer:
        self.log.append(f"property {key}={value}")
        return self

    def createOrReplace(self) -> None:
        self.log.append(f"createOrReplace {self.table}")

    def overwritePartitions(self) -> None:
        self.log.append(f"overwritePartitions {self.table}")


class Frame:
    def __init__(self, dtypes: list[tuple[str, str]], rows: list[tuple[Any, ...]], log: list[str]) -> None:
        self.dtypes = dtypes
        self.rows = rows
        self.log = log
        self.schema = self

    @property
    def fields(self) -> list[Any]:
        return [
            type("Field", (), {"name": name, "dataType": type("Type", (), {"simpleString": lambda self, kind=kind: kind})()})() for name, kind in self.dtypes
        ]

    def exceptAll(self, other: Frame) -> Count:
        return Count(sum((Counter(self.rows) - Counter(other.rows)).values()))

    def coalesce(self, _count: int) -> Frame:
        return self

    def writeTo(self, table: str) -> Writer:
        return Writer(self.log, table)

    def collect(self) -> list[Any]:
        return self.rows


class Result:
    def __init__(self, rows: list[dict[str, str]]) -> None:
        self.rows = rows

    def collect(self) -> list[dict[str, str]]:
        return self.rows


class Catalog:
    def __init__(self, tables: dict[str, Frame]) -> None:
        self.tables = tables

    def tableExists(self, name: str) -> bool:
        return name in self.tables


class Session:
    def __init__(self, tables: dict[str, Frame], properties: dict[str, str], log: list[str]) -> None:
        self.tables = tables
        self.catalog = Catalog(tables)
        self.held_properties = properties
        self.log = log

    def table(self, name: str) -> Frame:
        return self.tables[name]

    def sql(self, text: str) -> Result:
        if text.startswith("SHOW TBLPROPERTIES"):
            return Result([{"key": key, "value": value} for key, value in self.held_properties.items()])
        self.log.append(text)
        return Result([])


TYPES = [("a", "string"), ("b", "bigint")]
TABLE = "lake.silver.cases"
PROPERTIES = {"never_feature_columns": "x,y"}


def lake(held: Frame | None, properties: dict[str, str] | None = None) -> tuple[Lake, list[str]]:
    log: list[str] = []
    tables = {} if held is None else {TABLE: held}
    return Lake(Session(tables, properties or {}, log), None, "b"), log  # type: ignore[arg-type]


def frame(rows: list[tuple[Any, ...]], log: list[str], dtypes: list[tuple[str, str]] | None = None) -> Frame:
    return Frame(dtypes or TYPES, rows, log)


class WriteFrameTest(unittest.TestCase):
    def test_a_table_that_already_holds_the_rows_and_the_property_is_not_written_and_says_unchanged(self) -> None:
        engine, log = lake(None)
        held = frame([("x", 1), ("y", 2)], log)
        engine, log = lake(held, PROPERTIES)
        new = frame([("y", 2), ("x", 1)], log)
        self.assertFalse(engine.write_frame(new, TABLE, PROPERTIES))
        self.assertEqual([], log)

    def test_a_table_that_holds_the_rows_but_not_the_property_gets_only_the_property_as_one_alter(self) -> None:
        engine, log = lake(None)
        held = frame([("x", 1)], log)
        engine, log = lake(held, {})
        self.assertTrue(engine.write_frame(frame([("x", 1)], log), TABLE, PROPERTIES))
        self.assertEqual(["ALTER TABLE lake.silver.cases SET TBLPROPERTIES ('never_feature_columns' = 'x,y')"], log)

    def test_a_table_with_one_row_more_or_less_is_overwritten_in_place_and_never_replaced(self) -> None:
        for held_rows in ([("x", 1)], [("x", 1), ("y", 2), ("z", 3)], [("x", 1), ("x", 1), ("y", 2)]):
            engine, log = lake(None)
            held = frame(held_rows, log)
            engine, log = lake(held, PROPERTIES)
            self.assertTrue(engine.write_frame(frame([("x", 1), ("y", 2)], log), TABLE, PROPERTIES), held_rows)
            self.assertEqual([f"overwritePartitions {TABLE}"], log, held_rows)

    def test_a_table_that_is_not_there_is_created_with_the_format_version_and_every_property(self) -> None:
        engine, log = lake(None)
        self.assertTrue(engine.write_frame(frame([("x", 1)], log), TABLE, PROPERTIES))
        self.assertEqual(["property format-version=2", "property never_feature_columns=x,y", f"createOrReplace {TABLE}"], log)

    def test_a_table_with_other_columns_or_types_is_replaced(self) -> None:
        engine, log = lake(None)
        held = frame([("x", 1)], log, [("a", "string"), ("b", "int")])
        engine, log = lake(held, PROPERTIES)
        self.assertTrue(engine.write_frame(frame([("x", 1)], log), TABLE, PROPERTIES))
        self.assertEqual(["property format-version=2", "property never_feature_columns=x,y", f"createOrReplace {TABLE}"], log)


class ExistingRulesTest(unittest.TestCase):
    def test_a_table_that_is_not_there_is_none(self) -> None:
        engine, _ = lake(None)
        self.assertEqual((None, None), engine.existing_rules())

    def test_a_held_table_with_other_columns_is_refused_and_never_read_as_the_frozen_one(self) -> None:
        log: list[str] = []
        engine = Lake(Session({"lake.silver.operation_length_measure": frame([("m", 1)], log, [("statistic", "string")])}, {}, log), None, "b")  # type: ignore[arg-type]
        with self.assertRaises(SilverCasesError) as raised:
            engine.existing_rules()
        self.assertEqual(["inherited_schema: 1 table"], raised.exception.problems)

    def test_the_held_cutoffs_are_read_back_into_the_rows_the_rules_compare(self) -> None:
        log: list[str] = []
        row = ("start", "opstart", 0, "fixed", "v", 3, 5400)
        rows = [dict(zip((name for name, _ in CUTOFF_COLUMNS), row, strict=True))]
        engine = Lake(Session({"lake.silver.score_cutoffs": frame(rows, log, list(CUTOFF_COLUMNS))}, {}, log), None, "b")  # type: ignore[arg-type]
        self.assertEqual((None, [CutoffRow("start", "opstart", 0, "fixed", "v", 3, 5400)]), engine.existing_rules())


if __name__ == "__main__":
    unittest.main()
