"""How the Spark steps of the Silver cases job decide to write a table, on a stand-in for the Spark session.

A stand-in cannot show that Spark does what the calls ask: the acceptance run does. These tests hold the decisions of the job: a table that already holds the
rows is not written again, a table with the same columns is overwritten in place, a table with other columns (a schema change of the job's own output, such
as ``icu_days`` from a whole number to a double) is dropped on the private branch and made again as a new table, and a table with other columns is never read as
if it had the expected ones.

Failure modes covered here:

- A rerun that writes a new snapshot of an unchanged table.
- A replace of an existing table, which stalls in Spark with the Nessie catalog: ``createOrReplace`` is never called, not for a table with the same columns and
  not for a table whose columns changed (the inherited ``silver.cases`` with ``icu_days`` as ``bigint``).
- A table with changed columns that is overwritten in place, so rows of one schema are written into the other, or that is dropped after the new rows were
  planned but made again with the old properties or format version.
- A property that is lost or never set, so the never-feature marking is missing.
- An inherited measure or cutoff table with other columns, read as if it were the frozen one.
"""

from __future__ import annotations

import unittest
from collections import Counter
from typing import Any

from scripts.sample.split import patient_part
from spark.jobs.silver_cases import CUTOFF_COLUMNS, MEASURE_COLUMNS, Lake
from spark.silver_cases import CASE_COLUMNS, LAB_COLUMNS, CutoffRow, SilverCasesError, cutoff_rows, measure_rows
from spark.split_table import RULE_VERSION, SET_NAMES


class Rows:
    def __init__(self, value: int) -> None:
        self.value = value

    def count(self) -> int:
        return self.value


class Difference:
    """The rows of one side that the other side lacks. A count of all of them must never be taken, because they may include held-out rows: only the first
    row may be asked for."""

    def __init__(self, value: int) -> None:
        self.value = value

    def count(self) -> int:
        raise AssertionError("a count of differing rows must not be taken")

    def limit(self, rows: int) -> Rows:
        return Rows(min(self.value, rows))


class Writer:
    def __init__(self, log: list[str], table: str) -> None:
        self.log = log
        self.table = table

    def tableProperty(self, key: str, value: str) -> Writer:
        self.log.append(f"property {key}={value}")
        return self

    def createOrReplace(self) -> None:
        self.log.append(f"createOrReplace {self.table}")

    def create(self) -> None:
        self.log.append(f"create {self.table}")

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

    def exceptAll(self, other: Frame) -> Difference:
        return Difference(sum((Counter(self.rows) - Counter(other.rows)).values()))

    def coalesce(self, _count: int) -> Frame:
        return self

    def writeTo(self, table: str) -> Writer:
        return Writer(self.log, table)

    def collect(self) -> list[Any]:
        return self.rows


class Result:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def collect(self) -> list[dict[str, Any]]:
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
        self.flag = 0

    def table(self, name: str) -> Frame:
        return self.tables[name]

    def sql(self, text: str) -> Result:
        if text.startswith("SELECT CASE WHEN COUNT(*) > 0"):
            return Result([{"n": self.flag}])
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


def frame(rows: list[Any], log: list[str], dtypes: list[tuple[str, str]] | None = None) -> Frame:
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
        self.assertEqual(["property format-version=2", "property never_feature_columns=x,y", f"create {TABLE}"], log)

    def test_a_table_with_other_columns_or_types_is_dropped_and_made_again_and_never_replaced(self) -> None:
        engine, log = lake(None)
        held = frame([("x", 1)], log, [("a", "string"), ("b", "int")])
        engine, log = lake(held, PROPERTIES)
        self.assertTrue(engine.write_frame(frame([("x", 1)], log), TABLE, PROPERTIES))
        self.assertEqual([f"DROP TABLE {TABLE}", "property format-version=2", "property never_feature_columns=x,y", f"create {TABLE}"], log)

    def test_the_inherited_cases_table_with_icu_days_as_a_whole_number_is_rebuilt_without_a_replace(self) -> None:
        old = [(name, "bigint" if name == "icu_days" else kind) for name, kind in ((column.name, column.type) for column in CASE_COLUMNS)]
        new = [(column.name, column.type) for column in CASE_COLUMNS]
        self.assertEqual("double", dict(new)["icu_days"])
        self.assertNotEqual(old, new)
        engine, log = lake(None)
        held = frame([("x", 1)], log, old)
        engine, log = lake(held, PROPERTIES)
        self.assertTrue(engine.write_frame(frame([("x", 1)], log, new), TABLE, PROPERTIES))
        self.assertNotIn(f"createOrReplace {TABLE}", log)
        self.assertNotIn(f"overwritePartitions {TABLE}", log)
        self.assertEqual([f"DROP TABLE {TABLE}", f"create {TABLE}"], [line for line in log if line.startswith(("DROP", "create", "overwrite"))])
        self.assertLess(log.index(f"DROP TABLE {TABLE}"), log.index(f"create {TABLE}"))

    def test_no_write_of_the_job_calls_a_replace(self) -> None:
        import inspect

        from spark.jobs import silver_cases

        self.assertNotIn(".createOrReplace(", inspect.getsource(silver_cases))


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


def split_table(count: int, relabel: int | None = None) -> Frame:
    rows = [
        {"subjectid": str(patient), "set": SET_NAMES[patient_part(patient)], "rule_version": RULE_VERSION, "source_snapshot_id": 7}
        for patient in range(1, count + 1)
    ]
    if relabel is not None:
        rows[relabel]["set"] = "training"
    return Frame([("subjectid", "string")], rows, [])  # type: ignore[arg-type]


class WriteRulesTest(unittest.TestCase):
    def test_held_rows_in_another_order_are_the_same_rows_and_nothing_is_written(self) -> None:
        log: list[str] = []
        lengths = [3600, 7200, 5400, 4800]
        measure, cutoffs = measure_rows(lengths), cutoff_rows(lengths)
        measure_rows_held = [{"statistic": r.statistic, "unit": r.unit, "value": r.value} for r in measure[::-1]]
        cutoff_rows_held = [
            {
                "cutoff": r.cutoff,
                "anchor": r.anchor,
                "elapsed_seconds": r.elapsed_seconds,
                "basis": r.basis,
                "rule_version": r.rule_version,
                "development_cases": r.development_cases,
                "median_length_seconds": r.median_length_seconds,
            }
            for r in cutoffs[::-1]
        ]
        tables = {
            "lake.silver.operation_length_measure": frame(measure_rows_held, log, list(MEASURE_COLUMNS)),
            "lake.silver.score_cutoffs": frame(cutoff_rows_held, log, list(CUTOFF_COLUMNS)),
        }
        engine = Lake(Session(tables, {}, log), None, "b")  # type: ignore[arg-type]
        self.assertFalse(engine.write_rules(measure, cutoffs))
        self.assertEqual([], log)


class LaboratoryScopeTest(unittest.TestCase):
    def engine(self) -> Lake:
        return Lake(Session({}, {}, []), None, "b")  # type: ignore[arg-type]

    def test_the_development_scope_keeps_the_development_sets_and_the_held_out_scope_keeps_every_other_row_with_a_case(self) -> None:
        engine = self.engine()
        self.assertIn("WHERE s.`set` IN ('training', 'validation')", engine.bronze_lab_scope(development=True))
        self.assertIn("WHERE COALESCE(s.`set`, '') NOT IN ('training', 'validation')", engine.bronze_lab_scope(development=False))
        self.assertIn("WHERE s.`set` IN ('training', 'validation')", engine.silver_lab_scope(development=True))
        self.assertIn("WHERE COALESCE(s.`set`, '') NOT IN ('training', 'validation')", engine.silver_lab_scope(development=False))

    def test_both_tables_are_scoped_by_the_bronze_case_owner_and_never_by_the_silver_case_row(self) -> None:
        engine = self.engine()
        for development in (True, False):
            for scope in (engine.bronze_lab_scope(development=development), engine.silver_lab_scope(development=development)):
                self.assertIn("lake.bronze.clinical_cases", scope)
                self.assertNotIn("lake.silver.cases", scope)

    def test_the_checks_of_a_written_table_use_the_plain_cast_and_the_build_side_uses_the_guarded_one(self) -> None:
        engine = self.engine()
        self.assertIn("TRY_CAST", engine.bronze_lab_scope(development=True))
        self.assertNotIn("TRY_CAST", engine.bronze_lab_scope(development=True, strict=True))


class OwnershipTest(unittest.TestCase):
    def test_a_bronze_case_with_more_than_one_row_is_one_code_and_a_clean_table_is_none(self) -> None:
        session = Session({}, {}, [])
        engine = Lake(session, None, "b")  # type: ignore[arg-type]
        self.assertEqual([], engine.ownership_problems())
        session.flag = 1
        self.assertEqual(["ambiguous_case_owner: a Bronze case has more than one row or more than one patient"], engine.ownership_problems())


class PinnedReadTest(unittest.TestCase):
    def test_the_snapshot_lookup_of_a_pinned_branch_names_the_branch_at_its_hash(self) -> None:
        seen: list[str] = []

        class Client:
            def request(self, method: str, path: str) -> dict[str, Any]:
                seen.append(path)
                return {"content": {"snapshotId": 5}}

        engine = Lake(Session({}, {}, []), Client(), "run_b", at="a" * 64)  # type: ignore[arg-type]
        self.assertEqual(5, engine.snapshot_id("silver.cases"))
        self.assertEqual([f"/trees/run_b@{'a' * 64}/contents/silver.cases"], seen)

    def test_a_branch_that_is_not_pinned_is_looked_up_by_name(self) -> None:
        seen: list[str] = []

        class Client:
            def request(self, method: str, path: str) -> dict[str, Any]:
                seen.append(path)
                return {}

        engine = Lake(Session({}, {}, []), Client(), "run_b")  # type: ignore[arg-type]
        self.assertIsNone(engine.snapshot_id("silver.cases"))
        self.assertEqual(["/trees/run_b/contents/silver.cases"], seen)


class SplitRuleTest(unittest.TestCase):
    def test_a_split_made_by_the_rule_passes(self) -> None:
        engine = Lake(Session({"lake.silver.patient_split": split_table(300)}, {}, []), None, "b")  # type: ignore[arg-type]
        self.assertEqual([], engine.split_problems())

    def test_a_held_out_patient_relabelled_training_is_refused_before_any_length_is_measured(self) -> None:
        held_out = next(patient for patient in range(1, 300) if patient_part(patient) == "test")
        engine = Lake(Session({"lake.silver.patient_split": split_table(300, held_out - 1)}, {}, []), None, "b")  # type: ignore[arg-type]
        self.assertEqual(["rule_mismatch: 1 patient"], engine.split_problems())


class OutputSchemaTest(unittest.TestCase):
    def tables(self, cases: list[tuple[str, str]], lab: list[tuple[str, str]]) -> Lake:
        log: list[str] = []
        held = {"lake.silver.cases": frame([], log, cases), "lake.silver.lab_results": frame([], log, lab)}
        return Lake(Session(held, {}, log), None, "b")  # type: ignore[arg-type]

    def test_tables_with_the_columns_and_types_of_the_rules_are_not_counted(self) -> None:
        engine = self.tables([(c.name, c.type) for c in CASE_COLUMNS], [(c.name, c.type) for c in LAB_COLUMNS])
        self.assertEqual(0, engine.output_schema_tables_wrong())

    def test_a_changed_type_an_extra_column_or_a_missing_table_is_counted_once_for_each_table(self) -> None:
        cases = [(c.name, "string" if c.name == "height" else c.type) for c in CASE_COLUMNS]
        lab = [*((c.name, c.type) for c in LAB_COLUMNS), ("extra", "string")]
        self.assertEqual(2, self.tables(cases, lab).output_schema_tables_wrong())
        engine = Lake(Session({}, {}, []), None, "b")  # type: ignore[arg-type]
        self.assertEqual(2, engine.output_schema_tables_wrong())


if __name__ == "__main__":
    unittest.main()
