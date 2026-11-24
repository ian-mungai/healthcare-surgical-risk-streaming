"""The production adapters of the Bronze clinical and laboratory job, called with doubles only at their outer boundaries.

The Nessie adapter runs on its real client against a scripted wire: every request is recorded and every answer is scripted. The Spark
adapter runs on a recording session, recording ``functions`` and a recording ``Window``: they log the table that each read goes through and
the exact expression that each function builds, because no Spark runs in the fast suite. The in-image check of the real rank expression is
in ``tests/e2e/bronze_clinical_lab.py``.
"""

from __future__ import annotations

import contextlib
import io
import json
import unittest
from collections.abc import Sequence
from typing import Any

from scripts.stack.nessie import NessieClient, NessieError
from spark.bronze.checks import Check, CheckResult
from spark.bronze_clinical_lab import BronzeRulesError, TableContent
from spark.bronze_clinical_lab_run import HISTORY_PAGE_SIZE, NessieAdapter
from spark.bronze_clinical_lab_spark import SparkAdapter, job_session_settings

HASH = "a" * 64
OTHER_HASH = "b" * 64
LAB_COLUMNS = ["caseid", "dt", "name", "result"]
CLINICAL_COLUMNS = ["caseid", "subjectid", "age"]
LOCATION = "file:/warehouse/raw_records/clinical_data_x/metadata/00000-y.metadata.json"


class ScriptedWire(NessieClient):
    """The real Nessie client with its one wire method replaced: requests are recorded, answers come from a script."""

    def __init__(self, script: dict[str, tuple[int, Any]]) -> None:
        super().__init__("http://nessie:19120/api/v2")
        self.script = script
        self.requests: list[tuple[str, str, Any]] = []

    def send(self, method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
        self.requests.append((method, target, json.loads(payload) if payload else None))
        status, document = self.script[f"{method} {target}"]
        return status, json.dumps(document).encode()


def entry(commit_hash: str) -> dict[str, Any]:
    return {"commitMeta": {"hash": commit_hash}}


def content_answer(location: str | None = LOCATION, snapshot: int | None = 7) -> tuple[int, Any]:
    return 200, {"content": {"type": "ICEBERG_TABLE", "metadataLocation": location, "snapshotId": snapshot, "schemaId": 0}}


class NessieAdapterTest(unittest.TestCase):
    def test_the_default_branch_is_the_one_that_nessies_configuration_names(self) -> None:
        wire = ScriptedWire({"GET /api/v2/config": (200, {"defaultBranch": "trunk", "noAncestorHash": "0" * 64})})
        self.assertEqual("trunk", NessieAdapter(wire).default_branch())
        self.assertEqual([("GET", "/api/v2/config", None)], wire.requests)

    def test_a_configuration_without_a_default_branch_is_refused(self) -> None:
        for document in ({}, {"defaultBranch": ""}, {"defaultBranch": 3}):
            wire = ScriptedWire({"GET /api/v2/config": (200, document)})
            with self.subTest(document), self.assertRaisesRegex(NessieError, "no default branch"):
                NessieAdapter(wire).default_branch()

    def test_the_history_of_the_branch_is_read_and_a_hash_in_it_is_found(self) -> None:
        target = f"/api/v2/trees/trunk/history?max-records={HISTORY_PAGE_SIZE}"
        wire = ScriptedWire({f"GET {target}": (200, {"logEntries": [entry(OTHER_HASH), entry(HASH)], "token": None})})
        self.assertTrue(NessieAdapter(wire).history_contains("trunk", HASH))
        self.assertEqual([("GET", target, None)], wire.requests)

    def test_a_hash_that_the_history_does_not_hold_is_not_found_so_an_unmerged_load_is_refused(self) -> None:
        target = f"/api/v2/trees/trunk/history?max-records={HISTORY_PAGE_SIZE}"
        wire = ScriptedWire({f"GET {target}": (200, {"logEntries": [entry(OTHER_HASH)], "token": None})})
        self.assertFalse(NessieAdapter(wire).history_contains("trunk", HASH))
        self.assertEqual(1, len(wire.requests))

    def test_the_history_is_read_page_by_page_until_the_hash_is_found(self) -> None:
        first = f"/api/v2/trees/trunk/history?max-records={HISTORY_PAGE_SIZE}"
        second = f"{first}&page-token=next"
        wire = ScriptedWire(
            {f"GET {first}": (200, {"logEntries": [entry(OTHER_HASH)], "token": "next"}), f"GET {second}": (200, {"logEntries": [entry(HASH)], "token": None})}
        )
        self.assertTrue(NessieAdapter(wire).history_contains("trunk", HASH))
        self.assertEqual([first, second], [request[1] for request in wire.requests])

    def test_the_history_that_ends_without_the_hash_stops_asking(self) -> None:
        first = f"/api/v2/trees/trunk/history?max-records={HISTORY_PAGE_SIZE}"
        second = f"{first}&page-token=next"
        wire = ScriptedWire({f"GET {first}": (200, {"logEntries": [], "token": "next"}), f"GET {second}": (200, {"logEntries": [], "token": None})})
        self.assertFalse(NessieAdapter(wire).history_contains("trunk", HASH))
        self.assertEqual(2, len(wire.requests))

    def test_the_content_of_a_table_at_a_hash_holds_the_metadata_location_and_the_snapshot_nessie_reports(self) -> None:
        wire = ScriptedWire({f"GET /api/v2/trees/trunk@{HASH}/contents/raw_records.clinical_data": content_answer()})
        found = NessieAdapter(wire).content(f"trunk@{HASH}", "raw_records.clinical_data")
        self.assertEqual(TableContent(LOCATION, 7), found)
        self.assertEqual([("GET", f"/api/v2/trees/trunk@{HASH}/contents/raw_records.clinical_data", None)], wire.requests)

    def test_two_locations_give_two_contents_so_a_metadata_only_change_shows(self) -> None:
        other = "file:/warehouse/raw_records/clinical_data_x/metadata/00001-z.metadata.json"
        wire = ScriptedWire({"GET /api/v2/trees/run/contents/t": content_answer(LOCATION), "GET /api/v2/trees/other/contents/t": content_answer(other)})
        adapter = NessieAdapter(wire)
        self.assertNotEqual(adapter.content("run", "t"), adapter.content("other", "t"))

    def test_a_table_that_the_reference_does_not_hold_is_none(self) -> None:
        wire = ScriptedWire({"GET /api/v2/trees/run/contents/t": (404, {"errorCode": "CONTENT_NOT_FOUND", "message": "no"})})
        self.assertIsNone(NessieAdapter(wire).content("run", "t"))

    def test_an_answer_without_a_location_or_a_snapshot_is_refused(self) -> None:
        for answer in (content_answer(None, 7), content_answer(LOCATION, None)):
            wire = ScriptedWire({"GET /api/v2/trees/run/contents/t": answer})
            with self.subTest(answer), self.assertRaisesRegex(NessieError, "no metadata location and snapshot"):
                NessieAdapter(wire).content("run", "t")

    def test_another_refusal_is_not_taken_for_a_missing_table(self) -> None:
        wire = ScriptedWire({"GET /api/v2/trees/run/contents/t": (500, {"message": "boom"})})
        with self.assertRaises(NessieError):
            NessieAdapter(wire).content("run", "t")

    def test_the_run_branch_is_created_from_the_default_branch_at_the_source_hash(self) -> None:
        wire = ScriptedWire({"POST /api/v2/trees?name=run&type=BRANCH": (200, {"reference": {"type": "BRANCH", "name": "run", "hash": HASH}})})
        NessieAdapter(wire).create_branch("run", "trunk", HASH)
        self.assertEqual([("POST", "/api/v2/trees?name=run&type=BRANCH", {"type": "BRANCH", "name": "trunk", "hash": HASH})], wire.requests)

    def test_a_tag_is_not_a_branch_and_the_head_is_the_hash_nessie_reports(self) -> None:
        reference = {"reference": {"type": "TAG", "name": "t", "hash": OTHER_HASH}}
        wire = ScriptedWire({"GET /api/v2/trees/t": (200, reference)})
        adapter = NessieAdapter(wire)
        self.assertFalse(adapter.is_branch("t"))
        self.assertEqual(OTHER_HASH, adapter.head("t"))


class Expression:
    """An expression that records how it was built."""

    def __init__(self, text: str) -> None:
        self.text = text

    def __str__(self) -> str:
        return self.text

    def alias(self, name: str) -> Expression:
        return Expression(f"{self.text} as {name}")

    def cast(self, kind: str) -> Expression:
        return Expression(f"{self.text}::{kind}")

    def isNull(self) -> Expression:
        return Expression(f"isnull({self.text})")

    def asc_nulls_first(self) -> Expression:
        return Expression(f"{self.text} asc_nulls_first")

    def over(self, window: Any) -> Expression:
        return Expression(f"{self.text} over ({window})")

    def __gt__(self, other: object) -> Expression:
        return Expression(f"({self.text} > {other})")

    def __ne__(self, other: object) -> Any:
        return Expression(f"({self.text} != {other})")

    def __or__(self, other: object) -> Expression:
        return Expression(f"({self.text} | {other})")


class RecordingFunctions:
    """``pyspark.sql.functions``: every function returns an expression that names it and its arguments."""

    def __getattr__(self, name: str) -> Any:
        return lambda *arguments: Expression(f"{name}({', '.join(str(argument) for argument in arguments)})")

    def col(self, name: str) -> Expression:
        return Expression(f"col({name})")


class RecordingWindow:
    """``pyspark.sql.Window``."""

    def __init__(self, text: str) -> None:
        self.text = text

    def __str__(self) -> str:
        return self.text

    @classmethod
    def partitionBy(cls, *columns: str) -> RecordingWindow:
        return cls(f"partitionBy({', '.join(columns)})")

    def orderBy(self, *columns: object) -> RecordingWindow:
        return RecordingWindow(f"{self.text}.orderBy({', '.join(str(column) for column in columns)})")


class Row(dict[str, Any]):
    """A collected row. A column that the test did not script reads as 0, so a count of nulls is 0 unless a test says otherwise."""

    def __missing__(self, key: str) -> int:
        return 0


class QuietCase(unittest.TestCase):
    """The adapter reports the seconds of each operation on stderr: the tests keep that out of their own output."""

    def setUp(self) -> None:
        self.enterContext(contextlib.redirect_stderr(io.StringIO()))


class RecordingFrame:
    """A frame that records the chain of calls that built it. ``collect`` and ``agg`` answer from the session's script."""

    def __init__(self, session: RecordingSession, text: str, columns: list[tuple[str, str]] | None = None) -> None:
        self.session = session
        self.text = text
        self.dtypes = columns or []

    def step(self, call: str) -> RecordingFrame:
        return RecordingFrame(self.session, f"{self.text}.{call}", self.dtypes)

    def select(self, *columns: str) -> RecordingFrame:
        return self.step(f"select({', '.join(columns)})")

    def withColumn(self, name: str, expression: object) -> RecordingFrame:
        return self.step(f"withColumn({name}, {expression})")

    def where(self, expression: object) -> RecordingFrame:
        return self.step(f"where({expression})")

    def drop(self, name: str) -> RecordingFrame:
        return self.step(f"drop({name})")

    def limit(self, rows: int) -> RecordingFrame:
        return self.step(f"limit({rows})")

    def groupBy(self, *columns: str) -> RecordingFrame:
        return self.step(f"groupBy({', '.join(columns)})")

    def agg(self, *expressions: object) -> RecordingFrame:
        return self.step(f"agg({', '.join(str(expression) for expression in expressions)})")

    def exceptAll(self, other: RecordingFrame) -> RecordingFrame:
        return self.step(f"exceptAll({other.text})")

    def writeTo(self, name: str) -> RecordingFrame:
        return self.step(f"writeTo({name})")

    def tableProperty(self, key: str, value: str) -> RecordingFrame:
        return self.step(f"tableProperty({key}={value})")

    def createOrReplace(self) -> None:
        self.session.executed.append(f"{self.text}.createOrReplace()")

    def collect(self) -> list[Any]:
        self.session.executed.append(f"{self.text}.collect()")
        if self.session.queue:
            return self.session.queue.pop(0)
        return self.session.answers.get(self.text, [Row()] if ".agg(" in self.text else [])

    def count(self) -> int:
        self.session.executed.append(f"{self.text}.count()")
        return int(self.session.counts.get(self.text, 0))


class MissingTable(Exception):
    pass


class RecordingSession:
    def __init__(self) -> None:
        self.tables_read: list[str] = []
        self.sql_run: list[str] = []
        self.executed: list[str] = []
        self.answers: dict[str, list[Any]] = {}
        self.queue: list[list[Any]] = []
        self.counts: dict[str, int] = {}
        self.missing: set[str] = set()
        self.stopped = False
        self.columns = {
            name: [(column, "string") for column in CLINICAL_COLUMNS] for name in ("source.raw_records.clinical_data", "lake.bronze.clinical_cases")
        }
        for name in ("source.raw_records.lab_data", "lake.bronze.lab_results"):
            self.columns[name] = [(column, "string") for column in LAB_COLUMNS]

    def table(self, name: str) -> RecordingFrame:
        self.tables_read.append(name)
        if name in self.missing:
            raise MissingTable(name)
        return RecordingFrame(self, f"table({name})", self.columns.get(name))

    def sql(self, text: str) -> None:
        self.sql_run.append(text)

    def stop(self) -> None:
        self.stopped = True


class RecordingColumnChecks:
    """The column checks that production runs in Great Expectations: each call is recorded with the table's text and the names of the checks.

    The unexpected count of each check is answered from a script by check name (zero when it has none).
    """

    def __init__(self, unexpected: dict[str, list[int]] | None = None) -> None:
        self.unexpected = unexpected or {}
        self.calls: list[tuple[str, list[str]]] = []

    def __call__(self, frame: RecordingFrame, checks: Sequence[Check]) -> list[CheckResult]:
        self.calls.append((frame.text, [check.name for check in checks]))
        results = []
        for check in checks:
            scripted = self.unexpected.get(check.name, [])
            count = scripted.pop(0) if scripted else 0
            results.append(CheckResult(check, count == 0, count))
        return results


def adapter(session: RecordingSession, column_checks: RecordingColumnChecks | None = None) -> SparkAdapter:
    return SparkAdapter(session, RecordingFunctions(), RecordingWindow, MissingTable, column_checks or RecordingColumnChecks())


RANK = "row_number() over (partitionBy(caseid, dt, name).orderBy(col(result) asc_nulls_first))"
SOURCE_LAB = "table(source.raw_records.lab_data).select(caseid, dt, name, result)"
BRONZE_LAB = "table(lake.bronze.lab_results)"
SIZED = f"{BRONZE_LAB}.withColumn(group_rows, count(lit(1)) over (partitionBy(caseid, dt, name)))"


class SessionSettingsTest(unittest.TestCase):
    ENVIRONMENT = {"nessie_uri": "http://nessie:19120/api/v2", "warehouse": "file:///warehouse/", "region": ""}

    def settings(self) -> dict[str, str]:
        return job_session_settings(**self.ENVIRONMENT, branch="run", default_branch="trunk", source_hash=HASH, heap="2925m", scratch="/scratch")

    def test_the_run_catalog_is_on_the_run_branch_and_the_source_catalog_is_on_the_default_branch_at_the_source_hash(self) -> None:
        settings = self.settings()
        self.assertEqual("run", settings["spark.sql.catalog.lake.ref"])
        self.assertEqual("trunk", settings["spark.sql.catalog.source.ref"])
        self.assertEqual(HASH, settings["spark.sql.catalog.source.ref.hash"])

    def test_a_branch_hash_pins_the_run_catalog_to_it_and_leaves_the_source_pin_alone(self) -> None:
        settings = job_session_settings(
            **self.ENVIRONMENT, branch="run", default_branch="trunk", source_hash=HASH, heap="2925m", scratch="/scratch", branch_hash=OTHER_HASH
        )
        self.assertEqual(OTHER_HASH, settings["spark.sql.catalog.lake.ref.hash"])
        self.assertEqual(HASH, settings["spark.sql.catalog.source.ref.hash"])

    def test_the_run_catalog_has_no_hash_pin_and_no_source_option_names_the_run_branch(self) -> None:
        settings = self.settings()
        self.assertNotIn("spark.sql.catalog.lake.ref.hash", settings)
        self.assertEqual([], [key for key, value in settings.items() if key.startswith("spark.sql.catalog.source") and value == "run"])

    def test_both_catalogs_open_the_same_nessie_and_the_same_warehouse(self) -> None:
        settings = self.settings()
        for suffix in ("uri", "warehouse", "io-impl", "client-api-version"):
            self.assertEqual(settings[f"spark.sql.catalog.lake.{suffix}"], settings[f"spark.sql.catalog.source.{suffix}"], suffix)

    def test_the_memory_and_scratch_settings_are_the_ones_given(self) -> None:
        settings = self.settings()
        self.assertEqual("2925m", settings["spark.driver.memory"])
        self.assertEqual("2925m", settings["spark.executor.memory"])
        self.assertEqual("/scratch", settings["spark.local.dir"])


class ReadsAndWritesTest(QuietCase):
    def test_the_loaded_tables_are_read_through_the_source_catalog_and_bronze_through_the_run_catalog(self) -> None:
        session = RecordingSession()
        subject = adapter(session)
        subject.source_columns("clinical")
        subject.source_columns("lab")
        subject.bronze_columns("clinical", CLINICAL_COLUMNS)
        subject.bronze_columns("lab", LAB_COLUMNS)
        self.assertEqual(
            ["source.raw_records.clinical_data", "source.raw_records.lab_data", "lake.bronze.clinical_cases", "lake.bronze.lab_results"], session.tables_read
        )

    def test_the_loaded_table_is_never_read_through_the_run_catalog(self) -> None:
        session = RecordingSession()
        subject = adapter(session)
        for kind, columns in (("clinical", CLINICAL_COLUMNS), ("lab", LAB_COLUMNS)):
            subject.source_columns(kind)
            subject.source_count(kind, columns)
            subject.difference(kind, columns)
            subject.write(kind, columns)
            subject.null_keys(kind, columns)
        self.assertEqual([], [name for name in session.tables_read if name.startswith("lake.raw_records")])
        self.assertTrue(all(name.startswith(("source.raw_records.", "lake.bronze.")) for name in session.tables_read))

    def test_a_loaded_table_that_the_pinned_catalog_does_not_have_is_refused_by_name(self) -> None:
        session = RecordingSession()
        session.missing = {"source.raw_records.lab_data"}
        with self.assertRaisesRegex(BronzeRulesError, "raw_records.lab_data") as raised:
            adapter(session).source_columns("lab")
        self.assertEqual("source_table_missing", raised.exception.code)

    def test_the_laboratory_table_is_written_from_the_loaded_table_with_the_rank_and_replaces_the_bronze_table(self) -> None:
        session = RecordingSession()
        adapter(session).write("lab", LAB_COLUMNS)
        self.assertEqual(
            [f"{SOURCE_LAB}.withColumn(source_occurrence, {RANK}).writeTo(lake.bronze.lab_results).tableProperty(format-version=2).createOrReplace()"],
            session.executed,
        )

    def test_the_clinical_table_is_written_as_it_is_with_no_added_column(self) -> None:
        session = RecordingSession()
        adapter(session).write("clinical", CLINICAL_COLUMNS)
        self.assertEqual(
            [
                "table(source.raw_records.clinical_data).select(caseid, subjectid, age)"
                ".writeTo(lake.bronze.clinical_cases).tableProperty(format-version=2).createOrReplace()"
            ],
            session.executed,
        )

    def test_the_namespace_is_created_on_the_run_catalog(self) -> None:
        session = RecordingSession()
        adapter(session).prepare()
        self.assertEqual(["CREATE NAMESPACE IF NOT EXISTS lake.bronze"], session.sql_run)

    def test_the_comparison_runs_both_ways_between_the_loaded_table_at_the_hash_and_bronze(self) -> None:
        session = RecordingSession()
        adapter(session).difference("lab", LAB_COLUMNS)
        expected = f"{SOURCE_LAB}.withColumn(source_occurrence, {RANK})"
        self.assertEqual([f"{expected}.exceptAll({BRONZE_LAB}).count()", f"{BRONZE_LAB}.exceptAll({expected}).count()"], session.executed)

    def test_the_session_is_stopped_on_close(self) -> None:
        session = RecordingSession()
        adapter(session).close()
        self.assertTrue(session.stopped)


class RankExpressionTest(QuietCase):
    def test_the_rank_is_a_row_number_over_the_group_ordered_by_the_result_with_a_missing_result_first(self) -> None:
        session = RecordingSession()
        adapter(session).source_count("lab", LAB_COLUMNS)
        self.assertEqual([f"{SOURCE_LAB}.withColumn(source_occurrence, {RANK}).count()"], session.executed)

    def test_the_rank_is_not_a_dense_rank_a_rank_or_a_descending_order(self) -> None:
        session = RecordingSession()
        adapter(session).source_count("lab", LAB_COLUMNS)
        text = session.executed[0]
        for wrong in ("dense_rank", "rank()", "desc", "asc_nulls_last"):
            self.assertNotIn(wrong, text)
        self.assertIn("row_number()", text)
        self.assertIn("partitionBy(caseid, dt, name)", text)
        self.assertIn("orderBy(col(result) asc_nulls_first)", text)

    def test_the_clinical_table_gets_no_rank(self) -> None:
        session = RecordingSession()
        adapter(session).source_count("clinical", CLINICAL_COLUMNS)
        self.assertNotIn("source_occurrence", session.executed[0])


class RepeatedRowsTest(QuietCase):
    COLLECT = f"{SIZED}.where((col(group_rows) > 1)).drop(group_rows).limit(101).collect()"

    def test_the_rows_of_the_groups_with_more_than_one_row_are_collected_with_one_row_more_than_the_cap(self) -> None:
        session = RecordingSession()
        session.answers[f"{SIZED}.where((col(group_rows) > 1)).drop(group_rows).limit(101)"] = [
            Row(caseid="1", dt="5", name="k", result="4.0", source_occurrence=1),
            Row(caseid="1", dt="5", name="k", result=None, source_occurrence=2),
        ]
        rows = adapter(session).repeated_rows(100)
        self.assertEqual([self.COLLECT], session.executed)
        self.assertEqual([("1", "5", "k", "4.0", 1), ("1", "5", "k", None, 2)], rows)

    def test_the_filter_keeps_only_groups_of_more_than_one_row_and_the_cap_is_the_one_given(self) -> None:
        for limit in (3, 100_000):
            session = RecordingSession()
            adapter(session).repeated_rows(limit)
            with self.subTest(limit):
                self.assertIn(".where((col(group_rows) > 1))", session.executed[0])
                self.assertIn(f".limit({limit + 1}).collect()", session.executed[0])
                self.assertIn("partitionBy(caseid, dt, name)", session.executed[0])

    def test_the_count_of_those_rows_uses_the_same_filter_and_no_cap(self) -> None:
        session = RecordingSession()
        session.counts[f"{SIZED}.where((col(group_rows) > 1))"] = 714
        self.assertEqual(714, adapter(session).repeated_row_count())
        self.assertEqual([f"{SIZED}.where((col(group_rows) > 1)).count()"], session.executed)


class KeyChecksTest(QuietCase):
    def test_the_null_keys_of_the_loaded_table_and_of_bronze_are_judged_by_the_column_checks_on_their_own_key_columns(self) -> None:
        session = RecordingSession()
        runner = RecordingColumnChecks({"caseid not null": [1, 0], "dt not null": [0, 0], "name not null": [0, 0], "source_occurrence not null": [2]})
        self.assertEqual((1, 2), adapter(session, runner).null_keys("lab", LAB_COLUMNS))
        self.assertEqual(
            [
                ("table(source.raw_records.lab_data)", ["caseid not null", "dt not null", "name not null"]),
                (BRONZE_LAB, ["caseid not null", "dt not null", "name not null", "source_occurrence not null"]),
            ],
            runner.calls,
        )
        self.assertEqual([], session.executed)

    def test_the_clinical_key_is_the_case_only(self) -> None:
        runner = RecordingColumnChecks({"caseid not null": [0, 3]})
        self.assertEqual((0, 3), adapter(RecordingSession(), runner).null_keys("clinical", CLINICAL_COLUMNS))
        self.assertEqual(["caseid not null"], [name for _, names in runner.calls for name in names][:1])
        self.assertTrue(all(names == ["caseid not null"] for _, names in runner.calls))

    def test_the_duplicate_keys_are_the_unexpected_rows_of_the_unique_check_over_the_whole_key_of_bronze(self) -> None:
        session = RecordingSession()
        runner = RecordingColumnChecks({"key unique": [5]})
        self.assertEqual(5, adapter(session, runner).key_duplicates("lab"))
        self.assertEqual([(BRONZE_LAB, ["key unique"])], runner.calls)

    def test_the_unique_check_is_over_the_key_columns_that_bronze_holds(self) -> None:
        seen: list[dict[str, object]] = []

        def runner(frame: RecordingFrame, checks: Sequence[Check]) -> list[CheckResult]:
            seen.extend(check.kwargs for check in checks)
            return [CheckResult(check, True, 0) for check in checks]

        adapter(RecordingSession(), runner).key_duplicates("lab")  # type: ignore[arg-type]
        self.assertEqual([{"column_list": ["caseid", "dt", "name", "source_occurrence"]}], seen)

    def test_the_groups_with_a_gap_are_counted_from_the_minimum_and_the_maximum_of_each_group(self) -> None:
        session = RecordingSession()
        adapter(session).occurrence_gaps()
        self.assertEqual(
            [
                f"{BRONZE_LAB}.groupBy(caseid, dt, name).agg(count(lit(1)) as n, min(source_occurrence) as low, max(source_occurrence) as high)"
                ".where(((col(low) != 1) | (col(high) != col(n)))).count()"
            ],
            session.executed,
        )


if __name__ == "__main__":
    unittest.main()
