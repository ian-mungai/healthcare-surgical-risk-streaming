"""How the Spark steps of the Gold dimensions job decide, on a stand-in for the Spark session and Nessie, and how one call is wired.

A stand-in cannot show that Spark does what the calls ask: the acceptance run does. These tests hold the decisions of the job: which source tables it refuses,
which facts it asks for, that a held table with other columns is never read as the expected one, that the key rule is checked in Python against what Spark
wrote, and that a verify reads at one hash and ends when the branch moved.

Failure modes covered here:

- A source table that is missing or has other columns, read as if it were the expected one.
- A held dimension with other columns whose members are compared as if it had the expected ones, or a first build that asks the missing table for its members.
- A key written by Spark that is not the key of the rule, or a member with no natural key, that the Python check lets through.
- A verify whose Spark session is not pinned to the hash that the job resolved first, or that reports success although the branch moved while it ran.
"""

from __future__ import annotations

import unittest
from argparse import Namespace
from collections import Counter
from dataclasses import dataclass
from typing import Any
from unittest import mock

from spark.gold_dimensions import DIMENSIONS, SOURCE_SCHEMAS, Dimension, GoldDimensionsError, dimension, surrogate_key
from spark.gold_dimensions_sql import (
    ambiguous_query,
    coverage_queries,
    desired_query,
    held_query,
    natural_duplicates_query,
    natural_keys_query,
    output_facts_query,
    source_facts_query,
)
from spark.jobs import gold_dimensions
from spark.jobs.gold_dimensions import GoldLake
from spark.settings import CATALOG
from spark.split_table import BRANCH_MOVED

FIRST, SECOND = "a" * 64, "b" * 64
CASE = dimension("dim_case")


class Rows:
    def __init__(self, value: int) -> None:
        self.value = value

    def count(self) -> int:
        return self.value


class Difference:
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
    def __init__(self, rows: list[Any]) -> None:
        self.rows = rows

    def collect(self) -> list[Any]:
        return self.rows


class Catalog:
    def __init__(self, tables: dict[str, Frame]) -> None:
        self.tables = tables

    def tableExists(self, name: str) -> bool:
        return name in self.tables


class Session:
    """A session that answers only the queries it was given, so a test shows which queries a step asks."""

    def __init__(self, tables: dict[str, Frame], answers: dict[str, list[Any]], log: list[str]) -> None:
        self.tables = tables
        self.catalog = Catalog(tables)
        self.answers = answers
        self.log = log

    def table(self, name: str) -> Frame:
        return self.tables[name]

    def sql(self, text: str) -> Any:
        if text in self.answers:
            self.log.append("ask " + text[:40])
            return Result(self.answers[text])
        if text.startswith("CREATE NAMESPACE"):
            self.log.append(text)
            return Result([])
        raise AssertionError(f"a query that the test did not expect: {text[:80]}")


def types_of(item: Dimension) -> list[tuple[str, str]]:
    return [(column.name, column.type) for column in item.columns]


def lake(tables: dict[str, Frame] | None = None, answers: dict[str, list[Any]] | None = None) -> tuple[GoldLake, list[str]]:
    log: list[str] = []
    return GoldLake(Session(tables or {}, answers or {}, log), None, "b"), log  # type: ignore[arg-type]


class SourceTests(unittest.TestCase):
    def sources(self, log: list[str]) -> dict[str, Frame]:
        return {f"{CATALOG}.{table}": Frame(list(fields), [], log) for table, fields in SOURCE_SCHEMAS.items()}

    def test_the_sources_that_have_the_expected_columns_pass(self) -> None:
        log: list[str] = []
        job, _ = lake(self.sources(log))
        job.check_sources()

    def test_a_missing_source_and_a_source_with_other_columns_are_refused_with_a_code_and_a_count(self) -> None:
        log: list[str] = []
        tables = self.sources(log)
        del tables["lake.bronze.tracks"]
        tables["lake.silver.cases"] = Frame([("caseid", "bigint")], [], log)
        job, _ = lake(tables)
        with self.assertRaises(GoldDimensionsError) as caught:
            job.check_sources()
        self.assertEqual(["source_missing: 1 table", "source_schema: 1 table"], caught.exception.problems)


class FactTests(unittest.TestCase):
    def test_the_input_facts_of_each_dimension_come_from_its_two_queries(self) -> None:
        answers: dict[str, list[Any]] = {}
        for number, item in enumerate(DIMENSIONS):
            answers[source_facts_query(item)] = [{"source_members": 10 + number, "key_null_rows": number}]
            answers[ambiguous_query(item)] = [{"n": 2 * number}]
        job, _ = lake(answers=answers)
        facts = job.source_facts()
        self.assertEqual({"source_members": 10, "key_null_rows": 0, "ambiguous_members": 0}, facts["dim_case"])
        self.assertEqual({"source_members": 15, "key_null_rows": 5, "ambiguous_members": 10}, facts["dim_lab_test"])

    def test_a_count_that_spark_does_not_give_as_a_count_is_refused(self) -> None:
        answers: dict[str, list[Any]] = {source_facts_query(item): [{"source_members": 1, "key_null_rows": 0}] for item in DIMENSIONS}
        answers |= {ambiguous_query(item): [{"n": 0}] for item in DIMENSIONS}
        answers[source_facts_query(CASE)] = [{"source_members": None, "key_null_rows": 0}]
        job, _ = lake(answers=answers)
        with self.assertRaises(GoldDimensionsError):
            job.source_facts()

    def test_a_first_build_asks_no_held_member_of_a_table_that_is_not_there(self) -> None:
        job, log = lake()
        self.assertEqual({item.name: (0, 0) for item in DIMENSIONS}, job.held_changes())
        self.assertEqual([], log)

    def test_a_held_table_is_asked_for_its_removed_and_its_changed_members(self) -> None:
        log: list[str] = []
        removed, changed = held_query(CASE)
        tables = {f"lake.gold.{item.name}": Frame(types_of(item), [], log) for item in DIMENSIONS}
        answers = {held_query(item)[0]: [{"n": 0}] for item in DIMENSIONS} | {held_query(item)[1]: [{"n": 0}] for item in DIMENSIONS}
        answers[removed], answers[changed] = [{"n": 3}], [{"n": 4}]
        job, _ = lake(tables, answers)
        self.assertEqual((3, 4), job.held_changes()["dim_case"])
        self.assertEqual((0, 0), job.held_changes()["dim_track"])

    def test_a_held_table_with_other_columns_is_never_read_as_the_expected_one(self) -> None:
        log: list[str] = []
        tables = {"lake.gold.dim_case": Frame([("case_key", "bigint"), ("case_id", "string")], [], log)}
        job, _ = lake(tables)
        with self.assertRaises(GoldDimensionsError) as caught:
            job.held_changes()
        self.assertEqual(["inherited_schema: 1 table"], caught.exception.problems)

    def test_the_tables_that_the_branch_does_not_hold_are_counted(self) -> None:
        log: list[str] = []
        tables = {f"lake.gold.{item.name}": Frame(types_of(item), [], log) for item in DIMENSIONS[:4]}
        job, _ = lake(tables)
        self.assertEqual(2, job.tables_missing())

    def test_the_tables_with_other_columns_or_types_or_none_are_counted_as_wrong(self) -> None:
        log: list[str] = []
        tables = {f"lake.gold.{item.name}": Frame(types_of(item), [], log) for item in DIMENSIONS}
        tables["lake.gold.dim_patient"] = Frame([("patient_key", "bigint"), ("subject_id", "bigint")], [], log)
        del tables["lake.gold.dim_track"]
        job, _ = lake(tables)
        self.assertEqual(2, job.output_schema_tables_wrong())

    def answers_for(self, item: Dimension, stored: list[dict[str, Any]], **counts: int) -> dict[str, list[Any]]:
        row = {"members": len(stored), "unknown_rows": 1, "unknown_not_empty": 0, "key_duplicates": 0, "key_out_of_range": 0, "natural_null": 0}
        answers: dict[str, list[Any]] = {output_facts_query(item): [row], natural_keys_query(item): stored}
        missing, extra = coverage_queries(item)
        answers[natural_duplicates_query(item)] = [{"n": counts.get("natural_duplicates", 0)}]
        answers[missing], answers[extra] = [{"n": counts.get("source_without_member", 0)}], [{"n": counts.get("member_without_source", 0)}]
        answers[held_query(item)[1]] = [{"n": counts.get("member_differs", 0)}]
        return answers

    def facts_of(self, stored_case: list[dict[str, Any]], **counts: int) -> dict[str, dict[str, int]]:
        answers: dict[str, list[Any]] = {}
        for item in DIMENSIONS:
            keyed = [
                {item.key: surrogate_key(item.name, value), item.natural.name: value} for value in ((1, 2) if item.natural.type == "bigint" else ("a", "b"))
            ]
            answers |= self.answers_for(item, stored_case if item is CASE else keyed, **(counts if item is CASE else {}))
        job, _ = lake(answers=answers)
        return job.table_facts()

    def stored(self, *values: int, wrong: int | None = None) -> list[dict[str, Any]]:
        rows = [{"case_key": surrogate_key("dim_case", value), "case_id": value} for value in values]
        if wrong is not None:
            rows[wrong]["case_key"] += 1
        return rows

    def test_the_output_facts_of_a_correct_dimension_are_all_zero_except_the_member_count(self) -> None:
        facts = self.facts_of(self.stored(1, 2, 3))
        self.assertEqual(
            {
                "members": 3,
                "unknown_rows": 1,
                "unknown_not_empty": 0,
                "key_duplicates": 0,
                "key_out_of_range": 0,
                "natural_null": 0,
                "natural_duplicates": 0,
                "key_rule_wrong": 0,
                "source_without_member": 0,
                "member_without_source": 0,
                "member_differs": 0,
            },
            facts["dim_case"],
        )

    def test_a_key_that_is_not_the_key_of_the_rule_is_counted_by_the_python_check(self) -> None:
        self.assertEqual(1, self.facts_of(self.stored(1, 2, 3, wrong=1))["dim_case"]["key_rule_wrong"])

    def test_a_member_with_no_natural_key_is_counted_as_a_wrong_key_and_does_not_stop_the_check(self) -> None:
        stored = [*self.stored(1), {"case_key": 5, "case_id": None}]
        self.assertEqual(1, self.facts_of(stored)["dim_case"]["key_rule_wrong"])

    def test_the_counts_of_the_queries_are_carried_into_the_facts(self) -> None:
        facts = self.facts_of(self.stored(1), natural_duplicates=2, source_without_member=3, member_without_source=4, member_differs=5)["dim_case"]
        self.assertEqual(
            (2, 3, 4, 5), tuple(facts[name] for name in ("natural_duplicates", "source_without_member", "member_without_source", "member_differs"))
        )

    def test_the_snapshot_of_each_table_is_asked_by_its_dotted_name(self) -> None:
        job, _ = lake()
        asked: list[str] = []

        def snapshot_id(table: str) -> int:
            asked.append(table)
            return len(asked)

        job.snapshot_id = snapshot_id  # type: ignore[method-assign]
        self.assertEqual({item.table: number for number, item in enumerate(DIMENSIONS, start=1)}, job.snapshot_ids())
        self.assertEqual([f"gold.{item.name}" for item in DIMENSIONS], asked)


class WriteTests(unittest.TestCase):
    def test_a_dimension_that_is_not_there_is_made_from_the_desired_rows_in_the_gold_namespace(self) -> None:
        log: list[str] = []
        desired = Frame(types_of(CASE), [(1, 1, 0, "a", "b", "c")], log)
        job, _ = lake(answers={desired_query(CASE): []})
        job.session.sql = lambda text: desired if text == desired_query(CASE) else log.append(text)
        self.assertTrue(job.write(CASE))
        self.assertEqual(["CREATE NAMESPACE IF NOT EXISTS lake.gold", "property format-version=2", "createOrReplace lake.gold.dim_case"], log)

    def test_a_dimension_that_already_holds_the_rows_is_not_written_again(self) -> None:
        log: list[str] = []
        rows = [(1, 1, 0, "a", "b", "c")]
        tables = {"lake.gold.dim_case": Frame(types_of(CASE), rows, log)}
        job, _ = lake(tables)
        job.session.sql = lambda text: Frame(types_of(CASE), rows, log) if text == desired_query(CASE) else log.append(text)
        job.properties = lambda source: {}  # type: ignore[method-assign]
        self.assertFalse(job.write(CASE))
        self.assertFalse([line for line in log if line.startswith(("createOrReplace", "overwritePartitions"))])

    def test_a_dimension_with_other_rows_is_overwritten_in_place(self) -> None:
        log: list[str] = []
        tables = {"lake.gold.dim_case": Frame(types_of(CASE), [(1, 1, 0, "a", "b", "c")], log)}
        job, _ = lake(tables)
        job.session.sql = lambda text: (
            Frame(types_of(CASE), [(1, 1, 0, "a", "b", "c"), (2, 2, 1, "a", "b", "c")], log) if text == desired_query(CASE) else log.append(text)
        )
        job.properties = lambda source: {}  # type: ignore[method-assign]
        self.assertTrue(job.write(CASE))
        self.assertIn("overwritePartitions lake.gold.dim_case", log)


@dataclass
class Reference:
    hash: str


class Client:
    """A Nessie stand-in whose branch answers each ask for its head with the next hash of ``heads``, the last one repeating."""

    def __init__(self, heads: list[str]) -> None:
        self.heads = list(heads)
        self.asked = 0

    def config(self) -> dict[str, Any]:
        return {}

    def reference(self, name: str) -> Reference:
        head = self.heads[min(self.asked, len(self.heads) - 1)]
        self.asked += 1
        return Reference(head)

    def create_branch(self, name: str, base: str) -> Reference:
        return Reference(FIRST)


class StoppableSession:
    def __init__(self) -> None:
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True


class Outcome:
    def facts(self) -> dict[str, Any]:
        return {"outcome": "verified"}


def call(mode: str, client: Client, sessions: list[StoppableSession]) -> tuple[dict[str, Any], list[tuple[Any, ...]]]:
    calls: list[tuple[Any, ...]] = []

    def build_session(name: str, ref: str, ref_hash: str | None = None) -> StoppableSession:
        calls.append((name, ref, ref_hash))
        sessions.append(StoppableSession())
        return sessions[-1]

    settings = mock.Mock(nessie_uri="http://nessie/api/v2", nessie_timeout_seconds=1.0, deadline_seconds=60.0)
    with (
        mock.patch.multiple(
            gold_dimensions,
            read_settings=mock.Mock(return_value=settings),
            open_client=mock.Mock(return_value=client),
            start_deadline=mock.Mock(),
            build_session=build_session,
            run_verify=mock.Mock(return_value=Outcome()),
            run_build=mock.Mock(return_value=Outcome()),
        ),
        mock.patch.object(GoldLake, "check_sources"),
    ):
        return gold_dimensions.execute(Namespace(mode=mode, branch="run_b", base="main")), calls


class PinnedVerifyTests(unittest.TestCase):
    def test_a_verify_resolves_the_hash_first_and_pins_its_session_and_its_result_to_it(self) -> None:
        sessions: list[StoppableSession] = []
        result, calls = call("verify", Client([FIRST]), sessions)
        self.assertEqual([("gold_dimensions", "run_b", FIRST)], calls)
        self.assertEqual(FIRST, result["branch_hash"])
        self.assertTrue(sessions[0].stopped)

    def test_a_verify_that_finds_the_branch_moved_fails_with_the_moved_code_and_still_stops_its_session(self) -> None:
        sessions: list[StoppableSession] = []
        with self.assertRaises(GoldDimensionsError) as raised:
            call("verify", Client([FIRST, SECOND]), sessions)
        self.assertEqual([BRANCH_MOVED], raised.exception.problems)
        self.assertTrue(sessions[0].stopped)

    def test_a_build_is_not_pinned_and_reports_the_hash_that_the_branch_has_at_the_end(self) -> None:
        result, calls = call("build", Client([SECOND]), [])
        self.assertEqual([("gold_dimensions", "run_b", None)], calls)
        self.assertEqual((FIRST, SECOND), (result["base_hash"], result["branch_hash"]))


if __name__ == "__main__":
    unittest.main()
