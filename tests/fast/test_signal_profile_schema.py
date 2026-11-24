"""The schema of each output table of the signal profile, the drop that rebuilds the predecessor table and the Spark steps of both. No Spark runs here.

Failure modes covered here:

- A table's schema is read as the card's when it is not, or the predecessor 12-column ``unscored_share`` is read as another schema (so the job refuses a
  table that it can rebuild) or as the card's (so the job overwrites it in place with 15 columns).
- A table that is not there is read as a schema, or a schema is read for a table that is not the unscored share's predecessor.
- The drop of a rebuilt table is accepted when it is not the head, not one commit directly on the checked hash, empty, or when it puts or deletes another key.
- The job's Spark steps read the schema with a different test than the card's, write the predecessor table with a replace or an overwrite, or drop a table that
  the job did not classify as the predecessor.
"""

from __future__ import annotations

import dataclasses
import importlib
import unittest
from typing import Any
from unittest import mock

from spark.jobs import signal_profile
from spark.profile import frames as profile_frames
from spark.profile.commits import drop_commit
from spark.profile.grid_rule import planned_rule
from spark.profile.run import COUNTS, PROFILE, RULE, UNSCORED
from spark.profile.tables import ABSENT, CURRENT, GRID_RULE, OTHER, PREDECESSOR, PREDECESSOR_COLUMNS, PROFILE_TABLES, schema_state
from spark.silver import frames as silver_frames

AT = "a" * 64
CHECKED = "b" * 64
DROPPED = "c" * 64
KEY = ("silver", "unscored_share")
CARDS = {**PROFILE_TABLES, RULE: GRID_RULE}


class SchemaState(unittest.TestCase):
    def test_a_table_that_is_not_there_is_absent(self) -> None:
        for table in CARDS.values():
            self.assertEqual(ABSENT, schema_state(table, None))

    def test_the_columns_of_the_card_are_current(self) -> None:
        for table in CARDS.values():
            self.assertEqual(CURRENT, schema_state(table, list(table.columns)))

    def test_the_12_column_predecessor_of_the_unscored_share_is_the_predecessor(self) -> None:
        columns = PREDECESSOR_COLUMNS[UNSCORED]
        self.assertEqual(12, len(columns))
        self.assertEqual(PREDECESSOR, schema_state(PROFILE_TABLES[UNSCORED], list(columns)))

    def test_the_predecessor_is_the_columns_of_the_card_without_the_three_that_the_grid_added(self) -> None:
        current = dict(PROFILE_TABLES[UNSCORED].columns)
        predecessor = dict(PREDECESSOR_COLUMNS[UNSCORED])
        self.assertEqual({"no_grid", "cutoff_offset", "last_margin"}, set(current) - set(predecessor))
        self.assertEqual(set(), set(predecessor) - set(current))
        self.assertEqual([name for name in current if name in predecessor], list(predecessor))

    def test_any_other_columns_are_other(self) -> None:
        table = PROFILE_TABLES[UNSCORED]
        columns = list(table.columns)
        for name, fields in (
            ("a column dropped", columns[:-1]),
            ("a column added", [*columns, ("extra", "string")]),
            ("a column retyped", [*columns[:-1], (columns[-1][0], "string")]),
            ("the columns reordered", [columns[1], columns[0], *columns[2:]]),
            ("a column renamed", [("other", columns[0][1]), *columns[1:]]),
            ("no column", []),
            ("the predecessor with one column retyped", [*PREDECESSOR_COLUMNS[UNSCORED][:-1], (PREDECESSOR_COLUMNS[UNSCORED][-1][0], "string")]),
            ("the predecessor with a column added", [*PREDECESSOR_COLUMNS[UNSCORED], ("extra", "int")]),
        ):
            with self.subTest(name):
                self.assertEqual(OTHER, schema_state(table, fields))

    def test_only_the_unscored_share_has_a_predecessor(self) -> None:
        self.assertEqual({UNSCORED}, set(PREDECESSOR_COLUMNS))
        for name in (PROFILE, COUNTS):
            self.assertEqual(OTHER, schema_state(PROFILE_TABLES[name], list(PREDECESSOR_COLUMNS[UNSCORED])))
        self.assertEqual(OTHER, schema_state(GRID_RULE, list(PREDECESSOR_COLUMNS[UNSCORED])))

    def test_a_table_with_no_columns_is_other_for_every_table_and_never_the_predecessor(self) -> None:
        for table in CARDS.values():
            with self.subTest(table=table.name):
                self.assertEqual(OTHER, schema_state(table, []))

    def test_the_current_card_is_not_taken_for_the_predecessor_and_the_reverse(self) -> None:
        self.assertNotEqual(list(PROFILE_TABLES[UNSCORED].columns), list(PREDECESSOR_COLUMNS[UNSCORED]))


def entry(*, parent: str = CHECKED, operations: list[dict[str, Any]] | None = None, commit: Any = DROPPED) -> dict[str, Any]:
    ops = [{"type": "DELETE", "key": {"elements": list(KEY)}}] if operations is None else operations
    return {"commitMeta": {"hash": commit}, "parentCommitHash": parent, "operations": ops}


class DropCommit(unittest.TestCase):
    def test_one_commit_directly_on_the_checked_hash_that_deletes_the_key_is_the_drop(self) -> None:
        self.assertEqual(DROPPED, drop_commit([entry()], CHECKED, KEY))

    def test_only_the_head_counts_and_an_empty_log_is_no_drop(self) -> None:
        self.assertIsNone(drop_commit([], CHECKED, KEY))
        self.assertIsNone(drop_commit([entry(parent="d" * 64), entry()], CHECKED, KEY))

    def test_a_commit_on_another_parent_is_no_drop(self) -> None:
        self.assertIsNone(drop_commit([entry(parent="e" * 64)], CHECKED, KEY))

    def test_a_commit_with_no_operation_is_no_drop(self) -> None:
        self.assertIsNone(drop_commit([entry(operations=[])], CHECKED, KEY))

    def test_a_put_or_another_key_is_no_drop(self) -> None:
        put = {"type": "PUT", "key": {"elements": list(KEY)}}
        other = {"type": "DELETE", "key": {"elements": ["silver", "signal_profile"]}}
        for name, operations in (
            ("a put of the key", [put]),
            ("a delete of another key", [other]),
            ("the drop and another delete", [entry()["operations"][0], other]),
            ("the drop and a put", [entry()["operations"][0], put]),
        ):
            with self.subTest(name):
                self.assertIsNone(drop_commit([entry(operations=operations)], CHECKED, KEY))

    def test_a_commit_with_no_hash_text_is_no_drop(self) -> None:
        self.assertIsNone(drop_commit([entry(commit=None)], CHECKED, KEY))
        self.assertIsNone(drop_commit([entry(commit=5)], CHECKED, KEY))


class Field:
    def __init__(self, name: str, kind: str) -> None:
        self.name = name
        self.dataType = mock.Mock(simpleString=lambda: kind)


def lake(tables: dict[str, list[tuple[str, str]]]) -> tuple[signal_profile.ProfileLake, mock.MagicMock, mock.MagicMock]:
    """A ``ProfileLake`` over a stand-in session that holds the tables ``tables`` (qualified name without the catalog, and its columns)."""
    session = mock.MagicMock()
    session.catalog.tableExists.side_effect = lambda name: name.split(".", 1)[1] in tables
    session.table.side_effect = lambda name: mock.Mock(schema=mock.Mock(fields=[Field(n, k) for n, k in tables[name.split(".", 1)[1]]]))
    client = mock.MagicMock()
    settings = {"nessie_uri": "http://nessie:19120/api/v2", "warehouse": "file:///tmp/warehouse/", "region": "r"}
    return signal_profile.ProfileLake(session, client, "branch", settings), session, client


class JobSteps(unittest.TestCase):
    def test_the_schema_of_each_output_table_is_classified_with_the_cards_test(self) -> None:
        held = {
            f"silver.{PROFILE}": list(PROFILE_TABLES[PROFILE].columns),
            f"silver.{UNSCORED}": list(PREDECESSOR_COLUMNS[UNSCORED]),
            f"silver.{RULE}": [*GRID_RULE.columns, ("extra", "string")],
        }
        job, _, _ = lake(held)
        self.assertEqual({PROFILE: CURRENT, UNSCORED: PREDECESSOR, COUNTS: ABSENT, RULE: OTHER}, job.schemas(AT))

    def test_a_table_that_is_not_there_is_absent_for_every_output_table(self) -> None:
        job, _, _ = lake({})
        self.assertEqual({PROFILE: ABSENT, UNSCORED: ABSENT, COUNTS: ABSENT, RULE: ABSENT}, job.schemas(AT))

    def test_the_drop_is_a_drop_of_the_table_on_the_write_catalog_and_never_a_replace(self) -> None:
        job, session, _ = lake({})
        job.drop_table(UNSCORED)
        session.sql.assert_called_once_with(f"DROP TABLE lake.silver.{UNSCORED}")

    def test_the_create_after_the_drop_is_a_create_and_not_a_replace_or_an_overwrite(self) -> None:
        job, _, _ = lake({})
        frame = mock.MagicMock()
        job.planned = {UNSCORED: frame}
        job.write_profile_table(UNSCORED, False)
        writer = frame.writeTo.return_value
        frame.writeTo.assert_called_once_with(f"lake.silver.{UNSCORED}")
        writer.tableProperty.return_value.create.assert_called_once_with()
        self.assertFalse(writer.overwrite.called)
        self.assertFalse(writer.tableProperty.return_value.createOrReplace.called)

    def test_the_hash_of_the_drop_is_read_from_the_log_of_the_branch(self) -> None:
        job, _, client = lake({})
        client.request.return_value = {"logEntries": [entry()]}
        self.assertEqual(DROPPED, job.drop_hash_of(CHECKED, UNSCORED))
        client.request.return_value = {"logEntries": [entry(parent="e" * 64)]}
        self.assertIsNone(job.drop_hash_of(CHECKED, UNSCORED))

    def test_the_drop_of_a_table_is_only_checked_against_that_table(self) -> None:
        job, _, client = lake({})
        client.request.return_value = {"logEntries": [entry()]}
        self.assertIsNone(job.drop_hash_of(CHECKED, PROFILE))

    def test_the_plan_gives_the_saved_row_to_the_grid_of_the_cases_and_to_the_unscored_frame(self) -> None:
        job, _, _ = lake({})
        saved = dataclasses.replace(planned_rule(5), last_cutoff_margin_seconds=999)
        job.development_ids = [1, 2]
        job.patients = {"training": 1, "validation": 1}
        held = {name: (mock.MagicMock(), name) for name in signal_profile.INPUTS}
        with (
            mock.patch.object(job, "inputs", return_value=held),
            mock.patch.object(importlib, "import_module"),
            mock.patch.object(profile_frames, "development_cases") as cases,
            mock.patch.object(profile_frames, "development_grid") as grid,
            mock.patch.object(profile_frames, "profile_frame"),
            mock.patch.object(profile_frames, "unscored_frame") as unscored,
            mock.patch.object(profile_frames, "counts_frame"),
            mock.patch.object(signal_profile, "count_rows"),
            mock.patch.object(silver_frames, "fingerprint"),
        ):
            cases.return_value.count.return_value = 2
            job.plan(AT, saved)
        grid.assert_called_once_with(held["clinical_cases"][0], held["patient_split"][0], saved)
        unscored.assert_called_once_with(held["waveform_seconds"][0], grid.return_value, saved)

    def test_the_operations_are_wired_to_the_schema_and_drop_steps(self) -> None:
        job, _, _ = lake({})
        operations = job.operations()
        self.assertEqual(job.schemas, operations.schemas)
        self.assertEqual(job.drop_table, operations.drop)
        self.assertEqual(job.drop_hash_of, operations.drop_hash)


if __name__ == "__main__":
    unittest.main()
