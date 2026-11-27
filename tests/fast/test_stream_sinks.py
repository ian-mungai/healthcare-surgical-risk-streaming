"""The order and the failure behaviour of the writes of one micro-batch.

Failure modes covered here:

- A feature-ready event is published before a lake write of its batch succeeded, or after a write failed, so a reader is told of a row that is not there.
- A write failure is swallowed, so the batch is taken as done and its offsets are committed.
- A batch that runs again after a stop adds a second row to a lake table or a second effect to a store keyed on the ID.
- The sinks are written in an order that leaves the feature table last, so a stop after the first sink commit leaves no feature row without its decisions.
"""

from __future__ import annotations

import unittest
from collections.abc import Sequence

from spark.streaming.model import DecisionRecord, ErrorRecord, FeatureRecord, LedgerRecord, Record
from spark.streaming.rows import TABLES, TableSpec
from spark.streaming.sinks import commit_batch

FEATURE = FeatureRecord("f1", "r", 0, "1.0", 1, "v", 60, 70, False, "{}", True, ())
DECISION = DecisionRecord("d1", "e", "r", 0, "1.0", "numeric", "t", 1, 2, 3, 1, "late_accepted", 0, 9)
LEDGER = LedgerRecord("l1", "r", 0, "1.0", 1, "{}")
ERROR = ErrorRecord("x1", "r", 0, "1.0", 60, "m")


class Store:
    """A stand-in for the lake and the topic: a table keeps one row for each key, as a merge of new keys does, and the topic keeps every message."""

    def __init__(self, fail_on: str | None = None) -> None:
        self.calls: list[str] = []
        self.tables: dict[str, dict[str, Record]] = {}
        self.topic: list[str] = []
        self.fail_on = fail_on

    def merge(self, spec: TableSpec, records: Sequence[Record]) -> None:
        self.calls.append(spec.name)
        if spec.name == self.fail_on:
            raise RuntimeError(f"the write to {spec.name} failed")
        table = self.tables.setdefault(spec.name, {})
        for record in records:
            table.setdefault(str(getattr(record, spec.key)), record)

    def publish_ready(self, records: Sequence[FeatureRecord]) -> None:
        self.calls.append("publish")
        self.topic.extend(record.feature_id for record in records)


class Commit(unittest.TestCase):
    def test_the_lake_tables_are_written_in_order_and_the_events_are_published_last(self) -> None:
        store = Store()
        counts = commit_batch([FEATURE, ERROR, LEDGER, DECISION], store)
        self.assertEqual(
            ["lake.stream.event_decisions", "lake.stream.occurrence_ledger", "lake.stream.feature_errors", "lake.feature.stream_features", "publish"],
            store.calls,
        )
        self.assertEqual({spec.name: 1 for spec in TABLES.values()}, counts)
        self.assertEqual(["f1"], store.topic)

    def test_a_failed_write_stops_the_batch_and_publishes_nothing(self) -> None:
        for failing in (spec.name for spec in TABLES.values()):
            with self.subTest(failing=failing):
                store = Store(fail_on=failing)
                with self.assertRaises(RuntimeError):
                    commit_batch([FEATURE, ERROR, LEDGER, DECISION], store)
                self.assertEqual([], store.topic)
                self.assertNotIn("publish", store.calls)
                self.assertEqual(failing, store.calls[-1])

    def test_a_batch_with_no_feature_row_publishes_nothing_and_an_empty_batch_writes_nothing(self) -> None:
        store = Store()
        commit_batch([DECISION], store)
        self.assertEqual(["lake.stream.event_decisions"], store.calls)
        empty = Store()
        self.assertEqual({spec.name: 0 for spec in TABLES.values()}, commit_batch([], empty))
        self.assertEqual([], empty.calls)

    def test_a_batch_run_again_adds_no_row_and_its_events_are_the_same_ids(self) -> None:
        store = Store()
        batch: list[Record] = [FEATURE, DECISION, LEDGER]
        commit_batch(batch, store)
        commit_batch(batch, store)
        self.assertEqual({spec.name: 1 for spec in TABLES.values() if spec.name in store.tables}, {name: len(rows) for name, rows in store.tables.items()})
        self.assertEqual(["f1", "f1"], store.topic)

    def test_the_hook_runs_after_each_write_that_happened_and_a_stop_there_leaves_the_later_writes_undone(self) -> None:
        store = Store()
        seen: list[int] = []

        class Stop(RuntimeError):
            pass

        def after(index: int) -> None:
            seen.append(index)
            raise Stop

        with self.assertRaises(Stop):
            commit_batch([FEATURE, DECISION], store, after)
        self.assertEqual([0], seen)
        self.assertEqual(["lake.stream.event_decisions"], store.calls)
        self.assertEqual([], store.topic)


if __name__ == "__main__":
    unittest.main()
