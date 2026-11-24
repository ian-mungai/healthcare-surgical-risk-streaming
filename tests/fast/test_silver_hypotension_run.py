"""The order of a build and of a verify of the pressure series and the event table, with stand-in Spark steps.

Failure modes covered here:

- A table is written although the input has a defect, so a branch holds a table built from seconds that cannot be used.
- A rebuild that changes nothing writes a new snapshot, or a rebuild that changes one table rewrites the other.
- A table is written after the branch moved, or the hash of the write is not the one commit of the write directly on the checked hash.
- A table is read back at another hash than the write's, or the read-back is not the table that was planned, or its checks are not run.
- The input changes between the first read and the read-back and nothing says so.
- The result names a count of events or a cause of one: the events are outcomes, and the held-out patients' are never counted.
- The frame that the job planned is not released at the end, also after a failure.
"""

from __future__ import annotations

import dataclasses
import unittest
from typing import Any

from spark.silver.checks import SilverSignalsError
from spark.silver.hypotension_checks import EventFacts, InputFacts, SeriesFacts
from spark.silver.hypotension_run import EVENTS, SERIES, Digest, InputRead, Operations, TableRead, run_build, run_verify
from spark.silver.run import BRANCH_MOVED, Fingerprint

HEAD, PREPARED, FIRST_WRITE, SECOND_WRITE = "a" * 64, "b" * 64, "c" * 64, "d" * 64
INPUT = InputRead(InputFacts(0, 0, 0, 0, 0, ()), (21, 22, 23))
FINGERPRINTS: dict[str, Fingerprint | Digest] = {SERIES: Fingerprint(5000, 5), EVENTS: Digest(9)}
SERIES_FACTS = SeriesFacts(0, 0, 0, 0, 0, 0, 0)
EVENT_FACTS = EventFacts(0, 0, 0, 0, 0)


def messages(error: SilverSignalsError) -> list[str]:
    """The complete messages of a refusal: a code is not enough, because its text could carry a count."""
    return error.problems


def failed(*names: str) -> list[str]:
    return [f"{name}: failed" for name in names]


class Fake:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.current = HEAD
        self.input = INPUT
        self.later_input: InputRead | None = None
        self.planned: dict[str, Fingerprint | Digest] = dict(FINGERPRINTS)
        self.held: dict[str, TableRead | None] = {SERIES: None, EVENTS: None}
        self.read_back: dict[str, TableRead | None] = {
            SERIES: TableRead(FINGERPRINTS[SERIES], 101, series=SERIES_FACTS),
            EVENTS: TableRead(FINGERPRINTS[EVENTS], 102, events=EVENT_FACTS),
        }
        self.hashes: dict[str, str | None] = {SERIES: FIRST_WRITE, EVENTS: SECOND_WRITE}
        self.moved_after: int | None = None
        self.events_fail = False

    def head(self) -> str:
        self.calls.append("head")
        return "e" * 64 if self.moved_after is not None and self.calls.count("head") > self.moved_after else self.current

    def read_input(self, at: str) -> InputRead:
        self.calls.append(f"read_input {at[:1]}")
        return self.later_input if self.later_input is not None and at != HEAD else self.input

    def plan(self, at: str) -> dict[str, Fingerprint | Digest]:
        self.calls.append(f"plan {at[:1]}")
        return self.planned

    def read_table(self, at: str, table: str, with_facts: bool) -> TableRead | None:
        self.calls.append(f"read_table {at[:1]} {table} facts={with_facts}")
        if self.events_fail and table == EVENTS and with_facts:
            raise RuntimeError("the events were recomputed from a damaged series")
        return self.held[table] if at == HEAD else self.read_back[table]

    def write(self, table: str, exists: bool) -> None:
        self.calls.append(f"write {table} exists={exists}")

    def write_hash(self, checked: str, table: str) -> str | None:
        self.calls.append(f"write_hash {checked[:1]} {table}")
        written = self.hashes[table]
        if written is not None:
            self.current = written
        return written

    def prepare(self, checked: str) -> str:
        self.calls.append(f"prepare {checked[:1]}")
        return PREPARED

    def release(self) -> None:
        self.calls.append("release")

    def operations(self) -> Operations:
        return Operations(self.head, self.read_input, self.plan, self.read_table, self.write, self.write_hash, self.prepare, self.release)


class Build(unittest.TestCase):
    def test_a_clean_build_writes_each_table_on_the_hash_before_and_reads_back_at_the_last_write(self) -> None:
        fake = Fake()
        outcome = run_build(fake.operations(), HEAD)
        self.assertEqual(
            [
                "read_input a",
                "plan a",
                f"read_table a {SERIES} facts=False",
                f"read_table a {EVENTS} facts=False",
                "head",
                "prepare a",
                f"write {SERIES} exists=False",
                f"write_hash b {SERIES}",
                f"write {EVENTS} exists=False",
                f"write_hash c {EVENTS}",
                "read_input d",
                f"read_table d {SERIES} facts=True",
                f"read_table d {EVENTS} facts=True",
                "head",
                "release",
            ],
            fake.calls,
        )
        self.assertEqual((HEAD, SECOND_WRITE, SECOND_WRITE), (outcome.checked_hash, outcome.write_hash, outcome.facts()["branch_hash"]))

    def test_the_result_holds_snapshots_pass_flags_and_the_development_events_digest_and_no_count_or_cause(self) -> None:
        facts = run_build(Fake().operations(), HEAD).facts()
        self.assertEqual({SERIES: 101, EVENTS: 102}, facts["snapshots"])
        self.assertEqual({"waveform_seconds": 21, "clinical_cases": 22, "patient_split": 23}, facts["source_snapshots"])
        self.assertEqual({SERIES: True, EVENTS: True}, facts["passed"])
        self.assertEqual(9, facts["development_events_digest"])
        self.assertEqual(
            {"checked_hash", "write_hash", "branch_hash", "source_snapshots", "snapshots", "passed", "development_events_digest", "checks_run"},
            set(facts),
        )
        self.assertGreaterEqual(len(facts["checks_run"]), 10)

    def test_a_split_that_fails_its_rule_or_a_case_with_two_owners_writes_nothing_and_stops_before_any_plan(self) -> None:
        for facts, wanted in (
            (InputFacts(0, 0, 0, 1, 0, ()), failed("ambiguous_case_owner")),
            (InputFacts(0, 0, 0, 0, 0, ("two_sets: 1 patient",)), failed("two_sets")),
            (InputFacts(0, 0, 0, 0, 1, ()), failed("case_without_split")),
        ):
            with self.subTest(wanted=wanted):
                fake = Fake()
                fake.input = InputRead(facts, (21, 22, 23))
                with self.assertRaises(SilverSignalsError) as caught:
                    run_build(fake.operations(), HEAD)
                self.assertEqual(wanted, messages(caught.exception))
                self.assertEqual(["read_input a", "release"], fake.calls)

    def test_an_input_defect_writes_nothing_and_releases(self) -> None:
        fake = Fake()
        fake.input = InputRead(InputFacts(1, 0, 0, 0, 0, ()), (21, 22, 23))
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(failed("input_empty"), messages(caught.exception))
        self.assertEqual(["read_input a", "release"], fake.calls)

    def test_a_rebuild_that_changes_nothing_writes_nothing_and_still_reads_back_and_checks(self) -> None:
        fake = Fake()
        fake.held = dict(fake.read_back)
        outcome = run_build(fake.operations(), HEAD)
        self.assertNotIn("outcome", outcome.facts())
        self.assertEqual((HEAD, None, HEAD), (outcome.checked_hash, outcome.write_hash, outcome.facts()["branch_hash"]))
        self.assertFalse([call for call in fake.calls if call.startswith(("write", "prepare"))])

    def test_a_rebuild_that_changes_one_table_rewrites_that_table_only_and_in_place(self) -> None:
        fake = Fake()
        fake.held = {SERIES: fake.read_back[SERIES], EVENTS: TableRead(Digest(8), 99)}
        run_build(fake.operations(), HEAD)
        self.assertEqual([f"write {EVENTS} exists=True"], [call for call in fake.calls if call.startswith("write ")])

    def test_a_branch_that_moved_before_the_write_gets_no_write(self) -> None:
        fake = Fake()
        fake.moved_after = 0
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual([BRANCH_MOVED], messages(caught.exception))
        self.assertFalse([call for call in fake.calls if call.startswith(("write", "prepare"))])

    def test_a_write_that_is_not_the_one_commit_on_the_checked_hash_stops_before_the_next_write(self) -> None:
        fake = Fake()
        fake.hashes[SERIES] = None
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual([BRANCH_MOVED], messages(caught.exception))
        self.assertNotIn(f"write {EVENTS} exists=False", fake.calls)

    def test_a_read_back_that_is_not_the_planned_table_is_named(self) -> None:
        fake = Fake()
        fake.read_back[EVENTS] = TableRead(Digest(10), 102, events=EVENT_FACTS)
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(["readback_differs: 1 table"], messages(caught.exception))

    def test_a_table_that_is_not_there_at_the_read_back_is_named(self) -> None:
        fake = Fake()
        fake.read_back[SERIES] = None
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(["table_missing: 1 table"], messages(caught.exception))

    def test_a_series_that_is_read_back_without_its_checks_is_named_as_a_missing_table(self) -> None:
        fake = Fake()
        fake.read_back[SERIES] = TableRead(FINGERPRINTS[SERIES], 101)
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(["table_missing: 1 table"], messages(caught.exception))

    def test_an_event_table_that_is_read_back_without_its_checks_is_named_as_a_missing_table(self) -> None:
        fake = Fake()
        fake.read_back[EVENTS] = TableRead(FINGERPRINTS[EVENTS], 102)
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(["table_missing: 1 table"], messages(caught.exception))

    def test_the_checks_of_the_series_run_on_the_read_back_and_come_first(self) -> None:
        fake = Fake()
        fake.read_back[SERIES] = TableRead(FINGERPRINTS[SERIES], 101, series=dataclasses.replace(SERIES_FACTS, differs=3))
        fake.read_back[EVENTS] = TableRead(FINGERPRINTS[EVENTS], 102, events=dataclasses.replace(EVENT_FACTS, short=1))
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(failed("series_differs"), messages(caught.exception))

    def test_the_checks_of_the_events_run_on_the_read_back_when_the_series_passes(self) -> None:
        fake = Fake()
        fake.read_back[EVENTS] = TableRead(FINGERPRINTS[EVENTS], 102, events=dataclasses.replace(EVENT_FACTS, short=1))
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(failed("event_short"), messages(caught.exception))

    def test_an_input_that_changed_between_the_reads_is_named(self) -> None:
        for later in (InputRead(INPUT.facts, (21, 22, 24)), InputRead(dataclasses.replace(INPUT.facts, null_keys=1), (21, 22, 23))):
            with self.subTest(later=later):
                fake = Fake()
                fake.later_input = later
                with self.assertRaises(SilverSignalsError) as caught:
                    run_build(fake.operations(), HEAD)
                self.assertEqual(failed("source_changed"), messages(caught.exception))

    def test_a_branch_that_moved_during_the_read_back_is_named(self) -> None:
        fake = Fake()
        fake.moved_after = 1
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual([BRANCH_MOVED], messages(caught.exception))

    def test_a_failure_that_is_not_a_check_still_releases_the_plan(self) -> None:
        fake = Fake()

        def broken(_at: str) -> dict[str, Fingerprint | Digest]:
            raise RuntimeError("stop")

        with self.assertRaises(RuntimeError):
            run_build(dataclasses.replace(fake.operations(), plan=broken), HEAD)
        self.assertEqual("release", fake.calls[-1])


class SeriesFirst(unittest.TestCase):
    """The events are recomputed from the stored series, so a series that fails its own checks is reported before the event table is read."""

    def damaged(self) -> Fake:
        fake = Fake()
        facts = dataclasses.replace(SERIES_FACTS, value_problem=1, differs=1)
        fake.read_back[SERIES] = TableRead(FINGERPRINTS[SERIES], 101, series=facts)
        fake.events_fail = True
        return fake

    def test_a_verify_reports_a_damaged_series_with_its_codes_and_never_reads_the_event_facts(self) -> None:
        fake = self.damaged()
        fake.held = dict(fake.read_back)
        with self.assertRaises(SilverSignalsError) as caught:
            run_verify(fake.operations(), HEAD)
        self.assertEqual(failed("series_value", "series_differs"), messages(caught.exception))
        self.assertNotIn(f"read_table a {EVENTS} facts=True", fake.calls)

    def test_a_build_reports_a_damaged_series_at_the_read_back_with_its_codes_and_never_reads_the_event_facts(self) -> None:
        fake = self.damaged()
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(failed("series_value", "series_differs"), messages(caught.exception))
        self.assertNotIn(f"read_table d {EVENTS} facts=True", fake.calls)

    def test_a_missing_series_table_is_named_without_reading_the_events(self) -> None:
        fake = self.damaged()
        fake.read_back[SERIES] = None
        fake.held = dict(fake.read_back)
        with self.assertRaises(SilverSignalsError) as caught:
            run_verify(fake.operations(), HEAD)
        self.assertEqual(["table_missing: 1 table"], messages(caught.exception))


class MissingTables(unittest.TestCase):
    """The message names the tables that are missing, one by one, and the event table is looked for without reading its facts when the series is not there."""

    def missing(self, series: bool, events: bool) -> tuple[list[str], list[str]]:
        fake = Fake()
        if series:
            fake.read_back[SERIES] = None
        if events:
            fake.read_back[EVENTS] = None
        fake.events_fail = series
        fake.held = dict(fake.read_back)
        with self.assertRaises(SilverSignalsError) as caught:
            run_verify(fake.operations(), HEAD)
        return caught.exception.problems, fake.calls

    def test_only_the_series_missing_is_one_table_and_the_events_are_found_without_their_facts(self) -> None:
        problems, calls = self.missing(series=True, events=False)
        self.assertEqual(["table_missing: 1 table"], problems)
        self.assertIn(f"read_table a {EVENTS} facts=False", calls)
        self.assertNotIn(f"read_table a {EVENTS} facts=True", calls)

    def test_only_the_events_missing_is_one_table(self) -> None:
        self.assertEqual(["table_missing: 1 table"], self.missing(series=False, events=True)[0])

    def test_both_tables_missing_are_two_tables(self) -> None:
        self.assertEqual(["table_missing: 2 tables"], self.missing(series=True, events=True)[0])


class Verify(unittest.TestCase):
    def verify(self, fake: Fake) -> Any:
        fake.held = dict(fake.read_back)
        return run_verify(fake.operations(), HEAD)

    def test_a_good_branch_is_verified_and_nothing_is_written(self) -> None:
        fake = Fake()
        outcome = self.verify(fake)
        self.assertNotIn("outcome", outcome.facts())
        self.assertEqual((HEAD, None, HEAD), (outcome.checked_hash, outcome.write_hash, outcome.facts()["branch_hash"]))
        self.assertEqual(["read_input a", f"read_table a {SERIES} facts=True", f"read_table a {EVENTS} facts=True", "head", "release"], fake.calls)

    def test_a_split_that_fails_its_rule_stops_a_verify_before_any_table_is_read(self) -> None:
        for facts, wanted in (
            (InputFacts(0, 0, 0, 1, 0, ()), failed("ambiguous_case_owner")),
            (InputFacts(0, 0, 0, 0, 0, ("two_sets: 1 patient",)), failed("two_sets")),
        ):
            with self.subTest(wanted=wanted):
                fake = Fake()
                fake.input = InputRead(facts, (21, 22, 23))
                with self.assertRaises(SilverSignalsError) as caught:
                    self.verify(fake)
                self.assertEqual(wanted, messages(caught.exception))
                self.assertEqual(["read_input a", "release"], fake.calls)

    def test_a_branch_without_a_table_is_named(self) -> None:
        fake = Fake()
        fake.read_back[EVENTS] = None
        with self.assertRaises(SilverSignalsError) as caught:
            self.verify(fake)
        self.assertEqual(["table_missing: 1 table"], messages(caught.exception))

    def test_a_defect_in_a_table_is_named(self) -> None:
        fake = Fake()
        fake.read_back[SERIES] = TableRead(FINGERPRINTS[SERIES], 101, series=dataclasses.replace(SERIES_FACTS, valid_with_cause=1))
        with self.assertRaises(SilverSignalsError) as caught:
            self.verify(fake)
        self.assertEqual(failed("series_valid_with_cause"), messages(caught.exception))

    def test_a_defect_in_the_input_is_named_alone_and_no_table_is_judged(self) -> None:
        fake = Fake()
        fake.read_back[SERIES] = TableRead(FINGERPRINTS[SERIES], 101, series=dataclasses.replace(SERIES_FACTS, valid_with_cause=1))
        fake.input = InputRead(dataclasses.replace(INPUT.facts, cases_without_split=1), (21, 22, 23))
        with self.assertRaises(SilverSignalsError) as caught:
            self.verify(fake)
        self.assertEqual(failed("case_without_split"), messages(caught.exception))

    def test_a_branch_that_moved_is_named(self) -> None:
        fake = Fake()
        fake.moved_after = 0
        with self.assertRaises(SilverSignalsError) as caught:
            self.verify(fake)
        self.assertEqual([BRANCH_MOVED], messages(caught.exception))


if __name__ == "__main__":
    unittest.main()
