"""The results and the refusals of the Silver signals job and the hypotension job hold nothing that a held-out row can change. No Spark runs here.

The test set is sealed: a count or a digest over every case changes when one held-out row does, so it must not leave a job.

Failure modes covered here:

- A result holds a row count, a digest, a fingerprint or a count of a cause over every case, held-out cases included.
- A result holds a number that is not a snapshot ID, so a held-out row could change it. The digest of the hypotension job's event table is one: a test-set
  event row that is inserted changes it, so the job writes no digest of it.
- Changing one held-out row (its counts, its fingerprint) changes the result in more than the snapshot IDs, the hashes and the pass flags.
- A refusal or a problem message carries a count of rows, cases, seconds, readings, keys or patients, or the size of a defect changes its text.
- A result holds the field ``outcome``, which says whether a table was written, so it changes when only a held-out row changes.
- A refusal of a table's columns carries a count of columns.
- The keys that the end-to-end runners allow in a result (``tests/fast/silver_accept.py``) differ from the keys that the job writes, so a runner would fail a
  valid result or pass an extra key.
- A split-rule problem (``two_sets: 1 patient``) reaches a caller with its count.
- The result that a job builds (its envelope and its facts) is not the document that the runners' schema accepts: a type, a table name or the list of check
  names differs between the job and ``tests/fast/silver_accept.py``, so a valid result fails or an invalid one passes.
"""

from __future__ import annotations

import ast
import dataclasses
import json
import re
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from spark.jobs import silver_hypotension as hypotension_job
from spark.jobs import silver_signals as signals_job
from spark.jobs.patient_split import Lake
from spark.jobs.silver_hypotension import HypotensionLake
from spark.jobs.silver_signals import SOURCES, SignalsLake
from spark.profile.run import PROFILE, UNSCORED
from spark.profile.run import run_build as run_profile_build
from spark.silver import hypotension_run as hypo_run
from spark.silver import run as signals_run
from spark.silver.checks import BronzeFacts, NumericFacts, SilverSignalsError, WaveformFacts
from spark.silver.cli import JOB as SIGNALS_JOB_NAME
from spark.silver.hypotension_checks import EventFacts, InputFacts, SeriesFacts
from spark.silver.run import BRANCH_MOVED, Fingerprint
from spark.silver.tables import ART_MBP_1S, HYPOTENSION_TABLES, NAMESPACE, SILVER_TABLES
from spark.split_table import COLUMNS, PatientSplitError, schema_problems
from tests.fast import test_silver_hypotension_run as hypo
from tests.fast import test_silver_signals_run as signals
from tests.fast.silver_accept import CHECKS, HYPOTENSION_JOB, RESULT_KEYS, SIGNALS_JOB, TABLE_KEYS, sealed
from tests.fast.test_signal_profile_run import HEAD as PROFILE_HEAD
from tests.fast.test_signal_profile_run import Fake as ProfileFake

MESSAGE = re.compile(r"^[a-z_]+: failed$|^" + re.escape(BRANCH_MOVED) + "$")
VOLATILE = ("snapshots", "source_snapshots", "checked_hash", "write_hash", "branch_hash")
BANNED_KEYS = ("outcome", "rows", "bronze_rows", "duplicates_dropped", "causes", "fingerprints", "fingerprint", "digest", "count", "counts")
# A key that holds one of these words is banned too, whatever else its name holds (``development_events_digest`` was one).
BANNED_WORDS = ("digest", "fingerprint", "count", "cause", "rows", "outcome")
SNAPSHOT_KEYS = ("snapshots", "source_snapshots")


def numbers(value: Any, path: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
    """The path of every number in a result that is not a flag, and every key on the way."""
    if isinstance(value, bool):
        return []
    if isinstance(value, int | float):
        return [path]
    if isinstance(value, dict):
        return [found for key, item in value.items() for found in numbers(item, (*path, str(key)))]
    if isinstance(value, list | tuple):
        return [found for item in value for found in numbers(item, path)]
    return []


def keys_of(value: Any) -> set[str]:
    if isinstance(value, dict):
        return {str(key) for key in value} | {name for item in value.values() for name in keys_of(item)}
    if isinstance(value, list | tuple):
        return {name for item in value for name in keys_of(item)}
    return set()


def stable(facts: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in facts.items() if key not in VOLATILE}


def refusal(call: Any) -> list[str]:
    try:
        call()
    except SilverSignalsError as error:
        return error.problems
    raise AssertionError("the call did not refuse")


class SignalsResult(unittest.TestCase):
    def other_world(self, fake: signals.Fake) -> signals.Fake:
        """The same job on an input with more rows, another fingerprint and another snapshot: a held-out row has changed."""
        fake.input = signals_run.InputRead(signals.BRONZE, (21, 22, 23))
        fake.planned = {signals_run.NUMERIC: Fingerprint(997, 55), signals_run.WAVEFORM: Fingerprint(305, 66)}
        fake.read_back = {
            signals_run.NUMERIC: signals_run.TableRead(fake.planned[signals_run.NUMERIC], 201, numeric=signals.NUMERIC_FACTS),
            signals_run.WAVEFORM: signals_run.TableRead(fake.planned[signals_run.WAVEFORM], 202, waveform=signals.WAVEFORM_FACTS),
        }
        fake.hashes = {signals_run.NUMERIC: "7" * 64, signals_run.WAVEFORM: "8" * 64}
        return fake

    def test_the_result_holds_no_key_for_a_count_a_digest_or_a_cause(self) -> None:
        facts = signals_run.run_build(signals.Fake().operations(), signals.HEAD).facts()
        self.assertEqual(set(), keys_of(facts) & set(BANNED_KEYS))

    def test_the_only_numbers_in_the_result_are_snapshot_ids(self) -> None:
        for facts in (
            signals_run.run_build(signals.Fake().operations(), signals.HEAD).facts(),
            self.verified().facts(),
        ):
            self.assertEqual([], [path for path in numbers(facts) if path[0] not in SNAPSHOT_KEYS], facts)

    def verified(self) -> Any:
        fake = signals.Fake()
        fake.held = dict(fake.read_back)
        return signals_run.run_verify(fake.operations(), signals.HEAD)

    def test_a_held_out_row_that_changes_every_count_and_fingerprint_changes_only_snapshot_ids_hashes_and_nothing_else(self) -> None:
        first = signals_run.run_build(signals.Fake().operations(), signals.HEAD).facts()
        second = signals_run.run_build(self.other_world(signals.Fake()).operations(), signals.HEAD).facts()
        self.assertNotEqual(first["snapshots"], second["snapshots"])
        self.assertEqual(stable(first), stable(second))

    def test_the_same_holds_for_a_verify(self) -> None:
        fake = self.other_world(signals.Fake())
        fake.held = dict(fake.read_back)
        second = signals_run.run_verify(fake.operations(), signals.HEAD).facts()
        self.assertEqual(stable(self.verified().facts()), stable(second))

    def test_each_table_has_one_pass_flag(self) -> None:
        self.assertEqual(
            {signals_run.NUMERIC: True, signals_run.WAVEFORM: True}, signals_run.run_build(signals.Fake().operations(), signals.HEAD).facts()["passed"]
        )


class SignalsFlags(unittest.TestCase):
    def reads(self, **replacements: Any) -> dict[str, Any]:
        reads = dict(signals.Fake().read_back)
        reads.update(replacements)
        return reads

    def test_a_table_that_fails_a_check_is_false_and_the_other_stays_true(self) -> None:
        bad = signals_run.TableRead(signals.FINGERPRINTS[signals_run.NUMERIC], 101, numeric=dataclasses.replace(signals.NUMERIC_FACTS, verdict_differs=3))
        got = signals_run.passed_flags(self.reads(**{signals_run.NUMERIC: bad}), None)
        self.assertEqual({signals_run.NUMERIC: False, signals_run.WAVEFORM: True}, got)

    def test_a_table_that_is_missing_or_read_without_its_checks_is_false(self) -> None:
        bare = signals_run.TableRead(signals.FINGERPRINTS[signals_run.WAVEFORM], 102)
        got = signals_run.passed_flags(self.reads(**{signals_run.NUMERIC: None, signals_run.WAVEFORM: bare}), None)
        self.assertEqual({signals_run.NUMERIC: False, signals_run.WAVEFORM: False}, got)

    def test_a_build_needs_the_table_to_be_the_planned_one(self) -> None:
        planned = {**signals.FINGERPRINTS, signals_run.WAVEFORM: Fingerprint(300, 10)}
        self.assertEqual({signals_run.NUMERIC: True, signals_run.WAVEFORM: False}, signals_run.passed_flags(self.reads(), planned))

    def test_a_count_that_does_not_add_up_to_bronze_is_false(self) -> None:
        short = signals_run.TableRead(signals.FINGERPRINTS[signals_run.NUMERIC], 101, numeric=dataclasses.replace(signals.NUMERIC_FACTS, row_count_differs=1))
        self.assertFalse(signals_run.passed_flags(self.reads(**{signals_run.NUMERIC: short}), None)[signals_run.NUMERIC])


class SignalsMessages(unittest.TestCase):
    def build(self, fake: signals.Fake) -> list[str]:
        return refusal(lambda: signals_run.run_build(fake.operations(), signals.HEAD))

    def messages(self, size: int) -> list[str]:
        """Every refusal that the job can make, each with a defect of the given size."""
        found: list[str] = []
        fake = signals.Fake()
        fake.input = signals_run.InputRead(BronzeFacts(0, size, size, size, size, size, size, size), (11, 12, 13))
        found += self.build(fake)
        found += refusal(lambda: signals_run.run_build(self.empty_input().operations(), signals.HEAD))
        fake = signals.Fake()
        fake.read_back[signals_run.NUMERIC] = signals_run.TableRead(
            signals.FINGERPRINTS[signals_run.NUMERIC],
            101,
            numeric=NumericFacts(size, size, size, size, size, size, size, source_differs=size, row_count_differs=size),
        )
        fake.read_back[signals_run.WAVEFORM] = signals_run.TableRead(
            signals.FINGERPRINTS[signals_run.WAVEFORM],
            102,
            waveform=WaveformFacts(size, size, size, size, size, size, size, source_differs=size, row_count_differs=size),
        )
        found += self.build(fake)
        fake = signals.Fake()
        fake.later_input = signals_run.InputRead(dataclasses.replace(signals.BRONZE, numeric_null_keys=size), (11, 12, 13))
        found += self.build(fake)
        fake = signals.Fake()
        fake.read_back[signals_run.WAVEFORM] = signals_run.TableRead(Fingerprint(300 + size, 10), 102, waveform=signals.WAVEFORM_FACTS)
        found += self.build(fake)
        fake = signals.Fake()
        fake.read_back[signals_run.NUMERIC] = None
        found += self.build(fake)
        fake = signals.Fake()
        fake.moved_after = 0
        found += self.build(fake)
        return found

    def empty_input(self) -> signals.Fake:
        fake = signals.Fake()
        fake.input = signals_run.InputRead(dataclasses.replace(signals.BRONZE, input_empty=1), (11, 12, 13))
        return fake

    def test_every_message_is_a_code_and_a_flag(self) -> None:
        found = self.messages(1)
        self.assertGreaterEqual(len(found), 25)
        for message in found:
            with self.subTest(message=message):
                self.assertRegex(message, MESSAGE)

    def test_the_size_of_a_defect_never_changes_a_message(self) -> None:
        self.assertEqual(self.messages(1), self.messages(40))

    def test_no_digit_follows_a_code(self) -> None:
        for message in self.messages(7):
            _, _, rest = message.partition(": ")
            with self.subTest(message=message):
                self.assertEqual(("failed", None), (rest, re.search(r"[0-9]", rest)))

    def test_a_verify_refuses_in_the_same_form(self) -> None:
        fake = signals.Fake()
        fake.input = signals_run.InputRead(BronzeFacts(0, 5, 5, 5, 5, 5, 5, 5), (11, 12, 13))
        fake.held = dict(fake.read_back)
        for message in refusal(lambda: signals_run.run_verify(fake.operations(), signals.HEAD)):
            self.assertRegex(message, MESSAGE)


class HypotensionResult(unittest.TestCase):
    def other_world(self, fake: hypo.Fake) -> hypo.Fake:
        """A held-out row has changed: the series has more seconds and another fingerprint, the snapshots differ, and the events of the development cases are
        the same."""
        fake.input = hypo_run.InputRead(InputFacts(0, 0, 0, 0, 0, ()), (31, 32, 33))
        fake.planned = {hypo_run.SERIES: Fingerprint(5007, 55), hypo_run.EVENTS: hypo_run.Digest(9)}
        fake.read_back = {
            hypo_run.SERIES: hypo_run.TableRead(fake.planned[hypo_run.SERIES], 301, series=hypo.SERIES_FACTS),
            hypo_run.EVENTS: hypo_run.TableRead(fake.planned[hypo_run.EVENTS], 302, events=hypo.EVENT_FACTS),
        }
        fake.hashes = {hypo_run.SERIES: "7" * 64, hypo_run.EVENTS: "8" * 64}
        return fake

    def verified(self, fake: hypo.Fake) -> Any:
        fake.held = dict(fake.read_back)
        return hypo_run.run_verify(fake.operations(), hypo.HEAD)

    def test_the_result_holds_no_key_for_a_count_a_digest_of_every_case_or_a_cause(self) -> None:
        facts = hypo_run.run_build(hypo.Fake().operations(), hypo.HEAD).facts()
        self.assertEqual(set(), keys_of(facts) & set(BANNED_KEYS))
        self.assertEqual([], [key for key in keys_of(facts) if any(word in key for word in BANNED_WORDS)])

    def test_the_only_numbers_in_the_result_are_snapshot_ids(self) -> None:
        for facts in (hypo_run.run_build(hypo.Fake().operations(), hypo.HEAD).facts(), self.verified(hypo.Fake()).facts()):
            self.assertEqual([], [path for path in numbers(facts) if path[0] not in SNAPSHOT_KEYS], facts)

    def test_the_result_holds_exactly_the_hashes_the_snapshot_ids_the_pass_flags_and_the_checks_that_ran(self) -> None:
        wanted = {"checked_hash", "write_hash", "branch_hash", "source_snapshots", "snapshots", "passed", "checks_run"}
        for facts in (hypo_run.run_build(hypo.Fake().operations(), hypo.HEAD).facts(), self.verified(hypo.Fake()).facts()):
            self.assertEqual(wanted, set(facts))

    def test_a_held_out_row_that_changes_the_series_changes_only_snapshot_ids_and_hashes(self) -> None:
        first = hypo_run.run_build(hypo.Fake().operations(), hypo.HEAD).facts()
        second = hypo_run.run_build(self.other_world(hypo.Fake()).operations(), hypo.HEAD).facts()
        self.assertNotEqual(first["snapshots"], second["snapshots"])
        self.assertEqual(stable(first), stable(second))
        self.assertEqual(stable(self.verified(hypo.Fake()).facts()), stable(self.verified(self.other_world(hypo.Fake())).facts()))

    def inserted_test_set_event(self, fake: hypo.Fake) -> hypo.Fake:
        """A row of the test set has been inserted into the event table: the digest of that table, which sums a hash of every row, is another one in the plan
        and in the table that is read back, and the snapshot IDs differ."""
        fake.planned = {**fake.planned, hypo_run.EVENTS: hypo_run.Digest(77)}
        fake.read_back = {
            hypo_run.SERIES: fake.read_back[hypo_run.SERIES],
            hypo_run.EVENTS: hypo_run.TableRead(hypo_run.Digest(77), 402, events=hypo.EVENT_FACTS),
        }
        return fake

    def test_a_test_set_event_row_changes_only_snapshot_ids_and_hashes(self) -> None:
        first = hypo_run.run_build(hypo.Fake().operations(), hypo.HEAD).facts()
        second = hypo_run.run_build(self.inserted_test_set_event(hypo.Fake()).operations(), hypo.HEAD).facts()
        self.assertNotEqual(first["snapshots"], second["snapshots"])
        self.assertEqual(stable(first), stable(second))
        self.assertEqual(
            stable(self.verified(hypo.Fake()).facts()),
            stable(self.verified(self.inserted_test_set_event(hypo.Fake())).facts()),
        )

    def test_each_table_has_one_pass_flag(self) -> None:
        self.assertEqual({hypo_run.SERIES: True, hypo_run.EVENTS: True}, hypo_run.run_build(hypo.Fake().operations(), hypo.HEAD).facts()["passed"])


class HypotensionFlags(unittest.TestCase):
    def reads(self, **replacements: Any) -> dict[str, Any]:
        reads = dict(hypo.Fake().read_back)
        reads.update(replacements)
        return reads

    def test_a_table_that_fails_a_check_is_false_and_the_other_stays_true(self) -> None:
        bad_series = hypo_run.TableRead(hypo.FINGERPRINTS[hypo_run.SERIES], 101, series=dataclasses.replace(hypo.SERIES_FACTS, differs=2))
        bad_events = hypo_run.TableRead(hypo.FINGERPRINTS[hypo_run.EVENTS], 102, events=dataclasses.replace(hypo.EVENT_FACTS, held_out_events=1))
        self.assertEqual({hypo_run.SERIES: False, hypo_run.EVENTS: True}, hypo_run.passed_flags(self.reads(**{hypo_run.SERIES: bad_series}), None))
        self.assertEqual({hypo_run.SERIES: True, hypo_run.EVENTS: False}, hypo_run.passed_flags(self.reads(**{hypo_run.EVENTS: bad_events}), None))

    def test_a_table_that_is_missing_or_read_without_its_checks_is_false(self) -> None:
        bare = hypo_run.TableRead(hypo.FINGERPRINTS[hypo_run.EVENTS], 102)
        got = hypo_run.passed_flags(self.reads(**{hypo_run.SERIES: None, hypo_run.EVENTS: bare}), None)
        self.assertEqual({hypo_run.SERIES: False, hypo_run.EVENTS: False}, got)

    def test_a_build_needs_the_table_to_be_the_planned_one(self) -> None:
        planned = {**hypo.FINGERPRINTS, hypo_run.EVENTS: hypo_run.Digest(10)}
        self.assertEqual({hypo_run.SERIES: True, hypo_run.EVENTS: False}, hypo_run.passed_flags(self.reads(), planned))


class HypotensionMessages(unittest.TestCase):
    def build(self, fake: hypo.Fake) -> list[str]:
        return refusal(lambda: hypo_run.run_build(fake.operations(), hypo.HEAD))

    def messages(self, size: int) -> list[str]:
        found: list[str] = []
        for facts in (
            InputFacts(0, size, size, 0, 0, ()),
            InputFacts(1, 0, 0, 0, 0, ()),
            InputFacts(0, 0, 0, 1, 0, ()),
            InputFacts(0, 0, 0, 0, size, ()),
            InputFacts(
                0, 0, 0, 0, 0, (f"two_sets: {size} patients", f"pinned_counts: counts a {size} are not exactly a {size + 1}", f"empty_table: {size} rows")
            ),
        ):
            fake = hypo.Fake()
            fake.input = hypo_run.InputRead(facts, (21, 22, 23))
            found += self.build(fake)
        fake = hypo.Fake()
        fake.read_back[hypo_run.SERIES] = hypo_run.TableRead(
            hypo.FINGERPRINTS[hypo_run.SERIES], 101, series=SeriesFacts(size, size, size, size, size, size, size, row_count_differs=size)
        )
        found += self.build(fake)
        fake = hypo.Fake()
        fake.read_back[hypo_run.EVENTS] = hypo_run.TableRead(hypo.FINGERPRINTS[hypo_run.EVENTS], 102, events=EventFacts(size, size, size, size, size))
        found += self.build(fake)
        fake = hypo.Fake()
        fake.later_input = hypo_run.InputRead(InputFacts(0, size, 0, 0, 0, ()), (21, 22, 23))
        found += self.build(fake)
        fake = hypo.Fake()
        fake.read_back[hypo_run.EVENTS] = hypo_run.TableRead(hypo_run.Digest(10 + size), 102, events=hypo.EVENT_FACTS)
        found += self.build(fake)
        fake = hypo.Fake()
        fake.read_back[hypo_run.SERIES] = None
        fake.read_back[hypo_run.EVENTS] = None
        found += self.build(fake)
        return found

    def test_every_message_is_a_code_and_a_flag(self) -> None:
        found = self.messages(1)
        self.assertGreaterEqual(len(found), 22)
        for message in found:
            with self.subTest(message=message):
                self.assertRegex(message, MESSAGE)

    def test_the_size_of_a_defect_never_changes_a_message(self) -> None:
        self.assertEqual(self.messages(1), self.messages(40))

    def test_no_digit_follows_a_code(self) -> None:
        for message in self.messages(7):
            _, _, rest = message.partition(": ")
            with self.subTest(message=message):
                self.assertEqual(("failed", None), (rest, re.search(r"[0-9]", rest)))

    def test_a_split_problem_reaches_the_caller_as_its_code_only(self) -> None:
        fake = hypo.Fake()
        fake.input = hypo_run.InputRead(InputFacts(0, 0, 0, 0, 0, ("two_sets: 1 patient", "unknown_set: 2 rows")), (21, 22, 23))
        self.assertEqual(["two_sets: failed", "unknown_set: failed"], self.build(fake))


class WrongColumnsSession:
    """A session whose every table exists and holds one column that no table of the project has: any schema check of a job must refuse it."""

    def __init__(self) -> None:
        field = SimpleNamespace(name="stray", dataType=SimpleNamespace(simpleString=lambda: "string"))
        self.conf = SimpleNamespace(set=lambda key, value: None)
        self.catalog = SimpleNamespace(tableExists=lambda name: True)
        self.table = lambda name: SimpleNamespace(schema=SimpleNamespace(fields=[field]))


SETTINGS = {"nessie_uri": "http://nessie:19120/api/v2", "warehouse": "file:///w/", "region": ""}
AT = "a" * 64


class SchemaRefusals(unittest.TestCase):
    """A table whose columns are not those of its card is refused as ``schema: failed`` and never with a count of columns."""

    def refused(self, call: Any, error: type[Exception]) -> list[str]:
        try:
            call()
        except error as caught:
            return list(caught.problems)  # type: ignore[attr-defined]
        raise AssertionError("the call did not refuse")

    def test_the_check_names_no_count_and_no_table(self) -> None:
        self.assertEqual([], schema_problems(list(COLUMNS), COLUMNS))
        for fields in ([], list(COLUMNS[:3]), [("subjectid", "bigint"), *COLUMNS[1:]], [*COLUMNS, ("extra", "string")]):
            with self.subTest(columns=len(fields)):
                self.assertEqual(["schema: failed"], schema_problems(fields, COLUMNS))

    def test_the_silver_signals_job_refuses_each_bronze_input_and_each_table_it_reads(self) -> None:
        lake = SignalsLake(WrongColumnsSession(), None, "b", SETTINGS)  # type: ignore[arg-type]
        for spec in SOURCES:
            with self.subTest(table=spec.name):
                self.assertEqual(["schema: failed"], self.refused(lambda spec=spec: lake.source(AT, spec), SilverSignalsError))
        for table in SILVER_TABLES:
            with self.subTest(table=table):
                self.assertEqual(["schema: failed"], self.refused(lambda table=table: lake.read_signal_table(AT, table, False), SilverSignalsError))

    def test_the_hypotension_job_refuses_each_input_and_each_table_it_reads(self) -> None:
        lake = HypotensionLake(WrongColumnsSession(), None, "b", SETTINGS)  # type: ignore[arg-type]
        for name, call in (("waveform", lake.waveform), ("clinical", lake.clinical), ("split", lake.split)):
            with self.subTest(table=name):
                self.assertEqual(["schema: failed"], self.refused(lambda call=call: call(AT), SilverSignalsError))
        for table in (ART_MBP_1S.name, "hypotension_events"):
            with self.subTest(table=table):
                self.assertEqual(["schema: failed"], self.refused(lambda table=table: lake.read_signal_table(AT, table, False), SilverSignalsError))

    def test_the_patient_split_job_refuses_its_source_and_its_table(self) -> None:
        lake = Lake(WrongColumnsSession(), None, "b", SETTINGS)  # type: ignore[arg-type]
        self.assertEqual(["schema: failed"], self.refused(lambda: lake.read_source(AT), PatientSplitError))
        self.assertEqual(["schema: failed"], self.refused(lambda: lake.read_table(AT), PatientSplitError))


def literal_keys_of_execute(path: str) -> set[str]:
    """The string keys of the dictionary that the job's ``execute`` returns, read from its source, and ``*`` where it spreads the facts of the outcome."""
    tree = ast.parse((Path(__file__).resolve().parents[2] / path).read_text())
    function = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == "execute")
    result = next(node.value for node in ast.walk(function) if isinstance(node, ast.Return) and isinstance(node.value, ast.Dict))
    return {str(key.value) if isinstance(key, ast.Constant) else "*" for key in result.keys}


class AllowedKeys(unittest.TestCase):
    def test_the_signals_runners_keys_are_the_keys_of_the_job(self) -> None:
        facts = signals_run.run_build(signals.Fake().operations(), signals.HEAD).facts()
        envelope = literal_keys_of_execute("spark/jobs/silver_signals.py") - {"*"}
        self.assertEqual(envelope | set(facts), RESULT_KEYS[SIGNALS_JOB])
        for name in ("snapshots", "source_snapshots", "passed"):
            self.assertEqual(set(TABLE_KEYS[SIGNALS_JOB][name]), set(facts[name]), name)

    def test_the_hypotension_runners_keys_are_the_keys_of_the_job(self) -> None:
        facts = hypo_run.run_build(hypo.Fake().operations(), hypo.HEAD).facts()
        envelope = literal_keys_of_execute("spark/jobs/silver_hypotension.py") - {"*"}
        self.assertEqual(envelope | set(facts), RESULT_KEYS[HYPOTENSION_JOB])
        for name in ("snapshots", "source_snapshots", "passed"):
            self.assertEqual(set(TABLE_KEYS[HYPOTENSION_JOB][name]), set(facts[name]), name)

    def document(self, job: str, mode: str, facts: dict[str, Any], tables: Any) -> dict[str, Any]:
        """The result of ``execute`` as the job builds it: its envelope and the facts of the outcome."""
        return {
            "mode": mode,
            "branch": "b",
            "base_hash": "a" * 64 if mode == "build" else None,
            "job": job,
            "tables": [f"{NAMESPACE}.{table}" for table in tables],
            **facts,
        }

    def test_the_result_that_each_job_builds_is_sealed_for_the_runners(self) -> None:
        signals_facts = signals_run.run_build(signals.Fake().operations(), signals.HEAD).facts()
        hypo_facts = hypo_run.run_build(hypo.Fake().operations(), hypo.HEAD).facts()
        self.assertTrue(sealed(self.document(SIGNALS_JOB_NAME, "build", signals_facts, SILVER_TABLES), SIGNALS_JOB))
        self.assertTrue(sealed(self.document(hypotension_job.JOB, "build", hypo_facts, HYPOTENSION_TABLES), HYPOTENSION_JOB))
        self.assertTrue(sealed(self.document(SIGNALS_JOB_NAME, "verify", self.verified_signals().facts(), SILVER_TABLES), SIGNALS_JOB))

    def test_the_check_names_of_the_runners_are_those_of_the_job_modules(self) -> None:
        self.assertEqual(list(signals_job.CHECK_NAMES), signals_run.run_build(signals.Fake().operations(), signals.HEAD).facts()["checks_run"])
        self.assertEqual(list(hypotension_job.CHECK_NAMES), hypo_run.run_build(hypo.Fake().operations(), hypo.HEAD).facts()["checks_run"])
        self.assertEqual(signals_job.CHECK_NAMES, CHECKS[SIGNALS_JOB])
        self.assertEqual(hypotension_job.CHECK_NAMES, CHECKS[HYPOTENSION_JOB])
        self.assertNotEqual(signals_job.CHECK_NAMES, hypotension_job.CHECK_NAMES)

    def test_a_verify_result_has_the_same_keys_as_a_build_result(self) -> None:
        self.assertEqual(set(signals_run.run_build(signals.Fake().operations(), signals.HEAD).facts()), set(self.verified_signals().facts()))

    def verified_signals(self) -> Any:
        fake = signals.Fake()
        fake.held = dict(fake.read_back)
        return signals_run.run_verify(fake.operations(), signals.HEAD)


def result_field_names(source: str) -> set[str]:
    """The string keys of every dictionary literal in a source that name a digest, a checksum, a fingerprint or a ``development_`` value."""
    keys = {
        node.value
        for dictionary in ast.walk(ast.parse(source))
        if isinstance(dictionary, ast.Dict)
        for node in dictionary.keys
        if isinstance(node, ast.Constant)
    }
    return {
        key for key in keys if isinstance(key, str) and (key.startswith("development_") or any(word in key for word in ("digest", "checksum", "fingerprint")))
    }


# The only result fields of a Spark job that are named for development cases or that hold a digest, each with its evidence.
# ``development_*`` of the Silver cases job: the laboratory counts come from ``lab_scope(development=True)`` (the cases of patients in the training and
# validation sets) and ``development_cases`` is the length of ``development_lengths_query`` (the development sets only). ``checksum`` of the Bronze decoder is a
# sum over the Bronze signal tables of the cases that the manifest names (its result is not sealed; listed for the lead). ``checksum`` of table
# maintenance is a sum over a Bronze signal table or the catalog proof table: the result drops it for every other table. The names in the table-check module
# are the names of a check of the track list's pinned digest, whose detail is one flag (``equal`` 0 or 1).
# The signal profile job (``spark/jobs/signal_profile.py``) names ``development_cases`` and ``development_patients_*``: its counts of the cases in the
# training and validation sets (``frames.development_cases``). The ``fingerprints`` of its result (``spark/profile/run.py``) are the row count and digest of
# the two development tables, ``signal_profile`` and ``unscored_share``, each built from the signal tables of development cases only (``profile_frame``,
# ``unscored_frame``). The ``source_counts`` table holds a flag over the full cohort for some counts, so a held-out recording changes its fingerprint and the
# name of a count that does not match: the result holds neither (one pass flag, ``ProfileResultIsSealed``).
RESULT_FIELDS = {
    "spark/jobs/signal_profile.py": {"development_cases", "development_patients_training", "development_patients_validation"},
    "spark/profile/run.py": {"fingerprints"},
    "spark/silver_cases_run.py": {"development_cases", "development_lab_rows", "development_lab_copies"},
    "spark/jobs/bronze_decode.py": {"checksum"},
    "spark/jobs/table_maintenance.py": {"checksum"},
    "spark/table_checks.py": {"digest_compared", "rows digest"},
}


class ProfileResultIsSealed(unittest.TestCase):
    """The profile job's result: its fingerprints are the two development tables' and the source counts table is one pass flag."""

    def test_the_fingerprints_are_the_two_development_tables_and_the_source_counts_table_is_a_pass_flag(self) -> None:
        for pinned in (False, True):
            with self.subTest(pinned=pinned):
                facts = run_profile_build(ProfileFake().operations(), PROFILE_HEAD, pinned=pinned).facts()
                self.assertEqual({PROFILE, UNSCORED}, set(facts["fingerprints"]))
                self.assertEqual({"pinned": pinned, "passed": True}, facts["source_counts"])
                self.assertNotIn("mismatched", json.dumps(facts))


class ResultFieldNames(unittest.TestCase):
    def test_a_job_that_names_a_digest_in_its_result_is_found(self) -> None:
        self.assertEqual({"development_events_digest"}, result_field_names('def facts():\n    return {"checked_hash": 1, "development_events_digest": 9}\n'))
        self.assertEqual({"events_checksum", "series_fingerprint"}, result_field_names('x = {"events_checksum": 1, "series_fingerprint": 2, "passed": True}'))
        self.assertEqual(set(), result_field_names('x = {"passed": True, "snapshots": {}}'))

    def test_the_fields_that_name_a_digest_or_development_cases_are_the_listed_ones_only(self) -> None:
        root = Path(__file__).resolve().parents[2]
        found = {str(path.relative_to(root)): result_field_names(path.read_text()) for path in sorted((root / "spark").rglob("*.py"))}
        self.assertEqual(RESULT_FIELDS, {path: names for path, names in found.items() if names})


if __name__ == "__main__":
    unittest.main()
