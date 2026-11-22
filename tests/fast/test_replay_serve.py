"""The replay's always-on service: it waits for a run record, runs it, returns to waiting and keeps its health file fresh.

The loop is tested here with a stand-in for one replay run, because a real run needs Kafka, the registry and the decoder, which only the replay image
holds. Isolation covers what a real run cannot produce on demand: a run that stops on a signal, a run that fails, a record that cannot be read and a loop
that stops making progress. The replay image, its real run and its Compose health check are proved by the acceptance test of the joined stack.

Failure modes, each with its case: an empty inbox (waits, health fresh, no run), a malformed record (moved to failed, serving continues), a failed run
(moved to failed with its exit code, serving continues), a stop while waiting (returns at once), a stop during a run (the record stays in the inbox
for the restart, which resumes it), two records (run one at a time in name order), a stale, future, infinite or not-a-number health stamp (unhealthy),
a record submitted twice (one record), a record that differs from the one already submitted under its run ID (refused, the first kept, also when two
submits race and after the run was archived, and when the ID already holds a record in the inbox, in done or in failed from before reservations
existed), a delayed submit that meets an archival (waits for it, then queues nothing), a record whose timing field is not a finite number from 0 to
86,400 or whose speed is not a finite number from 0.01 to 1,000 (refused at submit, and failed with its reason when planted in the queue), a restart
after a result was written but before the record was archived (the result stays, the record is archived, nothing runs again), a replay that raises an
arithmetic or value error (that request fails at once), a request that stops the service three starts in a row whatever the error (fails on the fourth
start and the next request runs), a stop by signal (not counted) and a submission that is not valid UTF-8 (refused with its reason).
"""

from __future__ import annotations

import io
import json
import os
import tempfile
import threading
import time
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

from replay.service import serve as serve_module
from replay.service.serve import Outcome, SubmitError, archive, healthy, held, queued_record, read_submission, serve, submit, submit_command
from scripts.ingestion.manifest import ManifestRef

REFERENCE = ManifestRef("bucket", "manifests/replay/" + "a" * 64 + ".json", "version_1", "b" * 64).to_json()
OTHER_REFERENCE = ManifestRef("bucket", "manifests/replay/" + "c" * 64 + ".json", "version_2", "d" * 64).to_json()
BAD_TIMES = (1e308, float("inf"), float("-inf"), float("nan"), -1, 86_401, 10**400)
BAD_SPEEDS = (0, -5, float("inf"), float("nan"), 10**400, 5e-324, 0.0099, 1000.5, 1001)
GOOD_SPEEDS = (0.01, 1000, 60)
TIMING_FIELDS = ("idle_checkpoint_seconds", "stagger_seconds", "turnover_seconds", "window_seconds")

RECORD = {
    "run_id": "replay_0a1b2c3d",
    "rooms": 1,
    "speed": 60.0,
    "stagger_seconds": 0,
    "turnover_seconds": 0,
    "window_seconds": 600,
    "manifest_reference": "/opt/replay/run/manifest_reference.json",
    "object_store": {"kind": "folder", "root": "/landed"},
    "kafka_bootstrap": "kafka:9092",
    "registry_url": "http://apicurio:8080/apis/registry/v3",
    "state_dir": "/opt/replay/state/replay_0a1b2c3d",
}


class Clock:
    """Wall seconds that move only when the loop sleeps or a run takes time."""

    def __init__(self) -> None:
        self.now = 1_000_000.0

    def __call__(self) -> float:
        return self.now


class Loop:
    """One serve loop over a temporary inbox, with a stand-in run that follows a script of outcomes and stops the loop after a set number of sleeps."""

    def __init__(self, outcomes: dict[str, Outcome], sleeps_before_stop: int = 3) -> None:
        folder = tempfile.TemporaryDirectory(prefix="replay_serve_")
        self.folder = folder
        self.root = Path(folder.name)
        self.inbox = self.root / "inbox"
        self.inbox.mkdir()
        self.health = self.root / "health.json"
        self.clock = Clock()
        self.outcomes = outcomes
        self.ran: list[str] = []
        self.sleeps = 0
        self.sleeps_before_stop = sleeps_before_stop
        self.stop = False
        self.health_seen: list[dict[str, Any]] = []
        self.logged: list[str] = []

    def run_request(self, path: Path, beat: Callable[[], None]) -> Outcome:
        self.ran.append(path.name)
        self.clock.now += 5
        beat()
        self.health_seen.append(json.loads(self.health.read_text()))
        outcome = self.outcomes[path.name]
        if outcome.kind == "stopped":
            self.stop = True
        return outcome

    def sleep(self, seconds: float) -> None:
        self.sleeps += 1
        self.clock.now += seconds
        if self.sleeps >= self.sleeps_before_stop:
            self.stop = True

    def serve(self) -> int:
        return serve(
            self.inbox,
            self.health,
            self.run_request,
            lambda: self.stop,
            self.clock,
            self.sleep,
            poll_seconds=2.0,
            log=lambda event, **fields: self.logged.append(json.dumps({"event": event, **fields})),
        )

    def put(self, name: str, text: str, reference: str | None = REFERENCE) -> None:
        (self.inbox / name).write_text(text)
        if reference is not None and name.endswith(".json"):
            (self.inbox / name).with_suffix(".reference").write_text(reference)


class ServeTest(unittest.TestCase):
    def loop(self, outcomes: dict[str, Outcome], sleeps_before_stop: int = 3) -> Loop:
        loop = Loop(outcomes, sleeps_before_stop)
        self.addCleanup(loop.folder.cleanup)
        return loop

    def test_an_empty_inbox_waits_with_a_fresh_health_file_and_runs_nothing(self) -> None:
        loop = self.loop({})
        self.assertEqual(0, loop.serve())
        self.assertEqual([], loop.ran)
        document = json.loads(loop.health.read_text())
        self.assertEqual("stopped", document["status"])
        self.assertEqual(3, loop.sleeps)

    def test_a_record_is_run_then_kept_in_done_with_its_result_and_the_loop_returns_to_waiting(self) -> None:
        loop = self.loop({"a.json": Outcome("finished", 0)})
        loop.put("a.json", json.dumps(RECORD))
        self.assertEqual(0, loop.serve())
        self.assertEqual(["a.json"], loop.ran)
        self.assertEqual("running", loop.health_seen[0]["status"])
        self.assertEqual("a", loop.health_seen[0]["request"])
        self.assertEqual([], sorted(path.name for path in loop.inbox.glob("*.json")))
        self.assertTrue((loop.inbox / "done" / "a.json").is_file())
        result = json.loads((loop.inbox / "results" / "a.json").read_text())
        self.assertEqual({"request": "a", "outcome": "finished", "exit_code": 0, "error": ""}, result)
        self.assertEqual(3, loop.sleeps)

    def test_records_run_one_at_a_time_in_name_order(self) -> None:
        loop = self.loop({"a.json": Outcome("finished", 0), "b.json": Outcome("finished", 0)})
        loop.put("b.json", json.dumps(RECORD))
        loop.put("a.json", json.dumps(RECORD))
        loop.serve()
        self.assertEqual(["a.json", "b.json"], loop.ran)

    def test_a_failed_run_is_kept_in_failed_with_its_exit_code_and_serving_continues(self) -> None:
        loop = self.loop({"a.json": Outcome("failed", 3, "refused before any payload was read"), "b.json": Outcome("finished", 0)})
        loop.put("a.json", json.dumps(RECORD))
        loop.put("b.json", json.dumps(RECORD))
        loop.serve()
        self.assertEqual(["a.json", "b.json"], loop.ran)
        self.assertTrue((loop.inbox / "failed" / "a.json").is_file())
        self.assertEqual(
            {"request": "a", "outcome": "failed", "exit_code": 3, "error": "refused before any payload was read"},
            json.loads((loop.inbox / "results" / "a.json").read_text()),
        )
        self.assertTrue((loop.inbox / "done" / "b.json").is_file())

    def test_a_run_stopped_by_a_signal_stays_in_the_inbox_for_the_restart(self) -> None:
        loop = self.loop({"a.json": Outcome("stopped", 2)})
        loop.put("a.json", json.dumps(RECORD))
        self.assertEqual(0, loop.serve())
        self.assertTrue((loop.inbox / "a.json").is_file())
        self.assertFalse((loop.inbox / "results" / "a.json").exists())
        restarted = Loop({"a.json": Outcome("finished", 0)})
        self.addCleanup(restarted.folder.cleanup)
        restarted.inbox.rmdir()
        restarted.inbox = loop.inbox
        restarted.serve()
        self.assertEqual(["a.json"], restarted.ran)
        self.assertTrue((loop.inbox / "done" / "a.json").is_file())

    def test_the_reference_beside_a_record_is_kept_with_it(self) -> None:
        loop = self.loop({"a.json": Outcome("finished", 0), "b.json": Outcome("failed", 2, "no")})
        for name in ("a", "b"):
            loop.put(f"{name}.json", json.dumps(RECORD))
            loop.put(f"{name}.reference", REFERENCE)
        loop.serve()
        self.assertEqual(REFERENCE, (loop.inbox / "done" / "a.reference").read_text())
        self.assertEqual(REFERENCE, (loop.inbox / "failed" / "b.reference").read_text())
        self.assertEqual([], sorted(path.name for path in loop.inbox.glob("*.reference")))

    def test_a_queued_record_that_does_not_parse_or_validate_fails_with_its_reason_and_serving_continues(self) -> None:
        planted = ["not json", "{}", json.dumps({**RECORD, "rooms": "one"})]
        planted += [json.dumps({**RECORD, field: value}) for field in TIMING_FIELDS for value in BAD_TIMES]
        planted += [json.dumps({**RECORD, "speed": value}) for value in BAD_SPEEDS]
        for index, text in enumerate(planted):
            with self.subTest(text=text[:70]):
                loop = self.loop({"b.json": Outcome("finished", 0)})
                loop.put("a.json", text)
                loop.put("b.json", json.dumps(RECORD))
                self.assertEqual(0, loop.serve(), index)
                self.assertEqual(["b.json"], loop.ran)
                self.assertTrue((loop.inbox / "failed" / "a.json").is_file())
                self.assertFalse((loop.inbox / "a.json").exists())
                result = json.loads((loop.inbox / "results" / "a.json").read_text())
                self.assertEqual(("failed", 2), (result["outcome"], result["exit_code"]))
                self.assertTrue(result["error"].startswith("RunRecordError: "), result["error"])
                self.assertTrue((loop.inbox / "done" / "b.json").is_file())

    def test_a_queued_record_without_its_reference_fails_and_the_reference_beside_a_record_replaces_the_one_in_it(self) -> None:
        loop = self.loop({"b.json": Outcome("finished", 0)})
        loop.put("a.json", json.dumps(RECORD), None)
        loop.put("b.json", json.dumps(RECORD))
        loop.serve()
        self.assertEqual(["b.json"], loop.ran)
        self.assertIn("manifest reference", json.loads((loop.inbox / "results" / "a.json").read_text())["error"])
        record = queued_record(loop.inbox / "done" / "b.json")
        self.assertEqual(str(loop.inbox / "done" / "b.reference"), record.manifest_reference)
        self.assertNotEqual(RECORD["manifest_reference"], record.manifest_reference)

    def test_a_restart_after_the_result_was_written_archives_the_record_and_runs_nothing_again(self) -> None:
        for kind, target in (("finished", "done"), ("failed", "failed")):
            with self.subTest(kind=kind):
                loop = self.loop({"a.json": Outcome("failed", 2, "a replay of a finished run"), "b.json": Outcome("finished", 0)})
                loop.put("a.json", json.dumps(RECORD))
                loop.put("a.reference", REFERENCE)
                loop.put("b.json", json.dumps(RECORD))
                kept = (
                    json.dumps({"request": "a", "outcome": kind, "exit_code": 0 if kind == "finished" else 3, "error": "" if kind == "finished" else "x"})
                    + "\n"
                )
                (loop.inbox / "results").mkdir()
                (loop.inbox / "results" / "a.json").write_text(kept)
                self.assertEqual(0, loop.serve())
                self.assertEqual(["b.json"], loop.ran)
                self.assertEqual(kept, (loop.inbox / "results" / "a.json").read_text())
                self.assertTrue((loop.inbox / target / "a.json").is_file())
                self.assertTrue((loop.inbox / target / "a.reference").is_file())
                self.assertFalse((loop.inbox / "a.json").exists())

    def test_a_result_that_is_not_a_terminal_one_does_not_keep_the_record_from_running_and_is_kept_beside_it_as_invalid(self) -> None:
        planted = (
            "not json",
            "[]",
            "",
            json.dumps({"outcome": "stopped"}),
            json.dumps({"outcome": "running"}),
            json.dumps({"request": "a"}),
            json.dumps({"outcome": []}),
            json.dumps({"outcome": {"finished": 1}}),
            json.dumps({"outcome": None}),
            json.dumps({"outcome": 7}),
            json.dumps({"outcome": ["finished"]}),
        )
        for text in planted:
            with self.subTest(text=text):
                loop = self.loop({"a.json": Outcome("finished", 0)})
                loop.put("a.json", json.dumps(RECORD))
                (loop.inbox / "results").mkdir()
                (loop.inbox / "results" / "a.json").write_text(text)
                loop.serve()
                self.assertEqual(["a.json"], loop.ran)
                self.assertEqual("finished", json.loads((loop.inbox / "results" / "a.json").read_text())["outcome"])
                self.assertEqual(text, (loop.inbox / "results" / "a.json.invalid").read_text())
                self.assertTrue(any("result_invalid" in line and "a.json" in line for line in loop.logged), loop.logged)
                self.assertTrue((loop.inbox / "done" / "a.json").is_file())

    def test_a_valid_terminal_result_is_not_renamed_and_nothing_is_logged(self) -> None:
        for outcome in ("finished", "failed"):
            with self.subTest(outcome=outcome):
                loop = self.loop({})
                loop.put("a.json", json.dumps(RECORD))
                (loop.inbox / "results").mkdir()
                (loop.inbox / "results" / "a.json").write_text(json.dumps({"request": "a", "outcome": outcome, "exit_code": 0, "error": ""}))
                loop.serve()
                self.assertEqual([], loop.ran)
                self.assertFalse((loop.inbox / "results" / "a.json.invalid").exists())
                self.assertEqual([], loop.logged)

    def test_a_planted_invalid_result_still_counts_the_start_and_three_stops_with_it_fail_the_request(self) -> None:
        loop = self.loop({"a.json": Outcome("stopped", 2)})
        loop.put("a.json", json.dumps(RECORD))
        (loop.inbox / "results").mkdir()
        (loop.inbox / "results" / "a.json").write_text(json.dumps({"outcome": []}))
        (loop.inbox / "a.attempts").write_text("3\n")
        loop.serve()
        self.assertEqual([], loop.ran)
        self.assertEqual("failed", json.loads((loop.inbox / "results" / "a.json").read_text())["outcome"])

    def test_a_stop_while_waiting_returns_at_once(self) -> None:
        loop = self.loop({}, sleeps_before_stop=1)
        self.assertEqual(0, loop.serve())
        self.assertEqual(1, loop.sleeps)

    def test_only_json_files_are_requests(self) -> None:
        loop = self.loop({})
        loop.put("a.json.writing", json.dumps(RECORD))
        loop.serve()
        self.assertEqual([], loop.ran)


class CrashLoopTest(unittest.TestCase):
    """A request that stops the whole service is counted at each start, so a service that restarts on it cannot loop for ever, whatever the error."""

    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="replay_crash_")
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name)
        self.inbox = self.root / "inbox"
        self.inbox.mkdir()
        self.ran: list[str] = []
        self.stop = False

    def put(self, name: str) -> None:
        (self.inbox / f"{name}.json").write_text(json.dumps(RECORD))
        (self.inbox / f"{name}.reference").write_text(REFERENCE)

    def start(self, run_request: Callable[[Path, Callable[[], None]], Outcome]) -> int:
        self.stop = False

        def sleep(seconds: float) -> None:
            self.stop = True

        return serve(self.inbox, self.root / "health.json", run_request, lambda: self.stop, Clock(), sleep, 1.0)

    def crash_on_a(self, path: Path, beat: Callable[[], None]) -> Outcome:
        self.ran.append(path.name)
        if path.name == "a.json":
            raise KeyError("an error that nothing handles")
        return Outcome("finished", 0)

    def test_a_request_that_stops_the_service_three_starts_in_a_row_fails_on_the_fourth_and_the_next_request_runs(self) -> None:
        self.put("a")
        self.put("b")
        for count in (1, 2, 3):
            with self.assertRaises(KeyError):
                self.start(self.crash_on_a)
            self.assertEqual(str(count), (self.inbox / "a.attempts").read_text().strip())
        self.assertEqual(["a.json"] * 3, self.ran)
        self.assertEqual(0, self.start(self.crash_on_a))
        self.assertEqual(["a.json"] * 3 + ["b.json"], self.ran)
        result = json.loads((self.inbox / "results" / "a.json").read_text())
        self.assertEqual(("failed", "the service stopped during this request 3 times"), (result["outcome"], result["error"]))
        self.assertTrue((self.inbox / "failed" / "a.json").is_file())
        self.assertTrue((self.inbox / "failed" / "a.reference").is_file())
        self.assertTrue((self.inbox / "done" / "b.json").is_file())
        self.assertEqual([], sorted(path.name for path in self.inbox.glob("*.attempts")))

    def test_two_stops_then_a_finish_is_not_a_failure_and_the_count_goes_with_the_archived_request(self) -> None:
        self.put("a")
        for _ in range(2):
            with self.assertRaises(KeyError):
                self.start(self.crash_on_a)
        self.assertEqual(0, self.start(lambda path, beat: Outcome("finished", 0)))
        self.assertTrue((self.inbox / "done" / "a.json").is_file())
        self.assertEqual([], sorted(path.name for path in self.inbox.glob("*.attempts")))

    def test_a_stop_by_signal_is_not_counted_so_any_number_of_deploys_keep_the_run(self) -> None:
        self.put("a")
        for _ in range(6):
            self.start(lambda path, beat: Outcome("stopped", 2))
            self.assertTrue((self.inbox / "a.json").is_file())
        self.assertEqual("0", (self.inbox / "a.attempts").read_text().strip())
        self.assertEqual(0, self.start(lambda path, beat: Outcome("finished", 0)))
        self.assertTrue((self.inbox / "done" / "a.json").is_file())

    def test_an_arithmetic_or_value_error_of_a_replay_fails_that_request_at_once_and_the_next_runs(self) -> None:
        for error in (OverflowError("int too large"), ZeroDivisionError("division"), ValueError("a value")):
            with self.subTest(error=type(error).__name__):
                self.setUp()
                self.put("a")
                self.put("b")

                def run(path: Path, beat: Callable[[], None], error: Exception = error) -> Outcome:
                    self.ran.append(path.name)
                    if path.name == "a.json":
                        raise error
                    return Outcome("finished", 0)

                self.assertEqual(0, self.start(run))
                self.assertEqual(["a.json", "b.json"], self.ran)
                result = json.loads((self.inbox / "results" / "a.json").read_text())
                self.assertEqual(("failed", 2, f"{type(error).__name__}: {error}"), (result["outcome"], result["exit_code"], result["error"]))
                self.assertTrue((self.inbox / "failed" / "a.json").is_file())
                self.assertTrue((self.inbox / "done" / "b.json").is_file())

    def test_a_queued_record_with_a_speed_that_overflows_the_loop_fails_without_a_run(self) -> None:
        (self.inbox / "a.json").write_text(json.dumps({**RECORD, "speed": 5e-324}))
        (self.inbox / "a.reference").write_text(REFERENCE)
        self.put("b")
        self.assertEqual(0, self.start(self.crash_on_a))
        self.assertEqual(["b.json"], self.ran)
        self.assertTrue((self.inbox / "failed" / "a.json").is_file())


class HealthTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="replay_health_")
        self.addCleanup(folder.cleanup)
        self.path = Path(folder.name) / "health.json"

    def write(self, status: str, updated: object) -> None:
        self.path.write_text(json.dumps({"status": status, "updated": updated, "request": "", "last": None}))

    def test_a_fresh_waiting_or_running_file_is_healthy(self) -> None:
        for status in ("waiting", "running"):
            with self.subTest(status=status):
                self.write(status, 1000.0)
                self.assertTrue(healthy(self.path, 1000.0 + 60, 60))

    def test_a_stamp_in_the_future_or_not_finite_is_unhealthy_and_the_limits_are_the_edges(self) -> None:
        for stamp in (1000.0 + 1, 1000.0 + 10**6, float("inf"), float("-inf"), float("nan"), True, "1000"):
            with self.subTest(stamp=stamp):
                self.write("waiting", stamp)
                self.assertFalse(healthy(self.path, 1000.0, 60))
        self.write("running", 1000.0)
        self.assertTrue(healthy(self.path, 1000.0, 60))
        self.assertFalse(healthy(self.path, 1000.0 + 61, 60))

    def test_a_stale_stopped_absent_or_unreadable_file_is_unhealthy(self) -> None:
        self.write("waiting", 1000.0)
        self.assertFalse(healthy(self.path, 1000.0 + 61, 60))
        self.write("stopped", 1000.0)
        self.assertFalse(healthy(self.path, 1000.0, 60))
        self.path.write_text("not json")
        self.assertFalse(healthy(self.path, 1000.0, 60))
        self.path.unlink()
        self.assertFalse(healthy(self.path, 1000.0, 60))


class SubmitTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="replay_submit_")
        self.addCleanup(folder.cleanup)
        self.inbox = Path(folder.name) / "inbox"
        self.name = "replay_0a1b2c3d"

    def files(self) -> list[str]:
        return sorted(str(path.relative_to(self.inbox)) for path in self.inbox.rglob("*") if path.is_file())

    def test_a_valid_record_lands_in_the_inbox_under_its_run_id_once_with_its_reference_beside_it(self) -> None:
        text = json.dumps(RECORD)
        self.assertEqual("submitted", submit(self.inbox, text, REFERENCE))
        self.assertEqual("already submitted", submit(self.inbox, text, REFERENCE))
        self.assertEqual(
            [f"locks/{self.name}", f"{self.name}.json", f"{self.name}.reference", f"reserved/{self.name}", f"reserved/{self.name}.reference"], self.files()
        )
        self.assertEqual(text, (self.inbox / f"{self.name}.json").read_text())
        self.assertEqual(REFERENCE, (self.inbox / f"{self.name}.reference").read_text())
        self.assertEqual(text, (self.inbox / "reserved" / self.name).read_text())
        self.assertEqual(REFERENCE, (self.inbox / "reserved" / f"{self.name}.reference").read_text())

    def test_a_record_already_done_is_not_submitted_again(self) -> None:
        text = json.dumps(RECORD)
        submit(self.inbox, text, REFERENCE)
        (self.inbox / "done").mkdir()
        for suffix in (".json", ".reference"):
            (self.inbox / f"{self.name}{suffix}").rename(self.inbox / "done" / f"{self.name}{suffix}")
        self.assertEqual("already submitted", submit(self.inbox, text, REFERENCE))
        self.assertFalse((self.inbox / f"{self.name}.json").exists())

    def test_another_record_under_the_same_run_id_is_refused_and_the_first_kept(self) -> None:
        submit(self.inbox, json.dumps(RECORD), REFERENCE)
        with self.assertRaises(SubmitError):
            submit(self.inbox, json.dumps({**RECORD, "speed": 120.0}), REFERENCE)
        self.assertEqual(json.dumps(RECORD), (self.inbox / f"{self.name}.json").read_text())
        self.assertEqual(json.dumps(RECORD), (self.inbox / "reserved" / self.name).read_text())

    def test_the_same_record_with_another_reference_is_refused_and_the_first_kept(self) -> None:
        submit(self.inbox, json.dumps(RECORD), REFERENCE)
        with self.assertRaises(SubmitError):
            submit(self.inbox, json.dumps(RECORD), OTHER_REFERENCE)
        self.assertEqual(REFERENCE, (self.inbox / f"{self.name}.reference").read_text())

    def test_the_reservation_outlives_the_archival_and_the_loss_of_the_archived_files(self) -> None:
        text = json.dumps(RECORD)
        submit(self.inbox, text, REFERENCE)
        folder = tempfile.TemporaryDirectory(prefix="replay_submit_serve_")
        self.addCleanup(folder.cleanup)
        stops = iter([False, True])
        ran: list[str] = []

        def run_request(path: Path, beat: object) -> Outcome:
            ran.append(path.name)
            return Outcome("finished", 0)

        serve(self.inbox, Path(folder.name) / "health.json", run_request, lambda: next(stops), Clock(), lambda seconds: None, 1.0)
        self.assertEqual([f"{self.name}.json"], ran)
        self.assertTrue((self.inbox / "done" / f"{self.name}.json").is_file())
        self.assertEqual(text, (self.inbox / "reserved" / self.name).read_text())
        self.assertEqual("already submitted", submit(self.inbox, text, REFERENCE))
        for path in (self.inbox / "done" / f"{self.name}.json", self.inbox / "done" / f"{self.name}.reference", self.inbox / "results" / f"{self.name}.json"):
            path.unlink()
        with self.assertRaises(SubmitError):
            submit(self.inbox, json.dumps({**RECORD, "speed": 120.0}), REFERENCE)
        self.assertFalse((self.inbox / f"{self.name}.json").exists())

    def test_a_submit_that_stopped_after_the_reservation_is_completed_by_the_same_submit_again(self) -> None:
        text = json.dumps(RECORD)
        submit(self.inbox, text, REFERENCE)
        for suffix in (".json", ".reference"):
            (self.inbox / f"{self.name}{suffix}").unlink()
        self.assertEqual("submitted", submit(self.inbox, text, REFERENCE))
        self.assertEqual(text, (self.inbox / f"{self.name}.json").read_text())
        self.assertEqual("already submitted", submit(self.inbox, text, REFERENCE))

    def race(self, texts: list[str], references: list[str]) -> list[str | SubmitError]:
        results: list[str | SubmitError] = [""] * len(texts)
        barrier = threading.Barrier(len(texts))

        def work(index: int) -> None:
            barrier.wait(timeout=30)
            try:
                results[index] = submit(self.inbox, texts[index], references[index])
            except SubmitError as error:
                results[index] = error

        threads = [threading.Thread(target=work, args=(index,)) for index in range(len(texts))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)
            self.assertFalse(thread.is_alive())
        return results

    def test_two_different_records_submitted_at_once_under_one_run_id_give_one_success_and_one_refusal(self) -> None:
        for round_ in range(40):
            with self.subTest(round=round_):
                folder = tempfile.TemporaryDirectory(prefix="replay_submit_race_")
                self.addCleanup(folder.cleanup)
                self.inbox = Path(folder.name) / "inbox"
                first, second = json.dumps(RECORD), json.dumps({**RECORD, "speed": 120.0})
                results = self.race([first, second], [REFERENCE, REFERENCE])
                self.assertEqual(1, sum(result == "submitted" for result in results), results)
                self.assertEqual(1, sum(isinstance(result, SubmitError) for result in results), results)
                winner = first if results[0] == "submitted" else second
                self.assertEqual(winner, (self.inbox / f"{self.name}.json").read_text())
                self.assertEqual([f"{self.name}.json"], sorted(path.name for path in self.inbox.glob("*.json")))

    def test_the_same_record_submitted_at_once_leaves_one_queue_entry_and_no_refusal(self) -> None:
        for round_ in range(40):
            with self.subTest(round=round_):
                folder = tempfile.TemporaryDirectory(prefix="replay_submit_race_")
                self.addCleanup(folder.cleanup)
                self.inbox = Path(folder.name) / "inbox"
                results = self.race([json.dumps(RECORD)] * 3, [REFERENCE] * 3)
                self.assertTrue(all(result in ("submitted", "already submitted") for result in results), results)
                self.assertEqual([f"{self.name}.json"], sorted(path.name for path in self.inbox.glob("*.json")))
                self.assertEqual(json.dumps(RECORD), (self.inbox / f"{self.name}.json").read_text())

    def test_a_timing_field_that_is_not_a_finite_number_from_0_to_86400_is_refused_and_nothing_lands(self) -> None:
        for field in TIMING_FIELDS:
            for value in BAD_TIMES:
                with self.subTest(field=field, value=value), self.assertRaises(SubmitError) as caught:
                    submit(self.inbox, json.dumps({**RECORD, field: value}), REFERENCE)
                self.assertIn(field, str(caught.exception))
        self.assertFalse(self.inbox.exists() and any(self.inbox.iterdir()))

    def test_the_edges_of_the_timing_range_are_accepted(self) -> None:
        for field in TIMING_FIELDS:
            for value in (0, 86_400, 0.5 if field == "idle_checkpoint_seconds" else 1):
                with self.subTest(field=field, value=value):
                    folder = tempfile.TemporaryDirectory(prefix="replay_submit_edge_")
                    self.addCleanup(folder.cleanup)
                    self.assertEqual("submitted", submit(Path(folder.name) / "inbox", json.dumps({**RECORD, field: value}), REFERENCE))

    def test_an_invalid_record_run_id_or_reference_is_refused_and_nothing_lands(self) -> None:
        cases = [("not json", REFERENCE), (json.dumps({**RECORD, "rooms": "one"}), REFERENCE), (json.dumps({**RECORD, "run_id": "../escape"}), REFERENCE)]
        cases += [(json.dumps(RECORD), "not json"), (json.dumps(RECORD), "{}"), (json.dumps(RECORD), "")]
        for text, reference in cases:
            with self.subTest(text=text[:30], reference=reference[:10]), self.assertRaises(SubmitError):
                submit(self.inbox, text, reference)
        self.assertFalse(self.inbox.exists() and any(self.inbox.iterdir()))

    def plant(self, folder: str, text: str = "", reference: str | None = REFERENCE) -> None:
        """A record that is held under this run ID without a reservation, as one was before reservations existed."""
        target = self.inbox / folder if folder else self.inbox
        target.mkdir(parents=True, exist_ok=True)
        (target / f"{self.name}.json").write_text(text or json.dumps(RECORD))
        if reference is not None:
            (target / f"{self.name}.reference").write_text(reference)

    def test_a_record_held_in_the_inbox_done_or_failed_without_a_reservation_is_already_submitted_and_gets_its_reservation(self) -> None:
        text = json.dumps(RECORD)
        for folder in ("", "done", "failed"):
            with self.subTest(folder=folder):
                self.setUp()
                self.plant(folder)
                self.assertEqual("already submitted", submit(self.inbox, text, REFERENCE))
                self.assertEqual(text, (self.inbox / "reserved" / self.name).read_text())
                self.assertEqual(REFERENCE, (self.inbox / "reserved" / f"{self.name}.reference").read_text())
                self.assertEqual(folder == "", (self.inbox / f"{self.name}.json").exists())
                self.assertEqual("already submitted", submit(self.inbox, text, REFERENCE))

    def test_a_different_record_or_reference_under_an_id_that_a_record_already_holds_is_refused_and_nothing_is_reserved(self) -> None:
        other_record = json.dumps({**RECORD, "speed": 120.0})
        for folder in ("", "done", "failed"):
            for label, text, reference in (("record", other_record, REFERENCE), ("reference", json.dumps(RECORD), OTHER_REFERENCE)):
                with self.subTest(folder=folder, different=label):
                    self.setUp()
                    self.plant(folder)
                    with self.assertRaises(SubmitError):
                        submit(self.inbox, text, reference)
                    self.assertFalse((self.inbox / "reserved").exists())
                    self.assertEqual(json.dumps(RECORD), (self.inbox / folder / f"{self.name}.json").read_text())

    def test_a_record_held_without_its_reference_is_not_the_same_submission(self) -> None:
        for folder in ("", "done", "failed"):
            with self.subTest(folder=folder):
                self.setUp()
                self.plant(folder, reference=None)
                with self.assertRaises(SubmitError):
                    submit(self.inbox, json.dumps(RECORD), REFERENCE)
                self.assertFalse((self.inbox / "reserved").exists())

    def test_a_delayed_submit_waits_for_the_lock_of_its_run_id_and_queues_nothing_when_the_record_was_archived_meanwhile(self) -> None:
        text = json.dumps(RECORD)
        reserved = self.inbox / "reserved"
        reserved.mkdir(parents=True)
        (reserved / self.name).write_text(text)
        (reserved / f"{self.name}.reference").write_text(REFERENCE)
        results: list[str] = []
        with held(self.inbox, self.name):
            thread = threading.Thread(target=lambda: results.append(submit(self.inbox, text, REFERENCE)))
            thread.start()
            thread.join(timeout=0.5)
            self.assertTrue(thread.is_alive())
            self.plant("done")
        thread.join(timeout=30)
        self.assertFalse(thread.is_alive())
        self.assertEqual(["already submitted"], results)
        self.assertFalse((self.inbox / f"{self.name}.json").exists())
        self.assertFalse((self.inbox / f"{self.name}.reference").exists())

    def test_an_archival_waits_for_the_lock_of_its_run_id(self) -> None:
        submit(self.inbox, json.dumps(RECORD), REFERENCE)
        thread = threading.Thread(target=lambda: archive(self.inbox, self.inbox / f"{self.name}.json", "finished"))
        with held(self.inbox, self.name):
            thread.start()
            thread.join(timeout=0.5)
            self.assertTrue(thread.is_alive())
            self.assertTrue((self.inbox / f"{self.name}.json").is_file())
        thread.join(timeout=30)
        self.assertTrue((self.inbox / "done" / f"{self.name}.json").is_file())

    def test_a_lock_that_is_not_released_in_time_fails_visibly(self) -> None:
        submit(self.inbox, json.dumps(RECORD), REFERENCE)
        failures: list[BaseException] = []

        def contend() -> None:
            try:
                with mock.patch.object(serve_module, "LOCK_SECONDS", 0.3), held(self.inbox, self.name):
                    pass
            except TimeoutError as error:
                failures.append(error)

        with held(self.inbox, self.name):
            thread = threading.Thread(target=contend)
            started = time.monotonic()
            thread.start()
            thread.join(timeout=30)
        self.assertLess(time.monotonic() - started, 10)
        self.assertEqual(1, len(failures))
        self.assertIn(self.name, str(failures[0]))

    def test_a_run_id_without_a_reservation_has_a_lock_too(self) -> None:
        results: list[str] = []

        def contend() -> None:
            with held(self.inbox, "nothing_reserved"):
                results.append("held")

        with held(self.inbox, "nothing_reserved"):
            thread = threading.Thread(target=contend)
            thread.start()
            thread.join(timeout=0.5)
            self.assertTrue(thread.is_alive())
        thread.join(timeout=30)
        self.assertEqual(["held"], results)

    def archive_paused_after_its_first_move(self, attempt: Callable[[], str]) -> list[str | SubmitError]:
        """Archive the queued request of this run ID and, between its two moves (the reference is in ``done/``, the record is still in the inbox), run
        ``attempt`` in a thread. The attempt gets half a second to finish, so one that waits for the lock is still waiting when the archival goes on."""
        results: list[str | SubmitError] = []
        threads: list[threading.Thread] = []
        real = os.replace

        def work() -> None:
            try:
                results.append(attempt())
            except SubmitError as error:
                results.append(error)

        def pausing(source: Any, target: Any) -> None:
            real(source, target)
            if Path(target).parent.name == "done" and str(target).endswith(".reference"):
                thread = threading.Thread(target=work)
                threads.append(thread)
                thread.start()
                thread.join(timeout=0.5)

        with mock.patch("os.replace", pausing):
            archive(self.inbox, self.inbox / f"{self.name}.json", "finished")
        for thread in threads:
            thread.join(timeout=30)
            self.assertFalse(thread.is_alive())
        return results

    def test_an_identical_submit_during_the_archival_of_a_reserved_request_is_not_refused_and_queues_nothing(self) -> None:
        text = json.dumps(RECORD)
        submit(self.inbox, text, REFERENCE)
        results = self.archive_paused_after_its_first_move(lambda: submit(self.inbox, text, REFERENCE))
        self.assertEqual(["already submitted"], results)
        self.assertFalse((self.inbox / f"{self.name}.json").exists())
        self.assertFalse((self.inbox / f"{self.name}.reference").exists())
        self.assertTrue((self.inbox / "done" / f"{self.name}.json").is_file())

    def test_an_identical_submit_during_the_archival_of_a_request_without_a_reservation_is_not_refused_and_gets_its_reservation(self) -> None:
        text = json.dumps(RECORD)
        self.plant("")
        results = self.archive_paused_after_its_first_move(lambda: submit(self.inbox, text, REFERENCE))
        self.assertEqual(["already submitted"], results)
        self.assertEqual(text, (self.inbox / "reserved" / self.name).read_text())
        self.assertFalse((self.inbox / f"{self.name}.json").exists())

    def test_a_different_submit_during_the_archival_is_still_refused_with_and_without_a_reservation(self) -> None:
        other = json.dumps({**RECORD, "speed": 120.0})
        for reserved in (True, False):
            with self.subTest(reserved=reserved):
                self.setUp()
                if reserved:
                    submit(self.inbox, json.dumps(RECORD), REFERENCE)
                else:
                    self.plant("")
                results = self.archive_paused_after_its_first_move(lambda: submit(self.inbox, other, REFERENCE))
                self.assertEqual(1, len(results))
                self.assertIsInstance(results[0], SubmitError)
                self.assertFalse((self.inbox / f"{self.name}.json").exists())

    def test_a_reserved_run_id_is_compared_with_its_reservation_only_whatever_the_files_that_archival_moved_hold(self) -> None:
        text = json.dumps(RECORD)
        submit(self.inbox, text, REFERENCE)
        for suffix in (".json", ".reference"):
            (self.inbox / f"{self.name}{suffix}").unlink()
        self.assertEqual("submitted", submit(self.inbox, text, REFERENCE))
        (self.inbox / f"{self.name}.json").write_text(json.dumps({**RECORD, "speed": 120.0}))
        self.assertEqual("already submitted", submit(self.inbox, text, REFERENCE))
        with self.assertRaises(SubmitError):
            submit(self.inbox, text, OTHER_REFERENCE)

    def test_a_record_with_crlf_line_ends_is_stored_with_its_exact_bytes_and_the_same_text_with_lf_is_another_record(self) -> None:
        lf = json.dumps(RECORD, indent=1).encode()
        crlf = lf.replace(b"\n", b"\r\n")
        reference_lf = REFERENCE.encode()
        reference_crlf = reference_lf.replace(b"\n", b"\r\n") if b"\n" in reference_lf else reference_lf + b"\r\n"
        self.assertNotEqual(lf, crlf)
        self.assertEqual("submitted", submit(self.inbox, crlf, reference_crlf))
        self.assertEqual(crlf, (self.inbox / f"{self.name}.json").read_bytes())
        self.assertEqual(reference_crlf, (self.inbox / f"{self.name}.reference").read_bytes())
        self.assertEqual(crlf, (self.inbox / "reserved" / self.name).read_bytes())
        self.assertEqual("already submitted", submit(self.inbox, crlf, reference_crlf))
        for label, record, reference in (("record", lf, reference_crlf), ("reference", crlf, reference_lf)):
            with self.subTest(different=label), self.assertRaises(SubmitError):
                submit(self.inbox, record, reference)
        self.assertEqual(crlf, (self.inbox / f"{self.name}.json").read_bytes())

    def test_a_record_with_invalid_utf_8_in_either_input_is_refused_with_its_reason_and_nothing_lands(self) -> None:
        for label, record, reference in (
            ("record", b'{"run_id": "\xff"}', REFERENCE.encode()),
            ("reference", json.dumps(RECORD).encode(), b'{"bucket": "\xff"}'),
        ):
            with self.subTest(label=label), self.assertRaises(SubmitError) as caught:
                submit(self.inbox, record, reference)
            self.assertIn(label, str(caught.exception))
            self.assertIn("UTF-8", str(caught.exception))
        self.assertFalse(self.inbox.exists() and any(self.inbox.iterdir()))

    def command(self, record: bytes, reference: bytes) -> int:
        """The submit command as the container runs it: the record on standard input as bytes and the reference in a file."""
        scratch = tempfile.TemporaryDirectory(prefix="replay_command_")
        self.addCleanup(scratch.cleanup)
        reference_file = Path(scratch.name) / "manifest_reference.json"
        reference_file.write_bytes(reference)
        return submit_command(self.inbox, reference_file, io.BytesIO(record), io.StringIO(), lambda event, **fields: None)

    def test_the_submit_command_keeps_exact_bytes_and_exits_2_on_a_different_line_end_or_invalid_utf_8(self) -> None:
        lf = json.dumps(RECORD, indent=1).encode()
        crlf = lf.replace(b"\n", b"\r\n")
        reference = REFERENCE.encode()
        self.assertEqual(0, self.command(crlf, reference))
        self.assertEqual(crlf, (self.inbox / f"{self.name}.json").read_bytes())
        self.assertEqual(0, self.command(crlf, reference))
        self.assertEqual(2, self.command(lf, reference))
        self.assertEqual(2, self.command(b'{"run_id": "\xff"}', reference))
        self.assertEqual(2, self.command(crlf, b'{"bucket": "\xff"}'))
        self.assertEqual(crlf, (self.inbox / f"{self.name}.json").read_bytes())

    def test_a_speed_outside_0_01_to_1000_is_refused_and_the_edges_and_a_normal_speed_are_accepted(self) -> None:
        for value in BAD_SPEEDS:
            with self.subTest(speed=value), self.assertRaises(SubmitError) as caught:
                submit(self.inbox, json.dumps({**RECORD, "speed": value}), REFERENCE)
            self.assertIn("speed", str(caught.exception))
        self.assertFalse(self.inbox.exists() and any(self.inbox.iterdir()))
        for value in GOOD_SPEEDS:
            with self.subTest(speed=value):
                folder = tempfile.TemporaryDirectory(prefix="replay_submit_speed_")
                self.addCleanup(folder.cleanup)
                self.assertEqual("submitted", submit(Path(folder.name) / "inbox", json.dumps({**RECORD, "speed": value}), REFERENCE))

    def test_a_submission_is_read_as_exact_bytes_and_a_reference_file_that_is_missing_is_refused_with_its_reason(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="replay_read_")
        self.addCleanup(scratch.cleanup)
        folder = Path(scratch.name)
        good, missing = folder / "good.reference", folder / "missing.reference"
        exact = REFERENCE.encode().replace(b"\n", b"\r\n") + b"\r\n\xff"
        good.write_bytes(exact)
        self.assertEqual((b'{"a": 1}\r\n', exact), read_submission(good, io.BytesIO(b'{"a": 1}\r\n')))
        with self.assertRaises(SubmitError) as caught:
            read_submission(missing, io.BytesIO(b"{}"))
        self.assertIn("reference", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
