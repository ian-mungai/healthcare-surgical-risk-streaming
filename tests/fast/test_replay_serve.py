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
from replay.service.serve import (
    Outcome,
    SubmitError,
    archive,
    healthy,
    held,
    keep,
    kept_outcome,
    queued_record,
    read_result,
    read_submission,
    result_text,
    serve,
    submit,
    submit_command,
)
from scripts.ingestion.gate import GateRefused, Problem
from scripts.ingestion.manifest import ManifestRef
from scripts.ingestion.notice import Cause, Code, Notice

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


def serve_until_idle(inbox: Path, state: Path, run_request: Callable[[Path, Callable[[], None]], Outcome]) -> int:
    """Serve the inbox until it is empty and the loop would sleep: the stop comes from the idle wait, never from a request."""
    stop = [False]
    return serve(inbox, state, run_request, lambda: stop[0], Clock(), lambda seconds: stop.__setitem__(0, True), 1.0)


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
            self.root,
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


def unreadable(path: Path) -> None:
    path.write_text("{}")
    path.chmod(0)


def gate_refusal_text(count: int, cause: Cause = Cause.MISSING) -> str:
    """The text of a gate refusal that names ``count`` keys: the reviewer's input is 6,389 missing versions."""
    problems = [Problem(position, cause, False) for position in range(1, count + 1)]
    return str(GateRefused(Notice(Code.GATE_REFUSED), *[problem.notice for problem in problems], problems=problems))


def open_descriptors() -> int:
    return len(os.listdir("/dev/fd"))


def nested(depth: int) -> str:
    return "[" * depth + "]" * depth


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
        self.assertEqual([], loop.logged, "a request with no result file is not a result to set aside")
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
                self.assertEqual("RunRecordError", result["error"])
                self.assertTrue((loop.inbox / "done" / "b.json").is_file())

    def test_a_queued_record_without_its_reference_fails_and_the_reference_beside_a_record_replaces_the_one_in_it(self) -> None:
        loop = self.loop({"b.json": Outcome("finished", 0)})
        loop.put("a.json", json.dumps(RECORD), None)
        loop.put("b.json", json.dumps(RECORD))
        loop.serve()
        self.assertEqual(["b.json"], loop.ran)
        self.assertEqual("RunRecordError", json.loads((loop.inbox / "results" / "a.json").read_text())["error"])
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

    def test_an_invalid_result_that_cannot_be_renamed_is_logged_and_still_counts_as_no_result(self) -> None:
        loop = self.loop({})
        (loop.inbox / "results").mkdir()
        (loop.inbox / "results" / "a.json").write_text("not json")
        with mock.patch("os.replace", side_effect=PermissionError("read-only")):
            self.assertIsNone(kept_outcome(loop.inbox, loop.inbox / "a.json", lambda event, **fields: loop.logged.append(event)))
        self.assertEqual(["result_invalid", "result_invalid_not_kept"], loop.logged)

    def planted_result(self, plant: Callable[[Path], object]) -> Loop:
        loop = self.loop({"a.json": Outcome("finished", 0)})
        loop.put("a.json", json.dumps(RECORD))
        (loop.inbox / "results").mkdir()
        plant(loop.inbox / "results" / "a.json")
        loop.serve()
        return loop

    def assert_set_aside(self, loop: Loop) -> None:
        self.assertEqual(["a.json"], loop.ran)
        self.assertEqual("finished", json.loads((loop.inbox / "results" / "a.json").read_text())["outcome"])
        self.assertTrue((loop.inbox / "results" / "a.json.invalid").exists())
        self.assertTrue(any("result_invalid" in line for line in loop.logged), loop.logged)
        self.assertTrue((loop.inbox / "done" / "a.json").is_file())

    def test_a_result_file_that_cannot_be_judged_never_stops_the_serve_loop_and_is_set_aside(self) -> None:
        padded = json.dumps({"outcome": "finished", "pad": "x" * 70_000})
        cases: dict[str, Callable[[Path], object]] = {
            "the reviewers deeply nested array of 256 KB": lambda path: path.write_text(nested(128_000)),
            "a nested array under the size cap": lambda path: path.write_text(nested(30_000)),
            "a file that cannot be opened": unreadable,
            "a result that is too large": lambda path: path.write_text(padded),
            "bytes that are not UTF-8": lambda path: path.write_bytes(b'{"outcome": "\xff"}'),
            "a directory in place of the file": lambda path: path.mkdir(),
        }
        for label, plant in cases.items():
            with self.subTest(case=label):
                self.assert_set_aside(self.planted_result(plant))

    def test_a_result_that_raises_a_recursion_error_while_it_is_parsed_is_invalid_and_does_not_raise(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="replay_result_")
        self.addCleanup(scratch.cleanup)
        path = Path(scratch.name) / "a.json"
        path.write_text(nested(100))
        with mock.patch("json.loads", side_effect=RecursionError("maximum recursion depth exceeded")):
            outcome, reason = serve_module.read_result(path)
        self.assertIsNone(outcome)
        self.assertIn("RecursionError", reason)

    def test_read_result_leaves_no_descriptor_open_on_any_path(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="replay_descriptors_")
        self.addCleanup(scratch.cleanup)
        folder = Path(scratch.name)
        plants: dict[str, Callable[[Path], object]] = {
            "a directory": lambda path: path.mkdir(),
            "a fifo": lambda path: os.mkfifo(path),
            "an unreadable file": unreadable,
            "a valid result": lambda path: path.write_text(json.dumps({"outcome": "finished"})),
            "a result that is too large": lambda path: path.write_text(json.dumps({"outcome": "finished", "pad": "x" * 70_000})),
            "text that is not JSON": lambda path: path.write_text("not json"),
            "no file": lambda path: None,
        }
        for label, plant in plants.items():
            with self.subTest(case=label):
                path = folder / label.replace(" ", "_")
                plant(path)
                before = open_descriptors()
                for _ in range(20):
                    read_result(path)
                self.assertEqual(before, open_descriptors())

    def test_a_fifo_in_place_of_the_result_file_is_set_aside_without_a_wait(self) -> None:
        self.assert_set_aside(self.planted_result(lambda path: os.mkfifo(path)))

    def test_a_valid_terminal_result_at_the_size_cap_is_kept_and_one_byte_more_is_not(self) -> None:
        def sized(size: int) -> str:
            base = json.dumps({"request": "a", "outcome": "finished", "exit_code": 0, "error": "", "pad": ""})
            return json.dumps({"request": "a", "outcome": "finished", "exit_code": 0, "error": "", "pad": "x" * (size - len(base))})

        loop = self.planted_result(lambda path: path.write_text(sized(serve_module.RESULT_LIMIT_BYTES)))
        self.assertEqual([], loop.ran)
        self.assertFalse((loop.inbox / "results" / "a.json.invalid").exists())
        self.assert_set_aside(self.planted_result(lambda path: path.write_text(sized(serve_module.RESULT_LIMIT_BYTES + 1))))

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


class ResultSizeTest(unittest.TestCase):
    """The writer and the reader of a result file share one limit, so every result the service writes is a result it reads back."""

    def request(self, name: str = "a") -> Path:
        return Path("/inbox") / f"{name}.json"

    def test_the_reviewers_refusal_of_6389_missing_versions_is_written_under_the_limit_and_read_back(self) -> None:
        text = gate_refusal_text(6389)
        self.assertGreater(len(text), serve_module.RESULT_LIMIT_BYTES)
        written = result_text(self.request(), Outcome("failed", 3, text))
        self.assertLessEqual(len(written.encode()), serve_module.RESULT_LIMIT_BYTES)
        document = json.loads(written)
        self.assertEqual(("failed", 3), (document["outcome"], document["exit_code"]))
        self.assertTrue(document["error"].startswith(text[:200]))
        self.assertIn(f"{len(text) - serve_module.ERROR_LIMIT_CHARS} more characters cut", document["error"])
        scratch = tempfile.TemporaryDirectory(prefix="replay_size_")
        self.addCleanup(scratch.cleanup)
        path = Path(scratch.name) / "a.json"
        path.write_text(written)
        self.assertEqual(("failed", ""), read_result(path))

    def test_a_result_that_is_short_is_written_as_it_was_and_the_fields_are_the_known_four(self) -> None:
        written = result_text(self.request(), Outcome("failed", 2, "KafkaStartError: the broker cannot be reached"))
        self.assertEqual({"request": "a", "outcome": "failed", "exit_code": 2, "error": "KafkaStartError: the broker cannot be reached"}, json.loads(written))
        self.assertEqual({"request": "a", "outcome": "finished", "exit_code": 0, "error": ""}, json.loads(result_text(self.request(), Outcome("finished", 0))))

    def test_the_largest_result_that_each_failure_kind_can_give_is_under_the_limit_and_valid(self) -> None:
        long_name = "é" * 120
        kinds: dict[str, Outcome] = {
            "a gate refusal of every manifest entry": Outcome("failed", 3, gate_refusal_text(6389)),
            "a gate refusal far larger": Outcome("failed", 3, gate_refusal_text(60_000, Cause.VERSION_SUBSTITUTED)),
            "an error with a huge text": Outcome("failed", 2, "ValueError: " + "x" * 5_000_000),
            "an error whose text escapes to six bytes a character": Outcome("failed", 2, '\x01é"\\' * 3_000_000),
            "an error of characters outside the plane": Outcome("failed", 2, "\U0001f600" * 1_000_000),
            "the count of stops": Outcome("failed", 2, f"the service stopped during this request {serve_module.MAX_ATTEMPTS} times"),
            "a finish": Outcome("finished", 0),
        }
        for label, outcome in kinds.items():
            for name in ("a", long_name):
                with self.subTest(kind=label, request=name[:3]):
                    written = result_text(self.request(name), outcome)
                    self.assertLessEqual(len(written.encode()), serve_module.RESULT_LIMIT_BYTES)
                    document = json.loads(written)
                    self.assertIn(document["outcome"], serve_module.TERMINAL_OUTCOMES)
                    self.assertNotIn("reason", document, "the cut error text was too wide, so the result fell back to the short failure")

    def test_a_result_that_is_still_too_large_is_replaced_by_a_short_failure_with_a_reason_code_and_never_raises(self) -> None:
        with mock.patch.object(serve_module, "ERROR_LIMIT_CHARS", 10**9):
            written = result_text(self.request(), Outcome("failed", 3, "x" * 200_000))
        self.assertLessEqual(len(written.encode()), 1_000)
        document = json.loads(written)
        self.assertEqual(("failed", "result_too_large"), (document["outcome"], document["reason"]))


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

        return serve(self.inbox, self.root, run_request, lambda: self.stop, Clock(), sleep, 1.0)

    def crash_on_a(self, path: Path, beat: Callable[[], None]) -> Outcome:
        self.ran.append(path.name)
        if path.name == "a.json":
            raise KeyError("an error that nothing handles")
        return Outcome("finished", 0)

    def test_no_result_is_written_after_the_stop_was_seen_whatever_the_request_ended_in_and_the_next_start_repeats_it(self) -> None:
        endings: dict[str, Exception | Outcome] = {
            "a value error": ValueError("a value"),
            "an arithmetic error": OverflowError("int too large"),
            "a gate refusal": Outcome("failed", 3, "an object version was refused"),
            "a failed run": Outcome("failed", 2, "KafkaStartError: the broker cannot be reached"),
            "a finish": Outcome("finished", 0),
        }
        for label, ending in endings.items():
            with self.subTest(ending=label):
                self.setUp()
                self.put("a")

                def run(path: Path, beat: Callable[[], None], ending: Exception | Outcome = ending) -> Outcome:
                    self.ran.append(path.name)
                    self.stop = True
                    if isinstance(ending, Exception):
                        raise ending
                    return ending

                self.assertEqual(0, self.start(run))
                self.assertEqual(["a.json"], self.ran)
                self.assertTrue((self.inbox / "a.json").is_file())
                self.assertTrue((self.inbox / "a.reference").is_file())
                self.assertFalse((self.inbox / "results" / "a.json").exists())
                self.assertFalse((self.inbox / "failed").exists() or (self.inbox / "done").exists())
                self.assertEqual("0", (self.inbox / "a.attempts").read_text().strip())
                self.assertEqual(0, self.start(lambda path, beat: Outcome("finished", 0)))
                self.assertTrue((self.inbox / "done" / "a.json").is_file())

    def test_control_a_gate_refusal_and_an_error_without_a_stop_are_kept_as_failed(self) -> None:
        for ending in (Outcome("failed", 3, "an object version was refused"), ValueError("a value")):
            with self.subTest(ending=type(ending).__name__):
                self.setUp()
                self.put("a")

                def run(path: Path, beat: Callable[[], None], ending: Exception | Outcome = ending) -> Outcome:
                    if isinstance(ending, Exception):
                        raise ending
                    return ending

                self.assertEqual(0, self.start(run))
                result = json.loads((self.inbox / "results" / "a.json").read_text())
                self.assertEqual("failed", result["outcome"])
                self.assertTrue((self.inbox / "failed" / "a.json").is_file())
                self.assertFalse((self.inbox / "a.json").exists())

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
                self.assertEqual(("failed", 2, type(error).__name__), (result["outcome"], result["exit_code"], result["error"]))
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
        ran: list[str] = []

        def run_request(path: Path, beat: object) -> Outcome:
            ran.append(path.name)
            return Outcome("finished", 0)

        serve_until_idle(self.inbox, Path(folder.name), run_request)
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

    def interrupted_archival(self, outcome: Outcome) -> None:
        """The result is written and the archival is cut off right after the reference moved: the reference is in ``done/`` or ``failed/`` and the record is
        still in the inbox."""
        real = os.replace

        def interrupting(source: Any, target: Any) -> None:
            real(source, target)
            if str(target).endswith(".reference") and Path(target).parent.name in ("done", "failed"):
                raise KeyboardInterrupt

        with mock.patch("os.replace", interrupting), self.assertRaises(KeyboardInterrupt):
            keep(self.inbox, self.inbox / f"{self.name}.json", outcome)

    def test_an_identical_submit_of_an_id_without_a_reservation_after_an_interrupted_archival_is_already_submitted_and_a_different_one_is_refused(self) -> None:
        text = json.dumps(RECORD)
        for outcome, folder in ((Outcome("finished", 0), "done"), (Outcome("failed", 2, "an error"), "failed")):
            with self.subTest(outcome=outcome.kind):
                self.setUp()
                self.plant("")
                self.interrupted_archival(outcome)
                self.assertTrue((self.inbox / folder / f"{self.name}.reference").is_file())
                self.assertTrue((self.inbox / f"{self.name}.json").is_file())
                for label, record, reference in (("record", json.dumps({**RECORD, "speed": 120.0}), REFERENCE), ("reference", text, OTHER_REFERENCE)):
                    with self.subTest(different=label), self.assertRaises(SubmitError):
                        submit(self.inbox, record, reference)
                    self.assertFalse((self.inbox / "reserved").exists())
                self.assertEqual("already submitted", submit(self.inbox, text, REFERENCE))
                self.assertEqual(text, (self.inbox / "reserved" / self.name).read_text())
                self.assertEqual(REFERENCE, (self.inbox / "reserved" / f"{self.name}.reference").read_text())
                self.assertEqual("already submitted", submit(self.inbox, text, REFERENCE))
                serve_until_idle(self.inbox, self.inbox.parent, lambda path, beat: Outcome("finished", 0))
                self.assertTrue((self.inbox / folder / f"{self.name}.json").is_file())
                self.assertFalse((self.inbox / f"{self.name}.json").exists())

    def test_an_interrupted_archival_of_the_reviewers_result_archives_the_request_without_a_second_run_or_result(self) -> None:
        self.plant("")
        self.interrupted_archival(Outcome("failed", 3, gate_refusal_text(6389)))
        written = (self.inbox / "results" / f"{self.name}.json").read_bytes()
        self.assertLessEqual(len(written), serve_module.RESULT_LIMIT_BYTES)
        ran: list[str] = []

        def gate_again(path: Path, beat: Callable[[], None]) -> Outcome:
            ran.append(path.name)
            return Outcome("failed", 3, "the gate refused again")

        serve_until_idle(self.inbox, self.inbox.parent, gate_again)
        self.assertEqual([], ran)
        self.assertTrue((self.inbox / "failed" / f"{self.name}.json").is_file())
        self.assertFalse((self.inbox / f"{self.name}.json").exists())
        self.assertEqual(written, (self.inbox / "results" / f"{self.name}.json").read_bytes())
        self.assertFalse((self.inbox / "results" / f"{self.name}.json.invalid").exists())

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
