"""The replay's always-on service: it waits for a run record, runs it, returns to waiting and keeps its health file fresh.

The loop is tested here with a stand-in for one replay run, because a real run needs Kafka, the registry and the decoder, which only the replay image
holds. Isolation covers what a real run cannot produce on demand: a run that stops on a signal, a run that fails, a record that cannot be read and a loop
that stops making progress. The replay image, its real run and its Compose health check are proved by the acceptance test of the joined stack.

Failure modes, each with its case: an empty inbox (waits, health fresh, no run), a malformed record (moved to failed, serving continues), a failed run (moved to
failed with its exit code, serving continues), a stop while waiting (returns at once), a stop during a run (the record stays in the inbox for the restart, which
resumes it), two records (run one at a time in name order), a stale health file (unhealthy), a record submitted twice (one record) and a record that differs
from the one already submitted under its run ID (refused, the first kept).
"""

from __future__ import annotations

import json
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any

from replay.service.serve import Outcome, SubmitError, healthy, serve, submit

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
        return serve(self.inbox, self.health, self.run_request, lambda: self.stop, self.clock, self.sleep, poll_seconds=2.0)

    def put(self, name: str, text: str) -> None:
        (self.inbox / name).write_text(text)


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
        loop.put("a.json", "{}")
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

    def test_a_stop_while_waiting_returns_at_once(self) -> None:
        loop = self.loop({}, sleeps_before_stop=1)
        self.assertEqual(0, loop.serve())
        self.assertEqual(1, loop.sleeps)

    def test_only_json_files_are_requests(self) -> None:
        loop = self.loop({})
        loop.put("a.json.writing", json.dumps(RECORD))
        loop.serve()
        self.assertEqual([], loop.ran)


class HealthTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="replay_health_")
        self.addCleanup(folder.cleanup)
        self.path = Path(folder.name) / "health.json"

    def write(self, status: str, updated: float) -> None:
        self.path.write_text(json.dumps({"status": status, "updated": updated, "request": "", "last": None}))

    def test_a_fresh_waiting_or_running_file_is_healthy(self) -> None:
        for status in ("waiting", "running"):
            with self.subTest(status=status):
                self.write(status, 1000.0)
                self.assertTrue(healthy(self.path, 1000.0 + 60, 60))

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

    def test_a_valid_record_lands_in_the_inbox_under_its_run_id_once(self) -> None:
        text = json.dumps(RECORD)
        self.assertEqual("submitted", submit(self.inbox, text))
        self.assertEqual("already submitted", submit(self.inbox, text))
        self.assertEqual(["replay_0a1b2c3d.json"], sorted(path.name for path in self.inbox.iterdir() if path.is_file()))
        self.assertEqual(text, (self.inbox / "replay_0a1b2c3d.json").read_text())

    def test_a_record_already_done_is_not_submitted_again(self) -> None:
        text = json.dumps(RECORD)
        (self.inbox / "done").mkdir(parents=True)
        (self.inbox / "done" / "replay_0a1b2c3d.json").write_text(text)
        self.assertEqual("already submitted", submit(self.inbox, text))
        self.assertFalse((self.inbox / "replay_0a1b2c3d.json").exists())

    def test_another_record_under_the_same_run_id_is_refused_and_the_first_kept(self) -> None:
        submit(self.inbox, json.dumps(RECORD))
        with self.assertRaises(SubmitError):
            submit(self.inbox, json.dumps({**RECORD, "speed": 120.0}))
        self.assertEqual(json.dumps(RECORD), (self.inbox / "replay_0a1b2c3d.json").read_text())

    def test_an_invalid_record_or_run_id_is_refused_and_nothing_lands(self) -> None:
        for text in ("not json", json.dumps({**RECORD, "rooms": "one"}), json.dumps({**RECORD, "run_id": "../escape"})):
            with self.subTest(text=text[:30]), self.assertRaises(SubmitError):
                submit(self.inbox, text)
        self.assertFalse(self.inbox.exists() and any(self.inbox.iterdir()))


if __name__ == "__main__":
    unittest.main()
