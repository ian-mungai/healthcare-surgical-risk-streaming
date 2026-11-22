"""The replay's always-on service: it waits for a run record, runs it, returns to waiting and keeps its health file fresh.

The loop is tested here with a stand-in for one replay run, because a real run needs Kafka, the registry and the decoder, which only the replay image
holds. Isolation covers what a real run cannot produce on demand: a run that stops on a signal, a run that fails, a record that cannot be read and a loop
that stops making progress. The replay image, its real run and its Compose health check are proved by the acceptance test of the joined stack.

Failure modes, each with its case: an empty inbox (waits, health fresh, no run), a malformed record (moved to failed, serving continues), a failed run (moved to
failed with its exit code, serving continues), a stop while waiting (returns at once), a stop during a run (the record stays in the inbox for the restart, which
resumes it), two records (run one at a time in name order), a stale, future, infinite or not-a-number health stamp (unhealthy), a record submitted twice (one
record), a record that differs from the one already submitted under its run ID (refused, the first kept, also when two submits race and after the run was
archived), a record whose timing field is not a finite number from 0 to 86,400 (refused at submit, and failed with its reason when planted in the queue) and a
restart after a result was written but before the record was archived (the result stays, the record is archived, nothing runs again).
"""

from __future__ import annotations

import json
import tempfile
import threading
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any

from replay.service.serve import Outcome, SubmitError, healthy, queued_record, serve, submit
from scripts.ingestion.manifest import ManifestRef

REFERENCE = ManifestRef("bucket", "manifests/replay/" + "a" * 64 + ".json", "version_1", "b" * 64).to_json()
OTHER_REFERENCE = ManifestRef("bucket", "manifests/replay/" + "c" * 64 + ".json", "version_2", "d" * 64).to_json()
BAD_TIMES = (1e308, float("inf"), float("-inf"), float("nan"), -1, 86_401, 10**400)
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
        planted += [json.dumps({**RECORD, "speed": value}) for value in (0, -5, float("inf"), float("nan"), 10**400)]
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

    def test_a_result_that_is_not_a_terminal_one_does_not_keep_the_record_from_running(self) -> None:
        for text in ("not json", "[]", json.dumps({"outcome": "stopped"}), json.dumps({"outcome": "running"}), json.dumps({"request": "a"})):
            with self.subTest(text=text):
                loop = self.loop({"a.json": Outcome("finished", 0)})
                loop.put("a.json", json.dumps(RECORD))
                (loop.inbox / "results").mkdir()
                (loop.inbox / "results" / "a.json").write_text(text)
                loop.serve()
                self.assertEqual(["a.json"], loop.ran)
                self.assertEqual("finished", json.loads((loop.inbox / "results" / "a.json").read_text())["outcome"])

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
        self.assertEqual([f"{self.name}.json", f"{self.name}.reference", f"reserved/{self.name}", f"reserved/{self.name}.reference"], self.files())
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


if __name__ == "__main__":
    unittest.main()
