"""The retention rule: a run that ended more than 30 days ago goes, with its compute logs, and nothing else does.

The decision is pure, over the runs' status and end time, so each edge is a plain case: the exact boundary, a run that is not over, the order of the three
deletions and a folder that cannot be written. Real runs removed through Dagster's own API, with their event records and compute log files, are proven by the
acceptance test.
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from orchestration.surgical_risk.retention import RUN_RETENTION_DAYS, EndedRun, expired, parse_removal, prune, removal_message
from orchestration.surgical_risk.run_folder import RunFacts, build_record

DAY = 86400.0
NOW = 1_800_000_000.0


def run(name: str, status: str, age_days: float | None) -> EndedRun:
    return EndedRun(run_id=name, job_name="ping_job", status=status, ended=None if age_days is None else NOW - age_days * DAY)


class Store:
    def __init__(self, folders: Path | None = None, fail_folder: bool = False) -> None:
        self.calls: list[tuple[str, str]] = []
        self.folders = folders
        self.fail_folder = fail_folder

    def ensure_folder(self, run_id: str) -> None:
        self.calls.append(("folder", run_id))
        if self.fail_folder:
            raise OSError("the run folders are read-only")
        if self.folders is not None and not (self.folders / run_id).exists():
            (self.folders / run_id).mkdir()
            (self.folders / run_id / "record.json").write_text("{}\n")

    def delete_compute_logs(self, run_id: str) -> None:
        self.calls.append(("logs", run_id))

    def delete_run(self, run_id: str) -> None:
        self.calls.append(("run", run_id))


class ExpiredTest(unittest.TestCase):
    def test_the_setting_is_thirty_days(self) -> None:
        self.assertEqual(30, RUN_RETENTION_DAYS)

    def test_a_run_that_ended_exactly_thirty_days_ago_stays_and_one_second_older_goes(self) -> None:
        at = EndedRun("at", "SUCCESS", "ping_job", NOW - 30 * DAY)
        over = EndedRun("over", "SUCCESS", "ping_job", NOW - 30 * DAY - 1)
        self.assertEqual(["over"], [item.run_id for item in expired([at, over], NOW)])

    def test_a_run_that_is_not_over_stays_whatever_its_times_say(self) -> None:
        runs = [run(status.lower(), status, 400) for status in ("QUEUED", "NOT_STARTED", "STARTING", "STARTED", "CANCELING")] + [run("no_end", "SUCCESS", None)]
        self.assertEqual([], expired(runs, NOW))

    def test_every_end_state_goes_when_old(self) -> None:
        runs = [run(status.lower(), status, 31) for status in ("SUCCESS", "FAILURE", "CANCELED")]
        self.assertEqual(["success", "failure", "canceled"], [item.run_id for item in expired(runs, NOW)])

    def test_a_recent_run_stays(self) -> None:
        self.assertEqual([], expired([run("new", "FAILURE", 1)], NOW))


class PruneTest(unittest.TestCase):
    def test_each_expired_run_gets_its_folder_then_its_compute_logs_removed_then_the_run_removed_and_no_other_run_is_touched(self) -> None:
        store = Store()
        removed = prune([run("old", "SUCCESS", 40), run("new", "SUCCESS", 2), run("live", "STARTED", 90)], NOW, store)
        self.assertEqual(["old"], [item.run_id for item in removed])
        self.assertEqual([("folder", "old"), ("logs", "old"), ("run", "old")], store.calls)

    def test_a_run_whose_folder_cannot_be_written_is_not_removed_and_the_error_shows(self) -> None:
        store = Store(fail_folder=True)
        with self.assertRaises(OSError):
            prune([run("old", "SUCCESS", 40)], NOW, store)
        self.assertEqual([("folder", "old")], store.calls)

    def test_a_second_pass_over_the_same_runs_removes_nothing_more(self) -> None:
        store = Store()
        self.assertEqual([], prune([], NOW, store))
        self.assertEqual([], store.calls)

    def test_the_run_folders_of_removed_runs_stay(self) -> None:
        with tempfile.TemporaryDirectory(prefix="retention_test_") as folder:
            folders = Path(folder)
            (folders / "kept").mkdir()
            (folders / "kept" / "record.json").write_text('{"run_id": "kept"}\n')
            prune([run("old", "FAILURE", 45), run("kept", "SUCCESS", 45)], NOW, Store(folders))
            self.assertEqual(["kept", "old"], sorted(path.name for path in folders.iterdir()))
            self.assertEqual('{"run_id": "kept"}\n', (folders / "kept" / "record.json").read_text())


class RecordTest(unittest.TestCase):
    def facts(self, removed: tuple[tuple[str, float], ...]) -> RunFacts:
        return RunFacts("r1", "prune_old_runs_job", "SUCCESS", {}, {}, None, None, NOW, NOW + 1, (), (), "", None, (), removed)

    def test_a_removal_message_gives_back_the_run_id_and_the_end_time_and_other_messages_give_nothing(self) -> None:
        old = EndedRun("6b1d2c3e-0000-4000-8000-000000000001", "SUCCESS", "ping_job", NOW - 40 * DAY + 0.25)
        self.assertEqual((old.run_id, old.ended), parse_removal(removal_message(old)))
        self.assertIsNone(parse_removal("ping"))
        self.assertIsNone(parse_removal("removed run without an end time"))

    def test_the_run_folder_of_the_pruning_run_lists_each_removal_with_its_run_id_and_end_time(self) -> None:
        record = build_record(self.facts((("old", NOW - 40 * DAY),)), commit="abc", environment={}, recorded=datetime.fromtimestamp(NOW, UTC))
        self.assertEqual([{"run_id": "old", "ended_utc": "2026-12-06T08:00:00Z"}], record["removed_runs"])

    def test_a_run_that_removed_nothing_has_an_empty_list(self) -> None:
        self.assertEqual([], build_record(self.facts(()), commit="abc", environment={}, recorded=datetime.fromtimestamp(NOW, UTC))["removed_runs"])


if __name__ == "__main__":
    unittest.main()
