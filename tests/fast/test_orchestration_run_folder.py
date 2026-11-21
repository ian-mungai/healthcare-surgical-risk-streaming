"""The run folder of a Dagster run: the record's fields, what it never holds and the one folder a run gets.

A real run through the real entry point, with a secret planted in its configuration and in its error text, is proven by the acceptance test. These tests cover
the edges that a real run cannot readily produce: every secret-looking key form, a secret inside a nested list, a second write of the same run and a run that
reported nothing about what it left running.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from orchestration.surgical_risk.run_folder import (
    LEFT_RUNNING_TAG,
    REDACTED,
    CheckFacts,
    RunFacts,
    StepFacts,
    build_record,
    left_running_tag_value,
    write_record,
)

NOW = datetime(2026, 11, 21, 12, 0, 30, tzinfo=UTC)
START = datetime(2026, 11, 21, 12, 0, 0, tzinfo=UTC).timestamp()
MARKER = "marker-7f3a91c2"
ENVIRONMENT = {"dagster": "1.13.31", "python": "3.14.0"}


def facts(**changes: object) -> RunFacts:
    base: dict[str, object] = {
        "run_id": "6b1d2c3e-0000-4000-8000-000000000001",
        "job_name": "ping_job",
        "status": "SUCCESS",
        "run_config": {"ops": {"ping": {"config": {"fail": False}}}},
        "tags": {"dagster/schedule_name": "ping_every_minute"},
        "enqueued": START - 5,
        "launched": START - 1,
        "started": START,
        "ended": START + 12.5,
        "steps": (StepFacts("ping", "SUCCESS", START + 1, START + 12),),
        "checks": (),
        "error": "",
        "parent_run_id": None,
        "asset_selection": (),
    }
    return RunFacts(**{**base, **changes})  # type: ignore[arg-type]


class RecordFieldsTest(unittest.TestCase):
    def test_a_passing_run_records_its_configuration_commit_timings_and_outcome(self) -> None:
        record = build_record(facts(), commit="0123abc", environment=ENVIRONMENT, recorded=NOW)
        self.assertEqual("pass", record["outcome"])
        self.assertEqual("SUCCESS", record["status"])
        self.assertEqual("0123abc", record["commit"])
        self.assertEqual({"ops": {"ping": {"config": {"fail": False}}}}, record["configuration"])
        self.assertEqual("2026-11-21T12:00:00Z", record["started_utc"])
        self.assertEqual("2026-11-21T12:00:12Z", record["ended_utc"])
        self.assertEqual(12.5, record["timings"]["run_seconds"])
        self.assertEqual(5.0, record["timings"]["queued_seconds"])
        self.assertEqual(ENVIRONMENT, record["environment"])

    def test_every_field_of_the_project_rule_is_present(self) -> None:
        record = build_record(facts(), commit="0123abc", environment=ENVIRONMENT, recorded=NOW)
        for field in ("started_utc", "environment", "commit", "commands", "reset", "inputs", "seeds", "assertions", "outputs", "limits", "cleanup", "rerun"):
            self.assertIn(field, record)
        self.assertIn("ping_job", " ".join(record["commands"]))
        self.assertIn("ping_job", record["rerun"])

    def test_a_failed_run_records_the_failed_step_and_its_error(self) -> None:
        failed = facts(status="FAILURE", steps=(StepFacts("ping", "FAILURE", START + 1, START + 2),), error="RuntimeError: ping was asked to fail")
        record = build_record(failed, commit="0123abc", environment=ENVIRONMENT, recorded=NOW)
        self.assertEqual("fail", record["outcome"])
        self.assertEqual([False], [item["passed"] for item in record["assertions"]])
        self.assertEqual("RuntimeError: ping was asked to fail", record["outputs"]["error"])

    def test_a_check_that_failed_makes_the_run_record_a_failed_assertion_even_when_the_run_succeeded(self) -> None:
        record = build_record(facts(checks=(CheckFacts("result_has_rows", "lineage_result", False),)), commit="0123abc", environment=ENVIRONMENT, recorded=NOW)
        names = {item["name"]: item["passed"] for item in record["assertions"]}
        self.assertEqual({"step ping": True, "check result_has_rows on lineage_result": False}, names)

    def test_a_run_that_never_started_has_no_timings_and_a_canceled_run_is_not_a_pass(self) -> None:
        record = build_record(facts(status="CANCELED", started=None, ended=None, steps=()), commit="0123abc", environment=ENVIRONMENT, recorded=NOW)
        self.assertEqual("canceled", record["outcome"])
        self.assertIsNone(record["started_utc"])
        self.assertIsNone(record["timings"]["run_seconds"])

    def test_an_unset_commit_is_recorded_as_unknown_and_not_left_out(self) -> None:
        record = build_record(facts(), commit="", environment=ENVIRONMENT, recorded=NOW)
        self.assertEqual("unknown", record["commit"])


class LeftRunningTest(unittest.TestCase):
    def test_a_run_that_reported_nothing_says_so_and_does_not_claim_that_nothing_is_left(self) -> None:
        record = build_record(facts(), commit="0123abc", environment=ENVIRONMENT, recorded=NOW)
        self.assertEqual([], record["left_running"])
        self.assertFalse(record["left_running_reported"])
        self.assertIn("reported nothing", record["cleanup"])

    def test_a_run_that_reported_services_names_them(self) -> None:
        tagged = facts(status="FAILURE", tags={LEFT_RUNNING_TAG: left_running_tag_value(["spark_worker", "spark_master"])})
        record = build_record(tagged, commit="0123abc", environment=ENVIRONMENT, recorded=NOW)
        self.assertEqual(["spark_worker", "spark_master"], record["left_running"])
        self.assertTrue(record["left_running_reported"])
        self.assertIn("spark_worker", record["cleanup"])

    def test_a_run_that_reported_an_empty_list_says_that_nothing_is_left(self) -> None:
        record = build_record(facts(tags={LEFT_RUNNING_TAG: left_running_tag_value([])}), commit="0123abc", environment=ENVIRONMENT, recorded=NOW)
        self.assertEqual([], record["left_running"])
        self.assertTrue(record["left_running_reported"])
        self.assertIn("nothing", record["cleanup"])


class SecretTest(unittest.TestCase):
    def serialised(self, **changes: object) -> str:
        return json.dumps(build_record(facts(**changes), commit="0123abc", environment=ENVIRONMENT, recorded=NOW))

    def test_a_secret_under_a_secret_looking_key_is_replaced_and_the_other_values_stay(self) -> None:
        config = {"ops": {"load": {"config": {"api_token": MARKER, "rows": 3, "fail": False}}}}
        record = build_record(facts(run_config=config), commit="0123abc", environment=ENVIRONMENT, recorded=NOW)
        loaded = record["configuration"]["ops"]["load"]["config"]
        self.assertEqual({"api_token": REDACTED, "rows": 3, "fail": False}, loaded)

    def test_every_secret_looking_key_form_is_replaced_at_any_depth_and_inside_lists(self) -> None:
        names = ("password", "db_passwd", "SECRET", "apiToken", "access-key", "unlock_key", "private_key", "credentials", "Authorization")
        for name in names:
            with self.subTest(name=name):
                config = {"outer": [{"inner": {name: MARKER}}]}
                self.assertNotIn(MARKER, self.serialised(run_config=config))

    def test_a_secret_value_is_removed_from_the_error_text_and_the_tags_too(self) -> None:
        config = {"ops": {"load": {"config": {"password": MARKER}}}}
        text = self.serialised(run_config=config, error=f"RuntimeError: login failed with {MARKER}", tags={"note": f"used {MARKER}"})
        self.assertNotIn(MARKER, text)
        self.assertIn("login failed with " + REDACTED, text)

    def test_a_secret_looking_tag_key_is_replaced(self) -> None:
        self.assertNotIn(MARKER, self.serialised(tags={"deploy_token": MARKER}))

    def test_an_error_text_is_cut_to_a_bounded_length(self) -> None:
        record = build_record(facts(status="FAILURE", error="x" * 5000), commit="0123abc", environment=ENVIRONMENT, recorded=NOW)
        self.assertLessEqual(len(record["outputs"]["error"]), 1000)


class WriteTest(unittest.TestCase):
    def setUp(self) -> None:
        self.runs = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="run_folder_test_")))

    def test_a_run_gets_one_folder_named_for_its_start_job_and_id_with_its_record_and_a_second_write_changes_nothing(self) -> None:
        record = build_record(facts(), commit="0123abc", environment=ENVIRONMENT, recorded=NOW)
        first = write_record(self.runs, record)
        self.assertEqual("20261121T120000Z_ping_job_6b1d2c3e-0000-4000-8000-000000000001", first.name)
        written = (first / "record.json").read_text()
        later = build_record(facts(), commit="fedcba9", environment=ENVIRONMENT, recorded=NOW)
        second = write_record(self.runs, later)
        self.assertEqual(first, second)
        self.assertEqual([first], list(self.runs.iterdir()))
        self.assertEqual(written, (first / "record.json").read_text())

    def test_a_run_that_never_started_is_named_for_its_recording_time_and_still_gets_one_folder_when_recorded_again_later(self) -> None:
        never = facts(status="CANCELED", started=None, ended=None, steps=())
        first = write_record(self.runs, build_record(never, commit="0123abc", environment=ENVIRONMENT, recorded=NOW))
        self.assertTrue(first.name.startswith("20261121T120030Z_ping_job_"))
        later = build_record(never, commit="0123abc", environment=ENVIRONMENT, recorded=datetime(2026, 11, 21, 13, 0, 0, tzinfo=UTC))
        self.assertEqual(first, write_record(self.runs, later))
        self.assertEqual(1, len(list(self.runs.iterdir())))

    def test_no_partial_folder_is_left_when_the_record_cannot_be_written(self) -> None:
        record = build_record(facts(), commit="0123abc", environment=ENVIRONMENT, recorded=NOW)
        record["outputs"] = {"bad": object()}
        with self.assertRaises(TypeError):
            write_record(self.runs, record)
        self.assertEqual([], list(self.runs.iterdir()))


if __name__ == "__main__":
    unittest.main()
