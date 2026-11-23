"""What the Bronze decode job reports and refuses at its command line: the document of a check or a publication, the text of a stop and the argument rules.

The Spark and Great Expectations sides are the acceptance run's. Here a fake Nessie and a fake checker stand in, so each case names the document that the job
would print and the exit code that follows from it.
"""

from __future__ import annotations

import json
import unittest

from spark.bronze.checks import CheckResult, plan, verdict
from spark.bronze.commands import branch_report, command_problem, exit_code, failed_text
from tests.fast.test_bronze_publish import BASE, BASE_HEAD, BRANCH, HEAD, KEYS, MERGED, Checker, FakeNessie, failing


class ReportTest(unittest.TestCase):
    def test_a_verify_report_holds_the_checked_hash_and_the_checks_and_no_publication(self) -> None:
        document = branch_report(FakeNessie(), Checker(), BRANCH, BASE, KEYS, publishing=False)
        self.assertEqual((HEAD, True), (document["checked_hash"], document["passed"]))
        self.assertNotIn("publication", document)
        self.assertEqual(len(plan()), len(document["checks"]["checks"]))
        self.assertEqual(0, exit_code(document))

    def test_a_publish_report_names_the_merge_and_the_snapshot_of_each_table_on_the_base(self) -> None:
        nessie = FakeNessie()
        document = branch_report(nessie, Checker(), BRANCH, BASE, KEYS, publishing=True)
        self.assertEqual(
            {"status": "merged", "base": BASE, "base_hash_before": BASE_HEAD, "merged_hash": MERGED, "snapshots": {KEYS[0]: 10, KEYS[1]: 11}, "blocked_by": []},
            document["publication"],
        )
        self.assertEqual(0, exit_code(document))
        json.dumps(document)

    def test_a_failed_check_is_a_report_that_stops_the_job_and_names_the_check_and_its_guard(self) -> None:
        nessie = FakeNessie()
        document = branch_report(nessie, Checker(failing("numeric_observations", "case_id not null")), BRANCH, BASE, KEYS, publishing=True)
        self.assertEqual(
            ("blocked", None, ["key"]), (document["publication"]["status"], document["publication"]["merged_hash"], document["publication"]["blocked_by"])
        )
        self.assertEqual(1, exit_code(document))
        self.assertEqual("numeric_observations: case_id not null (key guard, 2 unexpected); key", failed_text(document))
        self.assertEqual([], nessie.merge_calls)

    def test_a_failed_verify_exits_with_one_and_publishes_nothing(self) -> None:
        document = branch_report(FakeNessie(), Checker(failing("tracks", "kind in set")), BRANCH, BASE, KEYS, publishing=False)
        self.assertEqual((False, 1), (document["passed"], exit_code(document)))
        self.assertIn("tracks: kind in set (structure guard", failed_text(document))

    def test_a_missing_check_result_is_reported_as_unexpected_minus_one(self) -> None:
        results = [CheckResult(check, True, 0) for check in plan() if check.name != "key unique"]
        document = branch_report(FakeNessie(), Checker(results), BRANCH, BASE, KEYS, publishing=False)
        self.assertEqual(1, exit_code(document))
        self.assertEqual(-1, document["checks"]["failed"][0]["unexpected"])
        self.assertTrue(verdict(plan(), results).failed)


class ArgumentTest(unittest.TestCase):
    def test_write_needs_a_manifest_reference(self) -> None:
        self.assertIn("--manifest-reference", command_problem("write", False, False, False, False) or "")
        self.assertIsNone(command_problem("write", True, False, False, False))
        self.assertIsNone(command_problem("write", True, True, True, True))

    def test_verify_and_publish_take_only_the_branch_and_the_base(self) -> None:
        for command in ("verify", "publish"):
            self.assertIsNone(command_problem(command, False, False, False, False))
            for index in range(4):
                flags = [index == position for position in range(4)]
                self.assertIn("--branch and --base only", command_problem(command, *flags) or "", f"{command} {flags}")


if __name__ == "__main__":
    unittest.main()
