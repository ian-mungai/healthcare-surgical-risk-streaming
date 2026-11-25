"""The command line of the table maintenance job: every refusal is one fixed code, and no text of an error, an argument or Spark reaches a stream.

No Spark runs here.

Failure modes covered here:

- A tagged read that fails forwards the exception name and the first line of Spark's message, which can name a path or a value.
- A refusal prints the text of its error (a table, a branch, a tag, a difference between two reads) or Nessie's own message.
- A bad argument is echoed by the argument parser, and an exception that nobody mapped prints a traceback.
"""

from __future__ import annotations

import contextlib
import io
import json
import re
import unittest
from typing import Any
from unittest import mock

from scripts.stack.nessie import NessieError
from scripts.stack.tags import TableSnapshot
from spark.jobs import table_maintenance as job
from spark.maintenance import RetentionError
from spark.settings import SettingsError
from tests.fast import test_table_maintenance as base
from tests.fast.internal_line_support import assert_internal

MARKER = "MARKER_6633"
CODES = re.compile(r"^table_maintenance: [a-z_]+\n$")


def main(argv: list[str], error: Exception | None = None, result: dict[str, Any] | None = None) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    code = 0
    with (
        contextlib.redirect_stdout(out),
        contextlib.redirect_stderr(err),
        mock.patch.object(job, "start_deadline"),
        mock.patch.object(job, "maintain", side_effect=error, return_value=result),
    ):
        try:
            code = job.main(argv)
        except SystemExit as stop:
            code = int(stop.code or 0)
    return code, out.getvalue(), err.getvalue()


class Endings(unittest.TestCase):
    def test_a_clean_run_prints_one_result_line(self) -> None:
        code, out, err = main([], result={"trunk": "main"})
        self.assertEqual((0, ""), (code, err))
        self.assertEqual({"trunk": "main"}, json.loads(out.removeprefix("RESULT ")))

    def test_every_refusal_is_one_fixed_code_and_the_text_of_the_error_is_in_no_output(self) -> None:
        endings: tuple[tuple[Exception, int, str], ...] = (
            (RetentionError(f"{MARKER} unsafe", code="unsafe_table"), 1, "unsafe_table"),
            (RetentionError(f"{MARKER} plain"), 1, "refused"),
            (NessieError(f"{MARKER} down", status=500), 1, "catalog_error"),
            (SettingsError(MARKER), 2, "settings"),
            (RuntimeError(MARKER), 70, "internal_error"),
            (KeyError(MARKER), 70, "internal_error"),
        )
        for error, status, expected in endings:
            with self.subTest(expected=expected, error=type(error).__name__):
                code, out, err = main([], error)
                if expected == "internal_error":
                    self.assertEqual((status, ""), (code, out))
                    assert_internal(self, err, "table_maintenance", "table_maintenance: internal_error", (f"builtins.{type(error).__name__}",), MARKER)
                else:
                    self.assertEqual((status, "", f"table_maintenance: {expected}\n"), (code, out, err))

    def test_a_bad_argument_is_usage_and_is_not_echoed(self) -> None:
        for argv in (["--table", f"a.{MARKER}!"], ["--table", f"{MARKER}"], ["--deadline-seconds", MARKER], [f"--{MARKER}"], ["--table"], ["--trunk"]):
            with self.subTest(argv=argv):
                self.assertEqual((2, "", "table_maintenance: usage\n"), main(argv))


class Codes(unittest.TestCase):
    """Each refusal that the job makes has its own code."""

    def refusal(self, call: Any) -> RetentionError:
        with self.assertRaises(RetentionError) as caught:
            call()
        return caught.exception

    def maintain(self, properties: dict[str, dict[str, str | None]], argv: list[str], side: bool = False) -> RetentionError:
        return self.refusal(lambda: base.Rewrites(properties).run(argv, base.stand_in(side=side)))

    def test_each_refusal_of_maintain_has_its_code(self) -> None:
        safe, unsafe = {"a.one": base.SAFE, "b.two": base.SAFE}, {"a.one": base.SAFE, "b.two": base.UNSAFE_GC}
        self.assertEqual("unsafe_table", self.maintain(unsafe, []).code)
        self.assertEqual("tables_not_on_trunk", self.maintain(safe, ["--table", "c.three"]).code)
        self.assertEqual("trunk_not_default", self.maintain(safe, ["--trunk", "side"], side=True).code)
        self.assertEqual("branches_in_flight", self.maintain(safe, [], side=True).code)

    def test_a_tagged_read_that_fails_has_a_code_and_its_message_holds_no_text_of_spark(self) -> None:
        tagged: dict[str, tuple[TableSnapshot, ...]] = {"run-a": (TableSnapshot("x.t", 5, "file:/w/m.json"),)}
        with mock.patch.object(job, "iceberg_table", side_effect=RuntimeError(f"Metadata not found {MARKER} s3://bucket/key")):
            error = self.refusal(lambda: job.read_tagged(mock.Mock(), tagged, {"run-a": "pin0"}))
        self.assertEqual("tag_unreadable", error.code)
        self.assertNotIn(MARKER, str(error))
        self.assertNotIn("RuntimeError", str(error))

    def test_a_tag_that_reads_at_another_snapshot_has_a_code(self) -> None:
        current = mock.Mock()
        current.snapshotId.return_value = 8
        table = mock.Mock()
        table.currentSnapshot.return_value = current
        session = mock.Mock()
        session.table.return_value.selectExpr.return_value.collect.return_value = [{"rows": 1, "checksum": 1}]
        tagged: dict[str, tuple[TableSnapshot, ...]] = {"run-a": (TableSnapshot("x.t", 5, "file:/w/m.json"),)}
        with mock.patch.object(job, "iceberg_table", return_value=table):
            error = self.refusal(lambda: job.read_tagged(session, tagged, {"run-a": "pin0"}))
        self.assertEqual("tag_snapshot_mismatch", error.code)

    def test_a_tag_that_does_not_read_as_it_did_has_a_code(self) -> None:
        nessie = base.stand_in()
        rewrites = base.Rewrites({"a.one": base.SAFE, "b.two": base.SAFE})
        reads = iter(
            [
                [{"tag": "t", "table": "a.one", "snapshot_id": 1, "rows": 1, "checksum": 1}],
                [{"tag": "t", "table": "a.one", "snapshot_id": 2, "rows": 1, "checksum": 1}],
            ]
        )
        with mock.patch.object(job, "read_tagged", side_effect=lambda *_: next(reads)):
            error = self.refusal(lambda: rewrites.run([], nessie))
        self.assertEqual("tag_changed", error.code)


if __name__ == "__main__":
    unittest.main()
