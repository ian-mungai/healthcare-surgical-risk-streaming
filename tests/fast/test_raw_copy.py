"""The driver of the raw copy's sync, which the live batch of phase 4 runs inside the code location container on the host.

Failure modes covered here, each against a stand-in of Airbyte's API (the real API is proven on the host):

- A sync starts on a connection that selects more or other files than the fixed sample (the full release on a dev host, a missing file, a repeated file),
  which would copy the full release or an incomplete sample.
- A sync starts on a connection whose destination is another bucket or another prefix than the project's landing prefix, which would copy into the wrong place.
- A sync starts while a job of the connection is active, which would make two copies at once.
- A cancel, a wait that cancels or a cancel of everything owned acts on a job that this tool did not start (another connection's, or one that a person started).
- A job's success that is read after its deadline passes for a success in time.
- A job that passes its deadline is left running, or a job whose cancellation is not confirmed is reported as stopped.
- The job record shows more than the closed set of facts, or a field of another type passes for a count.
- A command writes a glob, an ID of Airbyte, a credential or the text of an exception.
"""

from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.ingestion import raw_copy
from scripts.ingestion.airbyte_api import AirbyteError
from scripts.ingestion.connection import CONNECTION_NAME, fixed_sample_selection
from scripts.ingestion.landing import LANDING_PREFIX
from scripts.ingestion.raw_copy import HttpRawCopyApi, JobLedger, RawConnection, RawCopyError, cancel, cancel_owned, job_record, start, wait
from tests.fast.record_support import FakeClock

WANTED = tuple(sorted(fixed_sample_selection().source_keys))
CONNECTION = "connection_1"
BUCKET = "example-bucket"
LANDING_PATH = LANDING_PREFIX.rstrip("/")


def ledger_in(test: unittest.TestCase) -> JobLedger:
    folder = tempfile.TemporaryDirectory()
    test.addCleanup(folder.cleanup)
    return JobLedger(Path(folder.name))


class StandIn:
    """Airbyte's job calls for one connection, with a switch for each fault."""

    def __init__(self, globs: tuple[str, ...] = WANTED, *, active: tuple[int, ...] = (), bucket: str = BUCKET, path: str = LANDING_PATH) -> None:
        self.globs = globs
        self.bucket = bucket
        self.path = path
        self.job_connection_id = CONNECTION
        self.clock: Any = None
        self.read_takes = 0.0
        self.active = list(active)
        self.log: list[str] = []
        self.statuses: dict[int, list[str]] = {}
        self.started = 0
        self.cancel_stops = True
        self.status_error = False
        self.document: dict[str, Any] = {"jobId": 7, "status": "succeeded", "rowsSynced": 11, "bytesSynced": 123456, "connectionId": CONNECTION}

    def raw_connection(self) -> RawConnection:
        self.log.append("raw_connection")
        return RawConnection(CONNECTION, self.globs, self.bucket, self.path)

    def running_jobs(self, connection_id: str) -> list[int]:
        self.log.append(f"running_jobs {connection_id}")
        return list(self.active)

    def start_sync(self, connection_id: str) -> int:
        self.log.append(f"start_sync {connection_id}")
        self.started += 1
        return 7

    def job_connection(self, job: int, *, timeout: float | None = None) -> str:
        self.log.append(f"job_connection {job}")
        return self.job_connection_id

    def job_status(self, job: int, *, timeout: float | None = None) -> str:
        self.log.append(f"job_status {job}")
        if self.clock is not None:
            self.clock.now += self.read_takes
        if self.status_error:
            raise AirbyteError("GET jobs/7 did not finish within its deadline")
        queue = self.statuses.get(job, ["running"])
        return queue.pop(0) if len(queue) > 1 else queue[0]

    def cancel_job(self, job: int, *, timeout: float | None = None) -> None:
        self.log.append(f"cancel_job {job}")
        if self.cancel_stops:
            self.statuses[job] = ["cancelled"]

    def job_document(self, job: int, *, timeout: float | None = None) -> dict[str, Any]:
        self.log.append(f"job_document {job}")
        return self.document


class StartTest(unittest.TestCase):
    def start(self, api: StandIn, ledger: JobLedger | None = None) -> int:
        return start(api, WANTED, ledger or ledger_in(self), BUCKET)

    def test_a_connection_that_selects_exactly_the_fixed_sample_starts_one_sync_and_returns_its_job(self) -> None:
        api = StandIn()
        self.assertEqual(7, self.start(api))
        self.assertEqual(["raw_connection", f"running_jobs {CONNECTION}", f"start_sync {CONNECTION}"], api.log)

    def test_the_job_and_its_connection_are_kept_from_the_start_answer_before_the_job_is_returned(self) -> None:
        ledger = ledger_in(self)
        self.assertEqual(7, self.start(StandIn(), ledger))
        self.assertEqual(CONNECTION, ledger.connection_of(7))
        self.assertEqual([7], ledger.jobs())
        self.assertIsNone(ledger.connection_of(8))

    def test_a_job_that_cannot_be_kept_is_cancelled_at_once_and_the_start_is_refused(self) -> None:
        api = StandIn()
        ledger = ledger_in(self)
        with mock.patch.object(ledger, "record", side_effect=OSError("disk full")), self.assertRaises(RawCopyError) as caught:
            self.start(api, ledger)
        self.assertEqual("state_unwritable", caught.exception.error_code)
        self.assertIn("cancel_job 7", api.log)

    def test_the_order_of_the_globs_is_not_a_difference(self) -> None:
        api = StandIn(tuple(reversed(WANTED)))
        self.assertEqual(7, self.start(api))

    def test_a_connection_that_selects_the_full_release_is_refused_before_any_job_call(self) -> None:
        api = StandIn((*WANTED, "vital_files/0001.vital"))
        with self.assertRaises(RawCopyError) as caught:
            self.start(api)
        self.assertEqual("selection_differs", caught.exception.error_code)
        self.assertEqual(0, api.started)
        self.assertEqual(["raw_connection"], api.log)

    def test_a_connection_that_lacks_one_file_of_the_sample_is_refused(self) -> None:
        api = StandIn(WANTED[1:])
        with self.assertRaises(RawCopyError) as caught:
            self.start(api)
        self.assertEqual("selection_differs", caught.exception.error_code)
        self.assertEqual(0, api.started)

    def test_a_connection_whose_list_holds_the_whole_sample_and_one_file_twice_is_refused(self) -> None:
        api = StandIn((*WANTED, WANTED[0]))
        with self.assertRaises(RawCopyError) as caught:
            self.start(api)
        self.assertEqual("selection_differs", caught.exception.error_code)
        self.assertEqual(0, api.started)

    def test_a_destination_in_another_bucket_or_under_another_prefix_is_refused_before_any_job_call(self) -> None:
        for bucket, path in (("another-bucket", LANDING_PATH), (BUCKET, "other"), (BUCKET, f"{LANDING_PATH}/sub"), (BUCKET, "")):
            with self.subTest(bucket=bucket, path=path):
                api = StandIn(bucket=bucket, path=path)
                with self.assertRaises(RawCopyError) as caught:
                    self.start(api)
                self.assertEqual("destination_differs", caught.exception.error_code)
                self.assertEqual(0, api.started)
                self.assertEqual(["raw_connection"], api.log)

    def test_an_active_job_of_the_connection_stops_the_start_and_names_no_second_sync(self) -> None:
        api = StandIn(active=(3,))
        with self.assertRaises(RawCopyError) as caught:
            self.start(api)
        self.assertEqual("job_active", caught.exception.error_code)
        self.assertEqual(0, api.started)


class WaitTest(unittest.TestCase):
    def wait(self, api: StandIn, deadline: float = 100.0, *, owned: bool = True, clock: FakeClock | None = None) -> tuple[str, FakeClock]:
        clock = clock or FakeClock()
        ledger = ledger_in(self)
        if owned:
            ledger.record(7, CONNECTION)
        return wait(api, 7, ledger, deadline_seconds=deadline, poll_seconds=5.0, cancel_confirm_seconds=30.0, clock=clock.time, sleep=clock.sleep), clock

    def test_a_job_that_ends_in_time_gives_its_terminal_status_and_is_not_cancelled(self) -> None:
        api = StandIn()
        api.statuses[7] = ["running", "running", "succeeded"]
        status, clock = self.wait(api)
        self.assertEqual("succeeded", status)
        self.assertEqual(2, len(clock.sleeps))
        self.assertNotIn("cancel_job 7", api.log)

    def test_a_failed_job_is_returned_as_failed_and_not_as_an_error(self) -> None:
        api = StandIn()
        api.statuses[7] = ["running", "failed"]
        self.assertEqual("failed", self.wait(api)[0])

    def test_a_job_that_passes_its_deadline_is_cancelled_confirmed_and_reported_as_a_deadline(self) -> None:
        api = StandIn()
        with self.assertRaises(RawCopyError) as caught:
            self.wait(api, deadline=20.0)
        self.assertEqual("deadline", caught.exception.error_code)
        self.assertEqual(1, api.log.count("cancel_job 7"))
        self.assertEqual(["cancelled"], api.statuses[7])

    def test_a_success_that_is_read_after_the_deadline_is_a_deadline_and_not_a_success(self) -> None:
        clock = FakeClock()
        api = StandIn()
        api.clock, api.read_takes = clock, 60.0
        api.statuses[7] = ["succeeded"]
        with self.assertRaises(RawCopyError) as caught:
            self.wait(api, deadline=20.0, clock=clock)
        self.assertEqual("deadline", caught.exception.error_code)

    def test_a_job_that_a_cancellation_does_not_stop_is_reported_as_still_active(self) -> None:
        api = StandIn()
        api.cancel_stops = False
        with self.assertRaises(RawCopyError) as caught:
            self.wait(api, deadline=20.0)
        self.assertEqual("job_still_active", caught.exception.error_code)

    def test_a_status_that_cannot_be_read_inside_the_budget_cancels_and_is_not_taken_for_a_stop(self) -> None:
        api = StandIn()
        api.status_error = True
        with self.assertRaises(RawCopyError) as caught:
            self.wait(api)
        self.assertEqual("job_still_active", caught.exception.error_code)
        self.assertIn("cancel_job 7", api.log)

    def test_a_job_that_this_tool_did_not_start_is_never_waited_for_or_cancelled(self) -> None:
        api = StandIn()
        with self.assertRaises(RawCopyError) as caught:
            self.wait(api, deadline=20.0, owned=False)
        self.assertEqual("job_not_owned", caught.exception.error_code)
        self.assertEqual([], [line for line in api.log if line.startswith(("cancel_job", "job_status"))])


class CancelTest(unittest.TestCase):
    def cancel(self, api: StandIn, ledger: JobLedger | None = None) -> str:
        clock = FakeClock()
        if ledger is None:
            ledger = ledger_in(self)
            ledger.record(7, CONNECTION)
        return cancel(api, 7, ledger, confirm_seconds=30.0, poll_seconds=5.0, clock=clock.time, sleep=clock.sleep)

    def test_a_running_job_is_cancelled_and_its_terminal_status_is_returned(self) -> None:
        api = StandIn()
        self.assertEqual("cancelled", self.cancel(api))
        self.assertEqual(["raw_connection", "job_connection 7", "job_status 7", "cancel_job 7"], api.log[:4])

    def test_a_job_that_ended_before_the_cancellation_keeps_its_own_status(self) -> None:
        api = StandIn()
        api.statuses[7] = ["succeeded"]
        self.assertEqual("succeeded", self.cancel(api))
        self.assertNotIn("cancel_job 7", api.log)

    def test_a_job_that_stays_active_after_the_cancellation_is_reported(self) -> None:
        api = StandIn()
        api.cancel_stops = False
        with self.assertRaises(RawCopyError) as caught:
            self.cancel(api)
        self.assertEqual("job_still_active", caught.exception.error_code)

    def test_a_job_that_is_not_in_the_ledger_is_refused_and_nothing_is_cancelled(self) -> None:
        api = StandIn()
        with self.assertRaises(RawCopyError) as caught:
            self.cancel(api, ledger_in(self))
        self.assertEqual("job_not_owned", caught.exception.error_code)
        self.assertNotIn("cancel_job 7", api.log)

    def test_a_job_that_airbyte_says_belongs_to_another_connection_is_refused(self) -> None:
        api = StandIn()
        api.job_connection_id = "connection_2"
        with self.assertRaises(RawCopyError) as caught:
            self.cancel(api)
        self.assertEqual("job_not_owned", caught.exception.error_code)
        self.assertNotIn("cancel_job 7", api.log)

    def test_a_job_that_the_ledger_ties_to_another_connection_is_refused(self) -> None:
        api = StandIn()
        ledger = ledger_in(self)
        ledger.record(7, "connection_2")
        with self.assertRaises(RawCopyError) as caught:
            self.cancel(api, ledger)
        self.assertEqual("job_not_owned", caught.exception.error_code)
        self.assertNotIn("cancel_job 7", api.log)


class CancelOwnedTest(unittest.TestCase):
    def test_every_active_job_of_the_ledger_is_cancelled_and_an_ended_one_is_left_alone(self) -> None:
        api = StandIn()
        api.statuses[7] = ["running"]
        api.statuses[8] = ["succeeded"]
        ledger = ledger_in(self)
        ledger.record(7, CONNECTION)
        ledger.record(8, CONNECTION)
        clock = FakeClock()
        found = cancel_owned(api, ledger, confirm_seconds=30.0, poll_seconds=5.0, clock=clock.time, sleep=clock.sleep)
        self.assertEqual({"jobs": 2, "still_active": 0}, found)
        self.assertIn("cancel_job 7", api.log)
        self.assertNotIn("cancel_job 8", api.log)

    def test_a_job_that_stays_active_is_counted_and_never_hidden(self) -> None:
        api = StandIn()
        api.cancel_stops = False
        ledger = ledger_in(self)
        ledger.record(7, CONNECTION)
        clock = FakeClock()
        found = cancel_owned(api, ledger, confirm_seconds=30.0, poll_seconds=5.0, clock=clock.time, sleep=clock.sleep)
        self.assertEqual({"jobs": 1, "still_active": 1}, found)

    def test_an_empty_ledger_cancels_nothing(self) -> None:
        api = StandIn()
        self.assertEqual({"jobs": 0, "still_active": 0}, cancel_owned(api, ledger_in(self)))
        self.assertEqual([], api.log)

    def test_the_ledger_keeps_the_latest_jobs_only_and_survives_a_reopen(self) -> None:
        ledger = ledger_in(self)
        for job in range(1, 40):
            ledger.record(job, CONNECTION)
        self.assertEqual(list(range(20, 40)), ledger.jobs())
        self.assertEqual(CONNECTION, JobLedger(ledger.directory).connection_of(39))


class RecordTest(unittest.TestCase):
    def test_the_record_holds_exactly_the_closed_facts_and_nothing_else_of_airbyte_s_answer(self) -> None:
        api = StandIn()
        api.document = api.document | {"configuration": {"secret": "marker_value"}, "startTime": "2026-11-25T00:00:00Z", "duration": "PT2M"}
        self.assertEqual(
            {"jobId": 7, "status": "succeeded", "rowsSynced": 11, "bytesSynced": 123456, "duration": "PT2M"},
            job_record(api, 7),
        )

    def test_a_count_that_is_not_a_whole_number_is_refused(self) -> None:
        for value in ("11", 11.0, True, None, -1):
            with self.subTest(value=value):
                api = StandIn()
                api.document = api.document | {"rowsSynced": value}
                with self.assertRaises(RawCopyError) as caught:
                    job_record(api, 7)
                self.assertEqual("bad_job_record", caught.exception.error_code)

    def test_a_record_of_another_job_is_refused(self) -> None:
        api = StandIn()
        api.document = api.document | {"jobId": 8}
        with self.assertRaises(RawCopyError) as caught:
            job_record(api, 7)
        self.assertEqual("bad_job_record", caught.exception.error_code)


class Fake(HttpRawCopyApi):
    """The client with its two reads answered from memory."""

    def __init__(self, connections: list[dict[str, Any]], sources: dict[str, dict[str, Any]]) -> None:
        self.connections = connections
        self.sources = sources
        self.calls: list[str] = []

    def workspace_id(self) -> str:
        return "workspace_1"

    def listing(self, path: str) -> list[dict[str, Any]]:
        self.calls.append(path)
        return self.connections

    def call(self, method: str, path: str, body: Any = None, *, timeout: float | None = None) -> dict[str, Any]:
        self.calls.append(f"{method} {path}")
        if path not in self.sources:
            raise AirbyteError(f"{method} {path} was refused with status 404")
        return self.sources[path]


def source_document(globs: list[Any]) -> dict[str, Any]:
    return {"sourceId": "source_1", "configuration": {"streams": [{"name": "vitaldb_files", "globs": globs}]}}


def destination_document(bucket: Any = BUCKET, path: Any = LANDING_PATH) -> dict[str, Any]:
    return {"destinationId": "destination_1", "configuration": {"s3_bucket_name": bucket, "s3_bucket_path": path}}


class ConnectionReadTest(unittest.TestCase):
    def api(self, connections: list[dict[str, Any]] | None = None, globs: list[Any] | None = None) -> Fake:
        found = (
            connections
            if connections is not None
            else [{"name": CONNECTION_NAME, "connectionId": CONNECTION, "sourceId": "source_1", "destinationId": "destination_1"}]
        )
        return Fake(
            found, {"/sources/source_1": source_document(list(WANTED) if globs is None else globs), "/destinations/destination_1": destination_document()}
        )

    def test_the_connection_is_found_by_its_name_and_its_globs_are_read_from_its_source(self) -> None:
        found = self.api(
            [
                {"name": "another", "connectionId": "x", "sourceId": "y", "destinationId": "z"},
                {"name": CONNECTION_NAME, "connectionId": CONNECTION, "sourceId": "source_1", "destinationId": "destination_1"},
            ]
        ).raw_connection()
        self.assertEqual(RawConnection(CONNECTION, WANTED, BUCKET, LANDING_PATH), found)

    def test_no_connection_of_the_name_and_two_connections_of_the_name_are_refused(self) -> None:
        for connections in ([], [{"name": CONNECTION_NAME, "connectionId": "a", "sourceId": "source_1", "destinationId": "destination_1"}] * 2):
            with self.subTest(count=len(connections)), self.assertRaises(AirbyteError):
                self.api(connections).raw_connection()

    def test_globs_of_another_shape_are_refused(self) -> None:
        for globs in ([1], [""], "vital_files/0001.vital"):
            with self.subTest(globs=globs), self.assertRaises(AirbyteError):
                self.api(globs=globs).raw_connection()  # type: ignore[arg-type]

    def test_a_source_with_two_streams_is_refused(self) -> None:
        api = self.api()
        api.sources["/sources/source_1"]["configuration"]["streams"].append({"name": "other", "globs": []})
        with self.assertRaises(AirbyteError):
            api.raw_connection()

    def test_a_destination_without_a_bucket_name_or_a_path_as_text_is_refused(self) -> None:
        for bucket, path in ((None, LANDING_PATH), (BUCKET, None), (7, LANDING_PATH), ("", "")):
            with self.subTest(bucket=bucket, path=path):
                api = self.api()
                api.sources["/destinations/destination_1"] = destination_document(bucket, path)
                if bucket == "" and path == "":
                    self.assertEqual(("", ""), (api.raw_connection().bucket, api.raw_connection().landing_path))
                else:
                    with self.assertRaises(AirbyteError):
                        api.raw_connection()

    def test_the_connection_of_a_job_is_read_from_the_job_route_and_only_a_sync_counts(self) -> None:
        api = Fake([], {"/jobs/7": {"jobId": 7, "status": "running", "connectionId": CONNECTION, "jobType": "sync"}})
        self.assertEqual(CONNECTION, api.job_connection(7))
        for document in (
            {"jobId": 7, "status": "running", "connectionId": CONNECTION, "jobType": "reset"},
            {"jobId": 8, "status": "running", "connectionId": CONNECTION, "jobType": "sync"},
            {"jobId": 7, "status": "running", "jobType": "sync"},
        ):
            with self.subTest(document=document):
                other = Fake([], {"/jobs/7": document})
                with self.assertRaises(AirbyteError):
                    other.job_connection(7)

    def test_the_job_document_is_read_from_the_job_route(self) -> None:
        api = Fake([], {"/jobs/7": {"jobId": 7, "status": "running"}})
        self.assertEqual({"jobId": 7, "status": "running"}, api.job_document(7))
        self.assertEqual(["GET /jobs/7"], api.calls)


class CommandTest(unittest.TestCase):
    def run_main(self, argv: list[str], api: StandIn | None = None, *, error: Exception | None = None, owned: tuple[int, ...] = ()) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        factory: Callable[[], Any] = (lambda: api or StandIn()) if error is None else mock.Mock(side_effect=error)
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        for job in owned:
            JobLedger(Path(folder.name)).record(job, CONNECTION)
        with (
            mock.patch.object(raw_copy, "open_api", factory),
            mock.patch.object(raw_copy, "open_ledger", lambda: JobLedger(Path(folder.name))),
            contextlib.redirect_stdout(out),
            contextlib.redirect_stderr(err),
        ):
            code = raw_copy.main(argv)
        self.last_ledger = JobLedger(Path(folder.name))
        return code, out.getvalue(), err.getvalue()

    def test_status_says_whether_the_selection_is_the_fixed_sample_and_how_many_jobs_are_active_and_writes_no_glob_or_id(self) -> None:
        code, out, err = self.run_main(["status", "--bucket", BUCKET], StandIn(active=(1, 2)))
        self.assertEqual(0, code)
        self.assertEqual({"selection_is_fixed_sample": True, "destination_is_project_landing": True, "files": len(WANTED), "active_jobs": 2}, json.loads(out))
        self.assertEqual("", err)
        self.assertNotIn(CONNECTION, out)

    def test_status_of_the_full_release_says_false_and_exits_0(self) -> None:
        code, out, _ = self.run_main(["status", "--bucket", BUCKET], StandIn((*WANTED, "vital_files/0001.vital")))
        self.assertEqual(0, code)
        self.assertEqual(False, json.loads(out)["selection_is_fixed_sample"])

    def test_status_of_another_destination_says_false_and_exits_0(self) -> None:
        code, out, _ = self.run_main(["status", "--bucket", BUCKET], StandIn(bucket="another-bucket"))
        self.assertEqual((0, False), (code, json.loads(out)["destination_is_project_landing"]))

    def test_start_prints_the_job_id_only(self) -> None:
        code, out, _ = self.run_main(["start", "--bucket", BUCKET])
        self.assertEqual((0, {"job": 7}), (code, json.loads(out)))
        self.assertEqual(CONNECTION, self.last_ledger.connection_of(7))

    def test_start_on_another_destination_exits_1_with_the_code_word_and_starts_nothing(self) -> None:
        api = StandIn(path="other")
        code, out, err = self.run_main(["start", "--bucket", BUCKET], api)
        self.assertEqual((1, "", "raw_copy refused: destination_differs\n"), (code, out, err))
        self.assertEqual(0, api.started)

    def test_start_on_the_full_release_exits_1_with_the_code_word_and_starts_nothing(self) -> None:
        api = StandIn((*WANTED, "vital_files/0001.vital"))
        code, out, err = self.run_main(["start", "--bucket", BUCKET], api)
        self.assertEqual((1, "", "raw_copy refused: selection_differs\n"), (code, out, err))
        self.assertEqual(0, api.started)

    def test_wait_prints_the_job_and_its_status_and_exits_1_for_a_failed_sync(self) -> None:
        api = StandIn()
        api.statuses[7] = ["failed"]
        code, out, _ = self.run_main(["wait", "--job", "7", "--deadline-seconds", "60"], api, owned=(7,))
        self.assertEqual((1, {"job": 7, "status": "failed"}), (code, json.loads(out)))

    def test_wait_exits_0_for_a_succeeded_sync(self) -> None:
        api = StandIn()
        api.statuses[7] = ["succeeded"]
        code, out, _ = self.run_main(["wait", "--job", "7", "--deadline-seconds", "60"], api, owned=(7,))
        self.assertEqual((0, {"job": 7, "status": "succeeded"}), (code, json.loads(out)))

    def test_wait_and_cancel_of_a_job_that_was_not_started_by_this_tool_exit_1_with_the_code_word(self) -> None:
        for command in (["wait", "--job", "7", "--deadline-seconds", "60"], ["cancel", "--job", "7"]):
            with self.subTest(command=command[0]):
                api = StandIn()
                code, out, err = self.run_main(command, api)
                self.assertEqual((1, "", "raw_copy refused: job_not_owned\n"), (code, out, err))
                self.assertNotIn("cancel_job 7", api.log)

    def test_cancel_owned_prints_two_counts_and_exits_1_when_a_job_stays_active(self) -> None:
        code, out, _ = self.run_main(["cancel-owned"], StandIn(), owned=(7,))
        self.assertEqual((0, {"jobs": 1, "still_active": 0}), (code, json.loads(out)))
        stuck = StandIn()
        stuck.cancel_stops = False
        code, out, _ = self.run_main(["cancel-owned", "--confirm-seconds", "1", "--poll-seconds", "1"], stuck, owned=(7,))
        self.assertEqual((1, {"jobs": 1, "still_active": 1}), (code, json.loads(out)))

    def test_cancel_prints_the_status_after_the_cancellation(self) -> None:
        code, out, _ = self.run_main(["cancel", "--job", "7"], owned=(7,))
        self.assertEqual((0, {"job": 7, "status": "cancelled"}), (code, json.loads(out)))

    def test_record_prints_the_closed_facts(self) -> None:
        code, out, _ = self.run_main(["record", "--job", "7"])
        self.assertEqual(0, code)
        self.assertEqual({"jobId", "status", "rowsSynced", "bytesSynced"}, set(json.loads(out)))

    def test_an_error_of_another_kind_shows_its_type_and_never_its_text(self) -> None:
        code, out, err = self.run_main(["status", "--bucket", BUCKET], error=AirbyteError("secret marker_5521 in the text"))
        self.assertEqual((1, ""), (code, out))
        self.assertEqual("AirbyteError\n", err)

    def test_a_job_number_that_is_not_a_whole_number_is_refused_by_the_parser_without_echoing_it(self) -> None:
        err = io.StringIO()
        with mock.patch.object(raw_copy, "open_api", lambda: StandIn()), contextlib.redirect_stderr(err), self.assertRaises(SystemExit) as caught:
            raw_copy.main(["wait", "--job", "marker_9", "--deadline-seconds", "60"])
        self.assertEqual(2, caught.exception.code)
        self.assertNotIn("marker_9", err.getvalue())


if __name__ == "__main__":
    unittest.main()
