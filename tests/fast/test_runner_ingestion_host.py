"""The phase 4 host runner walks its cases in order, and each of its assertions fails for the fault it names.

Failure modes covered here, each against a stand-in of the host, of Airbyte, of Dagster, of Nessie and of S3 (the real ones are the lead's host run):

- The runner starts without ``INGESTION_HOST=1``, or goes on when the raw connection does not select exactly the fixed sample.
- A sync that cannot be interrupted (all files land first) passes for the interrupted copy.
- A second load of the same manifest makes a branch of its own and the idempotence assertion still passes.
- The planted load record is merged, or is left on the host, and the failed-check case still passes.
- A planted object stays in the bucket after the run, or a passing planted set is published as a manifest.
- The held-out rule: a table check's counts or values reach the evidence files (only check names and flags may).

The runner is untracked (``tests/e2e/ingestion_host.py``), so these tests run only where it is on disk. The walk of the happy path is also the proof that every
name, attribute and call of the runner's flow fits together, which no other test reaches before the host run.
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest import mock

from scripts.ingestion.connection import fixed_sample_selection
from scripts.ingestion.landing import LANDING_PREFIX
from scripts.ingestion.manifest import MANIFEST_PREFIX, ManifestRef
from scripts.ingestion.record_load import RefState
from tests.e2e.run_record import Record
from tests.fast.memory_store import MemoryStore

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "tests" / "e2e" / "ingestion_host.py"
SAMPLE = sorted(fixed_sample_selection().source_keys)
SOURCE_PREFIX = "vitaldb/1.0.0/"
HASH_BEFORE, HASH_AFTER, BRANCH_HEAD = "a" * 64, "b" * 64, "c" * 64
SNAPSHOTS = {"clinical_data": 11, "lab_data": 12, "track_list": 13}


class World:
    """The host as the runner sees it: one bucket, Airbyte's syncs, Dagster's runs and Nessie's main, with a switch for each fault."""

    def __init__(self) -> None:
        self.store = MemoryStore("example-bucket")
        self.syncs = 0
        self.main = HASH_BEFORE
        self.merged = False
        self.branch = ""
        self.planted_record = False
        self.selection_ok = True
        self.interruptible = True
        self.second_load_branches = False
        self.planted_merges = False
        self.delete_fails = False
        self.faults: set[str] = set()
        self.table_check_runs = 0
        self.expected: dict[str, bytes] = {}
        self.runs: dict[str, tuple[str, dict[str, Any]]] = {}
        self.checks: dict[str, bool] = {}
        self.calls: list[str] = []
        self.sha = hashlib.sha256(b"manifest").hexdigest()

    def land(self, count: int) -> None:
        for number, key in enumerate(SAMPLE[:count]):
            landed = f"{LANDING_PREFIX}{key.removeprefix(SOURCE_PREFIX)}"
            if not any(item.key == landed for item in self.store.list_versions(landed)):
                data = f"payload {number}".encode()
                self.store.put_object(landed, data)
                self.expected[key] = data
        if count and not self.store.list_versions(f"{LANDING_PREFIX}meta"):
            self.store.put_object(f"{LANDING_PREFIX}meta_1.jsonl", b"{}")


class FakeAws:
    def __init__(self, world: World) -> None:
        self.world = world

    def call(self, *arguments: str, **options: Any) -> Any:
        if arguments[1] == "put-object":
            version = self.world.store.put_object(arguments[arguments.index("--key") + 1], Path(arguments[arguments.index("--body") + 1]).read_bytes())
            return {"VersionId": version.version_id}
        if arguments[1] == "delete-object" and not self.world.delete_fails:
            self.world.store.remove_version(arguments[arguments.index("--key") + 1], arguments[arguments.index("--version-id") + 1])
        return {}


class FakeNessie:
    def __init__(self, world: World) -> None:
        self.world = world

    def head(self, name: str) -> str:
        return self.world.main if name == "main" else BRANCH_HEAD

    def reference(self, name: str) -> RefState | None:
        hidden = "second_load_hidden_branch" in self.world.faults and name.startswith("load_run_") and name != "load_run_20261125_abcd1234"
        if name == self.world.branch or hidden or (self.world.second_load_branches and name.startswith("load_run_") and name != "load_run_20261125_abcd1234"):
            return RefState("BRANCH", BRANCH_HEAD)
        return None

    def snapshot_ids(self, name: str) -> dict[str, int]:
        if name == "main" and self.world.merged and "wrong_snapshots" in self.world.faults:
            return {**SNAPSHOTS, "clinical_data": 999}
        return dict(SNAPSHOTS) if name == self.world.branch or (name == "main" and self.world.merged) else {}

    def table_keys(self, name: str) -> list[str]:
        early = "raw_on_main_early" in self.world.faults and bool(self.world.branch)
        return [f"raw_records.{table}" for table in SNAPSHOTS] if name == "main" and (self.world.merged or early) else []

    def has_recorded_manifest(self, sha256: str) -> bool:
        return self.world.merged and sha256 == self.world.sha


def build(module: ModuleType, world: World) -> type:
    class FakeHost(module.Host):  # type: ignore[name-defined,misc]
        def __init__(self) -> None:
            self.world = world
            self.port = 3000
            self.compose = ["docker", "compose"]

        def run(self, args: list[str], *, stdin: str | None = None, timeout: float = 0, quiet: bool = False) -> tuple[int, str, str]:
            if "--quiet" in args:
                return 0, "container\n", ""
            if "inspect" in args:
                return 0, "healthy\n", ""
            if "port" in args:
                return 0, "127.0.0.1:3000\n", ""
            if args[:3] == ["k3s", "kubectl", "get"]:
                return 0, "node/node_1\n", ""
            if args[0] == "df":
                return 0, "Avail Size\n123 456\n", ""
            return 0, "", ""

        def in_code(self, args: list[str], *, stdin: str | None = None, timeout: float = 0, quiet: bool = False) -> tuple[int, str, str]:
            world.calls.append(" ".join(args[:3]))
            if args[0] == "cat" and args[1].endswith(".load_record.json"):
                return 0, json.dumps({"tables": SNAPSHOTS, "branch": world.branch}), ""
            if args[0] == "cat" and args[1].endswith("table_checks.json"):
                receipt = {
                    "passed": True,
                    "merged": world.merged,
                    "merged_hash": HASH_AFTER,
                    "main_hash_before": HASH_BEFORE,
                    "checked": {
                        "tables": [{"table": "clinical_data", "checks": [{"name": "row count", "passed": "failed_flag" not in world.faults, "rows": 6090}]}]
                    },
                }
                return 0, json.dumps(receipt), ""
            if args[0] == "python":
                world.planted_record = True
                return 0, "", ""
            if args[0] == "sh" and "wc -l" in args[-1]:
                return 0, "0\n", ""
            if args[0] == "rm":
                world.planted_record = "planted_record_stays" in world.faults
                return 0, "", ""
            if args[0] == "test":
                return (0 if world.planted_record else 1), "", ""
            return 0, "", ""

        def raw_copy(self, *arguments: str, timeout: float = 0) -> tuple[int, dict[str, Any], str]:
            world.calls.append(f"raw_copy {arguments[0]}")
            if arguments[0] == "status":
                return 0, {"selection_is_fixed_sample": world.selection_ok, "files": 11, "active_jobs": 0}, ""
            if arguments[0] == "start":
                world.syncs += 1
                world.land(3 if world.syncs == 1 and world.interruptible else 11)
                if world.syncs == 3 and "resync_adds_version" in world.faults:
                    world.store.put_object(f"{LANDING_PREFIX}{SAMPLE[0].removeprefix(SOURCE_PREFIX)}", b"again")
                return 0, {"job": world.syncs}, ""
            if arguments[0] == "cancel":
                return 0, {"job": 1, "status": "cancelled" if world.interruptible else "succeeded"}, ""
            if arguments[0] == "wait":
                return 0, {"job": world.syncs, "status": "succeeded"}, ""
            return 0, {"jobId": world.syncs, "status": "succeeded", "rowsSynced": 11, "bytesSynced": 5000}, ""

        def launch(self, job: str, config: dict[str, Any]) -> str:
            run = f"dagster_run_{len(world.runs) + 1}"
            world.runs[run] = (job, config)
            return run

        def finished(self, run_id: str, deadline_seconds: float) -> dict[str, Any] | None:
            job, config = world.runs[run_id]
            if job == "table_checks_job":
                planted = "_planted" in config["ops"]["table_checks_step"]["config"]["load_record"]
                if planted and not world.planted_merges:
                    if "planted_moves_main" in world.faults:
                        world.main = "e" * 64
                    return {"status": "FAILURE", "tags": []}
                world.table_check_runs += 1
                if world.table_check_runs == 2 and "second_checks_move_main" in world.faults:
                    world.main = "f" * 64
                else:
                    world.merged, world.main = True, HASH_AFTER
                return {"status": "SUCCESS", "tags": []}
            given = config["ops"]["load_record_tables"]["config"]["run_id"]
            first = not world.branch
            world.branch = world.branch or f"load_{given}"
            if first and "load_moves_main" in world.faults:
                world.main = "d" * 64
            if first and "copies_left" in world.faults:
                world.store.put_object(f"records_input/{given}/clinical_data.csv", b"copy")
            tags = {
                "surgical_risk/records/load_branch": world.branch,
                "surgical_risk/records/head_hash": BRANCH_HEAD,
                "surgical_risk/records/base_hash": "9" * 64 if "base_wrong" in world.faults else HASH_BEFORE,
                **{f"surgical_risk/records/snapshot_{table}": str(snapshot) for table, snapshot in SNAPSHOTS.items()},
            }
            if first is False and world.second_load_branches:
                tags["surgical_risk/records/load_branch"] = f"load_{given}"
            return {"status": "SUCCESS", "tags": [{"key": key, "value": value} for key, value in tags.items()]}

        def folder(self, run_id: str) -> dict[str, Any] | None:
            document: dict[str, Any] = {name: "x" for name in module.REQUIRED_FIELDS}
            job, config = world.runs[run_id]
            planted = job == "table_checks_job" and "_planted" in config["ops"]["table_checks_step"]["config"]["load_record"]
            wrong = "planted_wrong_error" in world.faults
            document["outputs"] = {"error": "the snapshot of clinical_data at the recorded hash is not the recorded one" if planted and not wrong else ""}
            if "folder_missing_field" in world.faults:
                document.pop("commit")
            return document

    class FakeContext(module.Context):  # type: ignore[name-defined,misc]
        def __init__(self, record: Record, log: list[str], scratch: Path, evidence: Path) -> None:
            self.record, self.log, self.scratch, self.evidence = record, log, scratch, evidence
            self.configuration = {"AWS_ACCOUNT_ID": "example_account", "S3_BUCKET": "example-bucket", "AWS_PROFILE": "p", "AWS_REGION": "us-west-2"}
            self.bucket = "example-bucket"
            self.aws = FakeAws(world)
            self.store = world.store
            self.host = FakeHost()
            self.nessie = FakeNessie(world)
            self.token = "abcd1234"  # noqa: S105 - the run token of a stand-in, no secret
            self.run_id = "run_20261125_abcd1234"
            self.attempts = iter(range(1, 10_000))
            self.main_before = ""
            self.main_after_merge = ""
            self.disk_before = ""
            self.disk_after_first = ""
            self.disk_after = ""
            self.retry_job = 0
            self.reference = None
            self.dagster_runs: list[str] = []
            self.load_branch = ""
            self.load_head = ""
            self.record_directory = module.DEFAULT_RECORD_DIRECTORY

        def cli(self, module_name: str, *arguments: str, timeout: float = 0) -> tuple[int, str]:
            options = {name: arguments[position + 1] for position, name in enumerate(arguments) if name.startswith("--") and position + 1 < len(arguments)}
            world.calls.append(f"cli {arguments[0] if arguments else module_name}")
            if module_name.endswith("fetch_list"):
                Path(options["--output"]).write_text("list")
                return 0, ""
            if module_name.endswith("staging_peak"):
                Path(options["--output"]).write_text(
                    json.dumps({"samples": 4, "peak_pod_bytes_total": 9, "peak_increase_bytes": 5, "min_node_free_bytes": 7, "node_capacity_bytes": 99})
                )
                return 0, ""
            command = arguments[0]
            if command == "pairings":
                landed = [item for item in world.store.list_versions(LANDING_PREFIX) if not item.key.endswith(".jsonl")]
                if len(landed) < 11:
                    return 1, "no metadata"
                Path(options["--pairings"]).write_text(
                    json.dumps(
                        [{"source_key": SOURCE_PREFIX + item.key.removeprefix(LANDING_PREFIX), "landed_key": item.key, "size": item.size} for item in landed]
                    )
                )
                return 0, ""
            if command == "check":
                selection = json.loads(Path(options["--selection"]).read_text())
                pairs = {entry["source_key"]: entry for entry in json.loads(Path(options["--pairings"]).read_text())}
                lines = []
                for name in selection["files"]:
                    key = SOURCE_PREFIX + name
                    entry = pairs.get(key)
                    current = [v for v in world.store.list_versions(entry["landed_key"]) if v.key == entry["landed_key"] and v.is_latest] if entry else []
                    if not current:
                        lines.append(f"{key}: missing")
                    elif world.store.find(current[0]).data != world.expected.get(key, world.store.find(current[0]).data):
                        lines.append(f"{key}: checksum_differs")
                world.checks[options["--result"]] = not lines
                Path(options["--result"]).write_text("{}")
                return (0, "") if not lines else (1, "\n".join(lines))
            if command == "publish":
                if not world.checks.get(options["--result"]):
                    return 1, "did not pass"
                name = json.loads(Path(options["--selection"]).read_text())["name"]
                key = f"{MANIFEST_PREFIX}{name}/{world.sha}.json"
                if world.store.list_versions(key) and "manifest_repeats" in world.faults:
                    world.store.put_object(key, b"manifest again")
                version = world.store.create_object(key, b"manifest") if not world.store.list_versions(key) else world.store.list_versions(key)[0]
                Path(options["--reference"]).write_text(ManifestRef("example-bucket", key, version.version_id, world.sha).to_json())
                return 0, ""
            return 0, ""

    return FakeContext


@unittest.skipUnless(RUNNER.is_file(), "the runner is untracked and not on this computer")
class WalkTest(unittest.TestCase):
    def setUp(self) -> None:
        self.module = importlib.import_module("tests.e2e.ingestion_host")
        self.world = World()
        self.patches: list[Any] = [
            mock.patch.object(self.module.time, "sleep"),
            mock.patch.object(self.module.Sampler, "start"),
            mock.patch.object(self.module, "verify_landed", lambda store, reference, selection, pins: tuple(range(11))),
            mock.patch.object(self.module, "tracked_pins", lambda: None),
            mock.patch.object(self.module, "read_sample", self.read_sample),
        ]
        for patch in self.patches:
            patch.start()
            self.addCleanup(patch.stop)

    def read_sample(self, scratch: Path) -> tuple[dict[str, bytes], dict[str, bytes]]:
        names = [key.removeprefix(SOURCE_PREFIX) for key in SAMPLE if key.endswith(".vital")][:3]
        files = {name: f"planted {name}".encode() for name in names}
        self.world.expected.update({SOURCE_PREFIX + name: data for name, data in files.items()})
        if "changed_names_other_key" in self.world.faults:
            self.world.expected[SOURCE_PREFIX + names[0]] = b"other bytes"
        return {names[0]: files[names[0]], names[1]: files[names[1]]}, {names[2]: files[names[2]]}

    def walk(self) -> tuple[Record, list[str], Path]:
        record = Record(feature="stand-in walk", slug="stand_in", rerun="none")
        log: list[str] = []
        scratch = Path(tempfile.mkdtemp(prefix="ingestion_host_test_"))
        self.addCleanup(lambda: __import__("shutil").rmtree(scratch, ignore_errors=True))
        evidence = scratch / "evidence"
        evidence.mkdir()
        context = build(self.module, self.world)(record, log, scratch, evidence)
        with contextlib.suppress(self.module.Batch):
            self.module.steps(context)
        return record, log, evidence

    def failed(self, record: Record) -> list[str]:
        return [f"{item.case} ({item.step})" for item in record.observations if not item.passed]

    def test_the_happy_path_passes_every_assertion_in_every_case_in_order(self) -> None:
        record, _, evidence = self.walk()
        self.assertEqual([], self.failed(record))
        cases = list(dict.fromkeys(item.case for item in record.observations))
        self.assertEqual(
            [
                "preflight",
                "copy interrupted once",
                "copy resumed",
                "retry",
                "complete verified set",
                "copy idempotence",
                "record load",
                "planted failed table check",
                "table checks",
                "load idempotence",
                "run folders",
                "planted changed file",
                "planted missing file",
                "planted files",
                "planted files removed",
            ],
            [case for case in cases if case != "copy"],
        )
        self.assertEqual([], [item.key for item in self.world.store.list_versions(f"{LANDING_PREFIX}p4_host_")])
        manifests = {item.key.split("/")[1] for item in self.world.store.list_versions(MANIFEST_PREFIX)}
        self.assertEqual({"fixed_sample"}, manifests)
        self.assertEqual(1, len([item for item in self.world.store.list_versions(MANIFEST_PREFIX)]))
        self.assertEqual(3, len([call for call in self.world.calls if call == "raw_copy start"]))

    def test_the_evidence_holds_check_names_and_flags_and_no_count_of_a_table(self) -> None:
        _, _, evidence = self.walk()
        flags = json.loads((evidence / "table_checks_flags.json").read_text())
        self.assertEqual({"passed", "merged", "checks"}, set(flags))
        self.assertNotIn("6090", json.dumps(flags))
        self.assertTrue(all(type(value) is bool for value in flags["checks"].values()))

    def test_without_the_host_flag_the_run_refuses_with_exit_2_and_writes_nothing(self) -> None:
        err = io.StringIO()
        with mock.patch.dict("os.environ", {}, clear=False), contextlib.redirect_stderr(err):
            import os

            os.environ.pop("INGESTION_HOST", None)
            code = self.module.main()
        self.assertEqual(2, code)
        self.assertIn("INGESTION_HOST=1", err.getvalue())

    def test_a_connection_that_does_not_select_the_fixed_sample_stops_before_any_sync(self) -> None:
        self.world.selection_ok = False
        record, _, _ = self.walk()
        self.assertEqual(["preflight (setup)"], self.failed(record))
        self.assertNotIn("raw_copy start", self.world.calls)

    def test_a_sync_that_cannot_be_interrupted_fails_the_interruption_assertion(self) -> None:
        self.world.interruptible = False
        record, _, _ = self.walk()
        self.assertIn("copy interrupted once (fault)", self.failed(record))

    def test_a_second_load_that_makes_a_branch_of_its_own_fails_the_load_idempotence(self) -> None:
        self.world.second_load_branches = True
        record, _, _ = self.walk()
        self.assertIn("load idempotence (second load)", self.failed(record))

    def test_a_planted_load_record_that_is_merged_fails_the_failed_check_case(self) -> None:
        self.world.planted_merges = True
        record, _, _ = self.walk()
        self.assertIn("planted failed table check (fault)", self.failed(record))

    def test_a_planted_object_that_stays_fails_the_cleanup_and_the_landed_check_still_runs(self) -> None:
        self.world.delete_fails = True
        with mock.patch.object(self.module, "GONE_DEADLINE_SECONDS", 0.2):
            record, _, _ = self.walk()
        self.assertIn("planted files (cleanup)", self.failed(record))

    def test_each_fault_of_the_stand_in_world_fails_the_assertion_that_names_it(self) -> None:
        cases = {
            "resync_adds_version": "copy idempotence (second run copies nothing)",
            "manifest_repeats": "copy idempotence (second run checks and publishes nothing new)",
            "load_moves_main": "record load (load branch)",
            "base_wrong": "record load (load branch)",
            "second_load_hidden_branch": "load idempotence (second load)",
            "raw_on_main_early": "record load (snapshots)",
            "copies_left": "record load (copies removed)",
            "planted_wrong_error": "planted failed table check (fault)",
            "planted_moves_main": "planted failed table check (fault)",
            "planted_record_stays": "planted failed table check (cleanup)",
            "failed_flag": "table checks (run)",
            "wrong_snapshots": "table checks (merge)",
            "second_checks_move_main": "load idempotence (second table checks)",
            "folder_missing_field": "run folders (saved)",
            "changed_names_other_key": "planted changed file (fault)",
        }
        for fault, assertion in cases.items():
            with self.subTest(fault=fault):
                self.world = World()
                self.world.faults = {fault}
                record, _, _ = self.walk()
                self.assertIn(assertion, self.failed(record))

    def test_a_landing_that_is_not_empty_stops_the_run_in_the_preflight(self) -> None:
        self.world.land(2)
        record, _, _ = self.walk()
        self.assertEqual(["preflight (setup)"], self.failed(record))

    def test_a_passing_planted_set_is_never_published(self) -> None:
        self.walk()
        names = {item.key.split("/")[1] for item in self.world.store.list_versions(MANIFEST_PREFIX)}
        self.assertEqual({"fixed_sample"}, names)


@unittest.skipUnless(RUNNER.is_file(), "the runner is untracked and not on this computer")
class HostCallsTest(unittest.TestCase):
    """The calls of the runner's ``Host`` and of its Nessie client, with the SSH command replaced by a recorder."""

    def setUp(self) -> None:
        self.module = importlib.import_module("tests.e2e.ingestion_host")
        self.host = self.module.Host.__new__(self.module.Host)
        self.host.log, self.host.port, self.host.compose = [], 3000, ["docker", "compose"]
        self.sent: list[tuple[list[str], str | None]] = []

    def answer(self, code: int, out: str) -> Any:
        def run(args: list[str], *, stdin: str | None = None, timeout: float = 0, quiet: bool = False) -> tuple[int, str, str]:
            self.sent.append((args, stdin))
            return code, out, ""

        return mock.patch.object(self.host, "run", run)

    def test_a_job_is_launched_with_its_own_name_its_configuration_and_the_run_id_of_the_answer_is_returned(self) -> None:
        body = json.dumps({"data": {"launchRun": {"__typename": "LaunchRunSuccess", "run": {"runId": "abc"}}}})
        with self.answer(0, f"200\n{body}"):
            run_id = self.host.launch("table_checks_job", {"ops": {"x": 1}})
        self.assertEqual("abc", run_id)
        variables = json.loads(self.sent[0][1] or "{}")["variables"]["params"]
        self.assertEqual({"repositoryLocationName": "surgical_risk", "repositoryName": "__repository__", "jobName": "table_checks_job"}, variables["selector"])
        self.assertEqual({"ops": {"x": 1}}, variables["runConfigData"])
        self.assertIn("3000", self.sent[0][0])

    def test_a_refused_launch_gives_no_run_id(self) -> None:
        with self.answer(0, '200\n{"data": {"launchRun": {"__typename": "RunConfigValidationInvalid"}}}'):
            self.assertEqual("", self.host.launch("table_checks_job", {}))

    def test_an_answer_that_is_not_json_or_not_200_is_an_error_and_never_a_run(self) -> None:
        for code, out in ((0, "200\nnot json"), (0, "500\n{}"), (255, "")):
            with self.subTest(out=out), self.answer(code, out):
                self.assertEqual("", self.host.launch("table_checks_job", {}))

    def test_a_command_of_the_container_that_prints_no_json_gives_an_empty_answer_and_its_error_text(self) -> None:
        def in_code(args: list[str], **options: Any) -> tuple[int, str, str]:
            self.sent.append((args, None))
            return 1, "not json", "raw_copy refused: selection_differs\n"

        with mock.patch.object(self.host, "in_code", in_code):
            self.assertEqual((1, {}, "raw_copy refused: selection_differs"), self.host.raw_copy("start"))
        self.assertEqual(["python", "-m", "scripts.ingestion.raw_copy", "start"], self.sent[0][0])

    def test_nessie_is_reached_with_curl_inside_its_container_and_the_status_is_read_from_the_last_line(self) -> None:
        client = self.module.ExecNessie(self.host)
        with self.answer(0, '{"hash": "x"}\n200'):
            status, body = client.send("POST", "/api/v2/trees/main", b'{"a": 1}')
        self.assertEqual((200, b'{"hash": "x"}'), (status, body))
        command, stdin = self.sent[0]
        self.assertEqual(["docker", "compose", "exec", "-T", "nessie", "curl"], command[:6])
        self.assertEqual('{"a": 1}', stdin)
        self.assertIn("http://127.0.0.1:19120/api/v2/trees/main", command)

    def test_a_curl_that_fails_or_times_out_is_an_error_the_client_turns_into_a_nessie_error(self) -> None:
        client = self.module.ExecNessie(self.host)
        with self.answer(28, ""), self.assertRaises(TimeoutError):
            client.send("GET", "/api/v2/config", None)
        with self.answer(7, ""), self.assertRaises(OSError):
            client.send("GET", "/api/v2/config", None)


if __name__ == "__main__":
    unittest.main()
