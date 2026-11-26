"""The phase 4 host runner walks its cases in order, and each of its assertions fails for the fault it names.

Failure modes covered here, each against a stand-in of the host, of Airbyte, of Dagster, of Nessie and of S3 (the real ones are the lead's host run):

- The runner starts without ``INGESTION_HOST=1``, on an environment other than ``dev``, on a host that is not the run's instance or deployed commit, or goes on
  when the raw connection selects another set of files than the fixed sample or writes another bucket or prefix than the project's landing prefix.
- A sync that cannot be interrupted (all files land before the cancel is asked) passes for the interrupted copy, or a sync that goes on writing after Airbyte
  confirmed its end does. A cancel that is asked while a part has landed fails only because the rest of this small sample lands while the cancel is on its way.
  The payloads that the stopped sync left fail the landed check of the resumed set, or the runner excuses any other payload there. A failed check of the
  complete set leaves no word of why in the record, or a file name with it. The staging sampler exits dirty, does not cover a sync, or a disk reading is
  unreadable, and the measurement still passes.
- A third sync that copies bytes, repeats an earlier job or changes a version of landing/ or of the warehouse prefix passes for the idempotent copy.
- A second load of the same manifest makes a branch of its own or writes to the warehouse, and the idempotence assertion still passes.
- A planted file, load record, results folder or staging folder that the run did not create is deleted by its cleanup, or one that it did create stays, or an
  absence is read from a failed readback.
- The planted Spark case writes to the lake: the runner starts a Spark job other than the table checks job (which writes nothing), starts it with a writable
  staging volume, or deletes or puts an object under the warehouse prefix.
- The planted staging folder is not the one the controller's own code builds: it skips the gate (a substituted object version is staged), another file than the
  laboratory copy differs from the verified bytes, the plan is of another form, or a refused staging leaves a staged file behind.
- The runner's launch differs from the controller's own one-off by more than the slot, or the job's container outlives the case (a deadline, a lost reply), or
  a container of another launch is removed.
- A planted-file case that passes without the exact failed-check refusal and finding, or with a changed manifest inventory.
- A Dagster run or an Airbyte job that the run started is still running after a failure, or a success that arrives after the deadline passes.
- A Dagster launch whose reply is lost, or whose run ID is another run's, leaves a run that nothing ends: each attempt is kept before it is sent and tagged with
  a token of its own, and the cleanup reconciles it by that tag, ends the run it finds and reports an attempt that stays unresolved as a failure. A failed
  lookup in one step of the cleanup stops no other step.
- A host script that refuses a run folder, a receipt or a lookup shows content, or a lookup error (a permission error) reads as an absence.
- The planted Spark case's results or staging folder is not made by this run (exclusively) and is removed anyway, or stays; its check list holds fewer checks
  than the required ones for a table that is not the laboratory table.
- The job's output (its result holds the checks of two sealed tables, and its standard error can hold row text) leaves the host, or an unexpected word of a host
  script is taken for a judged result.
- A sampler that is started and fails to be ready is left running.
- The held-out rule: a table check's counts or values reach the host's answer or the evidence files (only check names and flags may). A receipt with a
  duplicated or a missing check passes, or a run folder with a secret-shaped text, another run's ID or another commit is copied into ``runs/``.

The runner is untracked (``tests/e2e/ingestion_host.py``), so these tests run only where it is on disk. The walk of the happy path is also the proof that every
name, attribute and call of the runner's flow fits together, which no other test reaches before the host run.
"""

from __future__ import annotations

import ast
import contextlib
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest import mock

from scripts.ingestion import table_checks as checks
from scripts.ingestion.connection import fixed_sample_selection
from scripts.ingestion.landing import LANDING_PREFIX
from scripts.ingestion.manifest import MANIFEST_PREFIX, ManifestRef, publish_manifest
from scripts.ingestion.record_load import RefState
from scripts.process import CompletedProcess
from scripts.stack import spark_slot
from scripts.stack.groups import Settings
from scripts.stack.job_workloads import job_command, profile_of_job, service_of
from scripts.stack.nessie import Reference
from scripts.stack.spark_slot import OWNER_LABEL, SLOT_NAME
from spark import table_checks as rules
from tests.e2e.run_record import Record
from tests.fast.ingestion_support import Landed, digest, landed_set, list_text_for, synthetic_pin
from tests.fast.memory_store import MemoryStore
from tests.fast.record_support import FakeClock
from tests.fast.runner_import import import_runner

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "tests" / "e2e" / "ingestion_host.py"
SAMPLE = sorted(fixed_sample_selection().source_keys)
SOURCE_PREFIX = "vitaldb/1.0.0/"
HASH_BEFORE, HASH_AFTER, BRANCH_HEAD = "a" * 64, "b" * 64, "c" * 64
PLANTED_SNAPSHOT = 99
SNAPSHOTS = {"clinical_data": 11, "lab_data": 12, "track_list": 13}
REQUIRED = {
    "clinical_data": ("staged file", "snapshot", "columns", "row count", "key multiset", "row payload"),
    "lab_data": ("staged file", "snapshot", "columns", "row count", "key multiset", "row payload"),
    "track_list": ("snapshot", "columns", "row count", "rows digest"),
}
LAB_FAULT = ("row count", "key multiset", "row payload")
COMMIT = "1" * 40
INSTANCE = "i-0123456789abcdef0"
TOKEN = "abcd1234"  # noqa: S105 - the run token of a stand-in, no secret
RUN_ID = "run_20261125_abcd1234"
PLANT_PREFIX = f"{LANDING_PREFIX}p4_host_{TOKEN}/"
LOAD_RECORD = f"/opt/dagster/runs/record_loads/{RUN_ID}.load_record.json"
STAGING = f"/opt/dagster/staging/p4_host_{TOKEN}_fault"


def run_owner() -> str:
    """The owner value that the runners' launch helper puts on this process's Spark containers."""
    return str(import_runner("spark_launch").RUN_OWNER)


def owner_of(job: list[str]) -> str:
    return job[job.index("--label") + 1].removeprefix(f"{OWNER_LABEL}=")


def job_words(module: Any, folder: str) -> list[str]:
    """The words of the one launch of the planted job, as the runner's launch gives them to the host."""
    seen: list[list[str]] = []

    def run(args: list[str], timeout: float) -> tuple[int, str, str]:
        if args[:2] == ["docker", "compose"]:
            seen.append(list(args))
            return 0, "judged", ""
        return 0, "", ""

    module.launch_table_check_job(run, ["docker", "compose"], "surgical_risk", folder)
    (job,) = seen
    return job


def good_tables(*, failed: tuple[tuple[str, str], ...] = ()) -> list[dict[str, Any]]:
    return [{"table": table, "checks": [{"name": name, "passed": (table, name) not in failed} for name in names]} for table, names in REQUIRED.items()]


class World:
    """The host as the runner sees it: one bucket, Airbyte's syncs, Dagster's runs, Nessie and the host's files, with a switch for each fault."""

    def __init__(self) -> None:
        self.store = MemoryStore("example-bucket")
        self.syncs = 0
        self.main = HASH_BEFORE
        self.merged = False
        self.branch = ""
        self.selection_ok = True
        self.destination_ok = True
        self.interruptible = True
        self.second_load_branches = False
        self.planted_merges = False
        self.delete_fails = False
        self.faults: set[str] = set()
        self.table_check_runs = 0
        self.expected: dict[str, bytes] = {}
        self.runs: dict[str, tuple[str, dict[str, Any]]] = {}
        self.final: set[str] = set()
        self.terminated: list[str] = []
        self.open_jobs: set[int] = set()
        self.sent: dict[int, int] = {}
        self.checks: dict[str, bool] = {}
        self.calls: list[str] = []
        self.sha = hashlib.sha256(b"manifest").hexdigest()
        self.files: dict[str, str] = {}
        self.receipts: dict[str, dict[str, Any]] = {}
        self.removed: list[str] = []
        self.deleted: list[tuple[str, str]] = []
        self.puts: list[tuple[str, str]] = []
        self.staged: dict[str, str] = {}
        self.containers: dict[str, str] = {}
        self.spark_jobs: list[list[str]] = []
        self.compose_runs: list[list[str]] = []
        self.catalog_writes: list[str] = []
        self.disk_reads = 0
        self.paired: dict[str, str] = {}
        self.stopped_payloads: set[str] = set()
        self.left_over_seen: list[frozenset[str]] = []
        self.write_in_settle = False
        self.sleeps: list[float] = []
        self.launch_seen: list[bool] = []
        self.run_tokens: dict[str, str] = {}
        self.finished_ids: list[str] = []
        self.lookup_failures = 0
        self.unfindable: set[str] = set()
        self.folders: set[str] = set()
        self.sampler_stops = 0
        self.sampler_starts = 0
        self.sampler_readies = 0
        self.instance = INSTANCE
        self.commit = COMMIT

    def landed_key(self, sync: int, source_key: str) -> str:
        """Each sync lands its files under a folder of its own, as the pinned destination does."""
        return f"{LANDING_PREFIX}sync_{sync}/{source_key}"

    def land(self, sync: int, count: int, *, completed: bool) -> None:
        """Land the first ``count`` payloads of one sync. Only a sync that completes writes the metadata that pairs them."""
        for number, key in enumerate(SAMPLE[:count]):
            landed = self.landed_key(sync, key)
            if not any(item.key == landed for item in self.store.list_versions(landed)):
                data = f"payload {number}".encode()
                self.store.put_object(landed, data)
                self.expected[key] = data
        if completed:
            self.store.put_object(f"{LANDING_PREFIX}sync_{sync}/meta.jsonl", b"{}")
            self.paired = {key: self.landed_key(sync, key) for key in SAMPLE[:count]}
        if count and "plant_prefix_taken" in self.faults and not self.store.list_versions(f"{PLANT_PREFIX}foreign"):
            self.store.put_object(f"{PLANT_PREFIX}foreign.jsonl", b"not the run's")


class FakeAws:
    def __init__(self, world: World) -> None:
        self.world = world

    def call(self, *arguments: str, **options: Any) -> Any:
        if arguments[1] == "put-object":
            key = arguments[arguments.index("--key") + 1]
            version = self.world.store.put_object(key, Path(arguments[arguments.index("--body") + 1]).read_bytes())
            self.world.puts.append((key, version.version_id))
            if "foreign_in_plant_prefix" in self.world.faults and not self.world.store.list_versions(f"{PLANT_PREFIX}foreign"):
                self.world.store.put_object(f"{PLANT_PREFIX}foreign.jsonl", b"not the run's")
            return {"VersionId": version.version_id}
        if arguments[1] == "delete-object":
            key, version_id = arguments[arguments.index("--key") + 1], arguments[arguments.index("--version-id") + 1]
            self.world.deleted.append((key, version_id))
            if not self.world.delete_fails:
                self.world.store.remove_version(key, version_id)
        return {}


class FakeClient:
    """The catalog client's calls that write: no case of the runner may make one, so each is kept."""

    def __init__(self, world: World) -> None:
        self.world = world

    def request(self, method: str, path: str, body: Any = None, query: Any = None) -> Any:
        return {"content": {"type": "ICEBERG_TABLE", "id": "id_1", "snapshotId": PLANTED_SNAPSHOT}}

    def create_reference(self, kind: str, name: str, source: Reference) -> Reference:
        self.world.catalog_writes.append(f"create_reference {name}")
        return Reference(kind, name, source.hash)

    def delete_reference(self, name: str, hash_value: str, kind: str | None = None) -> None:
        self.world.catalog_writes.append(f"delete_reference {name}")


class FakeNessie:
    def __init__(self, world: World) -> None:
        self.world = world
        self.client = FakeClient(world)

    def head(self, name: str) -> str:
        return self.world.main if name == "main" else BRANCH_HEAD

    def reference(self, name: str) -> RefState | None:
        if name == self.world.branch and "spark_moves_load_branch" in self.world.faults and self.world.spark_jobs:
            return RefState("BRANCH", "8" * 64)
        hidden = "second_load_hidden_branch" in self.world.faults and name.startswith("load_run_") and name != f"load_{RUN_ID}"
        if name == self.world.branch or hidden or (self.world.second_load_branches and name.startswith("load_run_") and name != f"load_{RUN_ID}"):
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
        context: Any

        def __init__(self) -> None:
            self.world = world
            self.port = 3000
            self.compose = ["docker", "compose"]
            self.bucket = "example-bucket"
            self.project = "surgical_risk"
            self.instance = INSTANCE
            self.log: list[str] = []

        def run(self, args: list[str], *, stdin: str | None = None, timeout: float = 0, quiet: bool = False) -> tuple[int, str, str]:
            if args[:2] == ["docker", "compose"] and "run" in args:
                world.compose_runs.append(list(args))
            if args[:2] == ["python3", "-c"] and len(args) == 7:
                return self.planted_job(args)
            if args[:3] == ["docker", "container", "ls"]:
                if "spark_slot_held" in world.faults:
                    world.containers.setdefault("another_launch", "another_owner")
                if "spark_listing_fails" in world.faults and world.spark_jobs:
                    return 1, "", ""
                return 0, "".join(f"{name}|exited|{held}\n" for name, held in world.containers.items()), ""
            if args[:4] == ["docker", "container", "rm", "--force"]:
                world.calls.append(f"container_rm {args[-1]}")
                if "spark_container_stays" not in world.faults:
                    world.containers.pop(args[-1], None)
                return 0, "", ""
            if "--quiet" in args:
                return 0, "container\n", ""
            if "inspect" in args:
                return 0, "healthy\n", ""
            if "port" in args:
                return 0, "127.0.0.1:3000\n", ""
            if args[:3] == ["k3s", "kubectl", "get"]:
                return 0, "node/node_1\n", ""
            if args[0] == "cat" and args[1].endswith("instance-id"):
                return 0, f"{world.instance}\n", ""
            if args[0] == "df":
                world.disk_reads += 1
                if "disk_unreadable_between" in world.faults and world.disk_reads == 2:
                    return 1, "", ""
                return 0, "Avail Size\n123 456\n", ""
            return 0, "", ""

        def planted_job(self, args: list[str]) -> tuple[int, str, str]:
            """The host script that runs the table checks job and hands its reply to the judge in the code location: the stand-in plays both."""
            job, judge = json.loads(args[5]), json.loads(args[6])
            world.spark_jobs.append(job)
            world.compose_runs.append(job)
            owner = owner_of(job)
            staging, run_folder = judge[-2], judge[-1]
            if "spark_slot_busy" in world.faults:
                world.containers["another_launch"] = "another_owner"
                return 1, "slot_busy", ""
            if "spark_job_deadline" in world.faults:
                world.containers["this_launch"] = owner
                return 1, "job_deadline", ""
            if "spark_container_stays" in world.faults or "spark_listing_fails" in world.faults:
                world.containers["this_launch"] = owner
            if "spark_unknown_word" in world.faults:
                return 0, "judged and 12 rows", ""
            if "spark_moves_main" in world.faults:
                world.main = "e" * 64
            if "spark_refused_before_spark" in world.faults or staging not in world.staged or job[-1] != f"/staging/{staging.rsplit('/', 1)[-1]}":
                return 1, "result_refused", ""
            if "spark_passes" in world.faults:
                failed: list[tuple[str, str]] = []
            else:
                failed = [("lab_data", name) for name in LAB_FAULT]
            if "spark_clinical_fails" in world.faults:
                failed.append(("clinical_data", "row count"))
            tables = good_tables(failed=tuple(failed))
            if "spark_clinical_checks_missing" in world.faults:
                tables[0]["checks"] = tables[0]["checks"][:2]
            if "spark_lab_checks_missing" in world.faults:
                tables[1]["checks"] = tables[1]["checks"][:3]
            passed = "spark_passes" in world.faults
            world.receipts[f"{run_folder}/table_checks.json"] = {"passed": passed, "merged": False, "merged_hash": "", "main_hash_before": "", "tables": tables}
            world.files[f"{run_folder}/table_checks.json"] = "{}"
            if not passed:
                world.files[f"{run_folder}/job_output.txt"] = "refusal: checks_failed\nexit code: 1\nresult lines: 1\n"
            return 0, "judged", ""

        def in_code(self, args: list[str], *, stdin: str | None = None, timeout: float = 0, quiet: bool = False) -> tuple[int, str, str]:
            world.calls.append(" ".join(args[:3]))
            if args[:2] == ["python", "-c"] and args[2] == getattr(module, "STAGE_PLANTED_LAB_SCRIPT", None):
                if "stage_refused" in world.faults:
                    return 1, "refused", ""
                if args[4] not in world.folders or args[3] not in world.files:
                    return 1, "refused", ""
                world.staged[args[4]] = args[3]
                world.files[f"{args[4]}/plan.json"] = "{}"
                return 0, "staged", ""
            if args[:2] == ["printenv", "PLATFORM_COMMIT"]:
                return 0, f"{world.commit}\n", ""
            return 0, "", ""

        def read_text(self, path: str, budget: float = 0) -> str | None:
            return world.files.get(path)

        def exists(self, path: str, budget: float = 0) -> bool | None:
            if "exists_unreadable" in world.faults:
                return None
            return path in world.files or path in world.folders or any(name.startswith(path + "/") for name in world.files)

        def entries(self, path: str, budget: float = 0) -> int | None:
            if "entries_unreadable" in world.faults:
                return None
            return sum(1 for name in world.files if name.startswith(path + "/"))

        def make_folder(self, path: str, budget: float = 0) -> bool:
            if self.exists(path) is not False:
                return False
            world.folders.add(path)
            return True

        def write_new(self, path: str, text: str, budget: float = 0) -> bool:
            if path in world.files or ("write_refused" in world.faults and path.endswith("_planted.load_record.json")):
                return False
            world.files[path] = text
            return True

        def remove(self, path: str, *, tree: bool = False, budget: float = 0) -> bool:
            world.calls.append(f"remove {path}")
            if "planted_record_stays" in world.faults and path.endswith("_planted.load_record.json"):
                return True
            if "results_dir_stays" in world.faults and tree and "table_checks_planted_" in path:
                return True
            if "staging_stays" in world.faults and tree and path.startswith("/opt/dagster/staging/"):
                return True
            world.removed.append(path)
            world.folders.discard(path)
            for name in [name for name in world.files if name == path or (tree and name.startswith(path + "/"))]:
                del world.files[name]
            return True

        def first_line(self, path: str, budget: float = 0) -> str | None:
            text = world.files.get(path)
            return None if text is None else text.splitlines()[0]

        def receipt_flags(self, path: str, budget: float = 0) -> dict[str, Any] | None:
            return world.receipts.get(path)

        def raw_copy(self, *arguments: str, timeout: float = 0) -> tuple[int, dict[str, Any], str]:
            world.calls.append(f"raw_copy {arguments[0]}")
            if arguments[0] == "status":
                found = {
                    "selection_is_fixed_sample": world.selection_ok,
                    "destination_is_project_landing": world.destination_ok,
                    "files": 11,
                    "active_jobs": 0,
                }
                return 0, found, ""
            if arguments[0] == "start":
                world.syncs += 1
                job = world.syncs
                if job == 3 and "resync_same_job" in world.faults:
                    job = 2
                world.open_jobs.add(job)
                world.sent[job] = (700 if "resync_copies_bytes" in world.faults else 0) if world.syncs == 3 else 5000
                if world.syncs == 1:
                    world.land(1, 3 if world.interruptible and "all_landed_when_asked" not in world.faults else 11, completed=False)
                elif world.syncs == 2:
                    world.land(2, 11, completed=True)
                if world.syncs == 3 and "resync_adds_version" in world.faults:
                    world.store.put_object(world.landed_key(2, SAMPLE[0]), b"again")
                if world.syncs == 3 and "warehouse_changes_on_resync" in world.faults:
                    world.store.put_object("warehouse/raw_records/clinical_data/data_2.parquet", b"more")
                if "start_lost_reply" in world.faults and world.syncs == 3:
                    return 255, {}, ""
                return 0, {"job": job}, ""
            if arguments[0] == "cancel-owned":
                stuck = "cancel_owned_still_active" in world.faults
                open_attempt = "cancel_owned_unresolved" in world.faults
                found = {"jobs": len(world.open_jobs), "still_active": len(world.open_jobs) if stuck else 0, "unresolved": 1 if open_attempt else 0}
                if not stuck:
                    world.open_jobs.clear()
                exit_zero = "cancel_owned_unresolved_exit_zero" in world.faults
                found["unresolved"] = 1 if open_attempt or exit_zero else 0
                return (1 if stuck or open_attempt else 0), found, ""
            job = int(arguments[arguments.index("--job") + 1]) if "--job" in arguments else 0
            if arguments[0] == "cancel":
                world.open_jobs.discard(job)
                if "rest_lands_during_the_cancel" in world.faults:
                    world.land(1, 11, completed=False)
                world.write_in_settle = "writes_after_the_confirmed_end" in world.faults
                world.stopped_payloads = {item.key for item in world.store.list_versions(f"{LANDING_PREFIX}sync_1/")}
                return 0, {"job": job, "status": "cancelled" if world.interruptible else "succeeded"}, ""
            if arguments[0] == "wait":
                if "retry_wait_fails" in world.faults and job == 2:
                    return 1, {"job": job, "status": "failed"}, ""
                world.open_jobs.discard(job)
                return 0, {"job": job, "status": "succeeded"}, ""
            return 0, {"jobId": job, "status": "succeeded", "rowsSynced": 11, "bytesSynced": world.sent.get(job, 5000)}, ""

        def launch(self, job: str, config: dict[str, Any], token: str) -> str:
            world.launch_seen.append(token in self.context.launch_attempts)
            run = f"dagster_run_{len(world.runs) + 1}"
            world.runs[run] = (job, config)
            world.run_tokens[run] = token
            if "launch_reply_lost" in world.faults and job == "record_load_job":
                return ""
            if "launch_reply_lost_unfindable" in world.faults and job == "record_load_job":
                world.unfindable.add(token)
                return ""
            if "launch_reply_lost_once_unfindable" in world.faults and job == "record_load_job":
                world.lookup_failures = 1
                return ""
            if "launch_returns_foreign_run" in world.faults and job == "record_load_job":
                return "foreign_run_1"
            return run

        def finished(self, run_id: str, deadline_seconds: float) -> dict[str, Any] | None:
            world.finished_ids.append(run_id)
            job, config = world.runs[run_id]
            if job == "record_load_job" and "load_never_ends" in world.faults and not world.branch:
                return None
            world.final.add(run_id)
            if job == "table_checks_job":
                path = config["ops"]["table_checks_step"]["config"]["load_record"]
                planted = "_planted" in path
                if planted and not world.planted_merges:
                    if "planted_moves_main" in world.faults:
                        world.main = "e" * 64
                    return {"status": "FAILURE", "tags": []}
                world.table_check_runs += 1
                if world.table_check_runs == 2 and "second_checks_move_main" in world.faults:
                    world.main = "f" * 64
                else:
                    world.merged, world.main = True, HASH_AFTER
                    tables = good_tables(failed=(("clinical_data", "row count"),) if "failed_flag" in world.faults else ())
                    if "duplicate_check" in world.faults:
                        tables[0]["checks"].append(dict(tables[0]["checks"][0]))
                    if "missing_table" in world.faults:
                        tables = tables[:2]
                    world.receipts[f"{module.RESULTS_ROOT}/{world.sha}/table_checks.json"] = {
                        "passed": "failed_flag" not in world.faults,
                        "merged": True,
                        "merged_hash": HASH_AFTER,
                        "main_hash_before": HASH_BEFORE,
                        "tables": tables,
                    }
                if world.table_check_runs == 2 and "second_checks_write_warehouse" in world.faults:
                    world.store.put_object("warehouse/raw_records/clinical_data/data_3.parquet", b"more")
                return {"status": "SUCCESS", "tags": []}
            given = config["ops"]["load_record_tables"]["config"]["run_id"]
            first = not world.branch
            world.branch = world.branch or f"load_{given}"
            if first:
                world.store.put_object("warehouse/raw_records/clinical_data/data_1.parquet", b"rows")
                world.files[f"{module.DEFAULT_RECORD_DIRECTORY}/{given}.load_record.json"] = json.dumps(
                    {
                        "format": "load_record_2",
                        "branch": world.branch,
                        "branch_hash": BRANCH_HEAD,
                        "base_hash": HASH_BEFORE,
                        "manifest": {"bucket": "example-bucket", "key": "manifests/x.json", "version_id": "v1", "sha256": world.sha},
                        "tables": SNAPSHOTS,
                    }
                )
            elif "second_load_writes_warehouse" in world.faults:
                world.store.put_object("warehouse/raw_records/clinical_data/data_4.parquet", b"again")
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

        def terminate(self, run_id: str, budget: float = 0) -> bool:
            world.terminated.append(run_id)
            if "terminate_fails" not in world.faults:
                world.final.add(run_id)
            return True

        def run_info(self, run_id: str, budget: float = 0) -> dict[str, Any]:
            if run_id not in world.runs:
                return {"status": "STARTED", "jobName": "somebody_elses_job", "tags": []}
            status = "SUCCESS" if run_id in world.final else "STARTED"
            return {"status": status, "jobName": world.runs[run_id][0], "tags": [{"key": module.LAUNCH_TAG, "value": world.run_tokens[run_id]}]}

        def runs_with_token(self, token: str, budget: float = 0) -> list[dict[str, Any]] | None:
            if world.lookup_failures:
                world.lookup_failures -= 1
                return None
            if token in world.unfindable:
                return None
            return [{"runId": run, **self.run_info(run)} for run, found in world.run_tokens.items() if found == token]

        def checked_folder(self, run_id: str, job: str, commit: str, budget: float = 0) -> dict[str, Any]:
            problems = []
            if "folder_missing_field" in world.faults:
                problems.append("fields_missing")
            if "folder_other_run" in world.faults:
                problems.append("run_id_differs")
            if "folder_other_commit" in world.faults:
                problems.append("commit_differs")
            if "folder_secret" in world.faults:
                problems.append("secret_shaped_text")
            document: dict[str, Any] = {name: "x" for name in module.REQUIRED_FIELDS}
            planted = job == "table_checks_job" and "_planted" in world.runs[run_id][1]["ops"]["table_checks_step"]["config"]["load_record"]
            wrong = "planted_wrong_error" in world.faults
            document["outputs"] = {"error": "the snapshot of clinical_data at the recorded hash is not the recorded one" if planted and not wrong else ""}
            document.update(run_id=run_id, job_name=job, commit=commit)
            leaked = "folder_leaks_record" in world.faults
            return {"ok": not problems, "problems": problems, "record": document if leaked or not problems else None}

    class FakeContext(module.Context):  # type: ignore[name-defined,misc]
        def __init__(self, record: Record, log: list[str], scratch: Path, evidence: Path) -> None:
            self.configuration = {
                "AWS_ACCOUNT_ID": "example_account",
                "S3_BUCKET": "example-bucket",
                "AWS_PROFILE": "p",
                "AWS_REGION": "us-west-2",
                "ENVIRONMENT": "dev",
            }
            self.bucket = "example-bucket"
            self.aws = FakeAws(world)
            self.store = world.store
            self.host = FakeHost()
            self.host.context = self
            self.nessie = FakeNessie(world)
            self.init_state(record, log, scratch, evidence)
            self.token = TOKEN
            self.run_id = RUN_ID

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
                if len(world.paired) < 11:
                    return 1, "no metadata"
                prefix = SOURCE_PREFIX if "pairings_name_no_selected_file" in world.faults else ""
                Path(options["--pairings"]).write_text(
                    json.dumps(
                        [
                            {"source_key": prefix + source, "landed_key": landed, "size": world.store.list_versions(landed)[-1].size}
                            for source, landed in world.paired.items()
                        ]
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
                found = [
                    {"position": position, "cause": line.rsplit(": ", 1)[1], "source_key": line.rsplit(": ", 1)[0]} for position, line in enumerate(lines, 1)
                ]
                result_text = "not a result" if "check_result_unreadable" in world.faults else json.dumps({"findings": found})
                Path(options["--result"]).write_text(result_text)
                total = len(selection["files"])
                if not lines:
                    return 0, f"checked {total} of {total} files\ncheck passed: {total} of {total} selected files verified; result x"
                extra = ["checked 1 of 1 files"] if "check_extra_line" in world.faults else []
                count = len(lines) + (1 if "check_two_faults" in world.faults else 0)
                summary = f"check failed: {count} faults in {total} selected files; no manifest may be published"
                causes = [line.rsplit(": ", 1)[1] for line in lines]
                kinds = [f"faults of kind {cause}: {causes.count(cause)}" for cause in sorted(set(causes))]
                return 1, "\n".join([*extra, f"checked {total} of {total} files", *lines, *kinds, summary])
            if command == "publish":
                if not world.checks.get(options["--result"]):
                    if "planted_publish_writes" in world.faults:
                        world.store.put_object(f"{MANIFEST_PREFIX}p4_host_changed/{world.sha}.json", b"a manifest of a planted set")
                    if "publish_extra_text" in world.faults:
                        return 1, "the check did not pass\nand a detail that should not be here"
                    return 1, "something else was wrong" if "publish_other_refusal" in world.faults else "the check did not pass"
                name = json.loads(Path(options["--selection"]).read_text())["name"]
                key = f"{MANIFEST_PREFIX}{name}/{world.sha}.json"
                if world.store.list_versions(key) and "manifest_repeats" in world.faults:
                    world.store.put_object(key, b"manifest again")
                version = world.store.create_object(key, b"manifest") if not world.store.list_versions(key) else world.store.list_versions(key)[0]
                Path(options["--reference"]).write_text(ManifestRef("example-bucket", key, version.version_id, world.sha).to_json())
                return 0, ""
            return 0, ""

    return FakeContext


def covering_samples(path: Path, *, covers: bool = True) -> None:
    after = 2**62 if covers else 1
    rows = [
        {"kind": "header", "schema": 1},
        {"kind": "sample", "seq": 0, "missing": [], "summary": {"wall_ns_before": 0, "wall_ns_after": 1}},
        {"kind": "sample", "seq": 1, "missing": [], "summary": {"wall_ns_before": 2, "wall_ns_after": after}},
        {"kind": "end", "reason": "stop_flag", "samples": 2},
    ]
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


@unittest.skipUnless(RUNNER.is_file(), "the runner is untracked and not on this computer")
class WalkTest(unittest.TestCase):
    def setUp(self) -> None:
        self.module = import_runner("ingestion_host")
        self.world = World()
        self.patches: list[Any] = [
            mock.patch.object(self.module.time, "sleep", self.sleep),
            mock.patch.object(self.module.Sampler, "start", lambda sampler: self.sampler_start(sampler)),
            mock.patch.object(self.module.Sampler, "ready", lambda sampler, seconds=0: self.sampler_ready()),
            mock.patch.object(self.module.Sampler, "stop", lambda sampler: self.sampler_stop(sampler)),
            mock.patch.object(self.module, "verify_landed", self.verify_landed),
            mock.patch.object(self.module, "tracked_pins", lambda: None),
            mock.patch.object(self.module, "read_sample", self.read_sample),
            mock.patch.object(self.module, "GONE_DEADLINE_SECONDS", 0.2),
        ]
        for patch in self.patches:
            patch.start()
            self.addCleanup(patch.stop)

    def sleep(self, seconds: float) -> None:
        self.world.sleeps.append(seconds)
        if self.world.write_in_settle and seconds == getattr(self.module, "CANCEL_SETTLE_SECONDS", None):
            self.world.write_in_settle = False
            self.world.store.put_object(self.world.landed_key(1, "a_late_payload"), b"written after the end was confirmed")

    def verify_landed(self, store: Any, reference: Any, selection: Any, pins: Any, left_over: Any = ()) -> tuple[int, ...]:
        self.world.left_over_seen.append(frozenset(left_over))
        return tuple(range(11))

    def sampler_ready(self) -> bool:
        self.world.sampler_readies += 1
        if "sampler_ready_raises" in self.world.faults or ("second_sampler_ready_raises" in self.world.faults and self.world.sampler_readies == 2):
            raise OSError("the sampler's file could not be read")
        return "sampler_not_ready" not in self.world.faults

    def sampler_start(self, sampler: Any) -> None:
        self.world.sampler_starts += 1
        if "sampler_start_raises" in self.world.faults or ("second_sampler_start_raises" in self.world.faults and self.world.sampler_starts == 2):
            raise OSError("the sampler could not be started")
        covering_samples(sampler.samples, covers="sampler_not_covering" not in self.world.faults)

    def sampler_stop(self, sampler: Any) -> int:
        self.world.sampler_stops += 1
        return 1 if "sampler_dirty_exit" in self.world.faults else 0

    def read_sample(self, scratch: Path) -> tuple[dict[str, bytes], dict[str, bytes]]:
        names = [key.removeprefix(SOURCE_PREFIX) for key in SAMPLE if key.endswith(".vital")][:3]
        files = {name: f"planted {name}".encode() for name in names}
        self.world.expected.update({SOURCE_PREFIX + name: data for name, data in files.items()})
        if "changed_names_other_key" in self.world.faults:
            self.world.expected[SOURCE_PREFIX + names[0]] = b"other bytes"
        return {names[0]: files[names[0]], names[1]: files[names[1]]}, {names[2]: files[names[2]]}

    def walk(self) -> tuple[Record, list[str], Path]:
        record = Record(feature="stand-in walk", slug="stand_in", rerun="none")
        record.revision = COMMIT
        log: list[str] = []
        scratch = Path(tempfile.mkdtemp(prefix="ingestion_host_test_"))
        self.addCleanup(lambda: __import__("shutil").rmtree(scratch, ignore_errors=True))
        evidence = scratch / "evidence"
        evidence.mkdir()
        context = build(self.module, self.world)(record, log, scratch, evidence)
        self.last_record = record
        with contextlib.suppress(self.module.Batch):
            self.module.steps(context)
        return record, log, evidence

    def failed(self, record: Record) -> list[str]:
        return [f"{item.case} ({item.step})" for item in record.observations if not item.passed]

    def walk_to_the_end(self, *faults: str) -> tuple[Record, Exception | None]:
        """A walk that keeps the exception that ends it, as ``main`` keeps it, so that the cleanup that ran after it can be read."""
        self.world = World()
        self.world.faults = set(faults)
        raised: Exception | None = None
        try:
            record, _, _ = self.walk()
        except OSError as error:
            raised = error
            record = self.last_record
        return record, raised

    def with_faults(self, *faults: str) -> list[str]:
        self.world = World()
        self.world.faults = set(faults)
        record, _, _ = self.walk()
        return self.failed(record)

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
                "planted failing spark check",
                "table checks",
                "load idempotence",
                "run folders",
                "planted files",
                "planted changed file",
                "planted missing file",
                "planted files removed",
                "cleanup",
            ],
            [case for case in cases if case != "copy"],
        )
        self.assertEqual([], [item.key for item in self.world.store.list_versions(f"{LANDING_PREFIX}p4_host_") if item.key.startswith(PLANT_PREFIX)])
        manifests = {item.key.split("/")[1] for item in self.world.store.list_versions(MANIFEST_PREFIX)}
        self.assertEqual({"fixed_sample"}, manifests)
        self.assertEqual(1, len([item for item in self.world.store.list_versions(MANIFEST_PREFIX)]))
        self.assertEqual(3, len([call for call in self.world.calls if call == "raw_copy start"]))
        self.assertEqual([], self.world.catalog_writes)
        self.assertEqual({}, self.world.containers)
        self.assertEqual([], [name for name in self.world.files if "_planted" in name or "table_checks_planted" in name or name.startswith(STAGING)])
        self.assertEqual(set(), self.world.folders)
        self.assertEqual(1, len(self.world.spark_jobs))

    def test_the_evidence_holds_check_names_and_flags_and_no_count_of_a_table(self) -> None:
        _, _, evidence = self.walk()
        flags = json.loads((evidence / "table_checks_flags.json").read_text())
        self.assertEqual({"passed", "merged", "checks"}, set(flags))
        self.assertTrue(all(type(value) is bool for value in flags["checks"].values()))
        self.assertEqual(sum(len(names) for names in REQUIRED.values()), len(flags["checks"]))

    def test_the_verified_load_tags_are_saved_with_their_hash_and_snapshot_ids(self) -> None:
        _, _, evidence = self.walk()
        saved = json.loads((evidence / "load_tags.json").read_text())
        self.assertEqual(f"load_{RUN_ID}", saved["load_branch"])
        self.assertEqual(BRANCH_HEAD, saved["head_hash"])
        self.assertEqual(HASH_BEFORE, saved["base_hash"])
        self.assertEqual(SNAPSHOTS, {table: int(value) for table, value in saved["snapshots"].items()})

    def test_the_run_folders_saved_hold_only_the_allowed_fields_and_name_the_run_and_the_commit(self) -> None:
        _, _, evidence = self.walk()
        saved = sorted(evidence.glob("dagster_run_folder_*"))
        self.assertGreaterEqual(len(saved), 5)
        for path in saved:
            document = json.loads(path.read_text())
            self.assertEqual(COMMIT, document["commit"])
            self.assertTrue(set(document) <= self.module.REQUIRED_FIELDS)

    def test_without_the_host_flag_the_run_refuses_with_exit_2_and_writes_nothing(self) -> None:
        err = io.StringIO()
        with mock.patch.dict("os.environ", {}, clear=False), contextlib.redirect_stderr(err):
            import os

            os.environ.pop("INGESTION_HOST", None)
            code = self.module.main()
        self.assertEqual(2, code)
        self.assertIn("INGESTION_HOST=1", err.getvalue())

    def test_an_environment_other_than_dev_is_refused_before_the_host_is_looked_up(self) -> None:
        with self.assertRaises(self.module.ConfigurationError):
            self.module.require_dev({"ENVIRONMENT": "prod"})
        self.module.require_dev({"ENVIRONMENT": "dev"})
        with (
            mock.patch.object(self.module, "load_configuration", lambda: {"ENVIRONMENT": "prod", "S3_BUCKET": "b"}),
            mock.patch.object(self.module, "host_peer") as peer,
            self.assertRaises(self.module.ConfigurationError),
        ):
            self.module.Context(Record(feature="x", slug="x", rerun="none"), [], Path("."), Path("."))
        peer.assert_not_called()

    def test_a_connection_that_does_not_select_the_fixed_sample_stops_before_any_sync(self) -> None:
        self.world.selection_ok = False
        record, _, _ = self.walk()
        self.assertEqual(["preflight (setup)"], self.failed(record))
        self.assertNotIn("raw_copy start", self.world.calls)

    def test_a_destination_that_is_not_the_project_landing_prefix_stops_before_any_sync(self) -> None:
        self.world.destination_ok = False
        record, _, _ = self.walk()
        self.assertEqual(["preflight (setup)"], self.failed(record))
        self.assertNotIn("raw_copy start", self.world.calls)

    def test_a_host_of_another_instance_or_commit_stops_before_any_sync(self) -> None:
        for fault, name in (("wrong_instance", "instance"), ("wrong_commit", "commit")):
            with self.subTest(fault=fault):
                self.world = World()
                setattr(self.world, name, "i-0000000000000000f" if name == "instance" else "2" * 40)
                record, _, _ = self.walk()
                self.assertEqual(["preflight (setup)"], self.failed(record))
                self.assertNotIn("raw_copy start", self.world.calls)

    def test_a_sync_that_cannot_be_interrupted_fails_the_interruption_assertion(self) -> None:
        self.world.interruptible = False
        record, _, _ = self.walk()
        self.assertIn("copy interrupted once (fault)", self.failed(record))

    def observed(self, record: Record, case: str, step: str) -> str:
        return next(item.observed for item in record.observations if (item.case, item.step) == (case, step))

    def test_a_cancel_asked_mid_copy_passes_when_the_rest_lands_while_it_is_on_its_way_and_nothing_changes_after_the_confirmed_end(self) -> None:
        self.world.faults = {"rest_lands_during_the_cancel"}
        record, _, _ = self.walk()
        self.assertEqual([], self.failed(record))
        text = self.observed(record, "copy interrupted once", "fault")
        self.assertIn("payloads landed when asked 3, after the cancellation 11 of 11", text)
        self.assertIn("landing/ unchanged 60 s after it: True", text)
        self.assertIn(self.module.CANCEL_SETTLE_SECONDS, self.world.sleeps)

    def test_a_sync_that_writes_after_airbyte_confirmed_its_end_fails_the_interruption(self) -> None:
        self.assertIn("copy interrupted once (fault)", self.with_faults("writes_after_the_confirmed_end"))

    def test_a_cancel_asked_after_every_payload_landed_fails_the_interruption_even_when_the_job_ends_as_cancelled(self) -> None:
        self.assertIn("copy interrupted once (fault)", self.with_faults("all_landed_when_asked"))

    def test_every_landed_check_is_given_exactly_the_payloads_that_the_stopped_sync_left(self) -> None:
        for faults in (set(), {"rest_lands_during_the_cancel"}):
            with self.subTest(faults=sorted(faults)):
                self.world = World()
                self.world.faults = faults
                self.walk()
                self.assertEqual(11 if faults else 3, len(self.world.stopped_payloads))
                self.assertGreater(len(self.world.left_over_seen), 1)
                self.assertEqual({frozenset(self.world.stopped_payloads)}, set(self.world.left_over_seen))

    def test_a_failed_check_of_the_complete_set_records_each_kind_of_fault_with_its_count_and_no_file(self) -> None:
        self.world.faults = {"pairings_name_no_selected_file"}
        record, _, _ = self.walk()
        text = self.observed(record, "complete verified set", "pairings, check, manifest")
        self.assertIn("'check': 1, 'fault_kinds': {'missing': 11}", text)
        self.assertNotIn("vital", text)
        self.assertNotIn("clinical", text)

    def test_a_result_that_cannot_be_read_is_recorded_as_unreadable_kinds_and_the_case_still_fails(self) -> None:
        self.world.faults = {"pairings_name_no_selected_file", "check_result_unreadable"}
        record, _, _ = self.walk()
        self.assertIn("complete verified set (pairings, check, manifest)", self.failed(record))
        self.assertIn("'fault_kinds': 'unreadable'", self.observed(record, "complete verified set", "pairings, check, manifest"))

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
        self.assertIn("planted files removed (landed)", [f"{item.case} ({item.step})" for item in record.observations])

    def test_each_fault_of_the_stand_in_world_fails_the_assertion_that_names_it(self) -> None:
        cases = {
            "resync_adds_version": "copy idempotence (second run copies nothing)",
            "resync_copies_bytes": "copy idempotence (second run copies nothing)",
            "resync_same_job": "copy idempotence (second run copies nothing)",
            "warehouse_changes_on_resync": "copy idempotence (second run copies nothing)",
            "manifest_repeats": "copy idempotence (second run checks and publishes nothing new)",
            "load_moves_main": "record load (load branch)",
            "base_wrong": "record load (load branch)",
            "second_load_hidden_branch": "load idempotence (second load)",
            "second_load_writes_warehouse": "load idempotence (second load)",
            "second_checks_write_warehouse": "load idempotence (second table checks)",
            "raw_on_main_early": "record load (snapshots)",
            "copies_left": "record load (copies removed)",
            "planted_wrong_error": "planted failed table check (fault)",
            "planted_moves_main": "planted failed table check (fault)",
            "planted_record_stays": "planted failed table check (cleanup)",
            "failed_flag": "table checks (run)",
            "duplicate_check": "table checks (run)",
            "missing_table": "table checks (run)",
            "wrong_snapshots": "table checks (merge)",
            "second_checks_move_main": "load idempotence (second table checks)",
            "folder_missing_field": "run folders (saved)",
            "folder_other_run": "run folders (saved)",
            "folder_other_commit": "run folders (saved)",
            "folder_secret": "run folders (saved)",
            "changed_names_other_key": "planted changed file (fault)",
            "check_extra_line": "planted changed file (fault)",
            "check_two_faults": "planted changed file (fault)",
            "planted_publish_writes": "planted changed file (fault)",
            "publish_other_refusal": "planted changed file (fault)",
            "sampler_not_ready": "copy (setup)",
            "sampler_dirty_exit": "copy (measure)",
            "sampler_not_covering": "copy (measure)",
            "disk_unreadable_between": "copy resumed (measure)",
            "spark_passes": "planted failing spark check (fault)",
            "spark_refused_before_spark": "planted failing spark check (fault)",
            "spark_clinical_fails": "planted failing spark check (fault)",
            "retry_wait_fails": "copy resumed (sync)",
            "stage_refused": "planted failing spark check (setup)",
            "staging_stays": "planted failing spark check (cleanup)",
            "spark_job_deadline": "planted failing spark check (fault)",
            "spark_slot_busy": "planted failing spark check (fault)",
            "spark_slot_held": "planted failing spark check (fault)",
            "spark_unknown_word": "planted failing spark check (fault)",
            "spark_moves_main": "planted failing spark check (fault)",
            "spark_moves_load_branch": "planted failing spark check (fault)",
            "spark_container_stays": "planted failing spark check (cleanup)",
            "spark_listing_fails": "planted failing spark check (cleanup)",
            "spark_clinical_checks_missing": "planted failing spark check (fault)",
            "spark_lab_checks_missing": "planted failing spark check (fault)",
            "publish_extra_text": "planted changed file (fault)",
            "cancel_owned_unresolved": "cleanup (airbyte jobs)",
            "cancel_owned_unresolved_exit_zero": "cleanup (airbyte jobs)",
        }
        for fault, assertion in cases.items():
            with self.subTest(fault=fault):
                self.assertIn(assertion, self.with_faults(fault))

    def test_a_planted_prefix_that_is_not_empty_is_refused_and_the_foreign_object_is_never_deleted(self) -> None:
        failed = self.with_faults("plant_prefix_taken")
        self.assertIn("planted files (reserve)", failed)
        self.assertEqual([], self.world.puts)
        self.assertEqual([], [deleted for deleted in self.world.deleted if deleted[0].startswith(PLANT_PREFIX)])
        self.assertEqual(1, len(self.world.store.list_versions(f"{PLANT_PREFIX}foreign")))

    def test_a_foreign_version_that_appears_in_the_planted_prefix_is_left_and_only_the_runs_own_versions_are_deleted(self) -> None:
        failed = self.with_faults("foreign_in_plant_prefix")
        self.assertIn("planted files (cleanup)", failed)
        self.assertEqual(sorted(self.world.puts), sorted(deleted for deleted in self.world.deleted if deleted[0].startswith(PLANT_PREFIX)))
        self.assertEqual(1, len(self.world.store.list_versions(f"{PLANT_PREFIX}foreign")))

    def test_a_planted_load_record_that_already_exists_is_refused_and_never_removed(self) -> None:
        self.world.files[f"/opt/dagster/runs/record_loads/{RUN_ID}_planted.load_record.json"] = "someone's"
        record, _, _ = self.walk()
        self.assertIn("planted failed table check (reserve)", self.failed(record))
        self.assertNotIn(f"remove /opt/dagster/runs/record_loads/{RUN_ID}_planted.load_record.json", self.world.calls)
        self.assertEqual("someone's", self.world.files[f"/opt/dagster/runs/record_loads/{RUN_ID}_planted.load_record.json"])

    def test_a_planted_load_record_that_another_writer_made_after_the_reservation_is_never_removed(self) -> None:
        failed = self.with_faults("write_refused")
        self.assertIn("planted failed table check (setup)", failed)
        self.assertNotIn(f"remove /opt/dagster/runs/record_loads/{RUN_ID}_planted.load_record.json", self.world.calls)
        self.assertNotIn("planted failed table check (cleanup)", [f"{item}" for item in failed])

    def test_a_planted_results_folder_that_already_exists_is_refused_and_never_removed(self) -> None:
        self.world.files[f"{self.module.PLANTED_RESULTS_ROOT}/old/table_checks.json"] = "someone's"
        record, _, _ = self.walk()
        self.assertIn("planted failed table check (reserve)", self.failed(record))
        self.assertIn(f"{self.module.PLANTED_RESULTS_ROOT}/old/table_checks.json", self.world.files)

    def test_the_spark_case_makes_its_own_results_and_staging_folders_exclusively_and_removes_them_and_reads_them_gone(self) -> None:
        record, _, _ = self.walk()
        self.assertEqual([], self.failed(record))
        for folder in (self.module.planted_results_root(TOKEN), STAGING):
            self.assertIn(f"remove {folder}", self.world.calls)
            self.assertNotIn(folder, self.world.folders)
            self.assertEqual([], [name for name in self.world.files if name.startswith(folder)])

    def test_a_results_or_staging_folder_of_the_spark_case_that_exists_already_is_refused_and_never_removed_even_when_it_is_empty(self) -> None:
        for folder in (self.module.planted_results_root(TOKEN), STAGING):
            for existing in ("folder_with_a_file", "empty_folder"):
                with self.subTest(folder=folder, existing=existing):
                    self.world = World()
                    if existing == "folder_with_a_file":
                        self.world.files[f"{folder}/old/table_checks.json"] = "someone's"
                    else:
                        self.world.folders.add(folder)
                    record, _, _ = self.walk()
                    failed = self.failed(record)
                    self.assertTrue({"planted failing spark check (reserve)", "planted failing spark check (setup)"} & set(failed), failed)
                    self.assertNotIn(f"remove {folder}", self.world.calls)
                    self.assertTrue(folder in self.world.folders or f"{folder}/old/table_checks.json" in self.world.files)
                    self.assertEqual([], self.world.spark_jobs)

    def test_a_results_folder_that_stays_after_its_removal_is_a_failed_cleanup_read_back_from_the_host(self) -> None:
        self.assertIn("planted failing spark check (cleanup)", self.with_faults("results_dir_stays"))

    def test_an_absence_is_never_taken_from_a_failed_readback(self) -> None:
        for fault in ("exists_unreadable", "entries_unreadable"):
            with self.subTest(fault=fault):
                failed = self.with_faults(fault)
                self.assertIn("planted failed table check (reserve)", failed)
                self.assertIn("planted failing spark check (reserve)" if fault == "entries_unreadable" else "planted failed table check (reserve)", failed)

    def test_a_run_that_never_ends_is_terminated_and_the_termination_is_confirmed(self) -> None:
        failed = self.with_faults("load_never_ends")
        self.assertIn("record load (run)", failed)
        self.assertEqual(["dagster_run_1"], self.world.terminated)
        self.assertNotIn("cleanup (dagster runs)", failed)

    def test_a_run_that_stays_running_after_the_termination_is_a_failed_cleanup(self) -> None:
        with mock.patch.object(self.module, "CLEANUP_RUN_SECONDS", 0.2):
            failed = self.with_faults("load_never_ends", "terminate_fails")
        self.assertIn("cleanup (dagster runs)", failed)

    def test_a_job_whose_reply_was_lost_or_whose_wait_failed_is_cancelled_in_the_cleanup(self) -> None:
        for fault in ("start_lost_reply", "retry_wait_fails"):
            with self.subTest(fault=fault):
                self.with_faults(fault)
                self.assertIn("raw_copy cancel-owned", self.world.calls)
                self.assertEqual(set(), self.world.open_jobs)

    def test_a_job_that_stays_active_after_the_cleanup_is_a_failed_assertion(self) -> None:
        self.assertIn("cleanup (airbyte jobs)", self.with_faults("retry_wait_fails", "cancel_owned_still_active"))

    def test_a_landing_that_is_not_empty_stops_the_run_in_the_preflight(self) -> None:
        self.world.land(0, 2, completed=False)
        record, _, _ = self.walk()
        self.assertEqual(["preflight (setup)"], self.failed(record))

    def test_a_passing_planted_set_is_never_published(self) -> None:
        self.walk()
        names = {item.key.split("/")[1] for item in self.world.store.list_versions(MANIFEST_PREFIX)}
        self.assertEqual({"fixed_sample"}, names)

    def test_each_launch_attempt_is_kept_before_it_is_sent_and_carries_a_token_of_its_own(self) -> None:
        self.walk()
        self.assertGreaterEqual(len(self.world.launch_seen), 5)
        self.assertTrue(all(self.world.launch_seen))
        tokens = list(self.world.run_tokens.values())
        self.assertEqual(len(tokens), len(set(tokens)))
        self.assertTrue(all(token.startswith(f"{TOKEN}-") for token in tokens))

    def test_a_launch_whose_reply_was_lost_is_reconciled_by_its_token_and_the_run_goes_on(self) -> None:
        record, _ = self.walk_to_the_end("launch_reply_lost")
        self.assertEqual([], self.failed(record))
        self.assertIn("dagster_run_1", self.world.finished_ids)

    def test_a_launch_that_was_lost_and_cannot_be_found_is_a_failed_cleanup_and_never_a_silent_pass(self) -> None:
        with mock.patch.object(self.module, "LAUNCH_SETTLE_SECONDS", 0.05), mock.patch.object(self.module, "CLEANUP_RUN_SECONDS", 0.3):
            record, _ = self.walk_to_the_end("launch_reply_lost_unfindable")
        self.assertIn("record load (run)", self.failed(record))
        self.assertIn("cleanup (dagster launches)", self.failed(record))

    def test_a_launch_that_cannot_be_looked_up_at_first_is_found_and_ended_by_the_cleanup(self) -> None:
        record, _ = self.walk_to_the_end("launch_reply_lost_once_unfindable")
        failed = self.failed(record)
        self.assertIn("record load (run)", failed)
        self.assertNotIn("cleanup (dagster launches)", failed)
        self.assertIn("dagster_run_1", self.world.terminated)

    def test_a_run_id_that_is_not_the_launched_run_is_never_waited_for_or_terminated_and_the_real_run_is_found_by_its_token(self) -> None:
        record, _ = self.walk_to_the_end("launch_returns_foreign_run")
        self.assertEqual([], self.failed(record))
        self.assertNotIn("foreign_run_1", self.world.finished_ids)
        self.assertNotIn("foreign_run_1", self.world.terminated)
        self.assertIn("dagster_run_1", self.world.finished_ids)

    def test_the_airbyte_cleanup_runs_even_when_the_dagster_cleanup_stops(self) -> None:
        with mock.patch.object(self.module, "reconcile_launches", side_effect=OSError("ssh broke")):
            _, raised = self.walk_to_the_end("retry_wait_fails")
        self.assertIsInstance(raised, OSError)
        self.assertIn("raw_copy cancel-owned", self.world.calls)

    def test_an_airbyte_attempt_that_stays_unresolved_is_a_failed_cleanup(self) -> None:
        self.assertIn("cleanup (airbyte jobs)", self.with_faults("cancel_owned_unresolved"))

    def test_a_run_folder_is_saved_only_on_a_passing_host_verdict_even_if_the_host_sent_a_record(self) -> None:
        self.world.faults = {"folder_other_commit", "folder_leaks_record"}
        record, _, evidence = self.walk()
        self.assertIn("run folders (saved)", self.failed(record))
        self.assertEqual([], sorted(evidence.glob("dagster_run_folder_*")))

    def test_the_error_of_a_run_is_read_only_from_a_run_folder_that_the_host_passed(self) -> None:
        self.world.faults = {"folder_other_run", "folder_leaks_record"}
        record, _, _ = self.walk()
        self.assertIn("planted failed table check (fault)", self.failed(record))

    def test_a_sampler_that_fails_to_start_or_to_be_ready_is_stopped(self) -> None:
        for fault, stops in (("sampler_start_raises", 1), ("sampler_ready_raises", 1), ("second_sampler_start_raises", 2), ("second_sampler_ready_raises", 2)):
            with self.subTest(fault=fault):
                _, raised = self.walk_to_the_end(fault)
                self.assertIsInstance(raised, OSError)
                self.assertEqual(stops, self.world.sampler_stops)

    def test_the_planted_job_stages_the_real_load_record_and_starts_the_table_checks_job_once_on_that_folder(self) -> None:
        record, _, _ = self.walk()
        self.assertEqual([], self.failed(record))
        self.assertEqual({STAGING: LOAD_RECORD}, self.world.staged)
        (job,) = self.world.spark_jobs
        self.assertEqual(run_owner(), owner_of(job))
        self.assertRegex(owner_of(job), r"[0-9a-f]{32}")
        self.assertEqual(job_words(self.module, f"p4_host_{TOKEN}_fault"), job)

    def test_a_container_that_holds_the_slot_before_the_start_refuses_the_planted_job_and_is_left_alone(self) -> None:
        failed = self.with_faults("spark_slot_held")
        self.assertIn("planted failing spark check (fault)", failed)
        self.assertEqual([], self.world.spark_jobs)
        self.assertEqual({"another_launch": "another_owner"}, self.world.containers)
        self.assertEqual([], [call for call in self.world.calls if call.startswith("container_rm")])

    def test_a_refused_staging_starts_no_job_and_the_cleanup_removes_both_folders(self) -> None:
        failed = self.with_faults("stage_refused")
        self.assertIn("planted failing spark check (setup)", failed)
        self.assertNotIn("planted failing spark check (cleanup)", failed)
        self.assertEqual([], self.world.spark_jobs)
        self.assertEqual(set(), self.world.folders)

    def test_a_job_that_passes_its_deadline_has_its_own_container_removed_and_read_back(self) -> None:
        failed = self.with_faults("spark_job_deadline")
        self.assertNotIn("planted failing spark check (cleanup)", failed)
        self.assertEqual(["container_rm this_launch"], [call for call in self.world.calls if call.startswith("container_rm")])
        self.assertEqual({}, self.world.containers)

    def test_a_container_of_another_launch_that_holds_the_slot_is_never_removed(self) -> None:
        failed = self.with_faults("spark_slot_busy")
        self.assertIn("planted failing spark check (fault)", failed)
        self.assertEqual({"another_launch": "another_owner"}, self.world.containers)
        self.assertEqual([], [call for call in self.world.calls if call.startswith("container_rm")])

    def test_the_only_compose_run_of_the_runner_is_the_table_checks_job_with_a_read_only_staging_volume_and_no_inline_script(self) -> None:
        record, _, _ = self.walk()
        self.assertEqual([], self.failed(record))
        self.assertEqual(1, len(self.world.compose_runs), self.world.compose_runs)
        (job,) = self.world.compose_runs
        self.assertEqual(["-m", "spark.jobs.table_checks", "--staging", f"/staging/p4_host_{TOKEN}_fault"], job[-4:])
        self.assertNotIn("-c", job)
        self.assertEqual(["surgical_risk_table_check_staging:/staging:ro"], [job[i + 1] for i, word in enumerate(job) if word == "--volume"])

    def test_the_run_writes_no_catalog_reference_and_deletes_and_puts_nothing_under_the_warehouse_prefix(self) -> None:
        self.world.store.put_object("warehouse/raw_records/lab_data/data_kept.parquet", b"the load's own")
        record, _, _ = self.walk()
        self.assertEqual([], self.failed(record))
        self.assertEqual([], self.world.catalog_writes)
        self.assertEqual([], [key for key, _ in [*self.world.puts, *self.world.deleted] if not key.startswith(LANDING_PREFIX)])


@unittest.skipUnless(RUNNER.is_file(), "the runner is untracked and not on this computer")
class HostCallsTest(unittest.TestCase):
    """The calls of the runner's ``Host`` and of its Nessie client, with the SSH command replaced by a recorder."""

    def setUp(self) -> None:
        self.module = import_runner("ingestion_host")
        self.host = self.module.Host.__new__(self.module.Host)
        self.host.log, self.host.port, self.host.compose, self.host.bucket = [], 3000, ["docker", "compose"], "example-bucket"
        self.sent: list[tuple[list[str], str | None, float]] = []

    def answer(self, code: int, out: str) -> Any:
        def run(args: list[str], *, stdin: str | None = None, timeout: float = 0, quiet: bool = False) -> tuple[int, str, str]:
            self.sent.append((args, stdin, timeout))
            return code, out, ""

        return mock.patch.object(self.host, "run", run)

    def test_a_job_is_launched_with_its_own_name_its_configuration_and_the_run_id_of_the_answer_is_returned(self) -> None:
        body = json.dumps({"data": {"launchRun": {"__typename": "LaunchRunSuccess", "run": {"runId": "abc"}}}})
        with self.answer(0, f"200\n{body}"):
            run_id = self.host.launch("table_checks_job", {"ops": {"x": 1}}, "abcd1234-7")
        self.assertEqual("abc", run_id)
        variables = json.loads(self.sent[0][1] or "{}")["variables"]["params"]
        self.assertEqual({"tags": [{"key": "p4_host_token", "value": "abcd1234-7"}]}, variables["executionMetadata"])
        self.assertEqual({"repositoryLocationName": "surgical_risk", "repositoryName": "__repository__", "jobName": "table_checks_job"}, variables["selector"])
        self.assertEqual({"ops": {"x": 1}}, variables["runConfigData"])
        self.assertIn("3000", self.sent[0][0])

    def test_a_refused_launch_gives_no_run_id(self) -> None:
        with self.answer(0, '200\n{"data": {"launchRun": {"__typename": "RunConfigValidationInvalid"}}}'):
            self.assertEqual("", self.host.launch("table_checks_job", {}, "t-1"))

    def test_an_answer_that_is_not_json_or_not_200_is_an_error_and_never_a_run(self) -> None:
        for code, out in ((0, "200\nnot json"), (0, "500\n{}"), (255, "")):
            with self.subTest(out=out), self.answer(code, out):
                self.assertEqual("", self.host.launch("table_checks_job", {}, "t-1"))

    def test_the_runs_of_a_token_are_looked_up_by_the_tag_and_a_failed_lookup_is_none_and_never_an_empty_list(self) -> None:
        found = {
            "__typename": "Runs",
            "results": [{"runId": "abc", "status": "STARTED", "jobName": "record_load_job", "tags": [{"key": "p4_host_token", "value": "t-1"}]}],
        }
        with self.answer(0, "200\n" + json.dumps({"data": {"runsOrError": found}})):
            runs = self.host.runs_with_token("t-1")
        self.assertEqual(["abc"], [run["runId"] for run in runs or []])
        sent = json.loads(self.sent[0][1] or "{}")
        self.assertEqual({"filter": {"tags": [{"key": "p4_host_token", "value": "t-1"}]}}, sent["variables"])
        with self.answer(0, "200\n" + json.dumps({"data": {"runsOrError": {"__typename": "Runs", "results": []}}})):
            self.assertEqual([], self.host.runs_with_token("t-1"))
        for code, out in ((0, "200\n" + json.dumps({"data": {"runsOrError": {"__typename": "PythonError", "message": "x"}}})), (0, "500\n{}"), (255, "")):
            with self.subTest(out=out[:30]), self.answer(code, out):
                self.assertIsNone(self.host.runs_with_token("t-1"))

    def test_a_command_of_the_container_that_prints_no_json_gives_an_empty_answer_and_its_error_text(self) -> None:
        def in_code(args: list[str], **options: Any) -> tuple[int, str, str]:
            self.sent.append((args, None, 0))
            return 1, "not json", "raw_copy refused: selection_differs\n"

        with mock.patch.object(self.host, "in_code", in_code):
            self.assertEqual((1, {}, "raw_copy refused: selection_differs"), self.host.raw_copy("start"))
            self.host.raw_copy("status")
            self.host.raw_copy("cancel", "--job", "3")
        self.assertEqual(["python", "-m", "scripts.ingestion.raw_copy", "start", "--bucket", "example-bucket"], self.sent[0][0])
        self.assertEqual(["python", "-m", "scripts.ingestion.raw_copy", "status", "--bucket", "example-bucket"], self.sent[1][0])
        self.assertEqual(["python", "-m", "scripts.ingestion.raw_copy", "cancel", "--job", "3"], self.sent[2][0])

    def test_nessie_is_reached_with_curl_inside_its_container_and_the_status_is_read_from_the_last_line(self) -> None:
        client = self.module.ExecNessie(self.host)
        with self.answer(0, '{"hash": "x"}\n200'):
            status, body = client.send("POST", "/api/v2/trees/main", b'{"a": 1}')
        self.assertEqual((200, b'{"hash": "x"}'), (status, body))
        command, stdin, _ = self.sent[0]
        self.assertEqual(["docker", "compose", "exec", "-T", "nessie", "curl"], command[:6])
        self.assertEqual('{"a": 1}', stdin)
        self.assertIn("http://127.0.0.1:19120/api/v2/trees/main", command)

    def test_a_curl_that_fails_or_times_out_is_an_error_the_client_turns_into_a_nessie_error(self) -> None:
        client = self.module.ExecNessie(self.host)
        with self.answer(28, ""), self.assertRaises(TimeoutError):
            client.send("GET", "/api/v2/config", None)
        with self.answer(7, ""), self.assertRaises(OSError):
            client.send("GET", "/api/v2/config", None)

    def run_finished(self, answers: list[tuple[float, str]], deadline: float) -> tuple[dict[str, Any] | None, list[float]]:
        """``finished`` with a clock: each read takes the seconds of its answer and says its status. Returns the facts and the timeout of each read."""
        clock = FakeClock()
        timeouts: list[float] = []
        queue = list(answers)

        def run(args: list[str], *, stdin: str | None = None, timeout: float = 0, quiet: bool = False) -> tuple[int, str, str]:
            timeouts.append(timeout)
            took, status = queue.pop(0) if len(queue) > 1 else queue[0]
            clock.now += took
            body = json.dumps({"data": {"runOrError": {"__typename": "Run", "runId": "abc", "status": status, "tags": []}}})
            return 0, f"200\n{body}", ""

        with (
            mock.patch.object(self.host, "run", run),
            mock.patch.object(self.module.time, "monotonic", clock.time),
            mock.patch.object(self.module.time, "sleep", clock.sleep),
        ):
            return self.host.finished("abc", deadline), timeouts

    def test_a_run_that_ends_in_time_gives_its_facts_and_every_read_gets_only_the_time_that_is_left(self) -> None:
        found, timeouts = self.run_finished([(1, "STARTED"), (1, "STARTED"), (1, "SUCCESS")], deadline=100)
        self.assertEqual("SUCCESS", (found or {}).get("status"))
        self.assertTrue(all(0 < timeout <= 100 for timeout in timeouts), timeouts)
        found, timeouts = self.run_finished([(50, "STARTED")] * 3, deadline=60)
        self.assertIsNone(found)
        self.assertTrue(all(timeout <= 60 for timeout in timeouts), timeouts)
        self.assertLessEqual(timeouts[-1], 10)

    def test_a_success_that_is_read_after_the_deadline_is_not_a_success(self) -> None:
        found, _ = self.run_finished([(90, "SUCCESS")], deadline=60)
        self.assertIsNone(found)

    def test_a_run_is_terminated_with_its_own_id_and_only_an_accepted_termination_counts(self) -> None:
        body = json.dumps({"data": {"terminateRun": {"__typename": "TerminateRunSuccess", "run": {"runId": "abc"}}}})
        with self.answer(0, f"200\n{body}"):
            self.assertTrue(self.host.terminate("abc"))
        self.assertEqual({"id": "abc"}, json.loads(self.sent[0][1] or "{}")["variables"])
        self.assertIn("terminateRun", json.loads(self.sent[0][1] or "{}")["query"])
        refused = json.dumps({"data": {"terminateRun": {"__typename": "TerminateRunFailure", "message": "x"}}})
        with self.answer(0, f"200\n{refused}"):
            self.assertFalse(self.host.terminate("abc"))
        with self.answer(255, ""):
            self.assertFalse(self.host.terminate("abc"))


@unittest.skipUnless(RUNNER.is_file(), "the runner is untracked and not on this computer")
class HostScriptsTest(unittest.TestCase):
    """The scripts that run on the host (the code location container), executed here against files: they decide what leaves the host."""

    def setUp(self) -> None:
        self.module = import_runner("ingestion_host")
        self.host = self.module.Host.__new__(self.module.Host)
        self.host.log, self.host.port, self.host.compose, self.host.bucket = [], 3000, ["docker", "compose"], "example-bucket"
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.folder = Path(folder.name)

        def in_code(args: list[str], *, stdin: str | None = None, timeout: float = 0, quiet: bool = False) -> tuple[int, str, str]:
            command = [sys.executable, "-I", *args[1:]] if args[0] == "python" else args
            result = subprocess.run(command, input=stdin, capture_output=True, text=True, timeout=30, check=False)  # noqa: S603
            return result.returncode, result.stdout, result.stderr

        patcher = mock.patch.object(self.host, "in_code", in_code)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_a_receipt_leaves_the_host_as_check_names_and_pass_flags_and_two_hashes_only(self) -> None:
        receipt = {
            "format": "x",
            "manifest": {"bucket": "b", "key": "k", "version_id": "v", "sha256": "d" * 64},
            "passed": True,
            "merged": True,
            "merged_hash": "e" * 64,
            "main_hash_before": "f" * 64,
            "notes": ["a note with 6090 rows"],
            "checked": {
                "tables": [
                    {"table": "track_list", "rows": 6090, "checks": [{"name": "row count", "passed": True, "detail": "row_count 6090 6090", "rows": 6090}]},
                    {"table": "clinical_data", "checks": [{"name": "columns", "passed": False, "detail": "marker_secret_value"}]},
                ]
            },
        }
        path = self.folder / "table_checks.json"
        path.write_text(json.dumps(receipt))
        flags = self.host.receipt_flags(str(path))
        self.assertEqual(
            {
                "passed": True,
                "merged": True,
                "merged_hash": "e" * 64,
                "main_hash_before": "f" * 64,
                "tables": [
                    {"table": "track_list", "checks": [{"name": "row count", "passed": True}]},
                    {"table": "clinical_data", "checks": [{"name": "columns", "passed": False}]},
                ],
            },
            flags,
        )
        self.assertNotIn("6090", json.dumps(flags))
        self.assertNotIn("marker_secret_value", json.dumps(flags))

    def test_a_receipt_of_another_shape_is_refused_and_prints_no_text_of_the_file(self) -> None:
        for text in (
            "not json",
            json.dumps({"passed": True}),
            json.dumps({"passed": True, "merged": True, "checked": {"tables": [{"table": "x" * 90, "checks": []}]}}),
        ):
            with self.subTest(text=text[:20]):
                path = self.folder / "bad.json"
                path.write_text(text)
                self.assertIsNone(self.host.receipt_flags(str(path)))
        self.assertIsNone(self.host.receipt_flags(str(self.folder / "missing.json")))

    def make_folder(self, folder_run: str, **changes: Any) -> None:
        folder = self.folder / f"20261125T000000Z_{folder_run}"
        folder.mkdir()
        document: dict[str, Any] = {name: "x" for name in self.module.REQUIRED_FIELDS} | {"run_id": folder_run, "job_name": "record_load_job", "commit": COMMIT}
        document |= {"tags": {"secret": "extra field"}, "recorded_utc": "x"}
        document |= changes
        (folder / "record.json").write_text(json.dumps(document))

    RUN = "11111111-2222-3333-4444-555555555555"

    def folder_verdict(self, **overrides: Any) -> dict[str, Any]:
        arguments = {"run_id": self.RUN, "job": "record_load_job", "commit": COMMIT} | overrides
        with mock.patch.object(self.module, "RUNS_ON_HOST", str(self.folder)):
            verdict: dict[str, Any] = self.host.checked_folder(arguments["run_id"], arguments["job"], arguments["commit"], budget=0)
        return verdict

    def test_a_run_folder_of_the_launched_run_and_the_deployed_commit_leaves_with_the_allowed_fields_only(self) -> None:
        self.make_folder(self.RUN)
        verdict = self.folder_verdict()
        self.assertEqual((True, []), (verdict["ok"], verdict["problems"]))
        self.assertEqual(set(self.module.REQUIRED_FIELDS), set(verdict["record"]))

    def test_each_fault_of_a_run_folder_is_named(self) -> None:
        cases = {
            "run_id_differs": ({"run_id": "99999999-2222-3333-4444-555555555555"}, "record_load_job"),
            "commit_differs": ({"commit": "2" * 40}, "record_load_job"),
            "job_differs": ({}, "table_checks_job"),
        }
        for problem, (changes, job) in cases.items():
            with self.subTest(problem=problem):
                for entry in self.folder.iterdir():
                    shutil.rmtree(entry)
                self.make_folder(self.RUN, **changes)
                verdict = self.folder_verdict(job=job)
                self.assertFalse(verdict["ok"])
                self.assertIn(problem, verdict["problems"])
        for entry in self.folder.iterdir():
            shutil.rmtree(entry)
        self.make_folder(self.RUN)
        document = json.loads(next(self.folder.glob("*/record.json")).read_text())
        document.pop("outputs")
        next(self.folder.glob("*/record.json")).write_text(json.dumps(document))
        self.assertIn("fields_missing", self.folder_verdict()["problems"])

    def test_a_secret_shaped_text_leaves_no_record_and_none_of_its_text(self) -> None:
        self.make_folder(self.RUN, outputs={"error": "client_secret=abc"})
        verdict = self.folder_verdict()
        self.assertFalse(verdict["ok"])
        self.assertIn("secret_shaped_text", verdict["problems"])
        self.assertIsNone(verdict["record"])
        self.assertNotIn("abc", json.dumps(verdict))

    def test_no_folder_or_two_folders_of_one_run_are_a_problem(self) -> None:
        self.assertIn("no_folder", self.folder_verdict()["problems"])
        self.make_folder(self.RUN)
        other = self.folder / f"20261126T000000Z_{self.RUN}"
        other.mkdir()
        (other / "record.json").write_text("{}")
        self.assertIn("no_folder", self.folder_verdict()["problems"])

    def test_a_refused_run_folder_shows_only_fixed_problem_words_and_none_of_its_content(self) -> None:
        marker = "marker_outputs_text_9f2c"
        for changes, job in (
            ({"run_id": "99999999-2222-3333-4444-555555555555"}, "record_load_job"),
            ({"commit": "2" * 40}, "record_load_job"),
            ({}, "table_checks_job"),
        ):
            with self.subTest(changes=changes, job=job):
                for entry in self.folder.iterdir():
                    shutil.rmtree(entry)
                self.make_folder(self.RUN, outputs={"error": marker}, **changes)
                with mock.patch.object(self.module, "RUNS_ON_HOST", str(self.folder)):
                    code, out, _ = self.host.in_code(
                        ["python", "-c", self.module.FOLDER_SCRIPT, str(self.folder), self.RUN, job, COMMIT, ",".join(sorted(self.module.REQUIRED_FIELDS))]
                    )
                self.assertEqual(0, code)
                answer = json.loads(out)
                self.assertFalse(answer["ok"])
                self.assertIsNone(answer["record"])
                self.assertNotIn(marker, out)
                self.assertTrue(all(re.fullmatch(r"[a-z_]+", word) for word in answer["problems"]))
        for entry in self.folder.iterdir():
            shutil.rmtree(entry)
        document = {name: "x" for name in self.module.REQUIRED_FIELDS} | {
            "run_id": self.RUN,
            "job_name": "record_load_job",
            "commit": COMMIT,
            "outputs": {"error": marker},
        }
        document.pop("limits")
        self.make_folder(self.RUN, outputs={"error": marker})
        next(self.folder.glob("*/record.json")).write_text(json.dumps(document))
        verdict = self.folder_verdict()
        self.assertIn("fields_missing", verdict["problems"])
        self.assertIsNone(verdict["record"])

    def test_a_receipt_with_a_table_or_a_check_name_that_the_required_checks_do_not_hold_is_refused_on_the_host(self) -> None:
        def receipt(table: str, check: str) -> str:
            return json.dumps({"passed": True, "merged": True, "checked": {"tables": [{"table": table, "checks": [{"name": check, "passed": True}]}]}})

        for table, check in (
            ("clinical_data", "six thousand ninety"),
            ("clinical_data", "rows digest"),
            ("another_table", "columns"),
            ("track list", "columns"),
        ):
            with self.subTest(table=table, check=check):
                path = self.folder / "unknown.json"
                path.write_text(receipt(table, check))
                self.assertIsNone(self.host.receipt_flags(str(path)))
        path = self.folder / "known.json"
        path.write_text(receipt("track_list", "rows digest"))
        self.assertEqual("rows digest", self.host.receipt_flags(str(path))["tables"][0]["checks"][0]["name"])

    @unittest.skipIf(os.geteuid() == 0, "root reads every folder")
    def test_a_permission_error_or_a_path_under_a_file_is_unreadable_and_never_absent(self) -> None:
        (self.folder / "locked").mkdir()
        (self.folder / "locked" / "child").write_text("x")
        (self.folder / "locked" / "folder").mkdir()
        (self.folder / "file").write_text("x")
        (self.folder / "locked").chmod(0)
        self.addCleanup((self.folder / "locked").chmod, 0o700)
        for path in (self.folder / "locked" / "child", self.folder / "locked" / "folder", self.folder / "file" / "child"):
            with self.subTest(path=path.name):
                self.assertIsNone(self.host.exists(str(path)))
                self.assertIsNone(self.host.entries(str(path)))
                self.assertFalse(self.host.remove(str(path), tree=True))
        self.assertIs(False, self.host.exists(str(self.folder / "missing")))
        self.assertEqual(0, self.host.entries(str(self.folder / "missing")))

    def test_a_folder_is_made_exclusively_and_one_that_exists_in_any_form_is_refused(self) -> None:
        path = self.folder / "made"
        self.assertTrue(self.host.make_folder(str(path)))
        self.assertTrue(path.is_dir())
        (path / "mine").write_text("x")
        self.assertFalse(self.host.make_folder(str(path)))
        self.assertEqual("x", (path / "mine").read_text())
        (self.folder / "file").write_text("x")
        self.assertFalse(self.host.make_folder(str(self.folder / "file")))
        self.assertFalse(self.host.make_folder(str(self.folder / "no" / "parent")))

    def test_a_tree_removal_refuses_what_is_not_a_folder_and_reads_a_missing_one_as_done(self) -> None:
        (self.folder / "file").write_text("x")
        self.assertFalse(self.host.remove(str(self.folder / "file"), tree=True))
        self.assertTrue((self.folder / "file").exists())
        self.assertTrue(self.host.remove(str(self.folder / "missing"), tree=True))

    def test_a_readback_of_a_path_gives_present_absent_or_unknown_and_never_absent_for_a_failure(self) -> None:
        present = self.folder / "a"
        present.write_text("x")
        self.assertIs(True, self.host.exists(str(present)))
        self.assertIs(False, self.host.exists(str(self.folder / "b")))
        with mock.patch.object(self.host, "in_code", lambda *a, **k: (1, "", "")):
            self.assertIsNone(self.host.exists(str(present)))
            self.assertIsNone(self.host.entries(str(present)))
        with mock.patch.object(self.host, "in_code", lambda *a, **k: (0, "weird", "")):
            self.assertIsNone(self.host.exists(str(present)))

    def test_entries_counts_a_folder_and_reads_a_missing_one_as_empty_and_a_file_as_unknown(self) -> None:
        (self.folder / "x").mkdir()
        (self.folder / "x" / "one").write_text("1")
        self.assertEqual(1, self.host.entries(str(self.folder / "x")))
        self.assertEqual(0, self.host.entries(str(self.folder / "missing")))
        (self.folder / "file").write_text("1")
        self.assertIsNone(self.host.entries(str(self.folder / "file")))

    def test_a_new_file_is_created_exclusively_and_an_existing_one_is_never_replaced_or_removed_by_the_failed_create(self) -> None:
        path = self.folder / "new.json"
        self.assertTrue(self.host.write_new(str(path), "first"))
        self.assertFalse(self.host.write_new(str(path), "second"))
        self.assertEqual("first", path.read_text())
        self.assertEqual("first", self.host.read_text(str(path)))
        self.assertEqual("first", self.host.first_line(str(path)))
        self.assertTrue(self.host.remove(str(path)))
        self.assertFalse(path.exists())
        (self.folder / "tree").mkdir()
        (self.folder / "tree" / "f").write_text("1")
        self.assertTrue(self.host.remove(str(self.folder / "tree"), tree=True))
        self.assertFalse((self.folder / "tree").exists())


class PureTest(unittest.TestCase):
    def setUp(self) -> None:
        if not RUNNER.is_file():
            self.skipTest("the runner is untracked and not on this computer")
        self.module = import_runner("ingestion_host")

    def test_a_disk_reading_is_valid_only_as_two_whole_numbers_with_free_space_inside_a_positive_size(self) -> None:
        self.assertEqual((123, 456), self.module.parse_disk("123 456"))
        for text in ("unreadable", "", "1 2 3", "x 5", "500 100", "-1 5", "5 0"):
            with self.subTest(text=text):
                self.assertIsNone(self.module.parse_disk(text))

    def window(self, rows: list[tuple[int, int]], *, end: bool = True, kinds: tuple[str, ...] = ()) -> str:
        lines = [{"kind": "header", "schema": 1}]
        lines += [{"kind": "sample", "seq": n, "missing": [], "summary": {"wall_ns_before": a, "wall_ns_after": b}} for n, (a, b) in enumerate(rows)]
        if end:
            lines.append({"kind": "end", "reason": "stop_flag", "samples": len(rows)})
        return "".join(json.dumps(line) + "\n" for line in lines)

    def test_samples_cover_a_sync_only_from_before_its_start_to_after_its_end_with_no_gap_longer_than_the_allowed(self) -> None:
        second = 1_000_000_000
        rows = [(0, second), (5 * second, 6 * second), (10 * second, 11 * second)]
        self.assertTrue(self.module.covers(self.window(rows), 2 * second, 10 * second, 5))
        self.assertFalse(self.module.covers(self.window(rows), -1, 10 * second, 5), "the first sample starts after the sync began")
        self.assertFalse(self.module.covers(self.window(rows), 2 * second, 12 * second, 5), "the last sample ended before the sync did")
        gappy = [(0, second), (60 * second, 61 * second)]
        self.assertFalse(self.module.covers(self.window(gappy), second, 60 * second, 5), "a gap longer than three intervals")
        self.assertFalse(self.module.covers(self.window(rows, end=False), 2 * second, 10 * second, 5), "a file with no end line was cut off")
        self.assertFalse(self.module.covers("not json\n", 0, 1, 5))
        self.assertFalse(self.module.covers(self.window([]), 0, 1, 5))

    def test_the_failed_check_refusal_is_exact_only_for_the_one_finding_and_the_fixed_summary(self) -> None:
        key = f"{SOURCE_PREFIX}vital_files/0001.vital"
        kind = "faults of kind checksum_differs: 1"
        good = f"checked 2 of 2 files\n{key}: checksum_differs\n{kind}\ncheck failed: 1 faults in 2 selected files; no manifest may be published"
        self.assertTrue(self.module.exact_refusal(good, [(key, "checksum_differs")], 2))
        for wrong in (good.replace(kind + "\n", ""), good.replace(kind, "faults of kind missing: 1"), good.replace(kind, kind + "\n" + kind)):
            with self.subTest(wrong=wrong):
                self.assertFalse(self.module.exact_refusal(wrong, [(key, "checksum_differs")], 2))
        for text in (
            good.replace("checksum_differs", "missing"),
            good.replace("1 faults", "2 faults"),
            good + f"\n{key}x: checksum_differs",
            good + "\na line that the check never prints",
            good.replace("\ncheck failed: 1 faults in 2 selected files; no manifest may be published", ""),
            "",
        ):
            with self.subTest(text=text[:40]):
                self.assertFalse(self.module.exact_refusal(text, [(key, "checksum_differs")], 2))

    def test_the_lab_fault_is_exact_only_for_the_full_check_list_of_every_table_with_only_the_three_laboratory_row_checks_failing(self) -> None:
        fault = good_tables(failed=tuple(("lab_data", name) for name in LAB_FAULT))
        self.assertEqual((True, []), self.module.exact_lab_fault(fault))
        empty_clinical = good_tables(failed=tuple(("lab_data", name) for name in LAB_FAULT))
        empty_clinical[0]["checks"] = []
        short_track = good_tables(failed=tuple(("lab_data", name) for name in LAB_FAULT))
        short_track[2]["checks"] = short_track[2]["checks"][:1]
        short_lab = good_tables(failed=(("lab_data", "columns"),))
        short_lab[1]["checks"] = short_lab[1]["checks"][:3]
        for name, tables in (
            ("nothing failed", good_tables()),
            ("a clinical check failed", good_tables(failed=(*[("lab_data", n) for n in LAB_FAULT], ("clinical_data", "row count")))),
            ("a track list check failed", good_tables(failed=(*[("lab_data", n) for n in LAB_FAULT], ("track_list", "snapshot")))),
            ("only the payload check failed", good_tables(failed=(("lab_data", "row payload"),))),
            ("the columns check failed as well", good_tables(failed=(("lab_data", "columns"), *[("lab_data", n) for n in LAB_FAULT]))),
            ("the snapshot check failed as well", good_tables(failed=(("lab_data", "snapshot"), *[("lab_data", n) for n in LAB_FAULT]))),
            ("the columns check failed alone and the rest are absent", short_lab),
            ("clinical checks absent", empty_clinical),
            ("track checks cut short", short_track),
            ("a table missing", good_tables(failed=tuple(("lab_data", n) for n in LAB_FAULT))[1:]),
            ("no tables", []),
        ):
            with self.subTest(name):
                self.assertFalse(self.module.exact_lab_fault(tables)[0])

    def test_the_publish_refusal_is_exact_only_for_the_fixed_text_alone(self) -> None:
        self.assertTrue(self.module.exact_publish_refusal("the check did not pass\n"))
        self.assertTrue(self.module.exact_publish_refusal("the check did not pass"))
        for text in ("", "the check did not pass\nand a detail", "before: the check did not pass", "the check did not pass again", "something else"):
            with self.subTest(text=text):
                self.assertFalse(self.module.exact_publish_refusal(text))

    def test_the_check_set_is_exact_only_for_every_table_in_order_with_its_required_checks_once_each_and_all_passed(self) -> None:
        ok, problems = self.module.exact_check_set(good_tables())
        self.assertEqual((True, []), (ok, problems))
        duplicated = good_tables()
        duplicated[0]["checks"].append(dict(duplicated[0]["checks"][0]))
        self.assertFalse(self.module.exact_check_set(duplicated)[0])
        self.assertFalse(self.module.exact_check_set(good_tables()[:2])[0])
        self.assertFalse(self.module.exact_check_set(good_tables(failed=(("lab_data", "columns"),)))[0])
        self.assertFalse(self.module.exact_check_set([])[0])
        renamed = good_tables()
        renamed[2]["checks"][0]["name"] = "snapshots"
        self.assertFalse(self.module.exact_check_set(renamed)[0])


LAB = b"caseid,dt,name,result\r\n7,5,alb,2.9\r\n8,6,cr,1.1\r\n"
LAB_ONE_ROW_MORE = LAB + b"7,5,alb,2.9\r\n"
CLINICAL = b"caseid,age\r\n7,60\r\n8,\r\n"


def host_script(script: str, arguments: list[str], stdin: str = "") -> tuple[int, str]:
    """Run a script that the runner sends to the code location, in this process (so that the project's store can be a stand-in): its exit code and output."""
    out = io.StringIO()
    code = 0
    with mock.patch.object(sys, "argv", ["-c", *arguments]), mock.patch.object(sys, "stdin", io.StringIO(stdin)), contextlib.redirect_stdout(out):
        try:
            exec(compile(script, "<host script>", "exec"), {"__name__": "__main__"})  # noqa: S102 - the runner's own script text
        except SystemExit as stopped:
            code = int(stopped.code or 0)
    return code, out.getvalue()


class Planted:
    """A published manifest of the two tables in a stand-in store, the load record of a load of it, and an empty staging folder that the test made."""

    def __init__(self, test: unittest.TestCase, *, lab: bytes = LAB) -> None:
        source = {"vital_files/0001.vital": b"a recording", "clinical_data.csv": CLINICAL, "lab_data.csv": lab}
        test.enterContext(synthetic_pin(list_text_for(source).encode()))
        folder = tempfile.TemporaryDirectory(prefix="ingestion_host_planted_")
        test.addCleanup(folder.cleanup)
        root = Path(folder.name)
        self.staging = root / "staging"
        self.staging.mkdir(mode=0o700)
        self.results = root / "results"
        self.landed: Landed = landed_set(source)
        self.reference = publish_manifest(self.landed.store, self.landed.selection, self.landed.check())
        self.record = checks.LoadRecord("load_run_one", "c" * 64, self.reference, dict(SNAPSHOTS), "a" * 64)
        self.record_path = root / "run_one.load_record.json"
        document = {
            "format": checks.LOAD_RECORD_FORMAT,
            "branch": self.record.branch,
            "branch_hash": self.record.branch_hash,
            "base_hash": self.record.base_hash,
            "manifest": json.loads(self.reference.to_json()),
            "tables": SNAPSHOTS,
        }
        self.record_path.write_text(json.dumps(document))
        real_secure = checks.secure
        # This computer's user is in no group 10001, so the staged files take the test's own group; their mode is the controller's.
        test.enterContext(mock.patch.object(checks, "secure", lambda path, mode, gid: real_secure(path, mode, os.getgid())))
        test.enterContext(mock.patch("scripts.ingestion.record_run.project_store", lambda environment: self.landed.store))

    def stage(self, module: ModuleType) -> tuple[int, str]:
        return host_script(module.STAGE_PLANTED_LAB_SCRIPT, [str(self.record_path), str(self.staging)])

    def staged_files(self) -> list[str]:
        return sorted(path.name for path in self.staging.iterdir()) if self.staging.is_dir() else []


@unittest.skipUnless(RUNNER.is_file(), "the runner is untracked and not on this computer")
class PlantedStagingTest(unittest.TestCase):
    """The staging script of the planted case, run for real: the controller's own functions build the folder, and only the laboratory copy is changed."""

    def setUp(self) -> None:
        self.module = import_runner("ingestion_host")

    def test_the_folder_is_the_controllers_own_with_one_laboratory_row_duplicated_and_a_plan_that_binds_the_changed_copy(self) -> None:
        planted = Planted(self)
        with (
            mock.patch.object(checks, "stage_verified_files", wraps=checks.stage_verified_files) as staged,
            mock.patch.object(checks, "build_plan", wraps=checks.build_plan) as built,
            mock.patch.object(checks, "write_plan", wraps=checks.write_plan) as written,
        ):
            code, out = planted.stage(self.module)
        self.assertEqual((0, "staged"), (code, out))
        self.assertEqual((1, 1, 1), (staged.call_count, built.call_count, written.call_count), "the folder is built by the controller's own functions")
        self.assertEqual(["clinical_data.csv", "lab_data.csv", "plan.json"], planted.staged_files())
        self.assertEqual(CLINICAL, (planted.staging / "clinical_data.csv").read_bytes())
        self.assertEqual(LAB_ONE_ROW_MORE, (planted.staging / "lab_data.csv").read_bytes())
        self.assertEqual({0o640}, {path.stat().st_mode & 0o777 for path in planted.staging.iterdir()})
        self.assertEqual(0o750, planted.staging.stat().st_mode & 0o777)
        plan = rules.parse_plan((planted.staging / "plan.json").read_text())
        files = [
            checks.Staged("clinical_data", "clinical_data.csv", digest(CLINICAL), len(CLINICAL)),
            checks.Staged("lab_data", "lab_data.csv", digest(LAB_ONE_ROW_MORE), len(LAB_ONE_ROW_MORE)),
        ]
        expected = checks.build_plan(planted.record, files, checks.read_track_pin(), rules.EMPTY_FORM, plan.run_id)
        self.assertEqual(expected, json.loads((planted.staging / "plan.json").read_text()))

    def test_a_substituted_object_version_is_refused_by_the_gate_and_nothing_stays_staged(self) -> None:
        planted = Planted(self)
        planted.landed.store.put_object(planted.landed.keys["lab_data.csv"], LAB)
        code, out = planted.stage(self.module)
        self.assertEqual((1, "refused"), (code, out))
        self.assertEqual([], planted.staged_files())

    def test_a_laboratory_file_without_a_whole_first_row_is_refused_and_nothing_stays_staged(self) -> None:
        for name, lab in (
            ("no row", b"caseid,dt,name,result\r\n"),
            ("blank first row", b"caseid,dt,name,result\r\n\r\n7,5,alb,2.9\r\n"),
            ("an open quoted field", b'caseid,dt,name,result\r\n7,5,"al\r\nb",2.9\r\n'),
        ):
            with self.subTest(name):
                planted = Planted(self, lab=lab)
                self.assertEqual((1, "refused"), planted.stage(self.module))
                self.assertEqual([], planted.staged_files())

    def test_a_last_row_without_a_line_end_is_closed_before_the_copy_is_added(self) -> None:
        planted = Planted(self, lab=b"caseid,dt,name,result\n7,5,alb,2.9")
        self.assertEqual((0, "staged"), planted.stage(self.module))
        self.assertEqual(b"caseid,dt,name,result\n7,5,alb,2.9\n7,5,alb,2.9\n", (planted.staging / "lab_data.csv").read_bytes())


def producer_results(plan: rules.Plan, *, lab: str) -> list[rules.TableResult]:
    """The results that ``spark.jobs.table_checks.check_table`` makes, built with the same ``rules`` calls and in its order. ``lab`` is ``rows`` for a lab
    table of the same schema whose rows differ (all six checks run and the three row checks fail) or ``columns`` for another schema (the job stops at the
    failed columns check, so the checks are a prefix)."""
    results = []
    for entry in plan.tables:
        spec = rules.SPEC_BY_NAME[entry.name]
        result = rules.TableResult(entry.name, entry.snapshot_id)
        names = ["a", "b"] if spec.source == "file" else list(spec.pinned_columns)
        same = [(name, "string") for name in names]
        if spec.source == "file":
            result.columns_source = result.columns_table = len(names)
            result.add("staged file", rules.detail("ok"))
            result.add("snapshot", rules.detail("ok"))
            if entry.name == "lab_data" and lab == "columns":
                result.columns_table = 2
                result.add("columns", rules.column_detail(names, [("x", "string"), ("y", "string")]))
                results.append(result)
                continue
            result.add("columns", rules.column_detail(names, same))
            equal = int(not (entry.name == "lab_data" and lab == "rows"))
            for check in ("row count", "key multiset", "row payload"):
                result.add(check, rules.detail("table_compared", equal=equal))
        else:
            result.columns_source = result.columns_table = len(names)
            result.rows_source = result.rows_table = entry.rows
            result.add("snapshot", rules.detail("ok"))
            result.add("columns", rules.column_detail(names, same))
            result.add("row count", rules.detail("row_count", source=entry.rows, table=entry.rows))
            result.add("rows digest", rules.detail("digest_compared", equal=1))
        results.append(result)
    return results


@unittest.skipUnless(RUNNER.is_file(), "the runner is untracked and not on this computer")
class ProducerReceiptTest(unittest.TestCase):
    """The planted case from the staged folder to the flags: the runner's staging script, the job's result made by its own rules, the runner's judge script
    (the controller's parse, refusal record and receipt), the runner's host script that selects the flags, and ``exact_lab_fault``."""

    def setUp(self) -> None:
        self.module = import_runner("ingestion_host")
        self.planted = Planted(self)
        self.assertEqual((0, "staged"), self.planted.stage(self.module))
        self.plan = rules.parse_plan((self.planted.staging / "plan.json").read_text())
        self.run_folder = self.planted.results / self.planted.reference.sha256
        self.host = self.module.Host.__new__(self.module.Host)
        self.host.log, self.host.port, self.host.compose, self.host.bucket = [], 3000, ["docker", "compose"], "example-bucket"

        def in_code(args: list[str], *, stdin: str | None = None, timeout: float = 0, quiet: bool = False) -> tuple[int, str, str]:
            result = subprocess.run([sys.executable, "-I", *args[1:]], input=stdin, capture_output=True, text=True, timeout=30, check=False)  # noqa: S603
            return result.returncode, result.stdout, result.stderr

        self.enterContext(mock.patch.object(self.host, "in_code", in_code))

    def judge(self, stdout: str, exit_code: int) -> tuple[int, str]:
        arguments = [str(self.planted.record_path), str(self.planted.staging), str(self.run_folder)]
        return host_script(self.module.JUDGE_TABLE_CHECK_SCRIPT, arguments, json.dumps({"exit_code": exit_code, "stdout": stdout}))

    def printed(self, lab: str) -> str:
        return "a log line of Spark\nRESULT " + json.dumps(rules.job_document(self.plan, producer_results(self.plan, lab=lab))) + "\n"

    def flags(self, lab: str) -> dict[str, Any]:
        self.assertEqual((0, "judged"), self.judge(self.printed(lab), 1))
        found = self.host.receipt_flags(str(self.run_folder / checks.RESULT_FILE))
        self.assertIsNotNone(found)
        return found or {}

    def test_a_laboratory_table_of_the_same_schema_with_other_rows_gives_the_full_check_list_and_is_the_intended_negative_result(self) -> None:
        found = self.flags("rows")
        self.assertEqual((False, False), (found["passed"], found["merged"]))
        self.assertEqual((True, []), self.module.exact_lab_fault(found["tables"]))
        lab = next(table for table in found["tables"] if table["table"] == "lab_data")
        self.assertEqual(
            list(LAB_FAULT), [check["name"] for check in lab["checks"] if not check["passed"]], "the staged file, the snapshot and the columns passed"
        )
        self.assertEqual("refusal: checks_failed", (self.run_folder / checks.JOB_OUTPUT_FILE).read_text().splitlines()[0])

    def test_a_laboratory_table_of_another_schema_stops_at_the_columns_check_and_is_not_the_intended_negative_result(self) -> None:
        found = self.flags("columns")
        lab = next(table for table in found["tables"] if table["table"] == "lab_data")
        self.assertEqual(["staged file", "snapshot", "columns"], [check["name"] for check in lab["checks"]])
        verdict, problems = self.module.exact_lab_fault(found["tables"])
        self.assertFalse(verdict)
        self.assertIn("checks_differ:lab_data", problems)

    def test_a_reply_that_the_controller_refuses_leaves_no_receipt_and_says_only_that_it_was_refused(self) -> None:
        for name, stdout, exit_code in (
            ("no result line", "Spark stopped, 4719238 rows\n", 1),
            ("an exit code that disagrees", self.printed("rows"), 0),
            ("a result that is not of the plan", 'RESULT {"rows": 4719238}\n', 1),
        ):
            with self.subTest(name):
                self.assertEqual((1, "result_refused"), self.judge(stdout, exit_code))
                self.assertFalse((self.run_folder / checks.RESULT_FILE).exists())
                self.assertNotIn("4719238", (self.run_folder / checks.JOB_OUTPUT_FILE).read_text())

    def test_an_input_that_cannot_be_read_says_only_that_the_judge_failed(self) -> None:
        arguments = [str(self.planted.record_path), str(self.planted.staging / "missing"), str(self.run_folder)]
        self.assertEqual((1, "judge_failed"), host_script(self.module.JUDGE_TABLE_CHECK_SCRIPT, arguments, '{"exit_code": 1, "stdout": ""}'))


@unittest.skipUnless(RUNNER.is_file(), "the runner is untracked and not on this computer")
class PlantedJobTest(unittest.TestCase):
    """The launch of the planted job: its arguments against the controller's own, the host script that keeps the job's output on the host (real processes) and
    the removal of the job's container."""

    def setUp(self) -> None:
        self.module = import_runner("ingestion_host")

    def test_the_launch_names_the_bronze_service_and_profile_the_slot_this_runs_owner_the_user_and_a_read_only_staging_volume(self) -> None:
        self.assertEqual(
            [
                "docker",
                "compose",
                "--profile",
                "batch_bronze",
                "run",
                "--no-TTY",
                "--no-deps",
                "--name",
                SLOT_NAME,
                "--label",
                f"{OWNER_LABEL}={run_owner()}",
                "--user",
                "10100:10001",
                "--volume",
                "surgical_risk_table_check_staging:/staging:ro",
                "--entrypoint",
                "python",
                "spark_bronze",
                "-m",
                "spark.jobs.table_checks",
                "--staging",
                "/staging/folder_1",
            ],
            job_words(self.module, "folder_1"),
        )

    def test_apart_from_the_owner_and_the_entry_point_the_launch_is_the_controllers_own_one_off(self) -> None:
        seen: list[list[str]] = []

        def docker(program: str, arguments: list[str], **options: Any) -> CompletedProcess[str]:
            if "run" in arguments and "compose" in arguments:
                seen.append(list(arguments))
            return CompletedProcess(list(arguments), 0, "", "")

        runner = checks.compose_runner(
            Settings(ROOT / "compose", "surgical_risk"),
            volume="surgical_risk_table_check_staging",
            directory="/opt/dagster/staging",
            deadline_seconds=5,
            running=lambda _: [],
        )
        with mock.patch.object(spark_slot, "run_command", docker):
            runner(Path("/opt/dagster/staging/folder_1"))
        controller = seen[0][seen[0].index("--profile") :]
        mine = job_words(self.module, "folder_1")[2:]
        user = mine.index("--user")
        service = user + 4
        self.assertEqual(profile_of_job("table_checks"), mine[1])
        self.assertEqual(controller[: controller.index("--name") + 1], mine[: mine.index("--name") + 1])
        self.assertEqual(controller[controller.index("--user") : controller.index("--user") + 4], mine[user:service])
        self.assertEqual(job_command("table_checks", "--staging", "/staging/folder_1"), controller[controller.index("--user") + 4 :])
        self.assertEqual(["--entrypoint", "python", service_of("table_checks"), "-m", "spark.jobs.table_checks"], mine[service : service + 5])
        self.assertEqual(["--staging", "/staging/folder_1"], mine[service + 5 :])

    def host_run(self, job: str, judge: str, *, job_seconds: int = 30) -> tuple[int, str]:
        command = [sys.executable, "-I", "-c", self.module.RUN_TABLE_CHECK_JOB_SCRIPT, str(job_seconds), "30"]
        command += [json.dumps([sys.executable, "-I", "-c", job]), json.dumps([sys.executable, "-I", "-c", judge])]
        done = subprocess.run(command, capture_output=True, text=True, timeout=60, check=False)  # noqa: S603
        self.assertEqual("", done.stderr)
        return done.returncode, done.stdout

    def test_the_jobs_output_goes_only_to_the_judge_with_its_exit_code_and_only_a_fixed_word_comes_back(self) -> None:
        job = "import sys; print('RESULT 4719238'); sys.stderr.write('ROW_TEXT'); sys.exit(1)"
        reads = "import json,sys; r=json.load(sys.stdin); ok = r == {'exit_code': 1, 'stdout': 'RESULT 4719238\\n'}; "
        self.assertEqual((0, "judged"), self.host_run(job, reads + "print('judged' if ok else 'result_refused')"))
        for printed, word in (("result_refused", "result_refused"), ("judged 4719238", "judge_failed"), ("Traceback ROW_TEXT", "judge_failed")):
            with self.subTest(printed=printed):
                self.assertEqual((1, word), self.host_run(job, f"print({printed!r})"))

    def test_a_job_past_its_deadline_is_a_fixed_word_and_the_judge_never_runs(self) -> None:
        self.assertEqual((1, "job_deadline"), self.host_run("import time; time.sleep(20)", "print('judged')", job_seconds=1))

    def test_a_job_that_cannot_take_the_slots_name_is_reported_as_the_slot_being_busy_and_the_judge_never_runs(self) -> None:
        job = "import sys; sys.stderr.write('Conflict. The container name \"/surgical-risk-spark-slot\" is already in use'); sys.exit(1)"
        self.assertEqual((1, "slot_busy"), self.host_run(job, "print('judged')"))

    def test_the_host_scripts_parse_as_the_hosts_older_python(self) -> None:
        ast.parse(self.module.RUN_TABLE_CHECK_JOB_SCRIPT, feature_version=(3, 12))

    def removal(self, replies: list[tuple[int, str]]) -> tuple[bool, list[list[str]]]:
        host = self.module.Host.__new__(self.module.Host)
        commands: list[list[str]] = []

        def run(args: list[str], *, stdin: str | None = None, timeout: float = 0, quiet: bool = False) -> tuple[int, str, str]:
            commands.append(list(args))
            code, out = replies.pop(0)
            return code, out, ""

        with mock.patch.object(host, "run", run, create=True):
            return host.remove_job_container(), commands

    def test_only_the_container_with_this_runs_owner_is_removed_and_its_end_is_read_back(self) -> None:
        mine = f"{'a' * 64}|exited|{run_owner()}\n"
        removed, commands = self.removal([(0, ""), (0, "")])
        self.assertEqual((True, ["ls"]), (removed, sorted({command[2] for command in commands})))
        removed, commands = self.removal([(0, f"{'b' * 64}|running|another_owner\n")])
        self.assertEqual((True, ["ls"]), (removed, [command[2] for command in commands]))
        removed, commands = self.removal([(0, mine), (0, ""), (0, "")])
        self.assertEqual((True, ["ls", "rm", "ls"]), (removed, [command[2] for command in commands]))
        self.assertEqual("a" * 64, commands[1][-1])

    def test_a_listing_that_fails_or_a_container_that_stays_is_not_removed(self) -> None:
        mine = f"{'a' * 64}|exited|{run_owner()}\n"
        self.assertFalse(self.removal([(1, ""), (1, "")])[0])
        self.assertFalse(self.removal([(0, mine), (1, ""), (0, mine)])[0])
        self.assertFalse(self.removal([(0, mine), (0, ""), (255, "")])[0])


FORBIDDEN_IN_CODE = ("warehouse/", "writeTo", "saveAsTable", "build_session", "INSERT INTO", ".append()")


@unittest.skipUnless(RUNNER.is_file(), "the runner is untracked and not on this computer")
class RunnerSourceTest(unittest.TestCase):
    """The contract is the text: the runner holds no Spark code of its own and names the warehouse prefix only where it lists it."""

    def setUp(self) -> None:
        self.tree = ast.parse(RUNNER.read_text())

    def code_strings(self) -> list[str]:
        """Every string of the runner's code, the host scripts among them, but no docstring."""
        documented = [node for node in ast.walk(self.tree) if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))]
        docstrings = {id(node.body[0].value) for node in documented if node.body and isinstance(node.body[0], ast.Expr)}
        return [n.value for n in ast.walk(self.tree) if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in docstrings]

    def test_the_runner_holds_no_spark_write_and_the_only_spark_job_it_names_is_the_table_checks_job(self) -> None:
        strings = self.code_strings()
        self.assertEqual([], [(word, text[:80]) for text in strings for word in FORBIDDEN_IN_CODE if word in text])
        self.assertEqual({"spark.jobs.table_checks"}, {found for text in strings for found in re.findall(r"spark\.jobs\.\w+", text)})
        self.assertEqual([], [text[:80] for text in strings if "from spark.session" in text or "import pyspark" in text])

    def test_the_warehouse_prefix_is_used_only_by_the_listing_of_the_inventories(self) -> None:
        holders = {
            function.name
            for function in ast.walk(self.tree)
            if isinstance(function, ast.FunctionDef)
            for node in ast.walk(function)
            if isinstance(node, ast.Name) and node.id == "WAREHOUSE_PREFIX"
        }
        self.assertEqual({"inventories"}, holders)
        loads = [node for node in ast.walk(self.tree) if isinstance(node, ast.Name) and node.id == "WAREHOUSE_PREFIX"]
        self.assertEqual(1, len(loads))
        inventories = next(node for node in ast.walk(self.tree) if isinstance(node, ast.FunctionDef) and node.name == "inventories")
        called = {node.attr for node in ast.walk(inventories) if isinstance(node, ast.Attribute)}
        self.assertEqual({"store", "list_versions"}, called)


class Clock:
    """A clock of the test: the time moves only by ``sleep`` and by the lookups that take time."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class ReconcileTest(unittest.TestCase):
    """``reconcile_launches`` closes an attempt that found no run only after ``LAUNCH_SETTLE_SECONDS`` of successful absence reads, and issues and credits no
    lookup after its deadline."""

    def setUp(self) -> None:
        if not RUNNER.is_file():
            self.skipTest("the runner is untracked and not on this computer")
        self.module = import_runner("ingestion_host")
        self.clock = Clock()
        for patch in (mock.patch.object(self.module.time, "monotonic", self.clock.monotonic), mock.patch.object(self.module.time, "sleep", self.clock.sleep)):
            patch.start()
            self.addCleanup(patch.stop)

    def reconcile(
        self, replies: Callable[[float], list[dict[str, Any]] | None], *, takes: float = 0.0, answered_before: float = 500.0
    ) -> tuple[int, Any, list[tuple[float, float]]]:
        """Run the reconcile with a lookup that answers by ``replies(seconds since the start)`` and takes ``takes`` seconds. Returns the attempts still open,
        the attempt and the (start, budget) of each lookup."""
        started = self.clock.now
        attempt = self.module.LaunchAttempt("record_load_job")
        attempt.answered = started - answered_before
        looked: list[tuple[float, float]] = []

        class Host:
            def runs_with_token(host_self, token: str, budget: float = 0) -> list[dict[str, Any]] | None:  # noqa: N805
                looked.append((self.clock.now - started, budget))
                found = replies(self.clock.now - started)
                self.clock.now += takes
                return found

        class Context:
            launch_attempts = {"t": attempt}
            host = Host()

            def adopt(context_self, found: Any, runs: list[str]) -> None:  # noqa: N805
                found.runs.extend(runs)

        return self.module.reconcile_launches(Context()), attempt, looked

    def test_an_attempt_whose_every_read_is_an_empty_answer_is_closed_after_the_settle_time_of_successful_reads(self) -> None:
        still, attempt, looked = self.reconcile(lambda at: [])
        self.assertEqual((0, True), (still, attempt.closed))
        self.assertGreaterEqual(looked[-1][0] - looked[0][0], self.module.LAUNCH_SETTLE_SECONDS)

    def test_one_unanswered_read_followed_by_empty_answers_does_not_close_the_attempt_before_the_settle_time_has_passed_again(self) -> None:
        late = self.module.CLEANUP_RUN_SECONDS - self.module.LAUNCH_SETTLE_SECONDS + 15
        still, attempt, _ = self.reconcile(lambda at: None if at < late else [])
        self.assertEqual((1, False), (still, attempt.closed))

    def test_an_unanswered_read_in_the_middle_of_the_empty_answers_restarts_the_settle_time(self) -> None:
        still, attempt, looked = self.reconcile(lambda at: None if 30 <= at < 36 else [])
        after = [at for at, _ in looked if at >= 36]
        self.assertEqual((0, True), (still, attempt.closed))
        self.assertTrue(after)
        self.assertGreaterEqual(after[-1] - after[0], self.module.LAUNCH_SETTLE_SECONDS, "the reads before the unanswered one count for nothing")

    def test_no_lookup_is_issued_after_the_deadline_and_none_gets_more_time_than_is_left(self) -> None:
        _, _, looked = self.reconcile(lambda at: None, takes=7.0)
        self.assertTrue(looked)
        self.assertTrue(all(at < self.module.CLEANUP_RUN_SECONDS for at, _ in looked))
        self.assertTrue(all(budget <= self.module.CLEANUP_RUN_SECONDS - at + 1e-9 for at, budget in looked))

    def test_a_lookup_that_ends_after_the_deadline_is_not_credited_as_an_absence(self) -> None:
        horizon = self.module.CLEANUP_RUN_SECONDS
        still, attempt, _ = self.reconcile(lambda at: [] if at < horizon - self.module.LAUNCH_SETTLE_SECONDS else None, takes=0.0)
        self.assertEqual((1, False), (still, attempt.closed))
        self.clock.now = 1000.0
        slow_still, slow_attempt, looked = self.reconcile(lambda at: [], takes=horizon + 5.0)
        self.assertEqual((1, False), (slow_still, slow_attempt.closed), "a lookup that started inside the deadline and ended after it counts for nothing")
        self.assertEqual(1, len(looked))

    def test_the_lookup_that_would_complete_the_settle_time_after_the_deadline_does_not_close_the_attempt(self) -> None:
        settle, horizon = self.module.LAUNCH_SETTLE_SECONDS, self.module.CLEANUP_RUN_SECONDS

        def replies(at: float) -> list[dict[str, Any]] | None:
            if at < horizon - settle - 5:
                return None
            if at >= horizon - 5:
                self.clock.now += 10.0
            return []

        still, attempt, _ = self.reconcile(replies)
        self.assertEqual((1, False), (still, attempt.closed))

    def test_a_run_that_a_lookup_finds_is_adopted_and_the_attempt_is_not_open(self) -> None:
        still, attempt, _ = self.reconcile(lambda at: [{"runId": "r1", "jobName": "record_load_job", "tags": [{"key": self.module.LAUNCH_TAG, "value": "t"}]}])
        self.assertEqual((0, ["r1"]), (still, attempt.runs))


if __name__ == "__main__":
    unittest.main()
