"""The table checks step: the order that makes a load trustworthy, the Spark job in the middle, and the merge only after every check passed.

``run_table_checks`` takes the record the load step left (its branch, the branch's hash, the manifest it loaded from and each table's snapshot ID) and does, in
this order, each step only when the one before it passed:

1. the gate (``verify_before_read``) confirms that the manifest's versions of ``clinical_data.csv`` and ``lab_data.csv`` are the current ones, before any
   payload is read;
2. the Nessie branch is still at the recorded hash and holds each table at the recorded snapshot, so the load is exactly what the record says;
3. the two verified versions are streamed into the staging folder and each is proven to be the manifest's bytes (SHA-256 and size), so the job compares with
   the verified file;
4. the Spark job (``spark/jobs/table_checks.py``, started by ``runner``) checks every table and prints its result;
5. the result is read and held to the plan: the same branch, hash, tables and snapshots, and an exit code that agrees with it;
6. only when every check passed, and the branch is still at the checked hash, the branch is merged into the default branch and the merged hash is recorded.

The result is saved as ``table_checks.json`` in the run folder, against the manifest's reference. A failed run merges nothing: the load branch stays for
inspection. A rerun after a merge runs nothing and merges nothing more. A rerun after a failed run checks again and replaces the result. Messages name no
data value.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import tempfile
import uuid
from collections.abc import Callable, Iterator, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

from scripts.ingestion.gate import verify_before_read
from scripts.ingestion.manifest import ManifestRef
from scripts.ingestion.selection import pinned_release
from scripts.ingestion.store import ObjectStore
from scripts.process import TimeoutExpired, run_command
from scripts.sample.candidates import RELEASE_FILE
from scripts.stack.groups import Settings, compose_arguments
from scripts.stack.nessie import NessieClient, NessieError
from spark import table_checks as rules

LOAD_RECORD_FORMAT = "load_record_1"
RUN_FORMAT = "table_checks_run_1"
RESULT_FILE = "table_checks.json"
PLAN_FILE = "plan.json"
RESULT_PREFIX = "RESULT "
JOB_OUTPUT_FILE = "job_output.txt"
LOCK_FILE = ".table_checks.lock"
STAGING_DIRECTORY = "/opt/dagster/staging"
STAGING_VOLUME = "table_check_staging"
HISTORY_PAGE_SIZE = 100
HISTORY_PAGE_LIMIT = 50
TRACKED_RELEASE_FILE = RELEASE_FILE
MANIFEST_LINE_PREFIX = "records-manifest: "
MANIFEST_FIELDS: Mapping[str, type] = {"bucket": str, "key": str, "version_id": str, "sha256": str}
LOAD_RECORD_FIELDS: Mapping[str, type] = {"format": str, "branch": str, "branch_hash": str, "manifest": dict, "tables": dict}
FILE_TABLES = tuple(spec.name for spec in rules.TABLES if spec.source == "file")


class TableChecksError(RuntimeError):
    """A step of the table checks cannot go on. The message names the step and never holds a value of the data."""


@dataclass(frozen=True, slots=True)
class LoadRecord:
    """What the load step recorded: the branch it wrote, that branch's head hash, the manifest it loaded from and each table's snapshot ID."""

    branch: str
    branch_hash: str
    manifest: ManifestRef
    tables: dict[str, int]

    @property
    def document(self) -> dict[str, Any]:
        return {"branch": self.branch, "branch_hash": self.branch_hash, "tables": dict(self.tables)}


@dataclass(frozen=True, slots=True)
class RunnerReply:
    """What the Spark job's process ended with."""

    exit_code: int
    stdout: str
    stderr: str


@dataclass(frozen=True, slots=True)
class Outcome:
    """The result of the step: whether every check passed, whether the branch was merged, and the saved document."""

    passed: bool
    merged: bool
    document: dict[str, Any]


Runner = Callable[[Path], RunnerReply]


def compose_runner(settings: Settings, *, volume: str, directory: str, deadline_seconds: float) -> Runner:
    """A runner that starts the job as a one-off of the stack's ``spark`` service, in the ``batch`` profile and with no service it depends on started.

    The staging folders are subfolders of ``directory``, which this process sees and which is the Docker volume ``volume``; the job sees that volume, read-only,
    at ``/staging``. A job that passes its deadline is stopped by the launcher and its reply carries the exit code 124.
    """
    root = Path(directory)

    def start(staging: Path) -> RunnerReply:
        if staging.parent != root:
            raise TableChecksError("the staging folder is not a folder of the staging volume, which the job could not see")
        arguments = compose_arguments(
            settings,
            "batch",
            *["run", "--rm", "--no-TTY", "--no-deps", "--volume", f"{volume}:/staging:ro", "--entrypoint", "python", "spark"],
            *["-m", "spark.jobs.table_checks", "--staging", f"/staging/{staging.name}"],
        )
        try:
            result = run_command("docker", arguments, timeout=deadline_seconds)
        except TimeoutExpired:
            return RunnerReply(124, "", f"the table checks job did not finish within {deadline_seconds:g} seconds and was stopped")
        return RunnerReply(result.returncode, result.stdout, result.stderr)

    return start


def closed(value: Any, fields: Mapping[str, type], what: str) -> dict[str, Any]:
    try:
        return rules.closed(value, fields, what)
    except rules.RulesError as error:
        raise TableChecksError(str(error)) from None


def parse_load_record(text: str) -> LoadRecord:
    """The load record from the text of the ``<run ID>.load_record.json`` file, which holds exactly its five fields.

    Refuse an unknown, missing or mistyped field, a hash that is not 64 hex digits and any table list but ours.
    """
    try:
        document = json.loads(text, object_pairs_hook=rules.reject_repeats)
    except rules.RulesError as error:
        raise TableChecksError(str(error)) from None
    except ValueError:
        raise TableChecksError("the load record is not JSON") from None
    fields = closed(document, LOAD_RECORD_FIELDS, "the load record")
    manifest = closed(fields["manifest"], MANIFEST_FIELDS, "the load record's manifest")
    tables = fields["tables"]
    names = [spec.name for spec in rules.TABLES]
    if fields["format"] != LOAD_RECORD_FORMAT or not fields["branch"] or fields["branch"] == "main":
        raise TableChecksError("the load record has another format or names no load branch")
    if not rules.HASH.fullmatch(fields["branch_hash"]) or not rules.HASH.fullmatch(manifest["sha256"]):
        raise TableChecksError("the load record holds a hash that is not 64 lower-case hex digits")
    if sorted(tables) != sorted(names) or any(type(snapshot) is not int for snapshot in tables.values()):
        raise TableChecksError(f"the load record must give an integer snapshot ID for exactly the tables {', '.join(names)}")
    return LoadRecord(fields["branch"], fields["branch_hash"], ManifestRef(**manifest), {name: tables[name] for name in names})


def manifest_line(sha256: str) -> str:
    """The line of a merge commit's message that names the manifest the merged load read. The load step reads it to skip a manifest main holds."""
    if rules.HASH.fullmatch(sha256) is None:
        raise TableChecksError("a manifest SHA-256 is 64 lower-case hex digits")
    return f"{MANIFEST_LINE_PREFIX}{sha256}"


def read_track_pin(path: Path = TRACKED_RELEASE_FILE) -> rules.TrackPin:
    """The track list's pinned row count and digest from the tracked release file, whose path is fixed in code. The default is the only caller's path."""
    try:
        return rules.track_pin(json.loads(path.read_text()))
    except (OSError, ValueError) as error:
        raise TableChecksError(f"the release file cannot be used for the track list's pin: {type(error).__name__}") from None


def table_snapshot(nessie: NessieClient, branch: str, branch_hash: str, table: str) -> int | None:
    """The snapshot ID that Nessie holds for the table on the branch at exactly that hash, or None when the table is not there."""
    try:
        document = nessie.request("GET", f"/trees/{quote(f'{branch}@{branch_hash}', safe='@')}/contents/{rules.NAMESPACE}.{table}") or {}
    except NessieError as error:
        if error.status == 404:
            return None
        raise
    snapshot = (document.get("content") or {}).get("snapshotId")
    return snapshot if type(snapshot) is int else None


def confirm_load(nessie: NessieClient, record: LoadRecord) -> None:
    """The branch is at the recorded hash and holds each table at the recorded snapshot. Raise ``TableChecksError`` otherwise."""
    head = nessie.reference(record.branch)
    if head.kind != "BRANCH" or head.hash != record.branch_hash:
        raise TableChecksError("the load branch has moved since the load step recorded its hash")
    for table, snapshot in record.tables.items():
        found = table_snapshot(nessie, record.branch, record.branch_hash, table)
        if found is None:
            raise TableChecksError(f"the table {table} is not on the load branch at the recorded hash")
        if found != snapshot:
            raise TableChecksError(f"the snapshot of {table} at the recorded hash is not the recorded one")


@dataclass(frozen=True, slots=True)
class Staged:
    table: str
    file: str
    sha256: str
    size: int


def stage_verified_files(store: ObjectStore, reference: ManifestRef, staging: Path) -> list[Staged]:
    """Stream the manifest's version of each file table into ``staging`` and prove its bytes. If one differs, raise and leave no staged file."""
    import hashlib

    source_prefix = pinned_release().source_prefix
    keys = [f"{source_prefix}{name}.csv" for name in FILE_TABLES]
    entries = verify_before_read(store, reference, keys)
    versions = {(version.key, version.version_id): version for version in store.list_versions("landing/")}
    staging.mkdir(parents=True, exist_ok=True)
    staged: list[Staged] = []
    try:
        for name, entry in zip(FILE_TABLES, entries, strict=True):
            version = versions.get((entry.key, entry.version_id))
            if version is None:
                raise TableChecksError(f"the manifest's version of the file for {name} is no longer listed")
            digest, size = hashlib.sha256(), 0
            fd, partial = tempfile.mkstemp(dir=staging, prefix=f".{name}.")
            with os.fdopen(fd, "wb") as handle:
                for block in store.iter_version(version):
                    digest.update(block)
                    handle.write(block)
                    size += len(block)
            if digest.hexdigest() != entry.sha256 or size != entry.size:
                Path(partial).unlink()
                raise TableChecksError(f"the bytes read for {name} are not the verified file of the manifest")
            os.chmod(partial, 0o644)
            os.replace(partial, staging / f"{name}.csv")
            staged.append(Staged(name, f"{name}.csv", entry.sha256, size))
    except BaseException:
        remove_staged(staging)
        raise
    return staged


def remove_staged(staging: Path) -> None:
    """Remove what this step puts in the staging folder: the staged files, the plan and any partial file."""
    if not staging.is_dir():
        return
    for name in (*(f"{table}.csv" for table in FILE_TABLES), PLAN_FILE):
        (staging / name).unlink(missing_ok=True)
    for partial in staging.glob(".*"):
        if partial.is_file():
            partial.unlink()


def build_plan(record: LoadRecord, staged: list[Staged], pin: rules.TrackPin, empty_form: str, run_id: str) -> dict[str, Any]:
    by_table = {item.table: item for item in staged}
    tables: list[dict[str, Any]] = []
    for spec in rules.TABLES:
        if spec.source == "file":
            item = by_table[spec.name]
            tables.append({"name": spec.name, "snapshot_id": record.tables[spec.name], "file": item.file, "file_sha256": item.sha256, "file_size": item.size})
        else:
            tables.append({"name": spec.name, "snapshot_id": record.tables[spec.name], "rows": pin.rows, "rows_sha256": pin.sha256})
    return {
        "format": rules.PLAN_FORMAT,
        "run_id": run_id,
        "branch": record.branch,
        "branch_hash": record.branch_hash,
        "empty_form": empty_form,
        "tables": tables,
    }


def keep_output(run_folder: Path, reply: RunnerReply) -> None:
    """Keep the raw output of the job in a private file of the run folder. It can hold values of the data, so it is never put into a message."""
    run_folder.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(run_folder / JOB_OUTPUT_FILE, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(f"exit code {reply.exit_code}\n--- standard output ---\n{reply.stdout}\n--- standard error ---\n{reply.stderr}\n")


def read_result(reply: RunnerReply, plan: rules.Plan, run_folder: Path) -> tuple[rules.JobResult, dict[str, Any]]:
    """The job's result held to the plan it was run for, with pass or fail derived from its checks, and an exit code that agrees with it.

    Every message here is fixed text and numbers. The raw output of the job goes to ``JOB_OUTPUT_FILE`` of the run folder, also when the result is refused.
    """
    lines = [line for line in reply.stdout.splitlines() if line.startswith(RESULT_PREFIX)]
    if not lines:
        keep_output(run_folder, reply)
        raise TableChecksError(
            f"the table checks job ended with exit code {reply.exit_code} and printed no result; its output is in {JOB_OUTPUT_FILE} of the run folder"
        )
    text = lines[-1].removeprefix(RESULT_PREFIX)
    try:
        result = rules.parse_result(text, plan)
    except rules.RulesError:
        keep_output(run_folder, reply)
        raise TableChecksError(
            f"the table checks job's result is not a result for this plan; the job's output is in {JOB_OUTPUT_FILE} of the run folder"
        ) from None
    if reply.exit_code != (0 if result.passed else 1):
        keep_output(run_folder, reply)
        raise TableChecksError(
            f"the table checks job ended with exit code {reply.exit_code}, which does not agree with its result; its output is in {JOB_OUTPUT_FILE}"
        )
    if not result.passed:
        keep_output(run_folder, reply)
    checked: dict[str, Any] = json.loads(text)
    return result, checked


@contextlib.contextmanager
def load_lock(run_folder: Path) -> Iterator[None]:
    """Hold the one lock of a load: a second run of the same load fails at once with a clear message. The lock ends with the process, so a crash leaves none."""
    run_folder.mkdir(parents=True, exist_ok=True)
    handle = (run_folder / LOCK_FILE).open("w")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise TableChecksError("another run of this load holds its lock, so this run did nothing") from None
    try:
        yield
    finally:
        handle.close()


def default_branch(nessie: NessieClient) -> str:
    name = str(nessie.config().get("defaultBranch") or "")
    if not name:
        raise TableChecksError("Nessie names no default branch to merge into")
    return name


def merged_commit(nessie: NessieClient, record: LoadRecord) -> str | None:
    """The hash of the commit on the default branch whose message ends with this load's ``records-manifest`` line, or None.

    The log is read newest first, page by page. A log too long to read within ``HISTORY_PAGE_LIMIT`` pages is refused, never taken for a log with no merge.
    """
    line = manifest_line(record.manifest.sha256)
    default = quote(default_branch(nessie), safe="")
    token = ""
    for _ in range(HISTORY_PAGE_LIMIT):
        query = {"max-records": str(HISTORY_PAGE_SIZE), **({"page-token": token} if token else {})}
        document = nessie.request("GET", f"/trees/{default}/history", query=query) or {}
        for entry in document.get("logEntries", []):
            meta = entry.get("commitMeta") or {}
            message, commit = meta.get("message"), meta.get("hash")
            if isinstance(message, str) and isinstance(commit, str) and message.rstrip().splitlines()[-1:] == [line]:
                return commit
        if not document.get("hasMore"):
            return None
        following = document.get("token")
        if not isinstance(following, str) or not following:
            raise TableChecksError("the log of the default branch has more pages but names no token for the next")
        token = following
    raise TableChecksError("the log of the default branch is too long to search for an earlier merge of this load")


def write_result(run_folder: Path, document: Mapping[str, Any]) -> None:
    """Save the result in the run folder in one step, so a reader never sees half of it."""
    run_folder.mkdir(parents=True, exist_ok=True)
    existing = run_folder / RESULT_FILE
    if existing.is_file() and document.get("merged") is not True:
        try:
            was_merged = json.loads(existing.read_text()).get("merged") is True
        except ValueError, AttributeError:
            was_merged = False
        if was_merged:
            raise TableChecksError("the receipt of this load says merged, and a receipt that says merged is never replaced by one that says it is not")
    fd, partial = tempfile.mkstemp(dir=run_folder, prefix=".table_checks.")
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write(json.dumps(document, indent=2, sort_keys=True) + "\n")
        os.chmod(partial, 0o644)
        os.replace(partial, run_folder / RESULT_FILE)
    except BaseException:
        Path(partial).unlink(missing_ok=True)
        raise


def saved_result(run_folder: Path, record: LoadRecord) -> dict[str, Any] | None:
    """The result already saved for this load, or None. A result of another load in the folder is never replaced."""
    path = run_folder / RESULT_FILE
    if not path.is_file():
        return None
    try:
        document = json.loads(path.read_text())
        same = document["manifest"] == asdict(record.manifest) and document["load"] == record.document
    except ValueError, KeyError, TypeError:
        same = False
    if not same:
        raise TableChecksError("the run folder already holds table check results of another load")
    held: dict[str, Any] = document
    return held


def confirm_main(nessie: NessieClient, record: LoadRecord, default: str, merged: str) -> None:
    """The default branch at the merge commit holds each table at the recorded snapshot."""
    for table, snapshot in record.tables.items():
        if table_snapshot(nessie, default, merged, table) != snapshot:
            raise TableChecksError(f"the default branch at the merge commit does not hold {table} at the recorded snapshot")


def merge_branch(nessie: NessieClient, record: LoadRecord) -> tuple[str, str]:
    """Merge the branch at the checked hash into the default branch and return the default branch's hash before and after the merge.

    The request carries the recorded hash, the one Spark read, so what is merged is what was checked. A branch that has moved since is refused here (Nessie
    would merge it at that hash only). When the log of the default branch already holds a merge of this load, nothing is sent and that commit is returned.
    """
    if nessie.reference(record.branch).hash != record.branch_hash:
        raise TableChecksError("the load branch moved after the checks, so what was checked is not what would be merged")
    default = default_branch(nessie)
    earlier = merged_commit(nessie, record)
    if earlier is not None:
        confirm_main(nessie, record, default, earlier)
        return "", earlier
    before = nessie.reference(default).hash
    message = f"merge {record.branch}: table checks passed\n\n{manifest_line(record.manifest.sha256)}"
    after = nessie.merge(record.branch, default, before, message, source_hash=record.branch_hash)
    confirm_main(nessie, record, default, after)
    return before, after


def run_table_checks(
    *,
    store: ObjectStore,
    nessie: NessieClient,
    record: LoadRecord,
    track_pin: rules.TrackPin,
    staging: Path,
    run_folder: Path,
    runner: Runner,
    empty_form: str = rules.EMPTY_FORM,
) -> Outcome:
    """Run the table checks for one load in the order of the module's description. Raise ``TableChecksError`` for a step that cannot go on."""
    if empty_form not in rules.EMPTY_FORMS:
        raise TableChecksError(f"the empty form must be one of {', '.join(rules.EMPTY_FORMS)}")
    with load_lock(run_folder):
        return run_locked(store, nessie, record, track_pin, staging, run_folder, runner, empty_form)


def receipt(record: LoadRecord, empty_form: str, checked: dict[str, Any] | None, *, passed: bool) -> dict[str, Any]:
    return {
        "format": RUN_FORMAT,
        "manifest": asdict(record.manifest),
        "load": record.document,
        "empty_form": empty_form,
        "passed": passed,
        "merged": False,
        "merged_into": "",
        "main_hash_before": "",
        "merged_hash": "",
        "notes": [rules.LAB_DATA_NOTE],
        "checked": checked,
    }


def run_locked(
    store: ObjectStore,
    nessie: NessieClient,
    record: LoadRecord,
    track_pin: rules.TrackPin,
    staging: Path,
    run_folder: Path,
    runner: Runner,
    empty_form: str,
) -> Outcome:
    held = saved_result(run_folder, record)
    if held is not None and held["merged"] is True:
        return Outcome(True, True, held)
    earlier = merged_commit(nessie, record)
    if earlier is not None:
        default = default_branch(nessie)
        confirm_main(nessie, record, default, earlier)
        document = held if held is not None else receipt(record, empty_form, None, passed=True)
        if held is None:
            document["notes"].append("the checks of the run that merged this load are not in this receipt: it was rebuilt from the log of the default branch")
        document.update(passed=True, merged=True, merged_into=default, merged_hash=earlier)
        write_result(run_folder, document)
        return Outcome(True, True, document)
    confirm_load(nessie, record)
    staged = stage_verified_files(store, record.manifest, staging)
    plan_document = build_plan(record, staged, track_pin, empty_form, uuid.uuid4().hex)
    plan = rules.parse_plan(json.dumps(plan_document))
    try:
        (staging / PLAN_FILE).write_text(json.dumps(plan_document, indent=1) + "\n")
        reply = runner(staging)
    finally:
        remove_staged(staging)
    result, checked = read_result(reply, plan, run_folder)
    document = receipt(record, empty_form, checked, passed=result.passed)
    write_result(run_folder, document)
    if result.passed:
        before, after = merge_branch(nessie, record)
        document.update(merged=True, merged_into=default_branch(nessie), main_hash_before=before, merged_hash=after)
        write_result(run_folder, document)
    return Outcome(result.passed, document["merged"], document)
