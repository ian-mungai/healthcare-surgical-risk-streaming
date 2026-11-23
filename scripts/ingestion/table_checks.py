"""The table checks step: the order that makes a load trustworthy, the Spark job in the middle, and the merge only after every check passed.

``run_table_checks`` takes the record the load step left (its branch, the branch's hash, the manifest it loaded from and each table's snapshot ID) and does, in
this order, each step only when the one before it passed:

1. the gate (``verify_before_read``) confirms that the manifest's versions of ``clinical_data.csv`` and ``lab_data.csv`` are the current ones, before any
   payload is read;
2. the Nessie branch is still at the recorded hash and holds each table at the recorded snapshot, so the load is exactly what the record says;
3. the two verified versions are streamed into the staging folder and each is proven to be the manifest's bytes (SHA-256 and size), so the job compares with
   the verified file;
4. the Spark job (``spark/jobs/table_checks.py``, started by ``runner``) checks every table and prints its result;
5. the result is read and held to the plan: the same branch, hash, tables and snapshots, every check's verdict derived from its counts, and an exit code that
   agrees with it;
6. only when every check passed, and the branch is still at the checked hash, a pending record is written and the branch is merged into the default branch.

The catalog's history is the one source of the merge. The receipt (``table_checks.json``, saved in the run folder against the manifest's reference) takes its
merge commit and that commit's parent from the commit in the history whose last three lines are this load's, never from what was sent or answered. A merge
request is sent once: after a failure the history is searched, and a run that finds a pending record younger than the settle time sends nothing.

A duplicate merge request to Nessie 0.110.1 is a no-op that its client reports as an error, but a request that timed out can still be applied later and Nessie
can accept an older expected hash. The design does not rely on the no-op: it never sends while the outcome can be open.

A failed run merges nothing: the load branch stays for inspection. A rerun after a merge runs nothing and merges nothing more. A rerun after a failed run checks
again and replaces the result. Messages name no data value.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import re
import tempfile
import time
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
from scripts.stack.groups import Settings, compose_arguments, rendered_environment
from scripts.stack.limits import load_limits
from scripts.stack.nessie import NessieClient, NessieError
from spark import table_checks as rules

LOAD_RECORD_FORMAT = "load_record_1"
RUN_FORMAT = "table_checks_run_1"
RESULT_FILE = "table_checks.json"
PLAN_FILE = "plan.json"
RESULT_PREFIX = "RESULT "
JOB_OUTPUT_FILE = "job_output.txt"
# One fixed place for the locks of every load, on the tmpfs the code location already has. A lock ends with its process, so a crash leaves none.
LOCK_DIRECTORY = Path("/run/dagster/table_check_locks")
# The job runs as the Spark user of the Spark image, with the launcher's own group as its primary group. The staged folder is 0750 and each staged file 0640
# in that group, set by the launcher, so the job reads them and no other user does.
SPARK_UID = 10100
STAGING_GID = 10001
STAGING_FOLDER_MODE = 0o750
STAGING_FILE_MODE = 0o640
BRANCH_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]*")
STAGING_DIRECTORY = "/opt/dagster/staging"
STAGING_VOLUME = "table_check_staging"
PENDING_FILE = "merge_pending.json"
PENDING_FORMAT = "merge_pending_1"
# A merge request that the client gave up on can still be applied by Nessie. A retry sends a new request only when the pending record is older than this: the
# client's own deadline is the Dagster job's 60 seconds, so 300 seconds is five deadlines, far beyond what a request that timed out can still take in Nessie. It
# must be at least twice the deadline of the client in use, which ``run_table_checks`` checks.
MERGE_SETTLE_SECONDS = 300.0
HISTORY_PAGE_SIZE = 100
HISTORY_PAGE_LIMIT = 50
TRACKED_RELEASE_FILE = RELEASE_FILE
MANIFEST_LINE_PREFIX = "records-manifest: "
SOURCE_LINE_PREFIX = "records-source: "
BRANCH_LINE_PREFIX = "records-branch: "
MANIFEST_FIELDS: Mapping[str, type] = {"bucket": str, "key": str, "version_id": str, "sha256": str}
LOAD_RECORD_FIELDS: Mapping[str, type] = {"format": str, "branch": str, "branch_hash": str, "manifest": dict, "tables": dict}
FILE_TABLES = tuple(spec.name for spec in rules.TABLES if spec.source == "file")


class TableChecksError(RuntimeError):
    """A step of the table checks cannot go on. The message names the step and never holds a value of the data."""


class MergeOutcomeOpen(TableChecksError):
    """``merge_outcome_open``: a merge request may still be applied and is not in the history yet. Nothing was sent again; the pending record stays."""


class MergeNotFound(TableChecksError):
    """``merge_not_found``: Nessie answered that the merge succeeded and the history holds no commit of this load. No receipt is written."""


@dataclass(frozen=True, slots=True)
class Timing:
    """The clock and the waits of the merge step, so that a test can run them without waiting."""

    clock: Callable[[], float] = time.time
    sleep: Callable[[float], None] = time.sleep
    settle_seconds: float = MERGE_SETTLE_SECONDS
    wait_seconds: float = 10.0
    poll_seconds: float = 2.0


@dataclass(frozen=True, slots=True)
class MergeCommit:
    """The merge commit of a load as the history holds it, and its parent: the two hashes of the receipt."""

    hash: str
    parent: str


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

    The staging folders are subfolders of ``directory``, which this process sees and which is the Docker volume ``volume``; the job sees that volume,
    read-only, at ``/staging``. The job runs as ``SPARK_UID`` with ``STAGING_GID`` as its group, the group of the staged files. The environment of the command
    is the one the group commands use: the memory limits that ``scripts.stack.limits`` renders from the limits file, then the process's own variables. A job
    that passes its deadline is stopped by the launcher and its reply carries the exit code 124.
    """
    root = Path(directory)

    def start(staging: Path) -> RunnerReply:
        if staging.parent != root:
            raise TableChecksError("the staging folder is not a folder of the staging volume, which the job could not see")
        arguments = compose_arguments(
            settings,
            "batch",
            *["run", "--rm", "--no-TTY", "--no-deps", "--user", f"{SPARK_UID}:{STAGING_GID}", "--volume", f"{volume}:/staging:ro"],
            *["--entrypoint", "python", "spark", "-m", "spark.jobs.table_checks", "--staging", f"/staging/{staging.name}"],
        )
        environment = rendered_environment(load_limits(settings.limits_file))
        try:
            result = run_command("docker", arguments, timeout=deadline_seconds, env=environment)
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
    if fields["format"] != LOAD_RECORD_FORMAT or not BRANCH_NAME.fullmatch(fields["branch"]) or fields["branch"] == "main":
        raise TableChecksError("the load record has another format or names no load branch")
    if not rules.HASH.fullmatch(fields["branch_hash"]) or not rules.HASH.fullmatch(manifest["sha256"]):
        raise TableChecksError("the load record holds a hash that is not 64 lower-case hex digits")
    if sorted(tables) != sorted(names) or any(type(snapshot) is not int for snapshot in tables.values()):
        raise TableChecksError(f"the load record must give an integer snapshot ID for exactly the tables {', '.join(names)}")
    return LoadRecord(fields["branch"], fields["branch_hash"], ManifestRef(**manifest), {name: tables[name] for name in names})


def manifest_line(sha256: str) -> str:
    """The last line of a merge commit's message: it names the manifest the merged load read. The load step reads it to skip a manifest main holds."""
    if rules.HASH.fullmatch(sha256) is None:
        raise TableChecksError("a manifest SHA-256 is 64 lower-case hex digits")
    return f"{MANIFEST_LINE_PREFIX}{sha256}"


def source_line(sha256: str) -> str:
    """The line of a merge commit's message that names the hash of the load branch that was checked and merged."""
    if rules.HASH.fullmatch(sha256) is None:
        raise TableChecksError("a source hash is 64 lower-case hex digits")
    return f"{SOURCE_LINE_PREFIX}{sha256}"


def branch_line(branch: str) -> str:
    """The line of a merge commit's message that names the load branch that was checked and merged."""
    if BRANCH_NAME.fullmatch(branch) is None:
        raise TableChecksError("a load branch name is letters, digits, dots, underscores, dashes and slashes")
    return f"{BRANCH_LINE_PREFIX}{branch}"


def read_track_pin(path: Path = TRACKED_RELEASE_FILE) -> rules.TrackPin:
    """The track list's pinned row count and digest from the tracked release file, whose path is fixed in code. The default is the only caller's path."""
    try:
        return rules.track_pin(json.loads(path.read_text()))
    except (OSError, ValueError) as error:
        raise TableChecksError(f"the release file cannot be used for the track list's pin: {type(error).__name__}") from None


def table_snapshot(nessie: NessieClient, branch: str, branch_hash: str, table: str) -> int | None:
    """The snapshot ID that Nessie holds for the table on the branch at exactly that hash, or None when Nessie says the table is not there (404).

    Any other answer must hold ``content.snapshotId`` as a whole number: an answer of another form is an error and never a missing table.
    """
    try:
        document = nessie.request("GET", f"/trees/{quote(f'{branch}@{branch_hash}', safe='@')}/contents/{rules.NAMESPACE}.{table}")
    except NessieError as error:
        if error.status == 404:
            return None
        raise
    content = document.get("content") if isinstance(document, dict) else None
    snapshot = content.get("snapshotId") if isinstance(content, dict) else None
    if type(snapshot) is not int:
        raise TableChecksError("Nessie's answer for a table is not in the expected form")
    return snapshot


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


def secure(path: Path, mode: int, gid: int) -> None:
    """Give a staged file or folder exactly ``mode`` and the group ``gid``, whatever the umask. A group the launcher cannot set stops the run."""
    os.chmod(path, mode)
    if path.stat().st_gid != gid:
        try:
            os.chown(path, -1, gid)
        except OSError:
            raise TableChecksError("the staged files could not be given the group that the job reads them through") from None


def stage_verified_files(store: ObjectStore, reference: ManifestRef, staging: Path, gid: int = STAGING_GID) -> list[Staged]:
    """Stream the manifest's version of each file table into ``staging`` and prove its bytes. If anything fails, raise and leave no staged file.

    The folder is ``STAGING_FOLDER_MODE`` and each file ``STAGING_FILE_MODE`` in the group ``gid``, set here and not by the umask.
    """
    import hashlib

    source_prefix = pinned_release().source_prefix
    keys = [f"{source_prefix}{name}.csv" for name in FILE_TABLES]
    entries = verify_before_read(store, reference, keys)
    versions = {(version.key, version.version_id): version for version in store.list_versions("landing/")}
    staging.mkdir(parents=True, exist_ok=True)
    staged: list[Staged] = []
    try:
        secure(staging, STAGING_FOLDER_MODE, gid)
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
            secure(Path(partial), STAGING_FILE_MODE, gid)
            os.replace(partial, staging / f"{name}.csv")
            staged.append(Staged(name, f"{name}.csv", entry.sha256, size))
    except BaseException:
        remove_staged(staging)
        raise
    return staged


def write_plan(staging: Path, document: Mapping[str, Any], gid: int) -> None:
    """Write the plan into the staging folder with the same mode and group as the staged files."""
    path = staging / PLAN_FILE
    path.write_text(json.dumps(document, indent=1) + "\n")
    secure(path, STAGING_FILE_MODE, gid)


def remove_staged(staging: Path) -> None:
    """Remove what this step puts in the staging folder: the staged files, the plan and any partial file."""
    if not staging.is_dir():
        return
    for name in (*(f"{table}.csv" for table in FILE_TABLES), PLAN_FILE):
        (staging / name).unlink(missing_ok=True)
    for partial in staging.glob(".*"):
        if partial.is_file():
            partial.unlink()
    with contextlib.suppress(OSError):
        staging.rmdir()


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
    """The job's one result held to the plan it was run for, with pass or fail derived from its checks, and an exit code that agrees with it.

    The output must hold exactly one ``RESULT`` line. The document kept is the validated one (``JobResult.document``), never the job's own text. Every message
    here is fixed text and numbers; the raw output of the job goes to ``JOB_OUTPUT_FILE`` of the run folder, also when the result is refused.
    """
    lines = [line for line in reply.stdout.splitlines() if line.startswith(RESULT_PREFIX)]
    if len(lines) != 1:
        keep_output(run_folder, reply)
        what = "printed no result" if not lines else "printed more than one result"
        raise TableChecksError(f"the table checks job ended with exit code {reply.exit_code} and {what}; its output is in {JOB_OUTPUT_FILE} of the run folder")
    try:
        result = rules.parse_result(lines[0].removeprefix(RESULT_PREFIX), plan)
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
    return result, result.document()


def lock_path(manifest_sha256: str, directory: Path = LOCK_DIRECTORY) -> Path:
    """The lock file of a load: in ``directory``, named by the manifest's SHA-256 only, so it does not depend on where the results are kept."""
    return directory / f"{manifest_sha256}.lock"


@contextlib.contextmanager
def load_lock(manifest_sha256: str, directory: Path = LOCK_DIRECTORY) -> Iterator[None]:
    """Hold the one lock of a load: a second run of the same load fails at once with a clear message. The lock ends with the process, so a crash leaves none."""
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    handle = lock_path(manifest_sha256, directory).open("w")
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
    name = nessie.config().get("defaultBranch")
    if type(name) is not str or not name:
        raise TableChecksError("Nessie names no default branch to merge into")
    return name


def history_page(document: Any) -> tuple[list[tuple[str, str, str]], str]:
    """The ``(hash, message, parent)`` entries of a log page, newest first, and the token of the next page or "" for the last.

    The page must hold ``logEntries`` (a list of entries with ``commitMeta`` holding a commit hash and a message, and ``parentCommitHash``) and a boolean
    ``hasMore``; a page with more pages must name a token. Fields that Nessie adds beyond these are not read. Any other form is an error, never an empty log.
    """
    if not isinstance(document, dict) or not isinstance(document.get("logEntries"), list) or type(document.get("hasMore")) is not bool:
        raise TableChecksError("Nessie's answer for the log is not in the expected form")
    entries: list[tuple[str, str, str]] = []
    for entry in document["logEntries"]:
        meta = entry.get("commitMeta") if isinstance(entry, dict) else None
        commit, message = (meta.get("hash"), meta.get("message")) if isinstance(meta, dict) else (None, None)
        parent = entry.get("parentCommitHash") if isinstance(entry, dict) else None
        if not all(isinstance(value, str) and rules.HASH.fullmatch(value) for value in (commit, parent)) or not isinstance(message, str):
            raise TableChecksError("Nessie's answer for the log is not in the expected form")
        entries.append((str(commit), message, str(parent)))
    token = document.get("token")
    if document["hasMore"] and (not isinstance(token, str) or not token):
        raise TableChecksError("Nessie's answer for the log has more pages but names no token for the next")
    return entries, token if document["hasMore"] and isinstance(token, str) else ""


def merged_commit(nessie: NessieClient, record: LoadRecord, at: str) -> MergeCommit | None:
    """The merge commit of this load in the log of the default branch, read from exactly the commit ``at``, with its parent, or None when none merged it.

    Only the final three lines of a commit's message identify a load, compared as one ordered unit: this load's ``records-branch`` line, ``records-source``
    line and ``records-manifest`` line. A commit whose last line is this load's manifest line and whose three lines are anything else is the merge of another
    load of this manifest, or one written in an older form: it is refused. The log is read newest first, page by page, and a log too long to read within
    ``HISTORY_PAGE_LIMIT`` pages is refused, never taken for a log with no merge.
    """
    wanted = [branch_line(record.branch), source_line(record.branch_hash), manifest_line(record.manifest.sha256)]
    default = quote(default_branch(nessie), safe="")
    token = ""
    for _ in range(HISTORY_PAGE_LIMIT):
        query = {"max-records": str(HISTORY_PAGE_SIZE), **({"page-token": token} if token else {})}
        entries, token = history_page(nessie.request("GET", f"/trees/{default}@{at}/history", query=query))
        for commit, message, parent in entries:
            lines = message.split("\n")
            if lines[-1] != wanted[-1]:
                continue
            if lines[-3:] != wanted:
                raise TableChecksError("main already holds a merge of another load of this manifest: another branch or source hash, or an older merge message")
            return MergeCommit(commit, parent)
        if not token:
            return None
    raise TableChecksError("the log of the default branch is too long to search for an earlier merge of this load")


def read_pending(run_folder: Path, record: LoadRecord) -> float | None:
    """The time the pending merge record of this load was written, or None when there is none. A record of another load or of another form is refused."""
    path = run_folder / PENDING_FILE
    if not path.is_file():
        return None
    try:
        pending = closed(
            json.loads(path.read_text()), {"format": str, "manifest": str, "branch": str, "source": str, "started": float}, "the pending merge record"
        )
    except ValueError:
        raise TableChecksError("the pending merge record is not JSON") from None
    if pending["format"] != PENDING_FORMAT:
        raise TableChecksError("the pending merge record has another format")
    if (pending["manifest"], pending["branch"], pending["source"]) != (record.manifest.sha256, record.branch, record.branch_hash):
        raise TableChecksError("the run folder holds a pending merge record of another load")
    return float(pending["started"])


def write_pending(run_folder: Path, record: LoadRecord, started: float) -> None:
    """Write the pending merge record in one step, before the merge request is sent."""
    document = {"format": PENDING_FORMAT, "manifest": record.manifest.sha256, "branch": record.branch, "source": record.branch_hash, "started": started}
    fd, partial = tempfile.mkstemp(dir=run_folder, prefix=".merge_pending.")
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write(json.dumps(document, indent=2, sort_keys=True) + "\n")
        os.replace(partial, run_folder / PENDING_FILE)
    except BaseException:
        Path(partial).unlink(missing_ok=True)
        raise


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


def main_head(nessie: NessieClient, default: str) -> str:
    """The hash of the default branch now, as a commit hash."""
    head = nessie.reference(default)
    if rules.HASH.fullmatch(head.hash) is None:
        raise TableChecksError("Nessie's hash for the default branch is not a commit hash")
    return head.hash


def wait_for_merge(nessie: NessieClient, record: LoadRecord, default: str, started: float, timing: Timing) -> MergeCommit | None:
    """Search the history of the default branch until the request's own deadline and a short wait have passed. Return the merge commit or None."""
    deadline = started + nessie.timeout_seconds + timing.wait_seconds
    while True:
        found = merged_commit(nessie, record, main_head(nessie, default))
        if found is not None or timing.clock() >= deadline:
            return found
        timing.sleep(timing.poll_seconds)


def merge_branch(nessie: NessieClient, record: LoadRecord, run_folder: Path, timing: Timing) -> MergeCommit:
    """Merge the branch at the checked hash into the default branch and return the merge commit as the history holds it.

    The request carries the recorded hash, the one Spark read, so what is merged is what was checked, and the head of the default branch that the log was
    searched from as its expected target hash. A branch that has moved since is refused here (Nessie would merge it at that hash only). The pending record is
    written before the request. The request is sent once: after a failure of it, whatever the failure, the history is searched until the request's deadline and
    a short wait have passed, and a merge not found by then ends as ``merge_outcome_open``, with nothing sent again and the pending record kept. After a success
    answer the merge commit must be in the history (``merge_not_found`` otherwise). The caller builds the receipt from the commit and its parent, never from
    what was sent or answered.

    A second request for the same source is a no-op in Nessie 0.110.1: it answers that nothing was applied, which ``NessieClient.merge`` raises as an error.
    The design does not rely on it, because a request that timed out can still be applied later and Nessie can accept an older expected hash after a
    compatible commit on main, so only the history says what happened.
    """
    if nessie.reference(record.branch).hash != record.branch_hash:
        raise TableChecksError("the load branch moved after the checks, so what was checked is not what would be merged")
    default = default_branch(nessie)
    head = main_head(nessie, default)
    earlier = merged_commit(nessie, record, head)
    if earlier is not None:
        confirm_main(nessie, record, default, earlier.hash)
        return earlier
    message = "\n".join(
        [f"merge {record.branch}: table checks passed", "", branch_line(record.branch), source_line(record.branch_hash), manifest_line(record.manifest.sha256)]
    )
    started = timing.clock()
    write_pending(run_folder, record, started)
    try:
        nessie.merge(record.branch, default, head, message, source_hash=record.branch_hash)
    except NessieError:
        found = wait_for_merge(nessie, record, default, started, timing)
        if found is None:
            raise MergeOutcomeOpen(
                "merge_outcome_open: the merge request failed or timed out and the load is not in the history of main yet; nothing was sent again, run it later"
            ) from None
        confirm_main(nessie, record, default, found.hash)
        return found
    found = merged_commit(nessie, record, main_head(nessie, default))
    if found is None:
        raise MergeNotFound(
            "merge_not_found: Nessie answered that the merge succeeded and the history of main holds no merge of this load; no receipt was written"
        )
    confirm_main(nessie, record, default, found.hash)
    return found


def check_pin(pin: rules.TrackPin) -> None:
    """The pin is a positive row count and a SHA-256, checked before anything is staged."""
    if type(pin.rows) is not int or pin.rows <= 0 or type(pin.sha256) is not str or rules.HASH.fullmatch(pin.sha256) is None:
        raise TableChecksError("the track list's pin is not a positive row count and a SHA-256")


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
    staging_gid: int = STAGING_GID,
    lock_directory: Path = LOCK_DIRECTORY,
    timing: Timing = Timing(),
) -> Outcome:
    """Run the table checks for one load in the order of the module's description. Raise ``TableChecksError`` for a step that cannot go on."""
    if empty_form not in rules.EMPTY_FORMS:
        raise TableChecksError(f"the empty form must be one of {', '.join(rules.EMPTY_FORMS)}")
    if 2 * nessie.timeout_seconds > timing.settle_seconds:
        raise TableChecksError("the settle time of the merge step must be at least twice the request deadline of the Nessie client")
    check_pin(track_pin)
    with load_lock(record.manifest.sha256, lock_directory):
        return run_locked(store, nessie, record, track_pin, staging, run_folder, runner, empty_form, staging_gid, timing)


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


def record_merge(run_folder: Path, document: dict[str, Any], default: str, commit: MergeCommit) -> Outcome:
    """Write the receipt of a merge from the commit that the history holds and its parent, then remove the pending record."""
    document.update(passed=True, merged=True, merged_into=default, main_hash_before=commit.parent, merged_hash=commit.hash)
    write_result(run_folder, document)
    (run_folder / PENDING_FILE).unlink(missing_ok=True)
    return Outcome(True, True, document)


def run_locked(
    store: ObjectStore,
    nessie: NessieClient,
    record: LoadRecord,
    track_pin: rules.TrackPin,
    staging: Path,
    run_folder: Path,
    runner: Runner,
    empty_form: str,
    staging_gid: int,
    timing: Timing,
) -> Outcome:
    held = saved_result(run_folder, record)
    if held is not None and held["merged"] is True:
        return Outcome(True, True, held)
    default = default_branch(nessie)
    pending = read_pending(run_folder, record)
    found = merged_commit(nessie, record, main_head(nessie, default))
    if found is not None:
        confirm_main(nessie, record, default, found.hash)
        document = held if held is not None else receipt(record, empty_form, None, passed=True)
        if held is None:
            document["notes"].append("the checks of the run that merged this load are not in this receipt: it was rebuilt from the log of the default branch")
        return record_merge(run_folder, document, default, found)
    if pending is not None and timing.clock() - pending < timing.settle_seconds:
        raise MergeOutcomeOpen(
            "merge_outcome_open: a merge request of this load was sent less than the settle time ago and is not in the history of main yet; nothing was sent"
        )
    confirm_load(nessie, record)
    try:
        staged = stage_verified_files(store, record.manifest, staging, staging_gid)
        plan_document = build_plan(record, staged, track_pin, empty_form, uuid.uuid4().hex)
        try:
            plan = rules.parse_plan(json.dumps(plan_document))
        except rules.RulesError:
            raise TableChecksError("the plan cannot be built from the inputs of this run") from None
        write_plan(staging, plan_document, staging_gid)
        reply = runner(staging)
    finally:
        remove_staged(staging)
    result, checked = read_result(reply, plan, run_folder)
    document = receipt(record, empty_form, checked, passed=result.passed)
    write_result(run_folder, document)
    if not result.passed:
        return Outcome(False, False, document)
    return record_merge(run_folder, document, default, merge_branch(nessie, record, run_folder, timing))
