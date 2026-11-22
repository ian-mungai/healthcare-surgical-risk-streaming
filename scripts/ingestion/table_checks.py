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

import json
import os
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from scripts.ingestion.gate import verify_before_read
from scripts.ingestion.manifest import ManifestRef
from scripts.ingestion.selection import pinned_release
from scripts.ingestion.store import ObjectStore
from scripts.process import TimeoutExpired, run_command
from scripts.stack.groups import Settings, compose_arguments
from scripts.stack.nessie import NessieClient, NessieError
from spark import table_checks as rules

LOAD_RECORD_FORMAT = "load_record_1"
RUN_FORMAT = "table_checks_run_1"
RESULT_FILE = "table_checks.json"
PLAN_FILE = "plan.json"
RESULT_PREFIX = "RESULT "
MANIFEST_FIELDS: Mapping[str, type] = {"bucket": str, "key": str, "version_id": str, "sha256": str}
LOAD_RECORD_FIELDS: Mapping[str, type] = {"format": str, "branch": str, "branch_hash": str, "manifest": dict, "tables": dict}
FILE_TABLES = tuple(spec.name for spec in rules.TABLES if spec.source == "file")
ERROR_TAIL_BYTES = 400


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
    """The load record from its JSON text. Refuse an unknown, missing or mistyped field, a hash that is not 64 hex digits and any table list but ours."""
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


def read_track_pin(path: Path) -> rules.TrackPin:
    """The track list's pinned row count and digest from the tracked release file."""
    try:
        return rules.track_pin(json.loads(path.read_text()))
    except (OSError, ValueError) as error:
        raise TableChecksError(f"the release file cannot be used for the track list's pin: {type(error).__name__}") from None


def table_snapshot(nessie: NessieClient, branch: str, branch_hash: str, table: str) -> int | None:
    """The snapshot ID that Nessie holds for the table on the branch at exactly that hash, or None when the table is not there."""
    from urllib.parse import quote

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


def build_plan(record: LoadRecord, staged: list[Staged], pin: rules.TrackPin, empty_form: str) -> dict[str, Any]:
    by_table = {item.table: item for item in staged}
    tables: list[dict[str, Any]] = []
    for spec in rules.TABLES:
        if spec.source == "file":
            item = by_table[spec.name]
            tables.append({"name": spec.name, "snapshot_id": record.tables[spec.name], "file": item.file, "file_sha256": item.sha256, "file_size": item.size})
        else:
            tables.append({"name": spec.name, "snapshot_id": record.tables[spec.name], "rows": pin.rows, "rows_sha256": pin.sha256})
    return {"format": rules.PLAN_FORMAT, "branch": record.branch, "branch_hash": record.branch_hash, "empty_form": empty_form, "tables": tables}


def read_result(reply: RunnerReply, record: LoadRecord) -> dict[str, Any]:
    """The job's result, held to the load record: its branch, hash, tables and snapshots, and an exit code that agrees with it."""
    lines = [line for line in reply.stdout.splitlines() if line.startswith(RESULT_PREFIX)]
    if not lines:
        tail = reply.stderr[-ERROR_TAIL_BYTES:].strip()
        raise TableChecksError(f"the table checks job ended with exit code {reply.exit_code} and printed no result" + (f": {tail}" if tail else ""))
    try:
        document = json.loads(lines[-1].removeprefix(RESULT_PREFIX))
    except ValueError:
        raise TableChecksError("the table checks job's result line is not JSON") from None
    tables = document.get("tables") if isinstance(document, dict) else None
    if not isinstance(tables, list) or any(not isinstance(entry, dict) for entry in tables):
        raise TableChecksError("the table checks job's result holds no list of tables")
    names = [spec.name for spec in rules.TABLES]
    if (
        document.get("format") != rules.RESULT_FORMAT
        or (document.get("branch"), document.get("branch_hash")) != (record.branch, record.branch_hash)
        or [entry.get("table") for entry in tables] != names
        or [entry.get("snapshot_id") for entry in tables] != [record.tables[name] for name in names]
        or any(type(entry.get("passed")) is not bool for entry in tables)
        or type(document.get("passed")) is not bool
    ):
        raise TableChecksError("the table checks job's result is not for this load: another branch, hash, table list or snapshot, or a field of the wrong type")
    passed = document["passed"] and all(entry["passed"] for entry in tables)
    if document["passed"] != all(entry["passed"] for entry in tables) or reply.exit_code != (0 if passed else 1):
        raise TableChecksError(f"the table checks job ended with exit code {reply.exit_code}, which does not agree with its result")
    checked: dict[str, Any] = document
    return checked


def write_result(run_folder: Path, document: Mapping[str, Any]) -> None:
    """Save the result in the run folder in one step, so a reader never sees half of it."""
    run_folder.mkdir(parents=True, exist_ok=True)
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


def merge_branch(nessie: NessieClient, record: LoadRecord) -> tuple[str, str]:
    """Merge the checked branch into the default branch and return the default branch's hash before and after. Refuse when the branch moved."""
    if nessie.reference(record.branch).hash != record.branch_hash:
        raise TableChecksError("the load branch moved after the checks, so what was checked is not what would be merged")
    default = str(nessie.config().get("defaultBranch") or "")
    if not default:
        raise TableChecksError("Nessie names no default branch to merge into")
    before = nessie.reference(default).hash
    after = nessie.merge(record.branch, default, before, f"merge {record.branch}: table checks passed for manifest {record.manifest.sha256[:12]}")
    for table, snapshot in record.tables.items():
        if table_snapshot(nessie, default, after, table) != snapshot:
            raise TableChecksError(f"after the merge the default branch does not hold {table} at the recorded snapshot")
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
    held = saved_result(run_folder, record)
    if held is not None and held["merged"] is True:
        return Outcome(True, True, held)
    confirm_load(nessie, record)
    staged = stage_verified_files(store, record.manifest, staging)
    try:
        (staging / PLAN_FILE).write_text(json.dumps(build_plan(record, staged, track_pin, empty_form), indent=1) + "\n")
        reply = runner(staging)
    finally:
        remove_staged(staging)
    checked = read_result(reply, record)
    document: dict[str, Any] = {
        "format": RUN_FORMAT,
        "manifest": asdict(record.manifest),
        "load": record.document,
        "empty_form": empty_form,
        "passed": checked["passed"],
        "merged": False,
        "merged_into": "",
        "main_hash_before": "",
        "merged_hash": "",
        "checked": checked,
    }
    write_result(run_folder, document)
    if checked["passed"]:
        before, after = merge_branch(nessie, record)
        document.update(merged=True, merged_into=str(nessie.config().get("defaultBranch")), main_hash_before=before, merged_hash=after)
        write_result(run_folder, document)
    return Outcome(bool(checked["passed"]), document["merged"], document)
