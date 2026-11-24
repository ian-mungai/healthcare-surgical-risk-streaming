"""The replay's inputs: the gate, then the verified clinical table and case files.

Before anything is read, ``verify_before_read`` confirms that every object version the run needs is the version in its passed manifest, and it refuses a
missing or substituted version before any payload is read. Then each file is read by its version ID into a temporary file, its SHA-256 is compared with the
manifest's, and only then is it decoded.
"""

from __future__ import annotations

import hashlib
import re
import tempfile
from pathlib import Path

from replay.events.recording import CaseRecording, CaseTimes
from replay.events.schedule import Plan
from replay.service.decode import clinical_times, decode_case
from replay.service.folder_store import FolderStore, ReadLog, refused
from replay.service.run_record import RunRecord, is_state_folder_name
from scripts.ingestion.check import CheckedFile
from scripts.ingestion.gate import verify_before_read
from scripts.ingestion.manifest import ManifestRef, read_manifest
from scripts.ingestion.store import ObjectStore, ObjectVersion, StoreError

CASE_KEY = re.compile(r".*/vital_files/(\d{4,})\.vital")
CLINICAL_SUFFIX = "/clinical_data.csv"
CLINICAL_LIMIT_BYTES = 16 * 1024 * 1024


class InputError(RuntimeError):
    """A manifest, a version or a file the replay needs is not what the manifest says."""


def read_log_path(text: str) -> Path:
    named = Path(text)
    if is_state_folder_name(named.name):
        raise refused()
    return named.parent.resolve() / named.name


def open_store(record: RunRecord, reference: ManifestRef) -> ObjectStore:
    """The object store of a run. A folder store's read log, when the record names one, is created here, once, as a new file, so every command that opens the
    store gets the same checks: a last name that the replay writes in its state folder (``is_state_folder_name``) is refused, and only the log's folder is
    resolved, never its last part, so a link in that place is left for the exclusive create to refuse. A refusal and a path that exists in any form are
    ``CANNOT_WRITE`` before any read, and nothing is created."""
    if record.object_store.kind == "folder" and record.object_store.root is not None:
        read_log = ReadLog.create(read_log_path(record.object_store.read_log)) if record.object_store.read_log else None
        return FolderStore(Path(record.object_store.root), reference.bucket, read_log)
    from replay.service.s3_store import S3ReadStore  # the folder store needs no boto3, and the host's store is only loaded on the host

    return S3ReadStore(reference.bucket, record.object_store.region or "")


def version_of(entry: CheckedFile) -> ObjectVersion:
    return ObjectVersion(entry.key, entry.version_id, entry.size, entry.retrieved_utc, True, False)


class VerifiedInputs:
    """The files the run needs, each bound to the object version of its passed manifest and checked by the gate."""

    def __init__(self, store: ObjectStore, entries: dict[str, CheckedFile], clinical: CheckedFile, cases: dict[int, CheckedFile]) -> None:
        self.store = store
        self.entries = entries
        self.clinical = clinical
        self.cases = cases

    def read_clinical(self) -> bytes:
        return self.store.get_version_bytes(version_of(self.clinical), CLINICAL_LIMIT_BYTES) if self.clinical.size else b""

    def times(self) -> dict[int, CaseTimes]:
        table = self.read_clinical()
        if hashlib.sha256(table).hexdigest() != self.clinical.sha256:
            raise InputError("the clinical table read is not the file whose SHA-256 the manifest holds")
        return clinical_times(table, self.cases)

    def decode(self, times: CaseTimes) -> CaseRecording:
        entry = self.cases[times.case_id]
        digest = hashlib.sha256()
        with tempfile.TemporaryDirectory(prefix="replay_case_") as scratch:
            target = Path(scratch) / f"{times.case_id:04d}.vital"
            try:
                with target.open("wb") as handle:
                    for block in self.store.iter_version(version_of(entry)):
                        digest.update(block)
                        handle.write(block)
            except StoreError as error:
                raise InputError(str(error)) from None
            if digest.hexdigest() != entry.sha256:
                raise InputError(f"case {times.case_id}: the version read is not the file whose SHA-256 the manifest holds")
            return decode_case(target, times)


def verify_inputs(record: RunRecord) -> VerifiedInputs:
    """Run the gate for the clinical table and the case files of the run and return the verified entries. A refusal is a ``GateRefused``."""
    try:
        reference = ManifestRef.from_json(Path(record.manifest_reference).read_text())
    except OSError:
        raise InputError("the manifest reference cannot be read") from None
    store = open_store(record, reference)
    manifest = read_manifest(store, reference)
    available = {int(match.group(1)): entry.source_key for entry in manifest.entries if (match := CASE_KEY.fullmatch(entry.source_key))}
    wanted = sorted(record.cases) if record.cases else sorted(available)
    absent = [case for case in wanted if case not in available]
    if absent:
        raise InputError(f"the manifest holds no case file for case {absent[0]}")
    clinical_keys = [entry.source_key for entry in manifest.entries if entry.source_key.endswith(CLINICAL_SUFFIX)]
    if len(clinical_keys) != 1:
        raise InputError("the manifest must hold the clinical table once")
    keys = [clinical_keys[0], *(available[case] for case in wanted)]
    entries = {entry.source_key: entry for entry in verify_before_read(store, reference, keys)}
    return VerifiedInputs(store, entries, entries[clinical_keys[0]], {case: entries[available[case]] for case in wanted})


def plan_of(record: RunRecord, times: dict[int, CaseTimes]) -> Plan:
    order = sorted(times) if record.cases is None else list(record.cases)
    return Plan(
        record.run_id,
        record.rooms,
        tuple(times[case] for case in order),
        record.stagger_seconds * 1_000_000,
        record.turnover_seconds * 1_000_000,
        record.window_seconds * 1_000_000,
    )
