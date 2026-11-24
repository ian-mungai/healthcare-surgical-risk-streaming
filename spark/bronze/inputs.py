"""The inputs of the Bronze job: the gate, then the bound object versions of the case files and the clinical table.

Before any payload is read, ``verify_before_read`` (``scripts/ingestion/gate.py``) confirms that the object version of each file that the run needs is the
version of its passed manifest, and refuses a missing or substituted version for the whole call. After that a worker reads each file by the version ID that the
manifest binds into a scratch file and compares its SHA-256 with the manifest's before anything is decoded, so a version that changed after the gate cannot be
read by mistake.

The object store is the replay's: the project's bucket read with boto3 on the host, or a folder of object versions in a local run
(``replay/service/folder_store.py``).
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from replay.service.folder_store import refused
from replay.service.run_record import is_state_folder_name
from scripts.ingestion.check import CheckedFile
from scripts.ingestion.gate import verify_before_read
from scripts.ingestion.manifest import ManifestRef, read_manifest
from scripts.ingestion.notice import ClosedError, Code, Notice
from scripts.ingestion.store import ObjectStore, ObjectVersion, StoreError

CASE_KEY = re.compile(r".*/vital_files/(\d{4,})\.vital")
CASE_NUMBER = re.compile(r"[0-9]{1,9}")
CLINICAL_SUFFIX = "/clinical_data.csv"
CLINICAL_LIMIT_BYTES = 16 * 1024 * 1024


class InputError(RuntimeError):
    """A manifest, a version or a file that the job needs is not what the manifest says."""


class CasesError(ClosedError, InputError):
    """The ``--cases`` list is not case numbers separated by commas. Its text is the fixed usage notice, so it shows no part of the list."""


def parse_cases(text: str) -> list[int] | None:
    """The case numbers of a ``--cases`` list (digits only, an empty item is skipped), or ``None`` for every case file of the manifest."""
    items = [item for item in text.split(",") if item]
    if not all(CASE_NUMBER.fullmatch(item) for item in items):
        raise CasesError(Notice(Code.USAGE))
    return [int(item) for item in items] or None


@dataclass(frozen=True, slots=True)
class StoreSpec:
    """Where the landed object versions are: a folder (a local run; ``read_log`` is a file that gets each read version) or the bucket in ``region``."""

    kind: str
    bucket: str
    root: str = ""
    region: str = ""
    read_log: str = ""

    @property
    def log_to_create(self) -> Path | None:
        """The read log that the driver makes before any read: only a folder store that carries a log path has one. A log whose last name is a file that the
        replay writes in its state folder (``is_state_folder_name``, any letter case) is refused with ``CANNOT_WRITE`` here, before anything is created."""
        if self.kind != "folder" or not self.read_log:
            return None
        path = Path(self.read_log)
        if is_state_folder_name(path.name):
            raise refused()
        return path


@dataclass(frozen=True, slots=True)
class CaseFile:
    """One case file as its manifest entry binds it: the object version, its size and its SHA-256."""

    case_id: int
    key: str
    version_id: str
    size: int
    sha256: str
    retrieved_utc: str

    @property
    def version(self) -> ObjectVersion:
        return ObjectVersion(self.key, self.version_id, self.size, self.retrieved_utc, True, False)


@dataclass(frozen=True, slots=True)
class VerifiedInputs:
    clinical: CheckedFile
    cases: tuple[CaseFile, ...]


def verify_inputs(store: ObjectStore, reference: ManifestRef, wanted: Sequence[int] | None) -> VerifiedInputs:
    """Run the gate for the clinical table and the wanted case files (every case file when ``wanted`` is None). A refusal is a ``GateRefused``."""
    manifest = read_manifest(store, reference)
    available = {int(match.group(1)): entry.source_key for entry in manifest.entries if (match := CASE_KEY.fullmatch(entry.source_key))}
    cases = sorted(available) if wanted is None else sorted(set(wanted))
    absent = [case for case in cases if case not in available]
    if absent:
        raise InputError(f"the manifest holds no case file for case {absent[0]}")
    if not cases:
        raise InputError("no case was asked for")
    clinical_keys = [entry.source_key for entry in manifest.entries if entry.source_key.endswith(CLINICAL_SUFFIX)]
    if len(clinical_keys) != 1:
        raise InputError("the manifest must hold the clinical table once")
    verified = {entry.source_key: entry for entry in verify_before_read(store, reference, [clinical_keys[0], *(available[case] for case in cases)])}
    files = []
    for case in cases:
        entry = verified[available[case]]
        files.append(CaseFile(case, entry.key, entry.version_id, entry.size, entry.sha256, entry.retrieved_utc))
    return VerifiedInputs(verified[clinical_keys[0]], tuple(files))


def fetch_case(store: ObjectStore, file: CaseFile, folder: Path) -> Path:
    """Read exactly the bound version of a case file into ``folder`` and check its SHA-256. The file is read in blocks and never held whole."""
    digest = hashlib.sha256()
    target = folder / f"{file.case_id:04d}.vital"
    try:
        with target.open("wb") as handle:
            for block in store.iter_version(file.version):
                digest.update(block)
                handle.write(block)
    except StoreError as error:
        raise InputError(f"case {file.case_id}: {error}") from None
    if digest.hexdigest() != file.sha256:
        raise InputError(f"case {file.case_id}: the version read is not the file whose SHA-256 the manifest holds")
    return target
