"""The parts of the Bronze job that need a library of the Spark image: the object store of the replay and the replay's reading of the clinical table.

The fast tests do not import this module: the stores need boto3 on the host and the clinical table's reader sits in the replay's decoder module,
which imports the publisher's library.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from replay.service.decode import clinical_times
from replay.service.folder_store import FolderStore, ReadLog
from scripts.ingestion.store import ObjectStore
from spark.bronze.inputs import CLINICAL_LIMIT_BYTES, CaseFile, InputError, StoreSpec, VerifiedInputs


def create_read_log(path: Path) -> None:
    """Make the read log of the job, once, in the driver and before any read: it is always a new file, so a path that exists in any form is refused with
    ``CANNOT_WRITE``. The driver's store and the partitions then attach to it (``open_store``)."""
    ReadLog.create(path).close()


def open_store(spec: StoreSpec) -> ObjectStore:
    if spec.kind == "folder" and spec.root:
        return FolderStore(Path(spec.root), spec.bucket, ReadLog.attach(Path(spec.read_log)) if spec.read_log else None)
    if spec.kind == "s3" and spec.region:
        from replay.service.s3_store import S3ReadStore  # boto3 is needed only on the host

        return S3ReadStore(spec.bucket, spec.region)
    raise InputError("the object store is a folder with its root or an s3 bucket with its region")


def caseend_times(store: ObjectStore, inputs: VerifiedInputs) -> dict[int, int]:
    """The clinical end of each wanted case in microseconds, from the clinical table that the manifest binds. The table's SHA-256 is checked first."""
    entry = inputs.clinical
    table = store.get_version_bytes(CaseFile(0, entry.key, entry.version_id, entry.size, entry.sha256, entry.retrieved_utc).version, CLINICAL_LIMIT_BYTES)
    if hashlib.sha256(table).hexdigest() != entry.sha256:
        raise InputError("the clinical table read is not the file whose SHA-256 the manifest holds")
    return {case: times.caseend_us for case, times in clinical_times(table, [file.case_id for file in inputs.cases]).items()}
