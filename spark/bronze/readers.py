"""The parts of the Bronze job that open the object store and read the clinical table, with the shared package (``casefile``) that the replay uses too.

The S3 store needs boto3 on the host, so it is imported only when a job names a bucket.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from casefile.decoding import clinical_times
from casefile.folder_store import FolderStore
from scripts.ingestion.store import ObjectStore
from spark.bronze.inputs import CLINICAL_LIMIT_BYTES, CaseFile, InputError, StoreSpec, VerifiedInputs


def open_store(spec: StoreSpec) -> ObjectStore:
    if spec.kind == "folder" and spec.root:
        return FolderStore(Path(spec.root), spec.bucket, Path(spec.read_log) if spec.read_log else None)
    if spec.kind == "s3" and spec.region:
        from casefile.s3_store import S3ReadStore  # boto3 is needed only on the host

        return S3ReadStore(spec.bucket, spec.region)
    raise InputError("the object store is a folder with its root or an s3 bucket with its region")


def caseend_times(store: ObjectStore, inputs: VerifiedInputs) -> dict[int, int]:
    """The clinical end of each wanted case in microseconds, from the clinical table that the manifest binds. The table's SHA-256 is checked first."""
    entry = inputs.clinical
    table = store.get_version_bytes(CaseFile(0, entry.key, entry.version_id, entry.size, entry.sha256, entry.retrieved_utc).version, CLINICAL_LIMIT_BYTES)
    if hashlib.sha256(table).hexdigest() != entry.sha256:
        raise InputError("the clinical table read is not the file whose SHA-256 the manifest holds")
    return {case: times.caseend_us for case, times in clinical_times(table, [file.case_id for file in inputs.cases]).items()}
