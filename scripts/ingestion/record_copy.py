"""The copy of the manifest-bound object versions of the two CSV tables to the run's read prefix.

Airbyte's S3 source cannot be given an object version ID: it lists the keys under a prefix and reads the current version of each. So the Dagster step copies
the exact version that the passed manifest binds, with ``CopyObject`` from that version ID, to ``records_input/<run ID>/<table>.csv``, which holds one
version of one object, and the source reads only that prefix. The order is fixed:

1. The gate (``verify_before_read``) confirms the manifest and that each table's current landed version is the bound one. A manifest that is missing or edited,
   a missing version or a substituted version is refused here, before any copy.
2. Each copy is made from the bound version, and its SHA-256 and size are compared with the manifest's. A copy that differs is refused, and the call raises
   before it returns anything that a sync could read.
3. A second call finds each copy already equal to the manifest and writes nothing, so a retry adds no version.

``confirm_copies`` lists the read prefix again and refuses it unless it holds exactly the recorded versions. The step calls it before it starts a sync
and after the sync ends, so a copy replaced while Airbyte read it is found.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from scripts.ingestion.check import stream_sha256
from scripts.ingestion.gate import verify_before_read
from scripts.ingestion.manifest import ManifestRef
from scripts.ingestion.record_definitions import CSV_TABLES, RECORD_SOURCE_KEYS, read_key, read_prefix
from scripts.ingestion.store import NO_SUCH_VERSION, NULL_VERSION_IDS, ObjectStore, ObjectVersion, S3Store

COPY_TIMEOUT_SECONDS = 300


class CopyError(RuntimeError):
    """A copy cannot be made or does not match the manifest. The message names a table, never a byte of its content."""


class CopyingStore(ObjectStore, Protocol):
    def copy_version(self, version: ObjectVersion, key: str) -> ObjectVersion:
        """Copy exactly ``version`` to ``key``, which gets a new version, and return it. Raise ``CopyError`` unless the store copied that source version."""
        ...


@dataclass(frozen=True, slots=True)
class Copied:
    """One table's copy in the read prefix: the version that the manifest binds, as it was copied and confirmed."""

    table: str
    source_key: str
    key: str
    version_id: str
    sha256: str
    size: int


class S3CopyStore(S3Store):
    def copy_version(self, version: ObjectVersion, key: str) -> ObjectVersion:
        reply = self.aws.call(
            "s3api",
            "copy-object",
            "--bucket",
            self.bucket,
            "--key",
            key,
            "--copy-source",
            f"{self.bucket}/{version.key}?versionId={version.version_id}",
            missing=NO_SUCH_VERSION,
            timeout=COPY_TIMEOUT_SECONDS,
        )
        if reply is None:
            raise CopyError("the object version to copy does not exist")
        if type(reply) is not dict:
            raise CopyError("the copy call gave a reply that is not an object")
        copied = reply.get("VersionId")
        if reply.get("CopySourceVersionId") != version.version_id:
            raise CopyError("the copy call did not name the version that was asked for as its source")
        if type(copied) is not str or copied in NULL_VERSION_IDS:
            raise CopyError("the copy call gave no version ID for the copy")
        for listed in self.list_versions(key):
            if listed.key == key and listed.version_id == copied:
                return listed
        raise CopyError("the copy is not in the listing of its key")


def current_object(versions: Sequence[ObjectVersion], key: str) -> ObjectVersion | None:
    found = [version for version in versions if version.key == key and version.is_latest and not version.is_delete_marker]
    return found[0] if found else None


def copy_bound_versions(store: CopyingStore, reference: ManifestRef, run_id: str) -> tuple[Copied, ...]:
    """Copy the version of each CSV table that the manifest binds to the run's read prefix and return the confirmed copies.

    Raise ``GateRefused`` before any copy when the manifest or a bound version is not as the passed check left it, ``ValueError`` for a run ID that is not a
    plain name, and ``CopyError`` for an object in the prefix that is not a table or a copy whose SHA-256 or size differs from the manifest's.
    """
    prefix = read_prefix(run_id)
    bound = {entry.source_key: entry for entry in verify_before_read(store, reference, RECORD_SOURCE_KEYS)}
    wanted = {read_key(run_id, table): (table, bound[table.source_key]) for table in CSV_TABLES if table.source_key}
    listed = store.list_versions(prefix)
    if {version.key for version in listed} - set(wanted):
        raise CopyError("the read prefix of the run holds an object that is not one of its tables")
    copied: list[Copied] = []
    for key, (table, entry) in wanted.items():
        existing = current_object(listed, key)
        if existing is not None and existing.size == entry.size and stream_sha256(store, existing) == entry.sha256:
            version = existing
        else:
            sources = [item for item in store.list_versions(entry.key) if item.key == entry.key and item.version_id == entry.version_id]
            if not sources:
                raise CopyError(f"the bound version of {table.name} is gone")
            version = store.copy_version(sources[0], key)
            if version.size != entry.size or stream_sha256(store, version) != entry.sha256:
                raise CopyError(f"the copy of {table.name} does not match the manifest's SHA-256 and size")
        copied.append(Copied(table.name, entry.source_key, key, version.version_id, entry.sha256, entry.size))
    return tuple(copied)


def confirm_copies(store: ObjectStore, run_id: str, copies: Sequence[Copied]) -> None:
    """Raise ``CopyError`` unless the run's read prefix holds exactly the recorded copies as their current versions and nothing else."""
    if not copies:
        raise CopyError("there is no copy to confirm")
    listed = store.list_versions(read_prefix(run_id))
    keys = {copy.key for copy in copies}
    extra = any(version.key not in keys for version in listed)
    changed = any(now is None or now.version_id != copy.version_id for copy in copies for now in [current_object(listed, copy.key)])
    if extra or changed:
        raise CopyError("the read prefix of the run is no longer exactly the copies that were confirmed")
