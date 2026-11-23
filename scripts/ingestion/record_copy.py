"""The copy of the manifest-bound object versions of the two CSV tables to the run's own read keys.

Airbyte's S3 source cannot be given an object version ID: it lists the keys under a prefix and reads the current version of each. So the Dagster step copies
the exact version that the passed manifest binds, with ``CopyObject`` from that version ID, to ``records_input/<run ID>/<SHA-256>/<table>.csv``: a key that
is unique to the run and to the verified content, and the source reads only that key. The order is fixed:

1. The gate (``verify_before_read``) confirms the manifest and that each table's current landed version is the bound one. A manifest that is missing or edited,
   a missing version and a substituted version are refused here, before any copy.
2. A version at a read key that the run's ledger does not name (a version of another writer, a delete marker, a leftover that a stop left unrecorded) is
   refused with a message and never deleted. The ledger is the run's state file: a copy's version ID is recorded right after the copy and before its check.
3. Each copy is made from the bound version, and its SHA-256 and size are compared with the manifest's. A copy that differs is refused, and a retry deletes
   it by its recorded version ID and copies again. A second call finds each copy already equal to the manifest and writes nothing.

``confirm_copies`` lists the read prefix again and refuses it unless each key's current version is the recorded one and nothing else is there. The step calls
it before each sync. ``verify_read_keys`` is the check after the last sync: every read key must hold exactly the one version the run wrote, current or not,
because a version that was current while Airbyte read, or one restored afterwards, leaves an entry (``copy_changed``).

``check_read_keys_owned`` is the first look at the read prefix, before anything is copied or changed: a version that the ledger does not name is refused.

``remove_copies`` deletes, by key and version ID, each version that the ledger names. A version leaves the ledger only after the delete of exactly that
version succeeded (a version that is already gone is not an error for the store's delete), never because a listing did not show it: a listing can stop early
or be behind. The load step calls it once the run's record is written, so the copies are gone after a load, and keeps them when the load fails, for inspection.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from scripts.ingestion.check import CheckedFile, stream_sha256
from scripts.ingestion.gate import verify_before_read
from scripts.ingestion.manifest import ManifestRef
from scripts.ingestion.record_definitions import CSV_TABLES, RECORD_SOURCE_KEYS, read_key, read_prefix
from scripts.ingestion.store import NO_SUCH_VERSION, NULL_VERSION_IDS, ObjectStore, ObjectVersion, S3Store

COPY_TIMEOUT_SECONDS = 300


class CopyError(RuntimeError):
    """A copy cannot be made or does not match the manifest. The message names a table, never a byte of its content."""


class CopyChanged(CopyError):
    """A read key does not hold exactly the one version the run wrote. The message starts with ``copy_changed``."""


class CopyLedger(Protocol):
    """The run's record of the object versions it wrote, which ``RunState`` keeps in a file."""

    def versions(self, key: str) -> frozenset[str]: ...

    def keys(self) -> frozenset[str]: ...

    def add(self, key: str, version_id: str) -> None: ...

    def discard(self, key: str, version_id: str) -> None: ...


class CopyingStore(ObjectStore, Protocol):
    def copy_version(self, version: ObjectVersion, key: str) -> ObjectVersion:
        """Copy exactly ``version`` to ``key``, which gets a new version, and return it. Raise ``CopyError`` unless the store copied that source version."""
        ...

    def delete_version(self, key: str, version_id: str) -> None:
        """Delete exactly this version or delete marker of ``key``. A version that is already gone is not an error. Any other failure raises."""
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

    def delete_version(self, key: str, version_id: str) -> None:
        reply = self.aws.call(
            "s3api",
            "delete-object",
            "--bucket",
            self.bucket,
            "--key",
            key,
            "--version-id",
            version_id,
            missing=NO_SUCH_VERSION,
            timeout=COPY_TIMEOUT_SECONDS,
        )
        if reply is None:
            return
        if type(reply) is not dict or reply.get("VersionId") != version_id:
            raise CopyError("the delete call did not name the version that was asked for")


def current_object(versions: Sequence[ObjectVersion], key: str) -> ObjectVersion | None:
    found = [version for version in versions if version.key == key and version.is_latest and not version.is_delete_marker]
    return found[0] if found else None


def refuse_unrecorded(listed: Sequence[ObjectVersion], ledger: CopyLedger) -> None:
    if any(version.version_id not in ledger.versions(version.key) for version in listed):
        raise CopyError("a version in the read prefix of the run is not recorded by this run: it is refused and not deleted")


def check_read_keys_owned(store: ObjectStore, run_id: str, ledger: CopyLedger) -> None:
    """Raise ``CopyError`` when the run's read prefix holds an object version or a delete marker that the ledger does not name. Nothing is deleted."""
    refuse_unrecorded(store.list_versions(read_prefix(run_id)), ledger)


def copy_bound_versions(
    store: CopyingStore, reference: ManifestRef, run_id: str, ledger: CopyLedger, entries: Sequence[CheckedFile] | None = None
) -> tuple[Copied, ...]:
    """Copy the version of each CSV table that the manifest binds to the run's read keys and return the confirmed copies.

    Raise ``GateRefused`` before any copy when the manifest or a bound version is not as the passed check left it, ``ValueError`` for a run ID that is not a
    plain name, and ``CopyError`` for an object in the prefix that is not a table, a version that the ledger does not name, or a copy whose SHA-256 or size
    differs from the manifest's. ``entries`` are the result of ``verify_before_read`` of this call's caller.
    """
    prefix = read_prefix(run_id)
    bound = {entry.source_key: entry for entry in (verify_before_read(store, reference, RECORD_SOURCE_KEYS) if entries is None else entries)}
    wanted = {read_key(run_id, table, bound[table.source_key].sha256): (table, bound[table.source_key]) for table in CSV_TABLES if table.source_key}
    listed = store.list_versions(prefix)
    if {version.key for version in listed} - set(wanted):
        raise CopyError("the read prefix of the run holds an object that is not one of its tables")
    refuse_unrecorded(listed, ledger)
    copied: list[Copied] = []
    for key, (table, entry) in wanted.items():
        here = [version for version in listed if version.key == key]
        existing = current_object(listed, key)
        good = existing if existing is not None and existing.size == entry.size and stream_sha256(store, existing) == entry.sha256 else None
        for version in here:
            if good is None or version.version_id != good.version_id:
                store.delete_version(version.key, version.version_id)
                ledger.discard(key, version.version_id)
        if good is None:
            sources = [item for item in store.list_versions(entry.key) if item.key == entry.key and item.version_id == entry.version_id]
            if not sources:
                raise CopyError(f"the bound version of {table.name} is gone")
            good = store.copy_version(sources[0], key)
            ledger.add(key, good.version_id)
            if good.size != entry.size or stream_sha256(store, good) != entry.sha256:
                raise CopyError(f"the copy of {table.name} does not match the manifest's SHA-256 and size")
        copied.append(Copied(table.name, entry.source_key, key, good.version_id, entry.sha256, entry.size))
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


def verify_read_keys(store: ObjectStore, copies: Sequence[Copied]) -> None:
    """Raise ``CopyChanged`` unless each read key holds exactly the one version that the run wrote: no other version, no delete marker, none current but it."""
    if not copies:
        raise CopyChanged("copy_changed: there is no copy to verify")
    for copy in copies:
        versions = [version for version in store.list_versions(copy.key) if version.key == copy.key]
        if [(version.version_id, version.is_delete_marker) for version in versions] != [(copy.version_id, False)]:
            raise CopyChanged(f"copy_changed: the versions at the read key of {copy.table} are not exactly the one version that this run wrote")


def remove_copies(store: CopyingStore, run_id: str, ledger: CopyLedger) -> int:
    """Delete each version of the run's read keys that the ledger names, by key and version ID, and return how many deletes succeeded.

    A version is forgotten right after its own delete succeeded, so a stop or a failure leaves every other version owned. A delete that fails raises, and the
    versions that were not deleted stay in the ledger for the next call. Nothing that the ledger does not name is deleted, and no listing is read.
    """
    prefix = read_prefix(run_id)
    removed = 0
    for key in sorted(ledger.keys()):
        if not key.startswith(prefix):
            continue
        for version_id in sorted(ledger.versions(key)):
            store.delete_version(key, version_id)
            ledger.discard(key, version_id)
            removed += 1
    return removed
