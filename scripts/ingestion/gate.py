"""The verify-before-read gate that the record connection, Bronze and the replay call before they read a payload.

Given the reference of a passed manifest and the source keys a reader needs, it confirms that the current object version in the bucket for
each key is the version the manifest binds, and that the bound version exists with the bound size. It lists versions and reads the manifest
only. It never reads a payload. A missing or substituted version, or a key the manifest does not bind, refuses the whole call and names each
refused key by its place, ``requested key N`` in the request or ``manifest entry N`` when no key was named, and its cause.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from scripts.ingestion.check import CheckedFile
from scripts.ingestion.landing import LANDING_PREFIX
from scripts.ingestion.manifest import ManifestError, ManifestRef, read_manifest
from scripts.ingestion.notice import Cause, ClosedError, Code, Notice
from scripts.ingestion.store import ObjectStore, StoreError


@dataclass(frozen=True, slots=True)
class Problem:
    """A refused key: its place in the request, or among the manifest's entries when no key was named, and its cause. Never the key's text."""

    position: int
    cause: Cause
    requested: bool

    @property
    def notice(self) -> Notice:
        return Notice(Code.REQUESTED_POSITION if self.requested else Code.ENTRY_POSITION, position=self.position, cause=self.cause)


class GateRefused(ClosedError, RuntimeError):
    def __init__(self, *notices: Notice, problems: Sequence[Problem] = ()) -> None:
        super().__init__(*notices)
        self.problems = tuple(problems)


def verify_before_read(store: ObjectStore, reference: ManifestRef, source_keys: Sequence[str] | None = None) -> tuple[CheckedFile, ...]:
    """Return the manifest entries for ``source_keys`` (every entry when None) once each is confirmed, or raise ``GateRefused`` before any payload is read."""
    try:
        manifest = read_manifest(store, reference)
        listed = store.list_versions(LANDING_PREFIX)
    except (ManifestError, StoreError) as error:
        raise GateRefused(Notice(Code.GATE_REFUSED), *error.notices) from None
    bound = {entry.source_key: entry for entry in manifest.entries}
    requested = list(bound if source_keys is None else source_keys)
    problems: list[Problem] = []
    entries: list[CheckedFile] = []
    seen: set[str] = set()
    for position, source_key in enumerate(requested, 1):
        if source_key in seen:
            continue
        seen.add(source_key)
        entry = bound.get(source_key)
        versions = [version for version in listed if entry and version.key == entry.key]
        current = next((version for version in versions if version.is_latest), None)
        held = next((version for version in versions if entry and version.version_id == entry.version_id and not version.is_delete_marker), None)
        named = source_keys is not None
        if entry is None:
            problems.append(Problem(position, Cause.NOT_IN_MANIFEST, named))
        elif entry.bucket != store.bucket:
            problems.append(Problem(position, Cause.BUCKET_DIFFERS, named))
        elif current is None or current.is_delete_marker or held is None:
            problems.append(Problem(position, Cause.MISSING, named))
        elif current.version_id != entry.version_id:
            problems.append(Problem(position, Cause.VERSION_SUBSTITUTED, named))
        elif held.size != entry.size:
            problems.append(Problem(position, Cause.SIZE_DIFFERS, named))
        else:
            entries.append(entry)
    if problems:
        raise GateRefused(Notice(Code.GATE_REFUSED), *(problem.notice for problem in problems), problems=problems)
    return tuple(entries)
