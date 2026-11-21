"""The verify-before-read gate that the record connection, Bronze and the replay call before they read a payload.

Given the reference of a passed manifest and the source keys a reader needs, it confirms that the current object version in the bucket for
each key is the version the manifest binds, and that the bound version exists with the bound size. It lists versions and reads the manifest
only. It never reads a payload. A missing or substituted version, or a key the manifest does not bind, refuses the whole call and names each
key and its cause.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from scripts.ingestion.check import CheckedFile
from scripts.ingestion.landing import LANDING_PREFIX
from scripts.ingestion.manifest import ManifestError, ManifestRef, read_manifest
from scripts.ingestion.store import ObjectStore, StoreError

NOT_IN_MANIFEST = "not_in_manifest"
BUCKET_DIFFERS = "bucket_differs"
MISSING = "missing"
VERSION_SUBSTITUTED = "version_substituted"
SIZE_DIFFERS = "size_differs"


@dataclass(frozen=True)
class Problem:
    source_key: str
    cause: str


class GateRefused(RuntimeError):
    def __init__(self, message: str, problems: Sequence[Problem] = ()) -> None:
        super().__init__(message)
        self.problems = tuple(problems)


def verify_before_read(store: ObjectStore, reference: ManifestRef, source_keys: Sequence[str] | None = None) -> tuple[CheckedFile, ...]:
    """Return the manifest entries for ``source_keys`` (every entry when None) once each is confirmed, or raise ``GateRefused`` before any payload is read."""
    try:
        manifest = read_manifest(store, reference)
        listed = store.list_versions(LANDING_PREFIX)
    except (ManifestError, StoreError) as error:
        raise GateRefused(f"refused before any payload was read: {error}") from error
    bound = {entry.source_key: entry for entry in manifest.entries}
    wanted = list(dict.fromkeys(bound if source_keys is None else source_keys))
    problems: list[Problem] = []
    entries: list[CheckedFile] = []
    for source_key in wanted:
        entry = bound.get(source_key)
        versions = [version for version in listed if entry and version.key == entry.key]
        current = next((version for version in versions if version.is_latest), None)
        held = next((version for version in versions if entry and version.version_id == entry.version_id and not version.is_delete_marker), None)
        if entry is None:
            problems.append(Problem(source_key, NOT_IN_MANIFEST))
        elif entry.bucket != store.bucket:
            problems.append(Problem(source_key, BUCKET_DIFFERS))
        elif current is None or current.is_delete_marker or held is None:
            problems.append(Problem(source_key, MISSING))
        elif current.version_id != entry.version_id:
            problems.append(Problem(source_key, VERSION_SUBSTITUTED))
        elif held.size != entry.size:
            problems.append(Problem(source_key, SIZE_DIFFERS))
        else:
            entries.append(entry)
    if problems:
        raise GateRefused("refused before any payload was read: " + "; ".join(f"{problem.source_key}: {problem.cause}" for problem in problems), problems)
    return tuple(entries)
