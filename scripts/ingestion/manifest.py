"""The manifest of passed files.

It is published only from a complete passed check and only for the object versions that the check verified. Each entry binds the landed
object version (bucket, key and non-null version ID), its SHA-256, its source key, its size, its release and its retrieval time, which is the
time S3 gave that version. The selection, the measurement period, the pinned checksum list and the digest of the check result are recorded once.

Where it lives: under ``manifests/<selection name>/<SHA-256 of its content>.json`` in the project's bucket, outside the payload prefix, as its own
object version. The key holds the content's digest, so the same content has the same key and a rerun adds no version. A reader holds a
``ManifestRef`` (bucket, key, version ID, SHA-256) and confirms three things: the version's bytes hash to the reference's SHA-256, the key ends
with that SHA-256, and the digest of the check result in the manifest equals the digest recomputed from its own entries.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any

from scripts.ingestion.check import CheckedFile, CheckResult, canonical, checked_file_from, current_versions, result_digest, sha256_of
from scripts.ingestion.closed import ClosedInputError, closed_object, parse_json
from scripts.ingestion.landing import LANDING_PREFIX
from scripts.ingestion.selection import Selection
from scripts.ingestion.store import ObjectStore, StoreError

MANIFEST_PREFIX = "manifests/"
MANIFEST_FORMAT = "landing_manifest_1"
MANIFEST_LIMIT_BYTES = 32 * 1024 * 1024
NULL_VERSION_IDS = ("", "null")


class PublishRefused(RuntimeError):
    """The manifest is not published: the check did not pass, is not complete, was edited or is no longer the state of the bucket."""


class ManifestError(RuntimeError):
    """A manifest cannot be trusted: it is missing, differs from its reference or is not a passed manifest."""


@dataclass(frozen=True)
class ManifestRef:
    bucket: str
    key: str
    version_id: str
    sha256: str

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=1, sort_keys=True) + "\n"

    @classmethod
    def from_json(cls, text: str) -> ManifestRef:
        try:
            return cls(
                **closed_object(
                    parse_json(text, "the manifest reference"), {"bucket": str, "key": str, "version_id": str, "sha256": str}, "the manifest reference"
                )
            )
        except ClosedInputError as error:
            raise ManifestError(str(error)) from None


@dataclass(frozen=True)
class Manifest:
    status: str
    measurement_period: str
    selection: dict[str, Any]
    checksum_list: dict[str, str]
    check_result_sha256: str
    entries: tuple[CheckedFile, ...]

    def recomputed_check_result_sha256(self) -> str:
        return result_digest(self.selection["keys_sha256"], self.checksum_list["sha256"], (asdict(entry) for entry in self.entries))


def manifest_key(selection_name: str, sha256: str) -> str:
    return f"{MANIFEST_PREFIX}{selection_name}/{sha256}.json"


def refusal(result: CheckResult, selection: Selection, store: ObjectStore) -> str | None:
    """Why this check result may not be published, or None. Each reason is judged in turn: pass, selection, completeness, edits, the bucket's state."""
    if result.findings or not result.files:
        return f"the check did not pass: {len(result.findings)} findings"
    if result.selection_sha256 != selection.keys_sha256 or result.selection_name != selection.name or result.list_sha256 != selection.checksum_list_sha256:
        return "the check result is not for this selection and its pinned checksum list"
    if {file.source_key for file in result.files} != set(selection.source_keys) or len(result.files) != len(selection.files):
        return "the check result does not hold every selected file once"
    if result.claimed_sha256 != result.result_sha256:
        return "the check result was edited after the check"
    current = current_versions(store.list_versions(LANDING_PREFIX))
    changed = []
    for file in result.files:
        now = current.get(file.key)
        if file.bucket != store.bucket:
            return "the check result is for another bucket"
        if now is None or now.is_delete_marker:
            changed.append(f"{file.source_key}: no longer present after the check")
        elif now.version_id != file.version_id:
            changed.append(f"{file.source_key}: a new version landed after the check")
    return "; ".join(changed) or None


def build_document(selection: Selection, result: CheckResult) -> dict[str, Any]:
    return {
        "format": MANIFEST_FORMAT,
        "status": "passed",
        "measurement_period": selection.measurement_period,
        "selection": selection.record(),
        "checksum_list": {"key": selection.checksum_list_key, "sha256": selection.checksum_list_sha256},
        "check_result_sha256": result.result_sha256,
        "entries": [asdict(file) for file in sorted(result.files, key=lambda file: file.source_key)],
    }


def publish_manifest(store: ObjectStore, selection: Selection, result: CheckResult) -> ManifestRef:
    """Publish the manifest of a complete passed check and return its reference. The same content gives the same reference and writes nothing more."""
    reason = refusal(result, selection, store)
    if reason:
        raise PublishRefused(reason)
    data = canonical(build_document(selection, result))
    sha256 = sha256_of(data)
    key = manifest_key(selection.name, sha256)
    existing = [version for version in store.list_versions(key) if version.key == key and version.is_latest and not version.is_delete_marker]
    if existing:
        if sha256_of(store.get_version_bytes(existing[0], MANIFEST_LIMIT_BYTES)) != sha256:
            raise PublishRefused("the manifest key holds other content than its name says")
        return ManifestRef(store.bucket, key, existing[0].version_id, sha256)
    version = store.put_object(key, data)
    if sha256_of(store.get_version_bytes(version, MANIFEST_LIMIT_BYTES)) != sha256:
        raise PublishRefused("the published manifest reads back with other content")
    return ManifestRef(store.bucket, key, version.version_id, sha256)


def read_manifest(store: ObjectStore, reference: ManifestRef) -> Manifest:
    """Read the manifest version that the reference names and confirm it. Raise ``ManifestError`` for each way it cannot be trusted."""
    if reference.bucket != store.bucket:
        raise ManifestError("the manifest reference names another bucket than the one in use")
    if not reference.key.startswith(MANIFEST_PREFIX) or not reference.key.endswith(f"/{reference.sha256}.json"):
        raise ManifestError("the manifest key does not hold the SHA-256 of the reference")
    wanted = [version for version in store.list_versions(reference.key) if version.key == reference.key and version.version_id == reference.version_id]
    if not wanted or wanted[0].is_delete_marker:
        raise ManifestError("the manifest version of the reference does not exist")
    try:
        data = store.get_version_bytes(wanted[0], MANIFEST_LIMIT_BYTES)
    except StoreError as error:
        raise ManifestError(f"the manifest cannot be read: {error}") from error
    if sha256_of(data) != reference.sha256:
        raise ManifestError("the manifest's SHA-256 differs from its reference")
    try:
        document = closed_object(
            parse_json(data, "the manifest"),
            {"format": str, "status": str, "measurement_period": str, "selection": dict, "checksum_list": dict, "check_result_sha256": str, "entries": list},
            "the manifest",
        )
        selection = closed_object(
            document["selection"], {"name": str, "source_bucket": str, "source_prefix": str, "file_count": int, "keys_sha256": str}, "the manifest's selection"
        )
        checksum_list = closed_object(document["checksum_list"], {"key": str, "sha256": str}, "the manifest's checksum list")
        manifest = Manifest(
            document["status"],
            document["measurement_period"],
            selection,
            checksum_list,
            document["check_result_sha256"],
            tuple(checked_file_from(entry, "an entry of the manifest") for entry in document["entries"]),
        )
    except ClosedInputError as error:
        raise ManifestError(str(error)) from None
    if document.get("format") != MANIFEST_FORMAT or manifest.status != "passed":
        raise ManifestError("the manifest is not a passed manifest of this format")
    if reference.key != manifest_key(manifest.selection["name"], reference.sha256):
        raise ManifestError("the manifest key names another selection than the manifest does")
    if any(entry.version_id in NULL_VERSION_IDS for entry in manifest.entries):
        raise ManifestError("the manifest binds an object with a null version ID")
    if manifest.recomputed_check_result_sha256() != manifest.check_result_sha256:
        raise ManifestError("the manifest's entries do not match the digest of the check that produced it")
    return manifest
