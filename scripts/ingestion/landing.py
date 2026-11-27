"""Where each selected file landed in the project's bucket: the pairing records.

Airbyte names a landed key from the sync that wrote it, so a landed key is not a function of the source key. The check takes the pairing
of each source key with its landed key and its size as records, and ``load_pairing_records`` is the one place that makes them. It reads the
neutral form described below. The reader of Airbyte's own metadata files, whose form the copy probe measures, goes here and returns the same
records. Nothing else guesses or builds a landed key: the manifest holds the key that each record named.

The neutral form is a JSON list of ``{"source_key": ..., "landed_key": ..., "size": ...}``.

``pairings_from_metadata`` reads the metadata record that Airbyte writes for each file of a raw copy, as JSON lines: an ``_airbyte_data`` object with
the file's ``file_name`` (and its ``folder``), its ``bytes`` and its ``_airbyte_file_path``. The pinned S3 source gives the folder and the name of the
object's whole key, so together they are the source key, release path included
(https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-s3/source_s3/v4/stream_reader.py, ``upload``). The pinned S3
destination gives the whole landed key: its bucket path, which is the landing prefix, a folder of the sync's own and the source path
(https://github.com/airbytehq/airbyte/blob/master/airbyte-cdk/bulk/toolkits/legacy-task-load-object-storage/src/main/kotlin/io/airbyte/cdk/load/pipline/object_storage/file/FileChunkTask.kt).
Neither prefix is added here. A record of another source path or of a path outside the landing prefix is refused. A wrong reading cannot pass: the check
refuses a pairing whose landed key names no object, one whose size differs, a file with no record and a file with two.

Each metadata line also names the sync that wrote it: ``_airbyte_meta.sync_id``, which the destination takes from the stream's sync ID
(https://github.com/airbytehq/airbyte/blob/master/airbyte-cdk/bulk/toolkits/legacy-task-loader/src/main/kotlin/io/airbyte/cdk/load/data/DestinationRecordToAirbyteValueWithMeta.kt)
and the platform sets to the number of the job
(https://github.com/airbytehq/airbyte-platform/blob/main/airbyte-config/config-persistence/src/main/kotlin/io/airbyte/config/persistence/helper/CatalogGenerationSetter.kt).
A stopped sync can have committed the records of files whose checkpoint Airbyte had not saved, so the next sync copies them again and writes records of
its own; a file whose checkpoint was saved is not copied again and keeps the stopped sync's record only. So for each source key the records of one sync
are taken: a sync that the caller names as succeeded before any other, and among those the one of the highest number. The metadata does not say whether a
sync succeeded; without a name, the highest number wins. Every record of that sync is kept, so a file that one sync recorded twice is still refused by the
check.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType

from scripts.ingestion.closed import ClosedInputError, closed_object, parse_json
from scripts.ingestion.notice import ClosedError, Code, Notice, Subject
from scripts.ingestion.selection import relative_path

LANDING_PREFIX = "landing/"
METADATA_SUFFIX = ".jsonl"
PAIRING_FIELDS: Mapping[str, type] = MappingProxyType({"source_key": str, "landed_key": str, "size": int})


class PairingError(ClosedError, ValueError):
    """The pairing records are not a list of a source key, a landed key and a size."""


@dataclass(frozen=True, slots=True)
class PairingRecord:
    source_key: str
    landed_key: str
    size: int


def load_pairing_records(text: str) -> tuple[PairingRecord, ...]:
    try:
        entries = parse_json(text, Subject.PAIRINGS)
        if type(entries) is not list:
            raise PairingError(Notice(Code.PAIRINGS_NOT_LIST))
        records = tuple(PairingRecord(**closed_object(entry, PAIRING_FIELDS, Subject.PAIRING)) for entry in entries)
    except ClosedInputError as error:
        raise PairingError(*error.notices) from None
    if any(not (record.source_key and record.landed_key and record.size >= 0) for record in records):
        raise PairingError(Notice(Code.PAIRING_EMPTY))
    return records


def landed_key_of(path: str) -> str:
    """The landed key of the file path of a metadata record: the path is the whole key, a plain path below the landing prefix. Raise ``ValueError`` for any
    other path.

    One leading slash is dropped. A path that does not start with the landing prefix is refused, and so are an empty part, a ``.`` part, a ``..`` part, a
    backslash and a control character, so no path leaves the landing prefix after it is normalised.
    """
    relative = path[1:] if path.startswith("/") else path
    parts = relative.split("/")
    if not relative.startswith(LANDING_PREFIX) or any(
        part in ("", ".", "..") or "\\" in part or any(ord(character) < 32 for character in part) for part in parts
    ):
        raise ValueError
    return relative


def metadata_pairing(line: str, source_prefix: str) -> tuple[int, PairingRecord]:
    """The sync and the pairing of one metadata line. Raise ``ValueError`` when the line is not a record of the connectors' form."""
    record = parse_json(line, Subject.PAIRINGS)
    data, sync = record["_airbyte_data"], record["_airbyte_meta"]["sync_id"]
    if type(sync) is not int or sync < 0:
        raise ValueError
    name, size, landed = data["file_name"], data["bytes"], data["_airbyte_file_path"]
    folder = data.get("folder", "")
    if type(name) is not str or type(folder) is not str or type(landed) is not str or type(size) is not int or size < 0:
        raise ValueError
    relative = f"{folder.strip('/')}/{name}" if folder.strip("/") and not name.startswith(folder.strip("/") + "/") else name
    if not relative_path(relative) or not relative.startswith(source_prefix):
        raise ValueError
    return sync, PairingRecord(relative, landed_key_of(landed), size)


def pairings_from_metadata(texts: Iterable[str], source_prefix: str, succeeded: Collection[int] = ()) -> tuple[PairingRecord, ...]:
    """The pairing records of the metadata files' lines, in file and line order. Raise ``PairingError`` naming the line of the first that is not a record.

    A landed file belongs to one source: a record that pairs a landed key with another source than an earlier record does, of whatever sync, is refused by
    its number. For each source key only the records of one sync are returned: of the syncs that hold one, a sync of ``succeeded`` before any other and then
    the highest number. A source that has two records in that sync is left to the check, which reports it as a duplicate pairing. Names in ``succeeded`` of
    which no record is are refused, because the choice would not be the caller's; one of several may have written none.
    """
    records: list[PairingRecord] = []
    syncs: list[int] = []
    owner: dict[str, str] = {}
    for text in texts:
        for number, line in enumerate(text.splitlines(), 1):
            if not line.strip():
                continue
            try:
                sync, found = metadata_pairing(line, source_prefix)
            except ClosedInputError, KeyError, TypeError, ValueError, AttributeError:
                raise PairingError(Notice(Code.METADATA_LINE, position=number)) from None
            records.append(found)
            syncs.append(sync)
            if owner.setdefault(found.landed_key, found.source_key) != found.source_key:
                raise PairingError(Notice(Code.PAIRING_LANDED_TWICE, position=len(records)))
    if succeeded and not set(succeeded) & set(syncs):
        raise PairingError(Notice(Code.PAIRINGS_NO_SUCCEEDED_SYNC))
    taken: dict[str, tuple[bool, int]] = {}
    for sync, record in zip(syncs, records, strict=True):
        taken[record.source_key] = max(taken.get(record.source_key, (False, 0)), (sync in succeeded, sync))
    return tuple(record for sync, record in zip(syncs, records, strict=True) if taken[record.source_key][1] == sync)
