"""Where each selected file landed in the project's bucket: the pairing records.

Airbyte names a landed key from the sync that wrote it, so a landed key is not a function of the source key. The check takes the pairing
of each source key with its landed key and its size as records, and ``load_pairing_records`` is the one place that makes them. It reads the
neutral form described below. The reader of Airbyte's own metadata files, whose form the copy probe measures, goes here and returns the same
records. Nothing else guesses or builds a landed key: the manifest holds the key that each record named.

The neutral form is a JSON list of ``{"source_key": ..., "landed_key": ..., "size": ...}``.

``pairings_from_metadata`` reads the metadata record that Airbyte writes for each file of a raw copy, as JSON lines, in the form of its
documentation: an ``_airbyte_data`` object with the file's ``file_name`` (and its ``folder`` when the sub-folders are kept), its ``bytes`` and its
``_airbyte_file_path``, the path of the landed file below the landing prefix. The source key is the release path and the file's path in the release.
The landed key is the landing prefix and the file path. The host's fixed-sample copy confirms the form. A wrong reading cannot pass: the check refuses
a pairing whose landed key names no object, one whose size differs, a file with no record and a file with two.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
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


def metadata_pairing(line: str, source_prefix: str) -> PairingRecord:
    """The pairing of one metadata line. Raise ``ValueError`` when the line is not a record of the documented form."""
    record = parse_json(line, Subject.PAIRINGS)
    data = record["_airbyte_data"]
    name, size, landed = data["file_name"], data["bytes"], data["_airbyte_file_path"]
    folder = data.get("folder", "")
    if type(name) is not str or type(folder) is not str or type(landed) is not str or type(size) is not int or size < 0:
        raise ValueError
    relative = f"{folder.strip('/')}/{name}" if folder.strip("/") and not name.startswith(folder.strip("/") + "/") else name
    key = landed.lstrip("/")
    if not relative_path(relative) or not key:
        raise ValueError
    return PairingRecord(source_prefix + relative, LANDING_PREFIX + key, size)


def pairings_from_metadata(texts: Iterable[str], source_prefix: str) -> tuple[PairingRecord, ...]:
    """The pairing records of the metadata files' lines, in file and line order. Raise ``PairingError`` naming the line of the first that is not a record."""
    records: list[PairingRecord] = []
    for text in texts:
        for number, line in enumerate(text.splitlines(), 1):
            if not line.strip():
                continue
            try:
                records.append(metadata_pairing(line, source_prefix))
            except ClosedInputError, KeyError, TypeError, ValueError, AttributeError:
                raise PairingError(Notice(Code.METADATA_LINE, position=number)) from None
    return tuple(records)
