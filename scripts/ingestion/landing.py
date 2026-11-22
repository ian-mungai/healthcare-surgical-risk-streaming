"""Where each selected file landed in the project's bucket: the pairing records.

Airbyte names a landed key from the sync that wrote it, so a landed key is not a function of the source key. The check takes the pairing
of each source key with its landed key and its size as records, and ``load_pairing_records`` is the one place that makes them. It reads the
neutral form described below. The reader of Airbyte's own metadata files, whose form the copy probe measures, goes here and returns the same
records. Nothing else guesses or builds a landed key: the manifest holds the key that each record named.

The neutral form is a JSON list of ``{"source_key": ..., "landed_key": ..., "size": ...}``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from scripts.ingestion.closed import ClosedInputError, closed_object, parse_json
from scripts.ingestion.notice import ClosedError, Code, Notice, Subject

LANDING_PREFIX = "landing/"
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
