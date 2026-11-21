"""Where each selected file landed in the project's bucket: the pairing records.

Airbyte names a landed key from the sync that wrote it, so a landed key is not a function of the source key. The check takes the pairing
of each source key with its landed key and its size as records, and ``load_pairing_records`` is the one place that makes them. It reads the
neutral form described below. The reader of Airbyte's own metadata files, whose form the copy probe measures, goes here and returns the same
records. Nothing else guesses or builds a landed key: the manifest holds the key that each record named.

The neutral form is a JSON list of ``{"source_key": ..., "landed_key": ..., "size": ...}``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

LANDING_PREFIX = "landing/"


class PairingError(ValueError):
    """The pairing records are not a list of a source key, a landed key and a size."""


@dataclass(frozen=True)
class PairingRecord:
    source_key: str
    landed_key: str
    size: int


def load_pairing_records(text: str) -> tuple[PairingRecord, ...]:
    try:
        entries = json.loads(text)
        if not isinstance(entries, list):
            raise TypeError("not a list")
        records = tuple(PairingRecord(entry["source_key"], entry["landed_key"], entry["size"]) for entry in entries)
    except (ValueError, KeyError, TypeError) as error:
        raise PairingError(f"the pairing records are not a list of source key, landed key and size: {type(error).__name__}") from error
    if any(not (record.source_key and record.landed_key and type(record.size) is int and record.size >= 0) for record in records):
        raise PairingError("a pairing record needs a source key, a landed key and a size of zero or more")
    return records
