"""The comparison form of a decoded payload: a canonical text and its SHA-256.

The manifest of expected events and the check of what Kafka holds compare the digest of each payload, so a numeric reading and a waveform second need no
copy in the manifest. A float is written as its exact hexadecimal form, tagged, so that a value differing in its last bit differs, negative zero differs from
zero and a value that JSON cannot hold still has a form. The tag cannot be a text, because a text stays a JSON string.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any


def canonical_value(value: Any) -> Any:
    if isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        return {"$f": value.hex()}
    if isinstance(value, list):
        return [canonical_value(item) for item in value]
    raise TypeError(f"a payload holds only booleans, integers, floats, texts and lists of them, not {type(value).__name__}")


def canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    """The canonical bytes of a payload: its fields in name order, floats in exact form, no spaces."""
    return json.dumps({name: canonical_value(value) for name, value in payload.items()}, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def payload_digest(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_bytes(payload)).hexdigest()
