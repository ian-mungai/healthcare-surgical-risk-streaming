"""Closed input: a JSON object is accepted only with exactly its known fields, each of its known type.

A message names the object and its fields, never a value of the file.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any


class ClosedInputError(ValueError):
    """An input is not valid JSON or is an object with an unknown, missing or mistyped field."""


def parse_json(text: str | bytes, what: str) -> Any:
    try:
        return json.loads(text)
    except ValueError:
        raise ClosedInputError(f"{what} is not valid JSON") from None


def closed_object(value: Any, required: Mapping[str, type], what: str, optional: Mapping[str, type] | None = None) -> dict[str, Any]:
    """Return ``value`` when it is an object whose fields are the required ones and any of the optional ones, each of exactly its type."""
    known = {**required, **(optional or {})}
    if type(value) is not dict or set(required) - set(value) or set(value) - set(known):
        raise ClosedInputError(
            f"{what} must be an object with the fields: {', '.join(sorted(required))}" + (f"; optional: {', '.join(sorted(optional))}" if optional else "")
        )
    wrong = sorted(name for name, expected in known.items() if name in value and type(value[name]) is not expected)
    if wrong:
        raise ClosedInputError(f"{what} has a field of the wrong type: {', '.join(wrong)}")
    return value
