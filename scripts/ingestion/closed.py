"""Closed input: a JSON object is accepted only with exactly its known fields, each of its known type.

A field named twice is refused, because the second value would silently replace the first. A refusal is a ``Notice`` that names the input by its
``Subject`` and the fields of its schema, never a value of the file. The command lines are closed the same way: ``QuietParser`` refuses a command line
that is not valid without repeating any of its words, under a fixed program name.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from typing import Any, NoReturn

from scripts.ingestion.notice import ClosedError, Code, Notice, Subject, render


class ClosedInputError(ClosedError, ValueError):
    """An input is not valid JSON or is an object with an unknown, missing or mistyped field."""


class RepeatedFieldError(ValueError):
    """A JSON object names one field twice."""


def reject_repeats(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    if len({name for name, _ in pairs}) != len(pairs):
        raise RepeatedFieldError
    return dict(pairs)


def parse_json(text: str | bytes, what: Subject) -> Any:
    """The JSON value of ``text``. Raise ``ClosedInputError`` when it is not JSON or when any object in it names a field twice."""
    try:
        return json.loads(text, object_pairs_hook=reject_repeats)
    except RepeatedFieldError:
        raise ClosedInputError(Notice(Code.REPEATED_FIELD, subject=what)) from None
    except ValueError:
        raise ClosedInputError(Notice(Code.NOT_JSON, subject=what)) from None


def closed_object(value: Any, required: Mapping[str, type], what: Subject, optional: Mapping[str, type] | None = None) -> dict[str, Any]:
    """Return ``value`` when it is an object whose fields are the required ones and any of the optional ones, each of exactly its type."""
    known = {**required, **(optional or {})}
    if type(value) is not dict or set(required) - set(value) or set(value) - set(known):
        if optional:
            raise ClosedInputError(Notice(Code.OBJECT_FIELDS_OPTIONAL, subject=what, fields=tuple(sorted(required)), optional=tuple(sorted(optional))))
        raise ClosedInputError(Notice(Code.OBJECT_FIELDS, subject=what, fields=tuple(sorted(required))))
    wrong = tuple(sorted(name for name, expected in known.items() if name in value and type(value[name]) is not expected))
    if wrong:
        raise ClosedInputError(Notice(Code.WRONG_TYPE, subject=what, fields=wrong))
    return value


class QuietParser(argparse.ArgumentParser):
    """An argument parser whose usage error names no word of the command line, so a mistyped value is never repeated on standard error."""

    def error(self, message: str) -> NoReturn:
        self.exit(2, f"{self.prog}: {render(Notice(Code.USAGE))}\n")
