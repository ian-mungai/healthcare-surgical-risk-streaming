"""A machine whose locale is not UTF-8: ``LANG=en_US.ISO8859-1`` with UTF-8 mode off, where a text read that names no encoding decodes with Latin-1."""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from pathlib import Path
from unittest import mock

LOCALE_ENCODING = "latin-1"


@contextlib.contextmanager
def non_utf_8_locale() -> Iterator[None]:
    """Make ``Path.read_text`` without an ``encoding`` read with Latin-1, in which every byte decodes, so ``0xff`` is read and is never an error."""
    real = Path.read_text

    def read_text(self: Path, encoding: str | None = None, errors: str | None = None, newline: str | None = None) -> str:
        return real(self, encoding or LOCALE_ENCODING, errors, newline)

    with mock.patch.object(Path, "read_text", read_text):
        yield
