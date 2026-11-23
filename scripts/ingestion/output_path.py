"""Refuse an output path that is the same file as an input path of the same command.

The commands write their outputs with truncation, so an output that is also an input would empty that input before the command had read it. The check
compares the resolved paths, and ``os.path.samefile`` for two paths that both exist, which also finds a hard link and a link through another folder.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path

from scripts.ingestion.closed import ClosedInputError
from scripts.ingestion.notice import Code, Notice


def spellings(path: Path) -> list[Path]:
    """The resolved paths that ``path`` can name: as written, and with its ``..`` parts removed first, which is how a missing folder before a ``..`` reads."""
    absolute = Path(os.path.abspath(path))
    return list(dict.fromkeys([path.resolve(), absolute.resolve()]))


def same_file(first: Path, second: Path) -> bool:
    """Whether two paths name one file: a resolved spelling in common, or two existing files that are one by ``os.path.samefile`` (a hard link too)."""
    firsts, seconds = spellings(first), spellings(second)
    if any(left == right for left in firsts for right in seconds):
        return True
    for left in firsts:
        for right in seconds:
            try:
                if os.path.samefile(left, right):
                    return True
            except OSError:
                continue
    return False


def inside_folder(path: Path, folder: Path) -> bool:
    """Whether ``path``, in any spelling, is the folder or lies below it."""
    return any(spelling == place or place in spelling.parents for spelling in spellings(path) for place in spellings(folder))


def refuse_output_inside_folders(outputs: Iterable[Path | None], folders: Iterable[Path | None]) -> None:
    """Raise ``ClosedInputError`` when an output is, or lies below, a folder that the command reads files from by names it finds later."""
    places = [place for place in folders if place is not None]
    for output in outputs:
        if output is not None and any(inside_folder(output, place) for place in places):
            raise ClosedInputError(Notice(Code.OUTPUT_IS_INPUT))


def refuse_output_over_input(outputs: Iterable[Path | None], inputs: Iterable[Path | None]) -> None:
    """Raise ``ClosedInputError`` when an output is the same file as an input. A ``None`` path is not given. Call it before anything is read or written."""
    sources = [source for source in inputs if source is not None]
    for output in outputs:
        if output is not None and any(same_file(output, source) for source in sources):
            raise ClosedInputError(Notice(Code.OUTPUT_IS_INPUT))
