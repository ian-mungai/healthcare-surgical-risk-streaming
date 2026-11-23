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


def same_file(first: Path, second: Path) -> bool:
    """Whether two paths name one file: the same resolved path, or one existing file reached by two paths."""
    if first.resolve() == second.resolve():
        return True
    try:
        return os.path.samefile(first, second)
    except OSError:
        return False


def refuse_output_over_input(outputs: Iterable[Path | None], inputs: Iterable[Path | None]) -> None:
    """Raise ``ClosedInputError`` when an output is the same file as an input. A ``None`` path is not given. Call it before anything is read or written."""
    sources = [source for source in inputs if source is not None]
    for output in outputs:
        if output is not None and any(same_file(output, source) for source in sources):
            raise ClosedInputError(Notice(Code.OUTPUT_IS_INPUT))
