"""A report output of the replay never lands inside the object store folder.

``manifest --out`` and ``observe --out`` name a path for a report. The folder store of a run keeps one file per object version under its root, so a report
written there would become a stored object. ``refuse_inside_store`` refuses an output whose path lies under the store root, and one whose folder is the store
folder or any folder inside it by identity (the same folder reached through a link, a ``..`` part, a relative path or a different letter case). It refuses
with fixed text and creates nothing.
"""

from __future__ import annotations

import os
from pathlib import Path

from scripts.ingestion.closed import ClosedInputError
from scripts.ingestion.notice import Code, Notice


def inside_by_path(root: Path, out: Path) -> bool:
    root_path = os.path.abspath(root)
    out_path = os.path.abspath(out)
    return os.path.commonpath([root_path, out_path]) == root_path


def same_folder(first: Path, second: Path) -> bool:
    try:
        return os.path.samestat(os.stat(first), os.stat(second))
    except OSError:
        return False


def inside_by_identity(root: Path, out: Path) -> bool:
    """Whether ``root`` is a folder on the way from the output's real location to the top: the output's folder, or any folder above it, is the store folder."""
    real = Path(os.path.realpath(out))
    return any(same_folder(root, folder) for folder in real.parents) or same_folder(root, real)


def refuse_inside_store(root: Path, out: Path) -> None:
    """Raise ``ClosedInputError`` when ``out`` lies inside the folder store at ``root``, by path or by the identity of a folder. Nothing is created."""
    if inside_by_path(root, out) or inside_by_path(Path(os.path.realpath(root)), Path(os.path.realpath(out))) or inside_by_identity(root, out):
        raise ClosedInputError(Notice(Code.OUTPUT_IN_STORE))
