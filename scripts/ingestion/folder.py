"""List a folder so that one that cannot be read is an error and never an empty listing.

``pathlib``'s ``glob``, ``rglob`` and ``iterdir`` return nothing for a folder that is not readable. A reader of a run, inbox or input folder then takes a
folder it cannot see for an empty one. ``list_folder`` reads through ``os.scandir`` and ``os.walk`` with ``onerror`` set, which raise.
"""

from __future__ import annotations

import fnmatch
import os
from pathlib import Path


def raise_error(error: OSError) -> None:
    raise error


def list_folder(folder: Path, pattern: str = "*", *, recursive: bool = False) -> list[Path]:
    """The entries of ``folder`` (files and folders) whose name matches ``pattern``, sorted, and at any depth when ``recursive``.

    A folder that does not exist holds nothing. A folder or a subfolder that cannot be read raises ``OSError`` (``PermissionError``).
    So does a file in place of the folder.
    """
    if not recursive:
        try:
            with os.scandir(folder) as entries:
                names = [entry.name for entry in entries]
        except FileNotFoundError:
            return []
        return sorted(folder / name for name in names if fnmatch.fnmatchcase(name, pattern))
    if not os.path.lexists(folder):
        return []
    if not folder.is_dir():
        raise NotADirectoryError(f"{folder.name} is not a folder")
    found: list[Path] = []
    for here, names, files in os.walk(folder, onerror=raise_error):
        found += [Path(here) / name for name in (*names, *files) if fnmatch.fnmatchcase(name, pattern)]
    return sorted(found)
