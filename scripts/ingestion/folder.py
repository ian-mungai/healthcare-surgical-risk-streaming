"""List a folder so that one that cannot be read is an error and never an empty listing.

``pathlib``'s ``glob``, ``rglob`` and ``iterdir`` return nothing for a folder that is not readable, and ``Path.is_file`` says no for a file that it cannot
examine. A reader of a run, inbox or input folder then takes a folder or a file that it cannot see for an empty one. The rule here is one for both modes:
a folder that does not exist holds nothing, and every other failure, at the folder or at any folder above or below it, raises ``OSError``. That includes a
link to nothing and a file in place of the folder.
"""

from __future__ import annotations

import fnmatch
import os
import stat
from pathlib import Path


def raise_error(error: OSError) -> None:
    raise error


def exists(folder: Path) -> bool:
    """Whether anything is at ``folder``, a link to nothing included. A missing path, or one below a missing folder, says no. Any other failure raises."""
    try:
        os.lstat(folder)
    except FileNotFoundError:
        return False
    return True


def list_folder(folder: Path, pattern: str = "*", *, recursive: bool = False) -> list[Path]:
    """The entries of ``folder`` (files and folders) whose name matches ``pattern``, sorted, and at any depth when ``recursive``.

    A folder that does not exist holds nothing. Anything else that stops the listing raises ``OSError`` (``PermissionError``, ``NotADirectoryError``, or
    ``FileNotFoundError`` for a link to nothing).
    """
    if not exists(folder):
        return []
    found: list[Path] = []
    for here, names, files in os.walk(folder, onerror=raise_error) if recursive else [flat(folder)]:
        found += [Path(here) / name for name in (*names, *files) if fnmatch.fnmatchcase(name, pattern)]
    return sorted(found)


def flat(folder: Path) -> tuple[str, list[str], list[str]]:
    """The names directly in ``folder``, in the form of one step of ``os.walk``, with every error raised."""
    with os.scandir(folder) as entries:
        return str(folder), [entry.name for entry in entries], []


def list_files(folder: Path, pattern: str = "*", *, recursive: bool = False, follow_links: bool = True) -> list[Path]:
    """The regular files of ``list_folder`` (a link to one counts, unless ``follow_links`` is false and every link is left out), sorted.

    A listed entry that cannot be examined raises and is never skipped. An entry that vanished between the listing and the examination is not listed.
    A link to nothing raises when links are followed.
    """
    files: list[Path] = []
    for path in list_folder(folder, pattern, recursive=recursive):
        try:
            mode = os.lstat(path).st_mode
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(mode):
            if not follow_links:
                continue
            mode = os.stat(path).st_mode
        if stat.S_ISREG(mode):
            files.append(path)
    return files
