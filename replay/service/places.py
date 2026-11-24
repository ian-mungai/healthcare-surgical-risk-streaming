"""Where a path of a run record is, compared with the inbox by what the file system says it is.

The service takes every ``*.json`` file in the inbox for a request, so a file that the service or a run writes there (a checkpoint, a read log, a result) is
taken for one. Every path that a run record names, and every folder that the service writes, is compared with the inbox here and nowhere else.

A path is first made absolute: a relative one is taken from the folder that holds the inbox, so ``submit`` and ``serve`` read it alike whatever their working
folders are. Then links and ``..`` are resolved without creating or reading anything. Two places are the same when their resolved names are the same or when the
file system gives them one device and one inode, so two spellings of one folder on a case-insensitive volume are caught. A path with a NUL or a character that
no file name holds is not a place: ``PathError``.
"""

from __future__ import annotations

import os
from pathlib import Path


class PathError(ValueError):
    """The text cannot be a path: it holds a NUL or a character that the file system cannot name."""


def absolute(path: str | Path) -> Path:
    """The path from the working folder when it is relative. ``..`` and links stay as they are: ``resolved`` follows them in order."""
    place = Path(path)
    return place if place.is_absolute() else Path.cwd() / place


def anchored(text: str, inbox: Path) -> Path:
    """The path ``text``, taken from the folder that holds the inbox when it is relative."""
    return absolute(absolute(inbox).parent / text)


def resolved(path: str | Path) -> Path:
    """The path with links and ``..`` resolved as far as it exists."""
    try:
        return Path(os.path.realpath(path))
    except ValueError as error:
        raise PathError(type(error).__name__) from None


def identity(path: Path) -> tuple[int, int] | None:
    """The device and inode of an existing file or folder, else None."""
    try:
        status = os.stat(path)
    except OSError:
        return None
    return status.st_dev, status.st_ino


def chain(path: Path) -> set[tuple[int, int]]:
    """The identity of the path and of each folder above it that exists."""
    found = (identity(place) for place in (path, *path.parents))
    return {item for item in found if item is not None}


def same_place(first: str | Path, second: str | Path) -> bool:
    """Whether two paths are one place: the same resolved name or the same device and inode."""
    one, other = resolved(first), resolved(second)
    return one == other or (identity(one) is not None and identity(one) == identity(other))


def inside(path: str | Path, folder: str | Path) -> bool:
    """Whether ``path`` is ``folder`` or inside it. Nothing is created or read."""
    place, holder = resolved(path), resolved(folder)
    held = identity(holder)
    return place.is_relative_to(holder) or (held is not None and held in chain(place))


def shares_place(path: str | Path, folder: str | Path) -> bool:
    """Whether ``path`` is ``folder``, inside it or a folder that holds it."""
    return inside(path, folder) or inside(folder, path)


def written_into(path: str | Path, folder: str | Path) -> bool:
    """Whether a write of ``path`` by ``atomic_write`` lands in ``folder`` or below it, or lands in the place that ``path`` links to.

    The write makes a temporary file beside ``path`` and renames it over ``path``, so a link in ``path`` is replaced by the file and the place that counts is
    the resolved folder that holds ``path``, not where the link points.
    The place that a link ``path`` points to counts too: a reader or an appender gets there."""
    spelled = absolute(path)
    return inside(spelled.parent, folder) or inside(spelled, folder)
