"""The staging walk: count the files under one shell pattern of absolute paths, for the capacity sampler.

``.venv/bin/python -m scripts.stack.staging_walk '<pattern>'`` prints one line of JSON, ``matches``, ``files``, ``apparent_bytes`` and ``allocated_bytes``,
and exits 0. The sampler starts it as a child program for each staging source and kills it at the end of the reading, so the walk has no deadline of its
own and no slow folder can hold the sampler's loop.

A segment with no wildcard is joined as written. A segment with a wildcard lists the folder reached so far and keeps the names that match, never a name that
starts with a dot unless the segment does. A link is never followed: at the last segment it is a match and counts as itself, and at any other wildcard
position it is skipped. A pattern that ends with a slash matches folders only. Each match is walked without following links and only files are counted. A name
or folder that is gone is skipped, because a pod removes its volume as it ends. Any other failure, a folder that may not be read among them, ends the walk
with the exit status ``EXIT_OS_ERROR`` and no output, so an unreadable folder is never an empty one. The walk reads and changes nothing in those folders.
"""

from __future__ import annotations

import fnmatch
import glob
import json
import os
import stat
import sys
from collections.abc import Sequence

EXIT_USAGE = 2
EXIT_OS_ERROR = 3
BLOCK_BYTES = 512
USAGE = "staging walk: the command line is one absolute pattern"


def expand_pattern(pattern: str) -> list[str]:
    """The paths that the pattern names, found by listing folders, under the rules of the module."""
    segments = [segment for segment in pattern.split("/") if segment]
    found: list[str] = []
    pending: list[tuple[str, int]] = [("/", 0)]
    while pending:
        base, index = pending.pop()
        if index == len(segments):
            found.append(base)
            continue
        segment = segments[index]
        if not glob.has_magic(segment):
            pending.append((os.path.join(base, segment), index + 1))
            continue
        try:
            names = os.listdir(base)
        except FileNotFoundError, NotADirectoryError:
            continue
        for name in names:
            if (name.startswith(".") and not segment.startswith(".")) or not fnmatch.fnmatchcase(name, segment):
                continue
            path = os.path.join(base, name)
            if index + 1 < len(segments):
                try:
                    if not stat.S_ISDIR(os.lstat(path).st_mode):
                        continue
                except FileNotFoundError:
                    continue
            pending.append((path, index + 1))
    return [path for path in found if exists(path, folder=pattern.endswith("/"))]


def exists(path: str, *, folder: bool = False) -> bool:
    """Whether a path names something, itself and not what a link leads to; with ``folder``, a folder. Only a name that is not there is False."""
    try:
        return stat.S_ISDIR(os.lstat(path).st_mode) or not folder
    except FileNotFoundError, NotADirectoryError:
        return False


def read_staging(pattern: str) -> dict[str, int]:
    """The matches, files and bytes of one pattern. An ``OSError`` that is not a missing name ends the walk."""
    matches = expand_pattern(pattern)
    pending = list(matches)
    files = apparent = allocated = 0
    while pending:
        path = pending.pop()
        try:
            info = os.lstat(path)
            if stat.S_ISDIR(info.st_mode):
                pending.extend(os.path.join(path, name) for name in os.listdir(path))
                continue
        except FileNotFoundError:
            continue
        files += 1
        apparent += info.st_size
        allocated += info.st_blocks * BLOCK_BYTES
    return {"matches": len(matches), "files": files, "apparent_bytes": apparent, "allocated_bytes": allocated}


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) != 1 or not arguments[0].startswith("/") or any(ord(character) < 32 for character in arguments[0]):
        sys.stderr.write(f"{USAGE}\n")
        return EXIT_USAGE
    try:
        row = read_staging(arguments[0])
    except OSError:
        return EXIT_OS_ERROR
    sys.stdout.write(json.dumps(row) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
