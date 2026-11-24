"""The two ways that production code writes a file: ``write_new_file`` and ``replace_owned_file``. A scan test fails for any other write.

Create an output file that did not exist, and never replace one that did.

A command writes an output that its command line names. The rule for every one of them: the path must be free in every form. A file, a link (also one that
leads nowhere), a folder and a name that differs from an existing one only in letter case are all refused, and so is a path whose folder does not exist
(this module makes no folder). ``write_new_file`` writes the content to a private temporary file in the folder of the output, then puts it in place with
``os.link``, which fails when the path has appeared since the check, so nothing is replaced even by a late writer. The temporary file is removed on every
path. An output that a command appends to as it runs calls ``check_new_output`` first and creates the file with an exclusive open of its own. The
temporary name is a dot, eight hex digits and ``.part``, never the name of the output, so an output name of 255 bytes works.

``replace_owned_file`` is for the state that a long-running command owns and must update (a heartbeat, a queue count, a saved position). ``state_root`` is a
folder from a fixed source: a module constant, a folder that the same command created in this run, or the service's state-folder setting. It is never built
from the path that is replaced (``path.parent``), because that check passes for every path; a scan test fails for such a call. The path must lie directly
inside it; the entry that is there, if any, must be a regular file and not a link; the write is a private temporary file in that folder, then ``os.replace``,
so a reader sees the old file or the new one whole. A path outside the folder, a link, a folder and any other kind of entry are refused with fixed text and
nothing is written.
"""

from __future__ import annotations

import os
import secrets
import stat
from pathlib import Path

from scripts.ingestion.closed import ClosedInputError
from scripts.ingestion.notice import Code, Notice, Subject


def check_new_output(path: Path) -> None:
    """Raise ``ClosedInputError`` unless ``path`` is free in every form and its folder exists. Nothing is created."""
    folder = path.parent
    try:
        folder_exists = stat.S_ISDIR(os.stat(folder).st_mode)
    except OSError:
        folder_exists = False
    if not folder_exists or not path.name or path.name in (".", ".."):
        raise ClosedInputError(Notice(Code.OUTPUT_FOLDER))
    try:
        os.lstat(path)
    except FileNotFoundError:
        pass
    except OSError:
        raise ClosedInputError(Notice(Code.OUTPUT_EXISTS)) from None
    else:
        raise ClosedInputError(Notice(Code.OUTPUT_EXISTS))
    try:
        with os.scandir(folder) as entries:
            held = any(entry.name.casefold() == path.name.casefold() for entry in entries)
    except OSError:
        raise ClosedInputError(Notice(Code.OUTPUT_FOLDER)) from None
    if held:
        raise ClosedInputError(Notice(Code.OUTPUT_EXISTS))


def write_new_file(path: Path, data: str | bytes, *, mode: int = 0o600) -> None:
    """Write ``data`` to a path that is free, whole or not at all. Raise ``ClosedInputError`` for a path that is not free or has no folder."""
    check_new_output(path)
    content = data.encode() if isinstance(data, str) else data
    temporary = path.parent / f".{secrets.token_hex(4)}.part"
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    except OSError:
        raise ClosedInputError(Notice(Code.OUTPUT_FOLDER)) from None
    try:
        try:
            with os.fdopen(descriptor, "wb") as handle:
                os.fchmod(handle.fileno(), mode)
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.link(temporary, path)
        except FileExistsError:
            raise ClosedInputError(Notice(Code.OUTPUT_EXISTS)) from None
        except OSError:
            raise ClosedInputError(Notice(Code.CANNOT_WRITE, subject=Subject.OUTPUT)) from None
    finally:
        temporary.unlink(missing_ok=True)


def replace_owned_file(path: Path, data: str | bytes, *, state_root: Path, mode: int = 0o600) -> None:
    """Replace ``path``, directly inside ``state_root``, with ``data``, whole or not at all. Raise ``ClosedInputError`` with fixed text for a path outside the
    state folder, a state folder that is not a folder, and an entry that is not a regular file (a link, a folder, a pipe)."""
    folder = Path(state_root)
    if path.parent != folder or ".." in path.parts or ".." in folder.parts or not path.name:
        raise ClosedInputError(Notice(Code.OWNED_REFUSED))
    try:
        if not stat.S_ISDIR(os.lstat(folder).st_mode):
            raise ClosedInputError(Notice(Code.OWNED_REFUSED))
    except OSError:
        raise ClosedInputError(Notice(Code.OWNED_REFUSED)) from None
    try:
        held = os.lstat(path)
    except FileNotFoundError:
        pass
    except OSError:
        raise ClosedInputError(Notice(Code.OWNED_REFUSED)) from None
    else:
        if not stat.S_ISREG(held.st_mode):
            raise ClosedInputError(Notice(Code.OWNED_REFUSED))
    content = data.encode() if isinstance(data, str) else data
    temporary = folder / f".{secrets.token_hex(4)}.part"
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    except OSError:
        raise ClosedInputError(Notice(Code.CANNOT_WRITE, subject=Subject.OUTPUT)) from None
    try:
        try:
            with os.fdopen(descriptor, "wb") as handle:
                os.fchmod(handle.fileno(), mode)
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
            parent = os.open(folder, os.O_RDONLY)
            try:
                os.fsync(parent)
            finally:
                os.close(parent)
        except OSError:
            raise ClosedInputError(Notice(Code.CANNOT_WRITE, subject=Subject.OUTPUT)) from None
    finally:
        temporary.unlink(missing_ok=True)
