"""Create an output file that did not exist, and never replace one that did.

A command writes an output that its command line names. The rule for every one of them: the path must be free in every form. A file, a link (also one that
leads nowhere), a folder and a name that differs from an existing one only in letter case are all refused, and so is a path whose folder does not exist
(this module makes no folder). ``write_new_file`` writes the content to a private temporary file in the folder of the output, then puts it in place with
``os.link``, which fails when the path has appeared since the check, so nothing is replaced even by a late writer. The temporary file is removed on every
path. An output that a command appends to as it runs calls ``check_new_output`` first and creates the file with an exclusive open of its own.
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
    temporary = path.parent / f".{path.name}.{os.getpid()}.{secrets.token_hex(6)}.part"
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
