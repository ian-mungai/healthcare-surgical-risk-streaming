"""The recovery of a Python traceback from the text of an error, as PySpark 4.1.4 does it: a vendored copy of the minimal helper, for the tests.

PySpark is not installed in the local environment, so the tests drive its own logic from a copy of the pinned source (``apache/spark`` tag ``v4.1.4``):

- ``recover_python_exception``: ``python/pyspark/errors/exceptions/base.py`` lines 470-491 (``populate_linecache`` is left out: it only fills ``linecache``).
- ``Traceback.from_string`` and ``Traceback.as_traceback``: ``python/pyspark/errors/exceptions/tblib.py`` lines 184-205 (the rebuilt code object) and the
  classes that they need (``Code``, ``Frame``, ``Traceback``, ``_AttrDict``), adapted from the ``tblib`` package, BSD 2-Clause License, Copyright (c) 2013-2023,
  Ionel Cristian Marieș. The text of the license is in the header of ``tblib.py`` at the tag.

What matters for the diagnostic line: the function takes the file name, the function name and the line number of every frame from the text of the error, and
``as_traceback`` makes a NEW code object for each one (``stub.replace(co_filename=..., co_name=..., co_firstlineno=...)``) and runs it, so the traceback of
the error then holds frames whose ``co_filename`` and ``co_name`` came from the message.
"""

from __future__ import annotations

import re
import sys
from types import CodeType, FrameType, TracebackType
from typing import Any

FRAME_RE = re.compile(r'^\s*File "(?P<co_filename>.+)", line (?P<tb_lineno>\d+)(, in (?P<co_name>.+))?$')


class _AttrDict(dict[str, Any]):
    __slots__ = ()

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None


class _TracebackMaker(Exception):
    pass


class Code:
    def __init__(self, code: CodeType) -> None:
        self.co_filename = code.co_filename
        self.co_name: str | None = code.co_name


class Frame:
    def __init__(self, frame: FrameType) -> None:
        self.f_locals: dict[str, Any] = {}
        self.f_globals = {k: v for k, v in frame.f_globals.items() if k in ("__file__", "__name__")}
        self.f_code = Code(frame.f_code)
        self.f_lineno = frame.f_lineno


class Traceback:
    tb_next: Traceback | None = None

    def __init__(self, tb: Any) -> None:
        self.tb_frame = Frame(tb.tb_frame)
        self.tb_lineno = int(tb.tb_lineno)
        _tb = tb.tb_next
        previous = self
        while _tb is not None:
            traceback = object.__new__(type(self))
            traceback.tb_frame = Frame(_tb.tb_frame)
            traceback.tb_lineno = int(_tb.tb_lineno)
            previous.tb_next = traceback
            previous = traceback
            _tb = _tb.tb_next

    def as_traceback(self) -> TracebackType | None:
        """tblib.py (v4.1.4) lines 184-205: one NEW code object for each frame, with the file name, function name and line number of the text."""
        current: Traceback | None = self
        top_tb = None
        tb = None
        stub = compile("raise _traceback_maker", "<string>", "exec")
        while current:
            f_code = current.tb_frame.f_code
            code = stub.replace(
                co_firstlineno=current.tb_lineno,
                co_argcount=0,
                co_filename=f_code.co_filename,
                co_name=f_code.co_name or stub.co_name,
                co_freevars=(),
                co_cellvars=(),
            )
            try:
                exec(code, dict(current.tb_frame.f_globals), dict(current.tb_frame.f_locals))  # noqa: S102 (the code of PySpark, as it is)
            except Exception:
                next_tb = sys.exc_info()[2].tb_next  # type: ignore[union-attr]
                if top_tb is None:
                    top_tb = next_tb
                if tb is not None:
                    tb.tb_next = next_tb
                tb = next_tb
                del next_tb
            current = current.tb_next
        try:
            return top_tb
        finally:
            del top_tb
            del tb

    @classmethod
    def from_string(cls, string: str, strict: bool = True) -> Traceback:
        """The frames of a text that holds ``Traceback (most recent call last):``, as tblib.py (v4.1.4) reads them."""
        frames: list[dict[str, str]] = []
        lines = string.splitlines()[::-1]
        if strict:
            while lines:
                line = lines.pop()
                if line == "Traceback (most recent call last):":
                    break
        while lines:
            line = lines.pop()
            frame_match = FRAME_RE.match(line)
            if frame_match:
                frames.append(frame_match.groupdict())
                if lines and lines[-1].startswith("    "):
                    lines.pop()
            elif line.startswith("  "):
                pass
            elif strict:
                break
        if not frames:
            raise ValueError(f"Could not find any frames in {string!r}.")
        previous = None
        for frame in reversed(frames):
            previous = _AttrDict(
                frame,
                tb_frame=_AttrDict(
                    frame,
                    f_globals=_AttrDict(__file__=frame["co_filename"], __name__="?"),
                    f_locals={},
                    f_code=_AttrDict(frame),
                    f_lineno=int(frame["tb_lineno"]),
                ),
                tb_next=previous,
            )
        return cls(previous)


def recover_python_exception[T: BaseException](e: T) -> T:
    """base.py (v4.1.4) lines 470-491: the traceback of ``e`` becomes the frames that its message describes."""
    python_exception_header = "Traceback (most recent call last):"
    try:
        message = str(e)
        start = message.find(python_exception_header)
        if start == -1:
            return e
        tb = Traceback.from_string(message[start:])
        return e.with_traceback(tb.as_traceback())
    except BaseException:
        return e


def message_naming(path: str, function: str, line: int, marker: str) -> str:
    """The text of a Java error with a Python traceback of one frame that names ``path`` and ``function`` and holds the number ``line`` and the ``marker``."""
    return f'JVM failure\nTraceback (most recent call last):\n  File "{path}", line {line}, in {function}\n    raise Marker("{marker}")\nValueError: {marker}'
