"""The one diagnostic line that a sealed job writes to standard error when an unexpected error ends it (``internal_error``).

A sealed job writes no text of an error, because a message, an argument or a local value can hold a value of the test set, and so can a name: a class that a
library built from data, a file name, or a function that PySpark 4 compiled from the message of a Java error. A run that ends as the bare code
``internal_error`` then cannot be told from any other, and each diagnosis takes a patch and a rerun of 30 to 80 minutes. This line names where the failure is
and nothing that it holds: ``internal_error class=<a> <- <b> frames=<f1> | <f2>``.

- ``class`` is the chain of the error (the cause, else the context, at most ``CLASS_LIMIT`` of them, ending at an error that it has seen). A class is named
  only when it is on a fixed list and is that very class (found by ``id``, never by name): the exception classes of ``builtins``, the library classes and
  the classes of the project's own packages (``spark/internal_classes.py``). Every other class is the word ``other``. No ``__name__``, ``__module__`` or
  attribute of an error class is read, and the cause and the context are read from the slots of ``BaseException``, so no property of an error runs.
- ``frames`` is the traceback of the first error, the last ``FRAME_LIMIT`` of them, the innermost last. The walk follows ``tb_next`` and reads the code
  object of each frame directly: no source line is read (``traceback`` and ``linecache`` are not used, so a source loader never runs). A frame is
  ``file-basename:line:function`` only when its file name begins with a folder of one of the project's packages (a comparison of text, no file system call)
  and the basename and the function have the form of a name of the project's code. Every other frame is the word ``other``, and a run of them is one.
- Any failure of the formatter gives the fixed line ``internal_error class=unavailable frames=unavailable``. The function never raises, so it cannot change
  the exit code of a job or skip its result write.

The same module holds the grammar of the line (``is_diagnostic``, ``accepted_diagnostics``), which the runners of the end-to-end checks import, so the producer
and the readers cannot differ. It imports no Spark and no project module (other than the lists in ``spark/internal_classes.py``), so every job can use it.
"""

from __future__ import annotations

import builtins
import os
import re
import sys
import types
from collections.abc import Sequence

from spark.internal_classes import LIBRARY_CLASSES, PACKAGES, PROJECT_CLASSES, PYSPARK_CLASSES, PYSPARK_MODULE

CLASS_LIMIT = 5
FRAME_LIMIT = 40
NAME_LIMIT = 100
WALK_LIMIT = 100_000
LINE_LIMIT = 9_999_999
INTERNAL_EXIT_CODE = 70
OTHER = "other"
UNAVAILABLE = "unavailable"
UNAVAILABLE_LINE = f"internal_error class={UNAVAILABLE} frames={UNAVAILABLE}"
# The fixed lines that the jobs write directly before the diagnostic line: the bare code, and the code with the flag of the sealed jobs.
CODE_LINES = ("internal_error", "internal_error: failed")

# The folders of the project's packages, as text (the root is the folder above ``spark``).
ROOT = os.path.dirname(os.path.dirname(__file__))
PREFIXES = tuple(f"{ROOT}{os.sep}{package}{os.sep}" for package in PACKAGES)

BASENAME = re.compile(rf"[A-Za-z0-9_]{{1,{NAME_LIMIT - 4}}}\.py")
FUNCTION = re.compile(rf"(?:[A-Za-z_][A-Za-z0-9_]{{0,{NAME_LIMIT - 1}}}|<module>|<lambda>|<listcomp>|<dictcomp>|<setcomp>|<genexpr>)")

# The slots of ``BaseException``, read without running any property that an error class may define.
_TRACEBACK = BaseException.__dict__["__traceback__"]
_CAUSE = BaseException.__dict__["__cause__"]
_CONTEXT = BaseException.__dict__["__context__"]

# An alias (``IOError`` is ``OSError``) keeps the name of the class.
BUILTIN_CLASSES = {
    id(item): f"builtins.{name}"
    for name, item in vars(builtins).items()
    if isinstance(item, type) and issubclass(item, BaseException) and item.__name__ == name
}


def trusted_classes() -> dict[int, str]:
    """The classes that may be named, by ``id``: the label of each. A library class that is not loaded is skipped, and nothing is imported (a class that a job
    did not load cannot have been raised)."""
    table = dict(BUILTIN_CLASSES)
    for path in (*PROJECT_CLASSES, *LIBRARY_CLASSES):
        module_name, _, name = path.rpartition(".")
        found = module_class(module_name, name)
        if found is not None:
            table.setdefault(id(found), path)
    base = module_class(PYSPARK_MODULE, "PySparkException")
    if base is not None:
        for name in PYSPARK_CLASSES:
            found = module_class(PYSPARK_MODULE, name)
            if found is not None and issubclass(found, base):
                table.setdefault(id(found), f"{PYSPARK_MODULE}.{name}")
    return table


def module_class(module_name: str, name: str) -> type[BaseException] | None:
    """The exception class ``name`` in the dictionary of a loaded module (no import and no module ``__getattr__``), or none."""
    module = sys.modules.get(module_name)
    if type(module) is not types.ModuleType:
        return None
    found = module.__dict__.get(name)
    return found if isinstance(found, type) and issubclass(found, BaseException) else None


def class_chain(error: BaseException) -> list[str]:
    """The labels of ``error`` and of what it came from (its cause, else its context), the first at most ``CLASS_LIMIT``."""
    table = trusted_classes()
    names: list[str] = []
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen and len(names) < CLASS_LIMIT:
        seen.add(id(current))
        names.append(table.get(id(type(current)), OTHER))
        cause = _CAUSE.__get__(current)
        current = cause if cause is not None else _CONTEXT.__get__(current)
    return names


def traceback_of(error: BaseException) -> types.TracebackType | None:
    """The traceback of ``error``, from its slot."""
    found: types.TracebackType | None = _TRACEBACK.__get__(error)
    return found


def frame_label(filename: object, line: object, name: object) -> str:
    """``file-basename:line:function`` of a frame of the project's packages, else ``other``. The three values come from a code object and a traceback."""
    if type(filename) is not str or type(line) is not int or type(name) is not str or not 0 <= line <= LINE_LIMIT:
        return OTHER
    if not filename.startswith(PREFIXES):
        return OTHER
    base = filename[filename.rfind(os.sep) + 1 :]
    return f"{base}:{line}:{name}" if BASENAME.fullmatch(base) and FUNCTION.fullmatch(name) else OTHER


def frames_of(error: BaseException) -> list[str]:
    """The last ``FRAME_LIMIT`` frames of the traceback of ``error``, the innermost last, a run of ``other`` frames as one."""
    labels: list[str] = []
    step = traceback_of(error)
    walked = 0
    while step is not None:
        walked += 1
        if walked > WALK_LIMIT:
            raise ValueError("the traceback does not end")
        code = step.tb_frame.f_code
        label = frame_label(code.co_filename, step.tb_lineno, code.co_name)
        if label != OTHER or not labels or labels[-1] != OTHER:
            labels.append(label)
        step = step.tb_next
    return labels[-FRAME_LIMIT:]


def internal_error_line(error: BaseException) -> str:
    """The diagnostic line (without the job name) of an error that ended a sealed job. It never raises."""
    try:
        return f"internal_error class={' <- '.join(class_chain(error))} frames={' | '.join(frames_of(error))}"
    except Exception:
        return UNAVAILABLE_LINE


def _any_of(names: Sequence[str]) -> str:
    return "(?:" + "|".join(re.escape(name) for name in sorted(set(names), key=len, reverse=True)) + ")"


CLASS_TOKEN = _any_of([*BUILTIN_CLASSES.values(), *PROJECT_CLASSES, *LIBRARY_CLASSES, *(f"{PYSPARK_MODULE}.{name}" for name in PYSPARK_CLASSES), OTHER])
FRAME_TOKEN = rf"(?:[A-Za-z0-9_]{{1,{NAME_LIMIT - 4}}}\.py:[0-9]{{1,7}}:{FUNCTION.pattern}|{OTHER})"
CLASSES = rf"{CLASS_TOKEN}(?: <- {CLASS_TOKEN}){{0,{CLASS_LIMIT - 1}}}"
FRAMES = rf"{FRAME_TOKEN}(?: \| {FRAME_TOKEN}){{0,{FRAME_LIMIT - 1}}}"
LINE = re.compile(rf"internal_error class=(?P<classes>{CLASSES}) frames=(?P<frames>{FRAMES})?")


def is_diagnostic(text: str) -> bool:
    """Whether ``text`` (a line without the name of the job) is a line that ``internal_error_line`` can return, and nothing else: the fixed unavailable line,
    or at most five class labels of the lists and at most forty frames, no two ``other`` frames side by side."""
    if text == UNAVAILABLE_LINE:
        return True
    matched = LINE.fullmatch(text)
    if matched is None:
        return False
    frames = matched["frames"].split(" | ") if matched["frames"] else []
    return not any(first == OTHER and second == OTHER for first, second in zip(frames, frames[1:], strict=False))


def accepted_diagnostics(bodies: Sequence[str], exit_code: int) -> frozenset[int]:
    """The places in ``bodies`` (the lines of one job's standard error that carry its name, without the name, in order) of the lines that are a diagnostic of
    an ``internal_error``: only when the job ended with exit code 70, only the line directly after a fixed ``internal_error`` line, and only of the form of
    ``is_diagnostic``. A second diagnostic line, or one after another code, is not one."""
    if exit_code != INTERNAL_EXIT_CODE:
        return frozenset()
    return frozenset(place for place in range(1, len(bodies)) if bodies[place - 1] in CODE_LINES and is_diagnostic(bodies[place]))
