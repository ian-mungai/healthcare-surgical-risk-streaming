"""The one diagnostic line that a sealed job writes to standard error when an unexpected error ends it (``internal_error``).

A sealed job writes no text of an error, because a message, an argument or a local value can hold a value of the test set, and so can a name: a class that a
library built from data, a file name, or a function that PySpark 4 compiled from the message of a Java error. A run that ends as the bare code
``internal_error`` then cannot be told from any other, and each diagnosis takes a patch and a rerun of 30 to 80 minutes. This line names where the failure is
and nothing that it holds: ``internal_error class=<a> <- <b> frames=<f1> | <f2>``.

- ``class`` is the chain of the error (the cause, else the context, at most ``CLASS_LIMIT`` of them, ending at an error that it has seen). A class is named
  only when it is on a fixed list and is that very class (found by ``id``, never by name): the exception classes of ``builtins``, the library classes and
  the classes of the project's own packages (``spark/internal_classes.py``). Every other class is the word ``other``. No ``__name__``, ``__module__`` or
  attribute of an error class is read, and the cause and the context are read from the slots of ``BaseException``, so no property of an error runs. A class
  is found with ``type()`` and the ``__mro__`` slot of each class: ``isinstance`` and ``issubclass`` are never used on a class that a library or an error
  owns, because they run its ``__class__``, ``__instancecheck__`` and ``__subclasscheck__``.
- ``frames`` is the traceback of the first error, the last ``FRAME_LIMIT`` of them, the innermost last. The walk follows ``tb_next`` and reads the code
  object of each frame directly: no source line is read (``traceback`` and ``linecache`` are not used, so a source loader never runs). A frame is
  ``path:function`` (the path from the root of the project, as in ``spark/result_file.py``) only when its file name, with ``..`` resolved as text, is
  exactly the root of the project and a path of the fixed inventory of tracked code (``spark/internal_inventory.py``, which
  ``scripts/quality/internal_inventory.py`` generates and a fast test checks against ``git ls-files`` and the AST of each file) and its function is a name
  that the inventory holds for that file (or one of ``FIXED_NAMES``). Every other frame is the word ``other``, and a run of them is one: a name that data can
  build (a generated file name, a function compiled at run time) is never in the inventory.
- A frame has **no line number**, in the producer or in the grammar. PySpark 4.1 rebuilds a traceback from the message of a Java error and sets the line of
  each frame (``co_firstlineno``, ``co_lines``) from a number in that message, so a value of the data could print as the line number beside the name of a
  tracked file and function. The line number is never read (``tb_lineno`` is not used), and a frame that has one is not a frame of the grammar. So a reader
  refuses a diagnostic of the older format (``path:line:function``): the runners and the image must carry the same version of this module as the job.
- Any failure of the formatter gives the fixed line ``internal_error class=unavailable frames=unavailable``. The function never raises, so it cannot change
  the exit code of a job or skip its result write.

The same module holds the grammar of the line (``is_diagnostic``, ``accepted_diagnostics``), which the runners of the end-to-end checks import, so the producer
and the readers cannot differ: a reader checks each frame against the same inventory and takes only one diagnostic for each invocation. It imports no Spark
and no project module (other than the lists in ``spark/internal_classes.py`` and the inventory), so every job can use it.
"""

from __future__ import annotations

import builtins
import os
import re
import sys
import types
from collections.abc import Sequence
from typing import cast

from spark.internal_classes import LIBRARY_CLASSES, NAME_LIMIT, PATH_LIMIT, PROJECT_CLASSES, PYSPARK_CLASSES, PYSPARK_MODULE
from spark.internal_inventory import INVENTORY

CLASS_LIMIT = 5
FRAME_LIMIT = 40
WALK_LIMIT = 100_000
ROOT_LIMIT = 4_096  # a file name longer than this is never a path of the project
INTERNAL_EXIT_CODE = 70
OTHER = "other"
UNAVAILABLE = "unavailable"
UNAVAILABLE_LINE = f"internal_error class={UNAVAILABLE} frames={UNAVAILABLE}"
# The fixed lines that the jobs write directly before the diagnostic line: the bare code, and the code with the flag of the sealed jobs.
CODE_LINES = ("internal_error", "internal_error: failed")

# The root of the project (the folder above ``spark``) with ``..`` resolved as text, and its slash. A frame file is this root plus a path of the inventory.
ROOT = os.path.normpath(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PREFIX = f"{ROOT}/"

# The names of code that has no function of its own, and so no entry in the inventory.
FIXED_NAMES = ("<module>", "<lambda>", "<genexpr>", "<listcomp>", "<dictcomp>", "<setcomp>")
FILES: dict[str, frozenset[str]] = {path: frozenset(names) for path, names in INVENTORY.items()}

# The slots of ``BaseException`` and ``type``, read without running any property or hook that a class or an error may define.
_TRACEBACK = BaseException.__dict__["__traceback__"]
_CAUSE = BaseException.__dict__["__cause__"]
_CONTEXT = BaseException.__dict__["__context__"]
_MRO = type.__dict__["__mro__"]


def mro_of(kind: type) -> tuple[type, ...]:
    """The method resolution order of the class ``kind``, from its slot: no ``__mro__`` property and no ``__subclasscheck__`` of a metaclass runs."""
    found: tuple[type, ...] = _MRO.__get__(kind)
    return found


def is_exception_class(item: object) -> bool:
    """Whether ``item`` is a class that derives from ``BaseException``, by ``type(item)`` and the ``__mro__`` slots: never ``isinstance`` or ``issubclass``."""
    return any(base is type for base in mro_of(type(item))) and any(base is BaseException for base in mro_of(cast(type, item)))


# An alias (``IOError`` is ``OSError``) keeps the name of the class.
BUILTIN_CLASSES = {id(item): f"builtins.{name}" for name, item in vars(builtins).items() if is_exception_class(item) and item.__name__ == name}


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
            if found is not None and any(parent is base for parent in mro_of(found)):
                table.setdefault(id(found), f"{PYSPARK_MODULE}.{name}")
    return table


def module_class(module_name: str, name: str) -> type[BaseException] | None:
    """The exception class ``name`` in the dictionary of a loaded module (no import, no module ``__getattr__`` and no hook of the class), or none."""
    module = sys.modules.get(module_name)
    if type(module) is not types.ModuleType:
        return None
    found = module.__dict__.get(name)
    return found if is_exception_class(found) else None


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


def frame_label(filename: object, name: object) -> str:
    """``path:function`` of a frame of the inventory, else ``other``. The two values come from a code object: the path (``..`` resolved as text) must be
    exactly the root of the project and a path of the inventory, and the function a name that the inventory holds for it. No line number is taken or printed."""
    if type(filename) is not str or type(name) is not str or len(filename) > ROOT_LIMIT:
        return OTHER
    path = os.path.normpath(filename)
    if not path.startswith(PREFIX):
        return OTHER
    relative = path[len(PREFIX) :]
    names = FILES.get(relative)
    return f"{relative}:{name}" if names is not None and (name in names or name in FIXED_NAMES) else OTHER


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
        label = frame_label(code.co_filename, code.co_name)
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
FRAME_BODY = rf"(?P<path>[A-Za-z0-9_/]{{1,{PATH_LIMIT}}}\.py):(?P<name>[A-Za-z_<][A-Za-z0-9_>]{{0,{NAME_LIMIT + 1}}})"
FRAME = re.compile(FRAME_BODY)
FRAME_TOKEN = rf"(?:{FRAME_BODY.replace('?P<path>', '').replace('?P<name>', '')}|{OTHER})"
CLASSES = rf"{CLASS_TOKEN}(?: <- {CLASS_TOKEN}){{0,{CLASS_LIMIT - 1}}}"
FRAMES = rf"{FRAME_TOKEN}(?: \| {FRAME_TOKEN}){{0,{FRAME_LIMIT - 1}}}"
LINE = re.compile(rf"internal_error class=(?P<classes>{CLASSES}) frames=(?P<frames>{FRAMES})?")


def is_frame(token: str) -> bool:
    """Whether ``token`` is a frame that ``frame_label`` can return: its path is in the inventory in the form that it prints and its function is a name of that
    file or a fixed name."""
    matched = FRAME.fullmatch(token)
    if matched is None:
        return False
    names = FILES.get(matched["path"])
    return names is not None and (matched["name"] in names or matched["name"] in FIXED_NAMES)


def is_diagnostic(text: str) -> bool:
    """Whether ``text`` (a line without the name of the job) is a line that ``internal_error_line`` can return, and nothing else: the fixed unavailable line,
    or at most five class labels of the lists and at most forty frames, each ``other`` or a frame of the inventory, no two ``other`` frames side by side."""
    if text == UNAVAILABLE_LINE:
        return True
    matched = LINE.fullmatch(text)
    if matched is None:
        return False
    frames = matched["frames"].split(" | ") if matched["frames"] else []
    if any(first == OTHER and second == OTHER for first, second in zip(frames, frames[1:], strict=False)):
        return False
    return all(frame == OTHER or is_frame(frame) for frame in frames)


def accepted_diagnostics(bodies: Sequence[str], exit_code: int) -> frozenset[int]:
    """The places in ``bodies`` (the lines of one job's standard error that carry its name, without the name, in order) of the lines that are a diagnostic of
    an ``internal_error``: only when the job ended with exit code 70, only the line directly after a fixed ``internal_error`` line, only of the form of
    ``is_diagnostic``, and only when it is the one such pair of the invocation. A second diagnostic line, one after another code, or a second complete pair
    (a code line and its diagnostic) makes none of them one: a job writes one."""
    if exit_code != INTERNAL_EXIT_CODE:
        return frozenset()
    places = frozenset(place for place in range(1, len(bodies)) if bodies[place - 1] in CODE_LINES and is_diagnostic(bodies[place]))
    return places if len(places) <= 1 else frozenset()
