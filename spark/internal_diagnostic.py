"""The one diagnostic line that a sealed job writes to standard error when an unexpected error ends it (``internal_error``).

A sealed job writes no text of an error, because a message, an argument or a local value can hold a value of the test set. A run that ends as the bare code
``internal_error`` then cannot be told from any other, and each diagnosis takes a patch and a rerun of 30 to 80 minutes. This line names where the failure
is and nothing that it holds: ``internal_error class=<a> <- <b> frames=<f1> | <f2>``.

- ``class`` is the chain of the error: the module-qualified class names (``builtins.KeyError``), the cause (``raise ... from``) before the context, at most
  ``CLASS_LIMIT`` of them, and the chain stops at an error that it has seen.
- ``frames`` is the traceback of the first error, each ``file-basename:line:function``, the last ``FRAME_LIMIT`` of them, the innermost last.
- Nothing else is read from the error: no message, no ``args``, no ``repr``, no local variable, no path beyond the basename of a file. A name holds only the
  characters of a name (letters, digits, ``_ . < > -``: every other character becomes ``_``) and at most ``NAME_LIMIT`` of them. Class and function names
  come from the code of the project and its libraries, and no code of the project builds a class or function name from data.

This module imports no Spark and no project module, so that every job can use it.
"""

from __future__ import annotations

import os
import re
import traceback

CLASS_LIMIT = 5
FRAME_LIMIT = 40
NAME_LIMIT = 120
NOT_A_NAME = re.compile(r"[^A-Za-z0-9_.<>-]")


def clean(text: str) -> str:
    """A name with every character that a name does not hold replaced and its length limited."""
    return NOT_A_NAME.sub("_", text)[:NAME_LIMIT]


def class_chain(error: BaseException) -> list[str]:
    """The module-qualified class names of ``error`` and of what it came from (its cause, else its context), the first at most ``CLASS_LIMIT``."""
    names: list[str] = []
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen and len(names) < CLASS_LIMIT:
        seen.add(id(current))
        kind = type(current)
        names.append(clean(f"{kind.__module__}.{kind.__qualname__}"))
        current = current.__cause__ if current.__cause__ is not None else current.__context__
    return names


def frames_of(error: BaseException) -> list[str]:
    """The last ``FRAME_LIMIT`` frames of the traceback of ``error`` as ``file-basename:line:function``, the innermost last."""
    summaries = traceback.extract_tb(error.__traceback__)[-FRAME_LIMIT:]
    return [f"{clean(os.path.basename(frame.filename))}:{frame.lineno}:{clean(frame.name)}" for frame in summaries]


def internal_error_line(error: BaseException) -> str:
    """The diagnostic line (without the job name) of an error that ended a sealed job."""
    return f"internal_error class={' <- '.join(class_chain(error))} frames={' | '.join(frames_of(error))}"
