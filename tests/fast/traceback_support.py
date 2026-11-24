"""The whole text of an error with its traceback, for the checks that no secret is in it."""

from __future__ import annotations

import traceback
from collections.abc import Callable

HEADER = "Traceback (most recent call last)"


def frame_names(error: BaseException) -> set[str]:
    """The function of each frame of the error and the errors shown with it, never read from the text, which a note can fill."""
    names: set[str] = set()
    seen: set[int] = set()
    pending: list[BaseException | None] = [error]
    while pending:
        current = pending.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        names.update(frame.name for frame in traceback.extract_tb(current.__traceback__))
        if current.__cause__ is not None:
            pending.append(current.__cause__)
        elif not current.__suppress_context__:
            pending.append(current.__context__)
    return names


def traceback_text(error: BaseException, *frames: str) -> str:
    """What a terminal shows for the error. Fails when the error has no traceback of its own or when a named function is in no frame of its shown chain."""
    if not traceback.extract_tb(error.__traceback__):
        raise AssertionError(f"the error has no traceback, so a check of its text reads the message and the chain only: {type(error).__name__}")
    text = "".join(traceback.format_exception(error))
    names = frame_names(error)
    for frame in frames:
        if frame not in names:
            raise AssertionError(f"the traceback has no frame in {frame}: {sorted(names)}")
    return text


def raised_text[E: BaseException](call: Callable[[], object], expected: type[E], *frames: str) -> tuple[E, str]:
    """The error that the call raises and its whole text, formatted inside the handler: ``assertRaises`` clears the traceback before the test can read it."""
    try:
        call()
    except expected as error:
        return error, traceback_text(error, *frames)
    raise AssertionError(f"the call raised no {expected.__name__}")
