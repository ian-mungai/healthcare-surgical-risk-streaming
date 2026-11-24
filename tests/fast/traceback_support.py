"""The whole text of an error with its traceback, for the checks that no secret is in it."""

from __future__ import annotations

import traceback
from collections.abc import Callable

HEADER = "Traceback (most recent call last)"


def traceback_text(error: BaseException, *frames: str) -> str:
    """What a terminal shows for the error. Fails when the error has no traceback or when a named function is not in it."""
    text = "".join(traceback.format_exception(error))
    if HEADER not in text:
        raise AssertionError(f"the error has no traceback, so a check of its text reads the message and the chain only: {type(error).__name__}")
    for frame in frames:
        if f", in {frame}\n" not in text:
            raise AssertionError(f"the traceback has no frame in {frame}: {text}")
    return text


def raised_text[E: BaseException](call: Callable[[], object], expected: type[E], *frames: str) -> tuple[E, str]:
    """The error that the call raises and its whole text, formatted inside the handler: ``assertRaises`` clears the traceback before the test can read it."""
    try:
        call()
    except expected as error:
        return error, traceback_text(error, *frames)
    raise AssertionError(f"the call raised no {expected.__name__}")
