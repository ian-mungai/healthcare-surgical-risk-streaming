"""Shared pieces of the cases that a secret must not reach an HTTP header unchecked or a message uncovered.

A value that is not of the closed header-safe form is refused before any request is made, with the client's own fixed message, and neither the message nor
the cause, the context, the arguments or the printed traceback of the error holds the value.
"""

from __future__ import annotations

import traceback
from collections.abc import Callable

# The characters of the closed header-safe form, besides a trailing run of ``=``. A printable character outside it is a control for a rule that was widened.
FORM_CHARACTERS = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~+/=")


def unsafe_values(marker: str) -> dict[str, str]:
    """Values that hold ``marker`` and a character that a header must not carry, by the name of that character."""
    return {
        "line break": f"{marker}\nX-Injected: 1",
        "carriage return": f"{marker}\rX-Injected: 1",
        "space": f"{marker} tail",
        "tab": f"{marker}\ttail",
        "NUL": f"{marker}\x00tail",
        "non-ASCII": f"{marker}étail",
        "non-Latin": f"{marker}中tail",
        "equals sign inside": f"{marker}=tail",
        "equals sign first": f"={marker}",
    } | {f"printable {chr(code)!r}": f"{marker}{chr(code)}tail" for code in range(0x21, 0x7F) if chr(code) not in FORM_CHARACTERS}


def everything_said_about(error: BaseException) -> str:
    """The text of an error and of each error in its cause and context chain, with its representation and the printed traceback."""
    parts: list[str] = []
    seen: list[BaseException] = []
    pending: list[BaseException | None] = [error]
    while pending:
        current = pending.pop()
        if current is None or any(current is earlier for earlier in seen):
            continue
        seen.append(current)
        parts += [str(current), repr(current), "".join(traceback.format_exception(current))]
        pending += [current.__cause__, current.__context__]
    return "\n".join(parts)


def raised_text[Raised: BaseException](call: Callable[[], object], kind: type[Raised], raiser: str) -> tuple[Raised, str]:
    """The error of ``kind`` that ``call`` raises and everything it says, formatted inside the handler, and a failed case unless the traceback names ``raiser``.

    ``assertRaises`` clears the frames of the traceback before a test can read it, so a check of its text passes with the traceback gone.
    """
    try:
        call()
    except kind as error:
        text = everything_said_about(error)
        if error.__traceback__ is None or all(frame.name != raiser for frame in traceback.extract_tb(error.__traceback__)):
            raise AssertionError(f"the traceback of the {kind.__name__} names no frame of {raiser}") from None
        return error, text
    raise AssertionError(f"the call raised nothing: expected {kind.__name__}")
