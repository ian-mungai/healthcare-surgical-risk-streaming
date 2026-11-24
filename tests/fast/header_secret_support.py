"""Shared pieces of the cases that a secret must not reach an HTTP header unchecked or a message uncovered.

A value that is not of the closed header-safe form is refused before any request is made, with the client's own fixed message, and neither the message nor
the cause, the context, the arguments or the printed traceback of the error holds the value.
"""

from __future__ import annotations

import traceback


def unsafe_values(marker: str) -> dict[str, str]:
    """Values that start with ``marker`` and hold a character that a header must not carry, by the name of that character."""
    return {
        "line break": f"{marker}\nX-Injected: 1",
        "carriage return": f"{marker}\rX-Injected: 1",
        "space": f"{marker} tail",
        "tab": f"{marker}\ttail",
        "NUL": f"{marker}\x00tail",
        "non-ASCII": f"{marker}étail",
        "non-Latin": f"{marker}中tail",
    }


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
