#!/usr/bin/python3 -I
"""Give the Airbyte deployments that hold database connections the Recreate strategy in the manifests that Helm renders, so that an upgrade never starts a new
pod beside an old one that still holds its connections.

Helm runs this script after it renders the chart (``helm upgrade --post-renderer``) and stores its output as the release's manifest, which a later ``helm
rollback`` also applies. The chart leaves a deployment such as Temporal's to the default rolling update, and its template for the workload API server states a
rolling update with a surge of 1 on every upgrade, so a patch after the upgrade comes too late: the surge would already have started. The script reads the
manifests on standard input and writes them on standard output with the same documents, except that each ``Deployment`` named ``<release>-<name>`` for a name
of its second argument has the one ``spec.strategy`` of ``type: Recreate`` and no other strategy figure (Kubernetes refuses a rolling update figure beside
Recreate).

Usage: ``recreate_post_renderer.py <release> <deployment>[,<deployment>...]``, for example ``airbyte temporal,cron,worker,workload-api-server``. A
deployment that the chart does not render is skipped. A document that is not a listed ``Deployment`` is copied byte for byte. A listed deployment whose
``spec`` cannot be read by its indentation stops the script with exit status 3 and the fixed text below, and no manifest is written, so Helm fails the upgrade
before it changes anything. A wrong command line is exit status 2. The text on standard error never holds a manifest, a name or a value.

It uses only the standard library and works on the text, so it runs on the host's Python with no package. It keeps to the one shape that ``helm template``
gives the chart (2-space block mappings, the comments of the template inside the strategy going with it): tests/fast/test_airbyte_recreate_post_renderer.py
runs it on that shape, and tests/slow/test_airbyte_rendered_environment.py parses its output for the real chart and compares every other field with the
chart's own."""

from __future__ import annotations

import re
import sys

NAME = re.compile(r"[a-z0-9]([-a-z0-9.]*[a-z0-9])?")
USAGE = "recreate_post_renderer: usage: recreate_post_renderer.py <release> <deployment>[,<deployment>...]"
REFUSAL = "recreate_post_renderer: the manifests could not be given the Recreate strategy"
NAME_LINE = re.compile(r"""^name:\s*(?:"([^"]*)"|'([^']*)'|([^\s#]+))\s*(?:#.*)?$""")


class Refusal(Exception):
    """A listed deployment whose manifest this script cannot edit with certainty."""


def indent_of(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def is_content(line: str) -> bool:
    return bool(line.strip()) and not line.lstrip().startswith("#")


def split_documents(text: str) -> list[list[str]]:
    """The lines of each document, each document starting with the ``---`` line that opens it (the first may be empty)."""
    documents: list[list[str]] = [[]]
    for line in text.splitlines(keepends=True):
        if line.rstrip() == "---":
            documents.append([])
        documents[-1].append(line)
    return documents


def top_level_block(lines: list[str], key: str) -> tuple[int, int] | None:
    """The line of the unindented ``key:`` and the end of its block (the next unindented line that is not a comment), or ``None`` when there is none."""
    starts = [index for index, line in enumerate(lines) if line.rstrip() == f"{key}:"]
    if len(starts) != 1:
        return None
    end = len(lines)
    for index in range(starts[0] + 1, len(lines)):
        line = lines[index]
        if line[:1] not in (" ", "\t", "\n", "\r", "#", ""):
            end = index
            break
    return starts[0], end


def deployment_name(lines: list[str]) -> str | None:
    """The ``metadata.name`` of a document that is a ``Deployment``, or ``None`` for any other document."""
    if not any(line.rstrip() == "kind: Deployment" for line in lines):
        return None
    block = top_level_block(lines, "metadata")
    if block is None:
        return None
    start, end = block
    content = [line for line in lines[start + 1 : end] if is_content(line)]
    if not content or indent_of(content[0]) == 0:
        return None
    width = indent_of(content[0])
    for line in content:
        if indent_of(line) == width:
            match = NAME_LINE.match(line.strip())
            if match is not None:
                return next(group for group in match.groups() if group is not None)
    return None


def strategy_block(lines: list[str], start: int, end: int, width: int) -> tuple[int, int] | None:
    """The first and one past the last line of the spec's own ``strategy`` entry (at the spec's indent, with its deeper lines and comments), or ``None``."""
    found = [index for index in range(start, end) if indent_of(lines[index]) == width and re.match(r"strategy:(\s|$)", lines[index][width:])]
    if len(found) > 1:
        raise Refusal
    if not found:
        return None
    last = found[0] + 1
    while last < end and (not lines[last].strip() or indent_of(lines[last]) > width):
        last += 1
    while last > found[0] + 1 and not lines[last - 1].strip():
        last -= 1
    return found[0], last


def recreate(lines: list[str]) -> list[str]:
    block = top_level_block(lines, "spec")
    if block is None:
        raise Refusal
    spec, end = block
    inner = lines[spec + 1 : end]
    if any(line.startswith("\t") or line.lstrip(" ").startswith("\t") for line in inner) or not lines[spec].endswith("\n"):
        raise Refusal
    content = [line for line in inner if is_content(line)]
    if not content or indent_of(content[0]) == 0:
        raise Refusal
    width = indent_of(content[0])
    old = strategy_block(lines, spec + 1, end, width)
    pad = " " * width
    new = [*lines[: spec + 1], f"{pad}strategy:\n", f"{pad}  type: Recreate\n"]
    new.extend(lines[spec + 1 :] if old is None else [*lines[spec + 1 : old[0]], *lines[old[1] :]])
    verify(new, width)
    return new


def verify(lines: list[str], width: int) -> None:
    """The edited document has the one strategy, of exactly the two lines, directly under ``spec``."""
    block = top_level_block(lines, "spec")
    if block is None:
        raise Refusal
    spec, end = block
    found = strategy_block(lines, spec + 1, end, width)
    pad = " " * width
    if found is None or lines[found[0] : found[1]] != [f"{pad}strategy:\n", f"{pad}  type: Recreate\n"] or found[0] != spec + 1:
        raise Refusal


def render(text: str, release: str, names: list[str]) -> str:
    wanted = {f"{release}-{name}" for name in names}
    output: list[str] = []
    for lines in split_documents(text):
        name = deployment_name(lines)
        output.extend(recreate(lines) if name in wanted else lines)
    return "".join(output)


def main(arguments: list[str]) -> int:
    if len(arguments) != 2 or not NAME.fullmatch(arguments[0]) or not all(NAME.fullmatch(name) for name in arguments[1].split(",")):
        sys.stderr.write(USAGE + "\n")
        return 2
    try:
        result = render(sys.stdin.read(), arguments[0], arguments[1].split(","))
    except Refusal:
        sys.stderr.write(REFUSAL + "\n")
        return 3
    sys.stdout.write(result)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
