"""Every command shows a caught exception through ``describe_error``: fixed text only, and the type name for any exception that is not a closed error.

``test_no_command_shows_the_text_of_an_exception`` scans every module that writes to standard error, or calls ``log`` or ``emit``, and fails for any use of a
caught exception there other than as the argument of ``describe_error`` or ``type``, or its ``status`` and ``error_code``, which Nessie's client keeps to
fixed values. It also fails for the text of a caught file error in a message that a ``raise`` builds, because that text names a path.
"""

from __future__ import annotations

import ast
import contextlib
import io
import os
import unittest
from unittest import mock

from scripts.ingestion.notice import ClosedError, Code, FixedTextError, Notice, Subject, describe_error, render
from scripts.process import run_command
from scripts.stack.nessie import NessieError
from spark.jobs import catalog_proof
from tests.fast.git_fixture import ROOT, TIMEOUT_SECONDS

FILE_ERRORS = frozenset({"OSError", "IOError", "FileNotFoundError", "PermissionError", "IsADirectoryError", "NotADirectoryError", "FileExistsError"})
LOG_CALLS = frozenset({"log", "emit"})
KEPT_ATTRIBUTES = frozenset({"status", "error_code", "notices", "strerror"})


def tracked_python() -> list[str]:
    listing = run_command("git", ["ls-files", "-z", "--", "*.py"], cwd=ROOT, timeout=TIMEOUT_SECONDS, check=True).stdout
    return [name for name in listing.split("\0") if name and (ROOT / name).is_file() and not name.startswith(("tests/", "ansible/"))]


def is_message_sink(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    function = node.func
    if isinstance(function, ast.Attribute) and function.attr == "write" and ast.unparse(function.value) == "sys.stderr":
        return True
    if isinstance(function, ast.Name) and function.id in LOG_CALLS:
        return True
    return (
        isinstance(function, ast.Name)
        and function.id == "print"
        and any(item.arg == "file" and ast.unparse(item.value) == "sys.stderr" for item in node.keywords)
    )


def exception_uses(source: str) -> list[int]:
    """Lines where a caught exception reaches a message other than through ``describe_error`` or ``type``."""
    tree = ast.parse(source)
    parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
    lines: set[int] = set()
    for handler in (node for node in ast.walk(tree) if isinstance(node, ast.ExceptHandler) and node.name):
        caught = {ast.unparse(item) for item in (handler.type.elts if isinstance(handler.type, ast.Tuple) else [handler.type])} if handler.type else set()
        for statement in handler.body:
            for node in ast.walk(statement):
                sink = is_message_sink(node)
                raised = isinstance(node, ast.Raise) and node.exc is not None and bool(caught & FILE_ERRORS)
                if not (sink or raised):
                    continue
                message = node.exc if isinstance(node, ast.Raise) and node.exc is not None else node
                for name in (part for part in ast.walk(message) if isinstance(part, ast.Name) and part.id == handler.name):
                    parent = parents[name]
                    if isinstance(parent, ast.Call) and isinstance(parent.func, ast.Name) and parent.func.id in {"describe_error", "type"}:
                        continue
                    if isinstance(parent, ast.Attribute) and parent.attr in KEPT_ATTRIBUTES:
                        continue
                    lines.add(name.lineno)
    return sorted(lines)


MARKED = {"infra.iam.scripts.manage_policies.KeyAdviceError"}


def marked_classes() -> set[str]:
    """Every class below ``FixedTextError`` in the modules that the commands load, named by module and class."""
    found: set[str] = set()
    pending = list(FixedTextError.__subclasses__())
    while pending:
        item = pending.pop()
        found.add(f"{item.__module__}.{item.__qualname__}")
        pending.extend(item.__subclasses__())
    return found


class DescribeErrorTests(unittest.TestCase):
    def test_a_marked_exception_shows_its_text_and_only_the_listed_classes_are_marked(self) -> None:
        import infra.iam.scripts.manage_policies as policies

        self.assertEqual("fixed advice", describe_error(policies.KeyAdviceError("fixed advice")))
        self.assertEqual(MARKED, marked_classes())

    def test_a_closed_error_shows_its_fixed_notices(self) -> None:
        error = ClosedError(Notice(Code.CANNOT_READ, subject=Subject.RESULT))
        self.assertEqual(render(Notice(Code.CANNOT_READ, subject=Subject.RESULT)), describe_error(error))

    def test_any_other_exception_shows_the_name_of_its_type_and_nothing_of_its_text(self) -> None:
        for error in (FileNotFoundError(2, "No such file", "/secret/path_marker"), ValueError("value_marker"), RuntimeError("reply_marker")):
            with self.subTest(error=type(error).__name__):
                self.assertEqual(type(error).__name__, describe_error(error))

    def test_the_catalog_proof_job_shows_no_word_of_a_refusal_from_nessie(self) -> None:
        marker = "reply_marker_branch"
        error = NessieError(f"GET /trees/{marker} was refused: {marker}", status=500, error_code="X")
        with (
            mock.patch.dict(os.environ, {"NESSIE_URI": "http://nessie.invalid:19120/api/v2"}),
            mock.patch.object(catalog_proof, "start_deadline"),
            mock.patch.object(catalog_proof, "NessieClient") as client,
            contextlib.redirect_stderr(io.StringIO()) as err,
        ):
            client.return_value.config.side_effect = error
            code = catalog_proof.main(["--branch", "argument_marker", "--rows", "1"])
        self.assertEqual(1, code)
        self.assertEqual(f"{catalog_proof.JOB}: cannot work with Nessie: NessieError\n", err.getvalue())

    def test_the_scan_finds_each_form_that_it_is_written_for(self) -> None:
        shown = "import sys\n\n\ndef main():\n    try:\n        pass\n    except ValueError as error:\n        sys.stderr.write(f'{error}')\n"
        text = "import sys\n\n\ndef main():\n    try:\n        pass\n    except ValueError as error:\n        sys.stderr.write(str(error))\n"
        logged = "def main():\n    try:\n        pass\n    except ValueError as error:\n        log('stopped', message=str(error))\n"
        file_text = "def main():\n    try:\n        pass\n    except OSError as error:\n        raise Failure(f'{error}')\n"
        described = (
            "import sys\n\n\ndef main():\n    try:\n        pass\n    except ValueError as error:\n"
            "        sys.stderr.write(describe_error(error) + type(error).__name__ + str(error.status))\n"
        )
        inner = "def main():\n    try:\n        pass\n    except ValueError as error:\n        raise Failure(f'{error}')\n"
        self.assertEqual([8], exception_uses(shown))
        self.assertEqual([8], exception_uses(text))
        self.assertEqual([5], exception_uses(logged))
        self.assertEqual([5], exception_uses(file_text))
        self.assertEqual([], exception_uses(described))
        self.assertEqual([], exception_uses(inner), "a domain error that a raise passes on is shown by the entry point, through the formatter")

    def test_no_command_shows_the_text_of_an_exception(self) -> None:
        found = {name: exception_uses((ROOT / name).read_text()) for name in tracked_python()}
        self.assertEqual({}, {name: lines for name, lines in found.items() if lines})


if __name__ == "__main__":
    unittest.main()
