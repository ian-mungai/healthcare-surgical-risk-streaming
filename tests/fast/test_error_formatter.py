"""Every command shows a caught exception through ``describe_error``: fixed text only, and the type name for any exception that is neither a closed error
nor a marked class.

``test_no_command_shows_the_text_of_an_exception`` scans every module that writes to standard error, or calls ``log`` or ``emit``, and fails for any use of a
caught exception there other than as the argument of ``describe_error`` or ``type``, or its ``status`` and ``error_code``, which Nessie's client keeps to
fixed values. It also fails for the text of a caught file error in a message that a ``raise`` builds, because that text names a path.
"""

from __future__ import annotations

import ast
import contextlib
import io
import os
import re
import sys
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from scripts import aws_cli
from scripts.infra import host, terraform_wrapper
from scripts.infra.state_bootstrap import RunBinding
from scripts.ingestion.notice import ClosedError, Code, Notice, Subject, describe_error, render
from scripts.process import run_command
from scripts.project_config import load_configuration
from scripts.stack import warehouse
from scripts.stack.limits import LimitsError
from scripts.stack.nessie import CONFLICT_TEXT, NessieConflict, NessieError
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


LAUNCHER_MARKED = {
    "scripts.infra.host.FixedHostError",
    "scripts.infra.host.NoPeerError",
    "scripts.infra.terraform_wrapper.FixedWrapperError",
    "scripts.project_config.ConfigurationError",
    "scripts.stack.warehouse.WarehouseError",
}
MARKED = {
    "scripts.images.pins.PinError",
    "scripts.infra.host.IntakeRefusal",
    "scripts.infra.host.LicenceRefusal",
    "scripts.images.publish.PublishError",
    "infra.iam.scripts.manage_policies.KeyAdviceError",
    "scripts.stack.tags.RecordNotPublished",
    "spark.streaming.checkpoint.CheckpointFormatError",
} | LAUNCHER_MARKED


def marked_classes() -> set[str]:
    """Every class in the source that has ``FixedTextError`` among its bases, or a class that is marked in this way, named by module path and class."""
    classes = [(name, node) for name in tracked_python() for node in ast.walk(ast.parse((ROOT / name).read_text())) if isinstance(node, ast.ClassDef)]
    marked: set[str] = set()
    while True:
        added = {
            node.name
            for _, node in classes
            if node.name not in marked and any(ast.unparse(base).endswith("FixedTextError") or ast.unparse(base) in marked for base in node.bases)
        }
        if not added:
            break
        marked |= added
    return {f"{name.removesuffix('.py').replace('/', '.')}.{node.name}" for name, node in classes if node.name in marked}


LAUNCHER_CLASS_NAMES = frozenset(name.rsplit(".", 1)[1] for name in LAUNCHER_MARKED)
ALLOWED_PARTS = frozenset(
    {
        "', '.join(missing)",
        "', '.join(problems)",
        "'; '.join(overrides)",
        "timeout_text('tailscale status', min(timeout, SSH_ATTEMPT_SECONDS))",
        "timeout_text(f'ansible-playbook {playbook}', budget)",
        "timeout_text(f'terraform {arguments[0]}', timeout)",
        "arguments[0]",
        "BUCKET_NAME_LIMIT",
        "deadline_seconds",
        "ENV_FILE.name",
        "host_name",
        "kept",
        "KEY_MESSAGES[problem].format(status=255, seconds=KEY_SEND_SECONDS)",
        "LICENCE_MESSAGES[problem].format(status=255, seconds=KEY_SEND_SECONDS)",
        "INTAKE_POLICY_MESSAGES[problem].format(status=255, seconds=INTAKE_SEND_SECONDS)",
        "label",
        "len(peers)",
        "mismatch",
        "mode_problem",
        "note",
        "PINNED_TERRAFORM",
        "result.returncode",
        "tag",
        "what",
    }
)
KEY_MESSAGE_PART = re.compile(
    r"(?:KEY|LICENCE)_MESSAGES\[(?:'[a-z_]+'|f'script_\{code\}')\](?:\.format\((?:seconds=(?:KEY_EXPORT_SECONDS|KEY_SEND_SECONDS|LICENCE_SEND_SECONDS)|status=(?:255|result\.returncode|sent\.returncode))\))?"
)
FIXED_RETURNS = {
    "scripts/infra/state_bootstrap.py": ("bucket_problem",),
    "scripts/infra/terraform_wrapper.py": ("region_problem", "environment_override_problems"),
}
FIXED_RETURN_PARTS = frozenset({"name"})
WAIT_LABEL_PARTS = frozenset({"host_name"})


def message_parts(node: ast.AST) -> list[str]:
    """The pieces of a message that are not a literal: each expression of an f-string, each side of a ``+`` and any other expression whole."""
    if isinstance(node, ast.Constant):
        return []
    if isinstance(node, ast.JoinedStr):
        return [part for value in node.values if isinstance(value, ast.FormattedValue) for part in message_parts(value.value)]
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return message_parts(node.left) + message_parts(node.right)
    return [ast.unparse(node)]


def part_is_allowed(part: str) -> bool:
    return part in ALLOWED_PARTS or KEY_MESSAGE_PART.fullmatch(part) is not None


def raised_class(node: ast.Raise) -> str | None:
    call = node.exc
    if isinstance(call, ast.Call):
        function = call.func
        return function.id if isinstance(function, ast.Name) else function.attr if isinstance(function, ast.Attribute) else None
    return None


def marked_raise_parts(source: str, names: frozenset[str] = LAUNCHER_CLASS_NAMES) -> list[tuple[int, str]]:
    """Each non-literal piece in the message of a ``raise`` of a marked class: its line and its text."""
    found = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Raise) and raised_class(node) in names and isinstance(node.exc, ast.Call):
            found += [(node.lineno, part) for argument in [*node.exc.args, *(item.value for item in node.exc.keywords)] for part in message_parts(argument)]
    return found


def function_return_parts(source: str, names: tuple[str, ...]) -> set[str]:
    """The non-literal pieces of the values that the named functions return, one list item at a time for a list that a comprehension builds."""
    parts: set[str] = set()
    for function in (node for node in ast.walk(ast.parse(source)) if isinstance(node, ast.FunctionDef) and node.name in names):
        for node in ast.walk(function):
            if isinstance(node, ast.Return) and node.value is not None:
                value = node.value.elt if isinstance(node.value, ast.ListComp) else node.value
                parts.update(message_parts(value))
    return parts


def wait_label_parts(source: str) -> set[str]:
    """The non-literal pieces of the first argument of each ``wait_for`` call."""
    return {
        part
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "wait_for" and node.args
        for part in message_parts(node.args[0])
    }


BOUND = RunBinding(
    "20260101t0000-abcdef",
    "c0ffee",
    "s3",
    "us-west-2",
    "example-project",
    "default",
    ["state/dev/terraform.tfstate", "state/dev/terraform.tfstate.tflock"],
    "verified",
)
CONFIG = {"AWS_PROFILE": "example_project_dev", "AWS_REGION": "eu-west-1", "ENVIRONMENT": "dev", "S3_BUCKET": "example-project"}
REGION_REFUSAL = "refused before any plan or change: the run is bound to a region other than the AWS_REGION of .env; bind the run again or correct AWS_REGION"
PLANTED = "planted_marker"


def host_stderr(raiser: Callable[[], object]) -> tuple[int, str]:
    """What ``host stop`` writes to standard error when ``raiser`` raises, at a real raise site of the program, inside the command."""
    with (
        mock.patch.object(sys, "argv", ["host", "stop"]),
        mock.patch.object(host, "load_configuration", return_value=CONFIG),
        mock.patch.object(host, "Aws"),
        mock.patch.object(host, "host_stop", side_effect=lambda *_: raiser()),
        contextlib.redirect_stderr(io.StringIO()) as err,
    ):
        code = host.main()
    return code, err.getvalue()


def refused_planted_limits() -> object:
    with mock.patch.object(terraform_wrapper, "load_limits", side_effect=LimitsError(f"{PLANTED} /secret/path")):
        return terraform_wrapper.heavy_inputs()


def failed_aws_call() -> object:
    reply = SimpleNamespace(returncode=1, stdout="", stderr=f"An error occurred (AccessDenied) when calling the X operation: {PLANTED}")
    with mock.patch.object(aws_cli, "run_command", return_value=reply), mock.patch.object(aws_cli, "keep_raw_error", side_effect=OSError(PLANTED)):
        return aws_cli.Aws("example_project_dev", "us-west-2").call("s3api", "head-bucket")


def bad_state_figure() -> object:
    state = {"values": {"outputs": {"heavy_memory_mib": {"value": PLANTED}}}}
    return host.heavy_memory_variables(state, 1)


class LauncherErrorTextTests(unittest.TestCase):
    """``make host-start`` and its sister commands show the fixed text of their own refusals and the type name of any other error."""

    def test_the_host_launcher_shows_the_region_refusal_of_the_wrapper(self) -> None:
        code, text = host_stderr(lambda: terraform_wrapper.initialise(BOUND, CONFIG))
        self.assertEqual(1, code)
        self.assertEqual(REGION_REFUSAL + "\n", text)

    def test_the_host_launcher_shows_the_fixed_text_of_a_host_refusal_and_of_a_missing_peer(self) -> None:
        self.assertEqual(
            (1, "the run's state does not hold exactly one host instance: run make host-up first\n"),
            host_stderr(lambda: host.instance_id({})),
        )
        self.assertEqual(
            (1, f"no online peer named platform-dev with {host.HOST_TAG} in the Tailscale network\n"),
            host_stderr(lambda: host.select_peer({}, "platform-dev", host.HOST_TAG)),
        )

    def test_the_host_launcher_shows_the_fixed_text_of_a_configuration_refusal_and_never_the_path(self) -> None:
        missing = Path(tempfile.gettempdir()) / "planted_folder" / "planted.env"
        code, text = host_stderr(lambda: load_configuration(missing))
        self.assertEqual((1, ".env is missing; copy .env.example to .env and fill it in\n"), (code, text))
        self.assertNotIn("planted", text)

    def test_the_host_launcher_shows_the_fixed_text_of_a_warehouse_refusal(self) -> None:
        code, text = host_stderr(lambda: warehouse.warehouse_location("Bad_Bucket"))
        self.assertEqual(1, code)
        self.assertEqual(
            "the bucket name is not a valid S3 bucket name: 3 to 63 lower case letters, digits and dashes, starting and ending with a letter or digit\n", text
        )

    def test_a_run_id_from_the_binding_file_never_reaches_a_shown_refusal(self) -> None:
        """The binding file is an input: a refusal shows where its output was kept by the file's fixed name, never by the run id the file gives."""
        from dataclasses import replace

        from scripts.process import TimeoutExpired

        marked = replace(BOUND, run_id=f"{PLANTED}/nested")
        failed = SimpleNamespace(returncode=1, stdout="out", stderr="err")
        expired = TimeoutExpired(["terraform"], 5, output=b"partial", stderr=b"")
        cases: dict[str, Callable[[], object]] = {}
        outcomes: tuple[tuple[str, dict[str, Any]], ...] = (("terraform failure", {"return_value": failed}), ("terraform timeout", {"side_effect": expired}))
        for label, outcome in outcomes:

            def call(outcome: dict[str, Any] = outcome) -> object:
                with (
                    mock.patch.object(terraform_wrapper, "run_command", **outcome),
                    mock.patch.object(terraform_wrapper, "save_raw"),
                    mock.patch.object(terraform_wrapper, "pinned_terraform", return_value=Path("terraform")),
                    mock.patch.object(terraform_wrapper, "configuration_folder", return_value=Path("folder")),
                ):
                    return terraform_wrapper.terraform(marked, ["plan"], 5)

            cases[label] = call
        ADVICE = {
            "terraform failure": "terraform plan failed with exit status 1; its full output is in the run's raw folder as terraform_plan_failure.txt\n",
            "terraform timeout": "terraform plan timed out after 5 seconds; its partial output is in the run's raw folder as terraform_plan_failure.txt\n",
            "missing plan": "no saved plan JSON of the run gives the planned heavy_memory_mib: run make host-up again to plan first\n",
        }
        with tempfile.TemporaryDirectory() as root, mock.patch.object(host, "ROOT", Path(root)):
            cases["missing plan"] = lambda: host.saved_plan(marked.run_id, applied=True)
            for label, raiser in cases.items():
                with self.subTest(case=label):
                    code, text = host_stderr(raiser)
                    self.assertEqual(1, code)
                    self.assertNotIn(PLANTED, text)
                    self.assertEqual(ADVICE[label], text)

    def test_a_class_that_is_not_marked_shows_its_type_name_and_nothing_of_a_planted_value(self) -> None:
        for name, raiser in (("WrapperError", refused_planted_limits), ("HostError", bad_state_figure), ("AwsError", failed_aws_call)):
            with self.subTest(error=name):
                self.assertEqual((1, f"{name}\n"), host_stderr(raiser))

    def test_the_other_commands_that_catch_these_classes_show_the_same_text(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["terraform_wrapper", "plan"]),
            mock.patch.object(terraform_wrapper, "load_configuration", return_value=CONFIG),
            mock.patch.object(terraform_wrapper, "load_run", return_value=BOUND),
            contextlib.redirect_stderr(io.StringIO()) as err,
        ):
            code = terraform_wrapper.main()
        self.assertEqual(1, code)
        self.assertEqual(REGION_REFUSAL + "\n", err.getvalue())
        with (
            mock.patch.object(warehouse, "load_configuration", return_value={"S3_BUCKET": "Bad_Bucket"}),
            contextlib.redirect_stderr(io.StringIO()) as err,
        ):
            code = warehouse.main()
        self.assertEqual(1, code)
        self.assertTrue(err.getvalue().startswith("warehouse: the bucket name is not a valid S3 bucket name"), err.getvalue())


class MarkedRaiseSiteTests(unittest.TestCase):
    """A marked class is raised with literals and the listed names only, so a path, an input value or a reply cannot reach its text."""

    def test_the_scan_finds_a_value_from_outside_in_a_marked_raise_and_passes_a_literal(self) -> None:
        path = "def f(path, error):\n    raise FixedHostError(f'cannot read {path}')\n"
        text = "def f(error):\n    raise FixedWrapperError(str(error))\n"
        joined = "def f(note):\n    raise FixedWrapperError('failed: ' + note)\n"
        other = "def f(path):\n    raise WrapperError(f'cannot read {path}')\n"
        literal = "def f():\n    raise FixedHostError('fixed advice')\n"
        self.assertEqual([(2, "path")], marked_raise_parts(path))
        self.assertEqual([(2, "str(error)")], marked_raise_parts(text))
        self.assertEqual([(2, "note")], marked_raise_parts(joined))
        self.assertEqual([], marked_raise_parts(other), "a class that is not marked may hold any text")
        self.assertEqual([], marked_raise_parts(literal))

    def test_no_marked_raise_holds_a_piece_that_is_not_listed(self) -> None:
        found = {
            f"{name}:{line}: {part}" for name in tracked_python() for line, part in marked_raise_parts((ROOT / name).read_text()) if not part_is_allowed(part)
        }
        self.assertEqual(set(), found)

    def test_every_listed_piece_is_used(self) -> None:
        used = {part for name in tracked_python() for _, part in marked_raise_parts((ROOT / name).read_text())}
        self.assertEqual(set(), set(ALLOWED_PARTS) - used)
        self.assertTrue(any(KEY_MESSAGE_PART.fullmatch(part) for part in used))

    def test_the_key_messages_are_all_literals(self) -> None:
        tree = ast.parse((ROOT / "scripts/infra/host.py").read_text())
        table = next(
            node.value
            for node in tree.body
            if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "KEY_MESSAGES" for target in node.targets)
        )
        if not isinstance(table, ast.Dict):
            self.fail("KEY_MESSAGES is not a dict literal")
        self.assertTrue(all(isinstance(item, ast.Constant) and isinstance(item.value, str) for item in [*table.keys, *table.values] if item is not None))
        self.assertIsNone(KEY_MESSAGE_PART.fullmatch("KEY_MESSAGES['empty'].format(status=error)"))

    def test_the_helpers_that_build_a_refusal_return_literals_and_the_listed_names_only(self) -> None:
        for name, functions in FIXED_RETURNS.items():
            with self.subTest(file=name):
                self.assertEqual(set(), function_return_parts((ROOT / name).read_text(), functions) - FIXED_RETURN_PARTS)
        planted = "def region_problem(run):\n    return f'bound to {run.region}'\n"
        self.assertEqual({"run.region"}, function_return_parts(planted, ("region_problem",)))

    def test_every_wait_of_the_host_commands_is_named_by_a_literal_or_the_host_name(self) -> None:
        self.assertEqual(set(), wait_label_parts((ROOT / "scripts/infra/host.py").read_text()) - WAIT_LABEL_PARTS)
        self.assertEqual({"path"}, wait_label_parts("wait_for(f'a {path}', probe, 1)\n"))


class DescribeErrorTests(unittest.TestCase):
    def test_a_marked_exception_shows_its_text_and_only_the_listed_classes_are_marked(self) -> None:
        import infra.iam.scripts.manage_policies as policies

        self.assertEqual("fixed advice", describe_error(policies.KeyAdviceError("fixed advice")))
        self.assertEqual(MARKED, marked_classes())

    def test_the_tag_advice_after_the_create_call_is_constant_and_names_no_tag_hash_or_exception_text(self) -> None:
        from scripts.stack import tags

        seen = set()
        for stage, made in (("after", False), ("posting", False), ("posting", True), ("after", True)):
            advice = tags.not_published_advice(stage, made=made)
            seen.add(advice)
            self.assertTrue(advice.startswith("RecordNotPublished: "), advice)
            self.assertNotIn("run-", advice)
            self.assertEqual(advice, describe_error(tags.RecordNotPublished(advice)))
        self.assertEqual(3, len(seen), "three situations: made, may have been made, already in place")
        error = tags.RecordNotPublished(tags.not_published_advice("after", made=True))
        self.assertIsInstance(error, NessieError)
        self.assertEqual(tags.not_published_advice("after", made=True), describe_error(error))

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

    def test_the_catalog_proof_job_shows_a_fixed_line_for_a_conflict_and_no_word_of_the_error_code(self) -> None:
        conflict = NessieConflict("POST /trees was refused: reply_marker", status=409, error_code="code_marker")
        with (
            mock.patch.dict(os.environ, {"NESSIE_URI": "http://nessie.invalid:19120/api/v2"}),
            mock.patch.object(catalog_proof, "start_deadline"),
            mock.patch.object(catalog_proof, "NessieClient") as client,
            contextlib.redirect_stderr(io.StringIO()) as err,
        ):
            client.return_value.config.side_effect = conflict
            code = catalog_proof.main(["--branch", "argument_marker", "--rows", "1"])
        self.assertEqual(1, code)
        self.assertEqual(f"{catalog_proof.JOB}: {CONFLICT_TEXT}\n", err.getvalue())

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
