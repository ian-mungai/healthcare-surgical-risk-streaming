"""A test imports an ignored runner only through ``tests/fast/runner_import.py``, which names a failure by file label, line or error class and nothing else.

Failure modes covered here (fix round 2 of the held-out count records):

- A runner that does not parse, or that fails while it is imported, printed the line of its source or the message of its error into the output of the test run,
  and so a figure that the line or the message holds.
- A test module imported a runner on its own (``importlib.import_module``, ``spec_from_file_location``, a ``from tests.e2e.<runner> import``), so the same
  failure came back through a path that the helper does not cover.
"""

from __future__ import annotations

import ast
import io
import sys
import tempfile
import traceback
import unittest
from pathlib import Path
from unittest import mock

from tests.fast import runner_import
from tests.fast.runner_import import RunnerImportFailed, import_runner, safe_execute, safe_import
from tests.fast.test_heldout_figures import FIGURE

HERE = Path(__file__).resolve().parent
TRACKED = frozenset({"run_record", "commit_guards", "deadline"})
LINE = "the_line_that_must_not_be_printed = ("
PATH_WORDS = ("RUNNER", "E2E", "e2e")
LOADERS = frozenset({"import_module", "spec_from_file_location", "run_path", "run_module", "__import__"})


def runner_imports(source: str) -> list[tuple[str, int]]:
    """The rule and the line of each place where a test module imports a runner of ``tests/e2e`` other than the tracked ones, by itself.

    ``static_import``: ``from tests.e2e.<name> import`` or ``import tests.e2e.<name>`` and ``from tests.e2e import <name>``. ``dynamic_import``: a call of
    ``import_module``, ``__import__``, ``run_path`` or ``run_module`` whose module name starts ``tests.e2e.`` and does not name a tracked module, or is built
    from a name. ``file_import``: a call of ``spec_from_file_location`` or ``run_path`` that is given a path that names ``RUNNER`` or ``E2E`` or ``e2e``."""
    found: list[tuple[str, int]] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom) and node.module == "tests.e2e":
            found += [("static_import", node.lineno) for item in node.names if item.name not in TRACKED]
        elif isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("tests.e2e."):
            found += [("static_import", node.lineno)] if node.module.split(".")[2] not in TRACKED else []
        elif isinstance(node, ast.Import):
            found += [("static_import", node.lineno) for item in node.names if item.name.startswith("tests.e2e.") and item.name.split(".")[2] not in TRACKED]
        elif isinstance(node, ast.Call):
            name = node.func.id if isinstance(node.func, ast.Name) else node.func.attr if isinstance(node.func, ast.Attribute) else None
            if name not in LOADERS:
                continue
            first = node.args[0] if node.args else None
            text = first.value if isinstance(first, ast.Constant) and isinstance(first.value, str) else None
            built = isinstance(first, ast.JoinedStr) and isinstance(first.values[0], ast.Constant) and str(first.values[0].value).startswith("tests.e2e.")
            if built or (text is not None and text.startswith("tests.e2e.") and text.split(".")[2] not in TRACKED):
                found.append(("dynamic_import", node.lineno))
            elif name in ("spec_from_file_location", "run_path") and any(word in ast.unparse(item) for item in node.args for word in PATH_WORDS):
                found.append(("file_import", node.lineno))
    return sorted(set(found), key=lambda item: (item[1], item[0]))


class RunnerImportCheck(unittest.TestCase):
    PLANTS = (
        ("dynamic_import", "importlib.import_module('tests.e2e.signal_profile')\n"),
        ("dynamic_import", "import importlib\nmodule = importlib.import_module(f'tests.e2e.{name}')\n"),
        ("dynamic_import", "module = import_module('tests.e2e.gold_dimensions')\n"),
        ("static_import", "from tests.e2e.signal_profile import Run\n"),
        ("static_import", "import tests.e2e.signal_profile\n"),
        ("static_import", "from tests.e2e import signal_profile\n"),
        ("file_import", "spec = importlib.util.spec_from_file_location('x', RUNNER)\n"),
        ("file_import", "spec = importlib.util.spec_from_file_location('x', E2E / 'signal_profile.py')\n"),
        ("file_import", "runpy.run_path(str(E2E / 'gold_dimensions.py'))\n"),
    )
    CONTROLS = (
        ("the tracked record module", "from tests.e2e import run_record\nfrom tests.e2e.run_record import Record\nimport tests.e2e.deadline\n"),
        ("a tracked module by name", "importlib.import_module('tests.e2e.commit_guards')\n"),
        ("a module of the project", "importlib.import_module('spark.jobs.patient_split')\nimportlib.import_module(f'ansible.module_utils.{name}')\n"),
        ("a helper file of a temporary folder", "spec = importlib.util.spec_from_file_location('guard_under_test', path)\n"),
        ("the helper", "from tests.fast.runner_import import import_runner\nmodule = import_runner('signal_profile')\n"),
    )

    def test_each_plant_is_named_by_its_rule(self) -> None:
        for rule, source in self.PLANTS:
            with self.subTest(plant=source):
                self.assertIn(rule, {name for name, _ in runner_imports(source)})

    def test_each_control_is_quiet(self) -> None:
        for label, source in self.CONTROLS:
            with self.subTest(control=label):
                self.assertEqual([], runner_imports(source))

    def test_no_test_module_imports_a_runner_on_its_own(self) -> None:
        found = {}
        for path in sorted(HERE.glob("*.py")):
            if path.name in ("runner_import.py", Path(__file__).name):
                continue
            if rules := runner_imports(path.read_text()):
                found[path.name] = rules
        self.assertEqual({}, found, "a test module imports a runner without tests/fast/runner_import.py: the file, the rule and the line")


class FailureIsSafe(unittest.TestCase):
    """The failure of a runner that does not import names the file label and a line or a class, and holds no line of its source and no message."""

    def folder_with(self, files: dict[str, str]) -> str:
        folder = self.enterContext(tempfile.TemporaryDirectory(prefix="runner_import_"))
        for name, text in files.items():
            (Path(folder) / name).write_text(text)
        self.enterContext(mock.patch.object(sys, "path", [folder, *sys.path]))
        for name in files:
            self.addCleanup(sys.modules.pop, name.removesuffix(".py"), None)
        return folder

    def output_of(self, module: str, label: str) -> str:
        class Probe(unittest.TestCase):
            def runTest(probe) -> None:  # noqa: N802
                safe_import(module, label)

        stream = io.StringIO()
        result = unittest.TextTestRunner(stream=stream, verbosity=0).run(Probe())
        self.assertFalse(result.wasSuccessful())
        return stream.getvalue()

    def assert_safe(self, output: str, *, also: tuple[str, ...] = ()) -> None:
        self.assertNotIn(str(FIGURE), output, "the failure output holds the figure")
        self.assertNotIn(f"{FIGURE:,}", output, "the failure output holds the figure with a separator")
        self.assertNotIn(LINE, output, "the failure output holds a line of the runner")
        for text in also:
            self.assertNotIn(text, output, "the failure output holds text of the source or of the error")

    def test_a_runner_that_does_not_parse_is_named_by_its_label_and_line_only(self) -> None:
        self.folder_with({"broken_runner.py": f"x = 1\n{LINE} {FIGURE}\n"})
        output = self.output_of("broken_runner", "broken_runner.py")
        self.assert_safe(output, also=("was never closed", "SyntaxError: "))
        self.assertIn("broken_runner.py: the runner does not parse (line 2)", output)

    def test_a_runner_that_raises_while_it_is_imported_is_named_by_the_class_only(self) -> None:
        self.folder_with({"raising_runner.py": f"raise ValueError('the figure {FIGURE} and {LINE}')\n"})
        output = self.output_of("raising_runner", "raising_runner.py")
        self.assert_safe(output, also=("the figure",))
        self.assertIn("raising_runner.py: the runner import failed with ValueError", output)

    def test_a_runner_that_cannot_import_a_module_is_named_by_the_class_only(self) -> None:
        self.folder_with({"missing_runner.py": f"import module_{FIGURE}_that_is_not_there\n"})
        output = self.output_of("missing_runner", "missing_runner.py")
        self.assert_safe(output, also=("module_", "No module named"))
        self.assertIn("ModuleNotFoundError", output)

    def test_a_runner_that_imports_a_module_that_does_not_parse_is_named_by_that_file_and_line(self) -> None:
        self.folder_with({"outer_runner.py": "import inner_broken\n", "inner_broken.py": f"y = 2\n{LINE} {FIGURE}\n"})
        output = self.output_of("outer_runner", "outer_runner.py")
        self.assert_safe(output)
        self.assertIn("inner_broken.py: the runner does not parse (line 2)", output)

    def test_a_failure_has_no_cause_and_hides_its_context(self) -> None:
        self.folder_with({"raising_two.py": f"raise ValueError('{FIGURE}')\n"})
        with self.assertRaises(RunnerImportFailed) as caught:
            safe_import("raising_two", "raising_two.py")
        self.assertIsNone(caught.exception.__cause__)
        self.assertTrue(caught.exception.__suppress_context__)
        self.assert_safe("".join(traceback.format_exception(caught.exception)))

    def test_a_file_that_is_executed_fails_in_the_same_way(self) -> None:
        folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="runner_execute_")))
        (folder / "broken.py").write_text(f"{LINE} {FIGURE}\n")
        (folder / "raising.py").write_text(f"raise KeyError('{FIGURE}')\n")
        for name, wanted in (
            ("broken.py", "broken.py: the runner does not parse (line 1)"),
            ("raising.py", "raising.py: the runner import failed with KeyError"),
        ):
            with self.subTest(file=name), self.assertRaises(RunnerImportFailed) as caught:
                safe_execute(folder / name, f"{name}_under_test")
            self.assertEqual(wanted, str(caught.exception))
            self.assert_safe("".join(traceback.format_exception(caught.exception)))

    def test_a_skip_passes_through_and_a_good_module_is_returned(self) -> None:
        self.folder_with({"skipping_runner.py": "import unittest\nraise unittest.SkipTest('later')\n", "good_runner.py": "VALUE = 3\n"})
        with self.assertRaises(unittest.SkipTest):
            safe_import("skipping_runner", "skipping_runner.py")
        self.assertEqual(3, safe_import("good_runner", "good_runner.py").VALUE)

    def test_an_absent_runner_is_a_skip_and_a_present_one_is_imported(self) -> None:
        empty = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="runner_absent_")))
        with mock.patch.object(runner_import, "E2E", empty), self.assertRaises(unittest.SkipTest) as skipped:
            import_runner("signal_profile")
        self.assertIn("not on this machine", str(skipped.exception))
        self.assertEqual("tests.e2e.deadline", safe_import("tests.e2e.deadline", "deadline.py").__name__)


if __name__ == "__main__":
    unittest.main()
