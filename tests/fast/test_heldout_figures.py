"""No runner holds or records the frozen test-set patient count.

The held-out rule: no count, value or row of a test-set patient leaves a job or reaches a record. A runner's source is attached to every record it writes,
and its expectation texts are the record, so neither may hold the pinned count of the test set. The figure is taken from ``spark.split_table`` when the test
runs, so the number is written nowhere in this file. The runners in ``tests/e2e/`` are Git-ignored: the scan covers those present in the checkout, and the
planted cases below hold the scanner itself to what it must find, with or without them.

What the scan finds in a runner (a file of ``tests/e2e/``, any depth):

- ``figure_text``: the figure in the source text, with or without a thousands separator, or in a string constant (a split of the figure over two string literals
  is one constant);
- ``figure_number``: a number constant equal to the figure, in any spelling;
- ``figure_in_expect``: a name that carries the pinned counts (an import from the rule module, or a constant that holds the figure, or a name assigned from
  one of them) inside the expectation or the observation of an ``expect`` call, where its value would be printed into the record. A comparison with the
  pinned counts is not a carrier: only whether it matched is recorded. A name assigned from the split rule itself (``patient_part``, ``SET_NAMES``), as
  a count of the patients of each set is, is a carrier too, unless it is a comparison or a ``bool``, ``all``, ``any`` or ``len`` of one. This is a best-effort
  check of the source: the sealed runners also hold ``sink_leaks`` of ``tests/fast/outcome_accept.py`` to what they record.

Counts of development patients are allowed in a runner.
"""

from __future__ import annotations

import ast
import contextlib
import io
import re
import tempfile
import traceback
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.process import run_command
from spark import split_table
from tests.fast.outcome_accept import sink_leaks

ROOT = Path(__file__).resolve().parents[2]
E2E = ROOT / "tests" / "e2e"
FIGURE = split_table.PINNED_COUNTS["test"]
RULE_MODULE = "spark.split_table"
RULE_NAMES = frozenset({"patient_part", "SET_NAMES"})
FLAG_CALLS = frozenset({"all", "any", "bool", "len"})
WRAPPERS = frozenset({"str", "repr", "ascii", "format", "dict", "list", "tuple", "set", "frozenset", "sorted"})
METHODS = frozenset({"format", "join", "items", "values", "copy", "get"})
MISSING = object()
GIT_SECONDS = 30
EXPECTATION_POSITIONS = {2: "expected", 3: "observed"}


def numbers_of(document: Any) -> set[float]:
    """Every number in a document that a stand-in prints: a number value, and each run of digits in a text (a comma or an underscore inside it is dropped)."""
    found: set[float] = set()
    if isinstance(document, bool):
        return found
    if isinstance(document, int | float):
        found.add(document)
    elif isinstance(document, str):
        found.update(int(run.replace(",", "").replace("_", "")) for run in re.findall(r"[0-9][0-9,_]*", document) if run[-1].isdigit())
    elif isinstance(document, dict):
        for key, item in document.items():
            found |= numbers_of(key) | numbers_of(item)
    elif isinstance(document, list | tuple | set | frozenset):
        for item in document:
            found |= numbers_of(item)
    return found


def figure_derivable(document: Any, figure: int = FIGURE) -> bool:
    """Whether the figure is one of the numbers of a document, or the sum or difference of two or three of them (as the test count is the difference of the full
    cohort's patients and the two development sets)."""
    values = numbers_of(document)
    signs = (1, -1)
    if figure in values:
        return True
    for first in values:
        for sign in signs:
            rest = figure - sign * first
            if rest in values or -rest in values:
                return True
            for second in values:
                for other in signs:
                    last = rest - other * second
                    if last in values or -last in values:
                        return True
    return False


def figure_pattern(figure: int) -> re.Pattern[str]:
    """The figure as a whole number, written with or without a thousands separator, and not part of a longer number, a word, a hash or a decimal."""
    groups = f"{figure:,}".split(",")
    return re.compile(r"(?<![0-9A-Za-z_.])" + "[,_']?".join(groups) + r"(?![0-9A-Za-z_]|\.[0-9])")


def holds(value: Any, figure: int, pattern: re.Pattern[str]) -> bool:
    """Whether a value, or a value inside it, is the figure or a text that holds it."""
    if isinstance(value, bool):
        return False
    if isinstance(value, int | float):
        return value == figure
    if isinstance(value, str):
        return pattern.search(value) is not None
    if isinstance(value, dict):
        return any(holds(item, figure, pattern) for item in value.values())
    if isinstance(value, list | tuple | set | frozenset):
        return any(holds(item, figure, pattern) for item in value)
    return False


def dotted(node: ast.AST) -> str | None:
    """``a.b.c`` for a chain of names and attributes, or ``None`` for any other expression."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute) and (head := dotted(node.value)) is not None:
        return f"{head}.{node.attr}"
    return None


class Carriers:
    """The names of one runner that carry the pinned counts, with the rule module's own names and every name that is assigned from a carrier."""

    def __init__(self, tree: ast.Module, figure: int) -> None:
        self.figure = figure
        self.pattern = figure_pattern(figure)
        self.modules: dict[str, str] = {}
        self.names: set[str] = set()
        self.objects: dict[str, Any] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == RULE_MODULE:
                self.objects.update({(item.asname or item.name): getattr(split_table, item.name) for item in node.names if hasattr(split_table, item.name)})
                self.names.update((item.asname or item.name) for item in node.names if holds(getattr(split_table, item.name, None), figure, self.pattern))
            elif isinstance(node, ast.ImportFrom) and node.module == "spark":
                self.modules.update({(item.asname or item.name): f"spark.{item.name}" for item in node.names if item.name == "split_table"})
            elif isinstance(node, ast.Import):
                self.modules.update({(item.asname or item.name): item.name for item in node.names if item.name == RULE_MODULE})
        assignments = [node for node in ast.walk(tree) if isinstance(node, ast.Assign | ast.AnnAssign | ast.AugAssign) and node.value is not None]
        changed = True
        while changed:
            changed = False
            for node in assignments:
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                names = {name for target in targets for part in ast.walk(target) if (name := dotted(part)) is not None}
                if node.value is not None and (self.carries(node.value) or self.derived(node.value)) and not names <= self.names:
                    self.names |= names
                    changed = True
                if node.value is not None and len(names) == 1 and (found := self.known(node.value)) is not MISSING and not names <= self.objects.keys():
                    self.objects[next(iter(names))] = found
                    changed = True

    def derived(self, node: ast.AST) -> bool:
        """Whether an expression is made from the split rule's own assignment and is not a Boolean or a length: a count or a list of sets."""
        if isinstance(node, ast.Compare | ast.BoolOp | ast.UnaryOp):
            return False
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in FLAG_CALLS:
            return False
        return any(isinstance(part, ast.Name) and part.id in RULE_NAMES for part in ast.walk(node))

    def known(self, node: ast.AST) -> Any:
        """The object of the rule module that a name or an attribute is, or ``MISSING`` for anything else."""
        name = dotted(node)
        if name is None:
            return MISSING
        if name in self.objects:
            return self.objects[name]
        head, _, attribute = name.rpartition(".")
        module = self.modules.get(head) or (head if head in self.modules.values() else None)
        return getattr(split_table, attribute, MISSING) if module == RULE_MODULE else MISSING

    def carries(self, node: ast.AST) -> bool:
        """Whether the value of an expression is, or holds, the pinned counts, without a comparison or any other computation between."""
        if isinstance(node, ast.Constant):
            return holds(node.value, self.figure, self.pattern)
        if isinstance(node, ast.Name):
            return node.id in self.names
        if isinstance(node, ast.Attribute):
            name = dotted(node)
            head, _, attribute = (name or "").rpartition(".")
            if name in self.names:
                return True
            module = self.modules.get(head) or (head if head in self.modules.values() else None)
            if module == RULE_MODULE and holds(getattr(split_table, attribute, None), self.figure, self.pattern):
                return True
            return self.carries(node.value)
        if isinstance(node, ast.Dict):
            return any(self.carries(item) for item in node.values)
        if isinstance(node, ast.List | ast.Tuple | ast.Set):
            return any(self.carries(item) for item in node.elts)
        if (
            isinstance(node, ast.Subscript)
            and isinstance(node.slice, ast.Constant)
            and isinstance(known := self.known(node.value), dict)
            and node.slice.value in known
        ):
            return holds(known[node.slice.value], self.figure, self.pattern)
        if isinstance(node, ast.Subscript | ast.Starred):
            return self.carries(node.value)
        if isinstance(node, ast.JoinedStr):
            return any(self.carries(item) for item in node.values)
        if isinstance(node, ast.FormattedValue):
            return self.carries(node.value)
        if isinstance(node, ast.BinOp):
            while isinstance(node, ast.BinOp):
                if self.carries(node.right):
                    return True
                node = node.left
            return self.carries(node)
        if isinstance(node, ast.Call):
            wrapper = (isinstance(node.func, ast.Name) and node.func.id in WRAPPERS) or (isinstance(node.func, ast.Attribute) and node.func.attr in METHODS)
            return wrapper and (
                any(self.carries(item) for item in [*node.args, *(keyword.value for keyword in node.keywords)]) or self.carries_callee(node.func)
            )
        return False

    def carries_callee(self, func: ast.AST) -> bool:
        return isinstance(func, ast.Attribute) and self.carries(func.value)


def expectation_arguments(call: ast.Call) -> list[ast.expr]:
    """The expectation and the observation of an ``expect`` call, given by position or by keyword."""
    func = call.func
    if not (isinstance(func, ast.Name) and func.id == "expect") and not (isinstance(func, ast.Attribute) and func.attr == "expect"):
        return []
    given = [item for position, item in enumerate(call.args) if position in EXPECTATION_POSITIONS]
    return given + [keyword.value for keyword in call.keywords if keyword.arg in EXPECTATION_POSITIONS.values()]


def figure_findings(source: str, figure: int = FIGURE) -> list[tuple[str, int]]:
    """The rule and the 1-based line of each place where a runner's source holds the figure or records it, in line order. An expression of constants counts as
    the constant that it gives (``fold``): the sum of two numbers, a concatenation of texts, escaped bytes and ``bytes.decode`` hold the figure as the text
    or the number does."""
    tree = ast.parse(source)
    pattern = figure_pattern(figure)
    found = [("figure_text", number) for number, line in enumerate(source.splitlines(), start=1) if pattern.search(line)]
    carriers = Carriers(tree, figure)
    folded = fold_all(tree)
    for node in ast.walk(tree):
        if isinstance(node, ast.expr):
            value = folded.get(id(node), MISSING)
            if (isinstance(value, str) and pattern.search(value)) or (isinstance(value, bytes) and pattern.search(value.decode("latin-1"))):
                found.append(("figure_text", node.lineno))
            elif is_number(value) and value == figure:
                found.append(("figure_number", node.lineno))
        if isinstance(node, ast.Call):
            found += [("figure_in_expect", item.lineno) for item in expectation_arguments(node) if carriers.carries(item)]
    return sorted(set(found), key=lambda item: (item[1], item[0]))


class ScanFailed(Exception):
    """A scan that did not finish. Its text names a file label, a class and a line, and never a cause: the message of a parse error or of a failed scan can hold
    the line of a runner, and so the figure."""


MAX_NUMBER = 10**15
MAX_LENGTH = 10_000
MAX_EXPONENT = 64


def is_number(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def bounded(value: Any) -> Any:
    """The value when it is a number of at most ``MAX_NUMBER`` or a text or bytes of at most ``MAX_LENGTH``, and ``MISSING`` otherwise."""
    if is_number(value):
        return value if abs(value) <= MAX_NUMBER else MISSING
    if isinstance(value, str | bytes):
        return value if len(value) <= MAX_LENGTH else MISSING
    return MISSING


def combine(operator: ast.operator, left: Any, right: Any) -> Any:
    """The sum, difference, product or bounded power of two numbers, the concatenation of two texts or of two byte strings, or a text or bytes repeated."""
    numbers = is_number(left) and is_number(right)
    if isinstance(operator, ast.Add) and (
        numbers or (isinstance(left, str) and isinstance(right, str)) or (isinstance(left, bytes) and isinstance(right, bytes))
    ):
        return bounded(left + right)
    if isinstance(operator, ast.Sub) and numbers:
        return bounded(left - right)
    if isinstance(operator, ast.Mult):
        if numbers:
            return bounded(left * right)
        for text, times in ((left, right), (right, left)):
            if (
                isinstance(text, str | bytes)
                and isinstance(times, int)
                and not isinstance(times, bool)
                and 0 <= times <= MAX_LENGTH
                and len(text) * times <= MAX_LENGTH
            ):
                return text * times
    if isinstance(operator, ast.Pow) and isinstance(left, int) and isinstance(right, int) and not isinstance(left, bool) and 0 <= right <= MAX_EXPONENT:
        return bounded(left**right) if abs(left) <= MAX_NUMBER else MISSING
    return MISSING


def fold_node(node: ast.AST, known: dict[int, Any]) -> Any:
    """The constant that one expression gives when each expression below it is already in ``known`` (by ``id``), or ``MISSING``."""

    def below(item: ast.AST) -> Any:
        return known.get(id(item), MISSING)

    if isinstance(node, ast.Constant):
        return bounded(node.value)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub | ast.UAdd):
        operand = below(node.operand)
        return bounded(-operand if isinstance(node.op, ast.USub) else operand) if is_number(operand) else MISSING
    if isinstance(node, ast.BinOp):
        left, right = below(node.left), below(node.right)
        return MISSING if left is MISSING or right is MISSING else combine(node.op, left, right)
    if isinstance(node, ast.JoinedStr):
        parts = [field_text(item, known) for item in node.values]
        return MISSING if any(part is MISSING for part in parts) else bounded("".join(parts))
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "decode" and not node.keywords and len(node.args) <= 1:
        raw = below(node.func.value)
        encoding = below(node.args[0]) if node.args else "utf-8"
        if isinstance(raw, bytes) and isinstance(encoding, str):
            try:
                return bounded(raw.decode(encoding))
            except UnicodeError, LookupError:
                return MISSING
    return MISSING


def field_text(node: ast.AST, known: dict[int, Any]) -> Any:
    """The text of one part of an f-string: a constant text, or a field with no conversion and no format that holds a text or a whole number."""
    if isinstance(node, ast.FormattedValue):
        if node.conversion != -1 or node.format_spec is not None:
            return MISSING
        value = known.get(id(node.value), MISSING)
        return str(value) if isinstance(value, str | int) and not isinstance(value, bool) else MISSING
    value = known.get(id(node), MISSING)
    return value if isinstance(value, str) else MISSING


def fold_all(root: ast.AST) -> dict[int, Any]:
    """The constant that each expression under ``root`` gives (by ``id``), evaluated from the leaves up with no recursion, so a chain of any length is as
    deep as a short one. A sub-expression that gives the figure counts, as the whole of an expression does.

    The evaluation is bounded in its values: a number of at most ``MAX_NUMBER``, a text of at most ``MAX_LENGTH`` characters and a power with an exponent of
    at most ``MAX_EXPONENT``. It knows constants, the sign of a number, sums, differences, products and powers of numbers, the concatenation and the repeat
    of texts and of byte strings (adjacent literals and escapes are already one constant), an f-string whose fields are constants, and ``bytes.decode``. A
    name, a call of anything else or a result out of bounds is ``MISSING``. A text built any other way (a ``join``, a format call) is a limit of this scan,
    which the ``sink_leaks`` check of the sealed runners backs."""
    known: dict[int, Any] = {}
    for node in reversed(list(ast.walk(root))):
        if isinstance(node, ast.expr):
            known[id(node)] = fold_node(node, known)
    return known


def fold(node: ast.AST) -> Any:
    """The constant that one expression of constants gives, or ``MISSING`` (see ``fold_all``)."""
    return fold_all(node).get(id(node), MISSING)


def scanned[T](label: str, scan: Callable[[str], T], source: str) -> T:
    """The result of a scan of one source. A scan that fails is a ``ScanFailed`` that names the file label and the line of a syntax error or the class of any
    other error, and never the message, the source or the context of the error."""
    try:
        return scan(source)
    except SyntaxError as error:
        raise ScanFailed(f"{label}: the source does not parse (line {error.lineno})") from None
    except Exception as error:
        raise ScanFailed(f"{label}: the scan failed with {type(error).__name__}") from None


def findings(source: str, label: str) -> list[tuple[str, int]]:
    """``figure_findings`` of one source, failing with a ``ScanFailed`` that names the label of a plant and never its source."""
    return scanned(label, figure_findings, source)


def label_of(path: Path, base: Path) -> str:
    try:
        return str(path.relative_to(base))
    except ValueError:
        return path.name


def all_sources() -> list[Path]:
    return sorted(E2E.rglob("*.py")) if E2E.is_dir() else []


def split_sources(paths: list[Path], tracked: set[str]) -> tuple[list[Path], list[Path]]:
    """The paths that Git tracks (by their path from the repository root) and the others, which are the ignored runners, each in the order given."""
    held = [path for path in paths if label_of(path, ROOT) in tracked]
    return held, [path for path in paths if label_of(path, ROOT) not in tracked]


def git_tracked() -> set[str]:
    """The paths of ``tests/e2e`` that Git tracks. A failure to ask is a ``ScanFailed``, so a checkout that cannot say never passes as having no runner."""
    try:
        listed = run_command("git", ["ls-files", "-z", "--", "tests/e2e"], cwd=ROOT, timeout=GIT_SECONDS, check=True).stdout
    except Exception as error:
        raise ScanFailed(f"git could not list the tracked sources of tests/e2e ({type(error).__name__})") from None
    return {name for name in listed.split("\0") if name}


def tracked_sources() -> list[Path]:
    return split_sources(all_sources(), git_tracked())[0]


def ignored_sources() -> list[Path]:
    return split_sources(all_sources(), git_tracked())[1]


def check_runners(test: unittest.TestCase, paths: list[Path], base: Path) -> None:
    """Scan each source in a sub-test of ``test``. A failure names the file, the rule and the line, or the class of the error: never the figure or a source."""
    for path in paths:
        label = label_of(path, base)
        with test.subTest(runner=label):
            try:
                source = path.read_text()
            except (OSError, UnicodeError) as error:
                raise ScanFailed(f"{label}: the file cannot be read as text ({type(error).__name__})") from None
            found = scanned(label, figure_findings, source)
            test.assertEqual([], found, f"{label}: the rule and the line of each place that holds or records the figure")


class RunnersHoldNoTestSetCount(unittest.TestCase):
    def test_the_figure_is_the_rule_modules_test_count(self) -> None:
        self.assertTrue(isinstance(FIGURE, int) and FIGURE > 99, "the test count of the rule module is not a whole number above 99")
        self.assertEqual({"training", "validation", "test"}, set(split_table.PINNED_COUNTS))

    def test_no_tracked_source_of_the_runner_folder_holds_the_figure(self) -> None:
        paths = tracked_sources()
        self.assertTrue(paths, "no source of tests/e2e is tracked by Git")
        check_runners(self, paths, ROOT)

    def test_no_ignored_runner_holds_the_figure(self) -> None:
        paths = ignored_sources()
        if not paths:
            self.skipTest("no ignored runner is in this checkout: only the tracked sources of tests/e2e were scanned")
        check_runners(self, paths, ROOT)

    def test_the_fixed_sample_runner_records_through_the_sealed_observation_check(self) -> None:
        path = E2E / "fixed_sample.py"
        if not path.is_file():
            self.skipTest("the ignored runner fixed_sample.py is not in this checkout")
        found = scanned("fixed_sample.py", sink_leaks, path.read_text())
        self.assertEqual(
            [], found, "fixed_sample.py: the rule and the line of each observation, fact, log line, command or print that is not the output of sealed_observed"
        )


class ScannerFindsEveryWay(unittest.TestCase):
    """Each plant is a runner source that holds or records the figure in one way. The scanner must name it, and must stay quiet on the controls."""

    PLANTS = (
        ("figure_text", "a docstring with a thousands separator", f'"""The test set holds {FIGURE:,} patients."""\n'),
        ("figure_text", "a docstring without a separator", f'"""The test set holds {FIGURE} patients."""\n'),
        ("figure_text", "a comment", f"x = 1  # the test set has {FIGURE:,}\n"),
        ("figure_text", "an underscore separator in a string", f'text = "{FIGURE:,}".replace(",", "_")\nother = "{str(FIGURE)[:1]}_{str(FIGURE)[1:]}"\n'),
        ("figure_text", "a split over two literals", f'text = "{str(FIGURE)[:1]}" "{str(FIGURE)[1:]}"\n'),
        ("figure_text", "an expectation text", f'record.expect("c", "k", "the test count is {FIGURE:,}", sealed_observed({{"a": True}}), ok is True)\n'),
        ("figure_number", "a constant", f'COUNTS = {{"test": {FIGURE}}}\n'),
        ("figure_number", "a grouped number", f"COUNT = {FIGURE:,}".replace(",", "_") + "\n"),
        ("figure_number", "a float", f"COUNT = {FIGURE}.0\n"),
        (
            "figure_in_expect",
            "the imported counts in an f-string expectation",
            "from spark.split_table import PINNED_COUNTS\n" + 'record.expect("c", "k", f"counts {PINNED_COUNTS}", x, ok)\n',
        ),
        (
            "figure_in_expect",
            "the test count by key",
            "from spark.split_table import PINNED_COUNTS\n" + 'record.expect("c", "k", f"{PINNED_COUNTS[\'test\']}", x, ok)\n',
        ),
        ("figure_in_expect", "a renamed import", "from spark.split_table import PINNED_COUNTS as frozen\n" + 'record.expect("c", "k", f"{frozen}", x, ok)\n'),
        (
            "figure_in_expect",
            "an alias",
            "from spark.split_table import PINNED_COUNTS\nwanted = PINNED_COUNTS\n" + 'record.expect("c", "k", f"{wanted}", x, ok)\n',
        ),
        (
            "figure_in_expect",
            "an attribute alias",
            "from spark.split_table import PINNED_COUNTS\n" + 'self.wanted = PINNED_COUNTS\nself.record.expect("c", "k", f"{self.wanted}", x, ok)\n',
        ),
        ("figure_in_expect", "a text built by str", "from spark.split_table import PINNED_COUNTS\n" + 'record.expect("c", "k", str(PINNED_COUNTS), x, ok)\n'),
        (
            "figure_in_expect",
            "the observation",
            "from spark.split_table import PINNED_COUNTS\n" + 'record.expect("c", "k", "e", f"seen {PINNED_COUNTS}", ok)\n',
        ),
        (
            "figure_in_expect",
            "the observation by keyword",
            "from spark.split_table import PINNED_COUNTS\n" + 'record.expect("c", "k", "e", observed=f"{PINNED_COUNTS}", passed=ok)\n',
        ),
        (
            "figure_in_expect",
            "the expectation by keyword",
            "from spark.split_table import PINNED_COUNTS\n" + 'record.expect("c", "k", expected=f"{PINNED_COUNTS}", observed="x", passed=ok)\n',
        ),
        ("figure_in_expect", "a module alias", "from spark import split_table as rule\n" + 'record.expect("c", "k", f"{rule.PINNED_COUNTS}", x, ok)\n'),
        ("figure_in_expect", "the module by its path", "import spark.split_table\n" + 'record.expect("c", "k", f"{spark.split_table.PINNED_COUNTS}", x, ok)\n'),
        ("figure_in_expect", "a bare expect function", "from spark.split_table import PINNED_COUNTS\n" + 'expect("c", "k", f"{PINNED_COUNTS}", x, ok)\n'),
        (
            "figure_in_expect",
            "a chain of two aliases",
            "from spark.split_table import PINNED_COUNTS\nfirst = PINNED_COUNTS\nsecond = first\n" + 'record.expect("c", "k", f"{second}", x, ok)\n',
        ),
    )
    HALVES = (FIGURE // 2, FIGURE - FIGURE // 2)
    DIVISOR = next((d for d in range(2, FIGURE) if FIGURE % d == 0), 1)
    DIGITS = str(FIGURE)
    ESCAPED = "".join(f"\\x{ord(digit):02x}" for digit in DIGITS)
    FOLDED_PLANTS = (
        ("figure_number", "a sum of two constants", f"COUNT = {HALVES[0]} + {HALVES[1]}\n"),
        ("figure_number", "a sum of three constants", f"COUNT = {HALVES[0]} + {HALVES[1] - 1} + 1\n"),
        ("figure_number", "a difference", f"COUNT = {FIGURE + 7} - 7\n"),
        ("figure_number", "a product", f"COUNT = {DIVISOR} * {FIGURE // DIVISOR}\n"),
        ("figure_number", "a sum of floats", f"COUNT = {FIGURE - 1}.5 + 0.5\n"),
        ("figure_number", "a power and a sum", f"COUNT = 10 ** 3 + {FIGURE - 1000}\n"),
        ("figure_number", "a negation", f"COUNT = 7 - -{FIGURE - 7}\n"),
        ("figure_number", "a sum inside a call", f"record.note(max(0, {HALVES[0]} + {HALVES[1]}))\n"),
        ("figure_text", "a concatenation of two texts", f'text = "{DIGITS[:2]}" + "{DIGITS[2:]}"\n'),
        ("figure_text", "a concatenation of three texts", f'text = "{DIGITS[:1]}" + "{DIGITS[1:3]}" + "{DIGITS[3:]}"\n'),
        ("figure_text", "a concatenation with a separator", f'text = "{FIGURE // 1000}" + "," + "{FIGURE % 1000:03d}"\n'),
        ("figure_text", "a repeated text", f'text = "{DIGITS[:2]}" * 1 + "{DIGITS[2:]}"\n'),
        ("figure_text", "an f-string of constants", f'text = f"{DIGITS[:2]}{{""}}{DIGITS[2:]}"\n'),
        ("figure_text", "adjacent bytes", f'data = b"{DIGITS[:2]}" b"{DIGITS[2:]}"\n'),
        ("figure_text", "escaped bytes", f'data = b"{ESCAPED}"\n'),
        ("figure_text", "decoded bytes", f'text = b"{DIGITS}".decode()\n'),
        ("figure_text", "decoded escaped bytes", f'text = b"{ESCAPED}".decode("ascii")\n'),
        ("figure_text", "decoded joined bytes", f'text = (b"{DIGITS[:2]}" + b"{DIGITS[2:]}").decode("utf-8")\n'),
        ("figure_text", "an escaped text", f'text = "{ESCAPED}"\n'),
    )
    DERIVED_PLANTS = (
        (
            "a count of the patients of each set in an observation",
            "totals = Counter(patient_part(p) for p in patients)\n" + 'record.expect("c", "k", "e", f"seen {totals}", ok)\n',
        ),
        ("a count of each set by name in an expectation", "totals = {SET_NAMES[a]: b for a, b in found}\n" + 'record.expect("c", "k", f"{totals}", x, ok)\n'),
        ("the same through an alias", "totals = Counter(patient_part(p) for p in patients)\nseen = totals\n" + 'record.expect("c", "k", "e", f"{seen}", ok)\n'),
        ("the same through str", "totals = Counter(patient_part(p) for p in patients)\n" + 'record.expect("c", "k", "e", str(totals), ok)\n'),
    )
    CONTROLS = (
        (
            "the comparison the runners make",
            "from spark.split_table import PINNED_COUNTS\nmatched = counts == PINNED_COUNTS\n"
            + 'record.expect("c", "k", "the pinned counts", sealed_observed({"a": matched}), matched is True)\n',
        ),
        (
            "the comparison inside the verdict",
            "from spark.split_table import PINNED_COUNTS\n"
            + 'record.expect("c", "k", "the pinned counts", sealed_observed({"a": True}), counts == PINNED_COUNTS)\n',
        ),
        (
            "a flag text of a comparison",
            "from spark.split_table import PINNED_COUNTS\n" + 'record.expect("c", "k", "e", f"matched {counts == PINNED_COUNTS}", ok)\n',
        ),
        (
            "the development counts by key",
            "from spark.split_table import PINNED_COUNTS\n"
            + 'record.expect("c", "k", f"{PINNED_COUNTS[\'training\']} and {PINNED_COUNTS[\'validation\']}", x, ok)\n',
        ),
        (
            "the development counts as numbers",
            f'DEVELOPMENT = {{"training": {split_table.PINNED_COUNTS["training"]}, "validation": {split_table.PINNED_COUNTS["validation"]}}}\n',
        ),
        ("the import with no use in a record", "from spark.split_table import PINNED_COUNTS\nok = counts == PINNED_COUNTS\n"),
        (
            "numbers that only look like it",
            f'x = "{FIGURE}0 and 0{FIGURE} and {FIGURE}.5 and 0.{FIGURE} and a{FIGURE}b and 1{FIGURE:,} and {FIGURE + 1}"\ny = {FIGURE + 1}\n',
        ),
        ("a hash that holds the digits", f'h = "9f{FIGURE}ac3e"\n'),
        ("a flag made from the split rule", "ok = all(SET_NAMES[patient_part(n)] != 'test' for n in ns)\n" + 'record.expect("c", "k", "e", f"{ok}", ok)\n'),
        (
            "a length made from the split rule",
            "n = len([p for p in ps if patient_part(p) == 'test'])\nk = len(ps)\n" + 'record.expect("c", "k", "e", f"{k}", ok)\n',
        ),
        (
            "the comparison of a count of the sets",
            "totals = Counter(patient_part(p) for p in patients)\nsame = totals == other\n" + 'record.expect("c", "k", "e", f"matched {same}", ok)\n',
        ),
        ("the published totals", 'text = "6,388 cases of 6,090 patients"\nTOTAL = 6090\n'),
        ("a sum of the development counts", f"DEVELOPMENT = {split_table.PINNED_COUNTS['training']} + {split_table.PINNED_COUNTS['validation']}\n"),
        ("a sum of the published totals", "TOTAL = 6000 + 90\n"),
        ("a product that is another number", "AREA = 3 * 7\nTEXT = 'a' + 'b'\n"),
        ("bytes that decode to another text", 'text = b"abc".decode()\ndata = b"12" b"34"\n'),
        ("a decode that fails", 'text = b"\\xff".decode("ascii")\n'),
        ("a text that is only a long repeat", 'text = "a" * 100000000\nnumber = 10 ** 100\n'),
        ("the difference that is not the figure", f"COUNT = {FIGURE + 1} - 2\n"),
    )

    def test_each_plant_is_named_by_its_rule(self) -> None:
        for rule, label, source in self.PLANTS + self.FOLDED_PLANTS:
            with self.subTest(plant=label):
                self.assertIn(rule, {name for name, _ in findings(source, label)}, f"the plant {label} is not named by {rule}")

    def test_a_count_made_from_the_split_rule_is_named_where_it_is_recorded(self) -> None:
        for label, source in self.DERIVED_PLANTS:
            with self.subTest(plant=label):
                self.assertIn("figure_in_expect", {name for name, _ in findings(source, label)}, f"the plant {label} is not named")

    def test_each_control_is_quiet(self) -> None:
        for label, source in self.CONTROLS:
            with self.subTest(control=label):
                self.assertEqual([], findings(source, label), f"the control {label} is named")

    def test_a_finding_names_the_line_and_never_the_figure(self) -> None:
        found = findings(f'x = 1\n"""holds {FIGURE:,}"""\n', "a docstring")
        self.assertEqual([("figure_text", 2)], found)
        self.assertFalse(str(FIGURE) in repr(found), "a finding holds the figure")

    def test_a_long_chain_of_constants_is_folded_whatever_its_length(self) -> None:
        for rule, label, source in (
            ("figure_number", "a chain of ones as long as the figure", "COUNT = " + " + ".join(["1"] * FIGURE) + "\n"),
            ("figure_text", "a chain of single digits", "TEXT = " + " + ".join(f'"{digit}"' for digit in str(FIGURE)) + "\n"),
            ("figure_text", "a long chain that ends in the figure", 'TEXT = "" + ' + " + ".join(['""'] * 3000) + f' + "{FIGURE}"\n'),
        ):
            with self.subTest(plant=label):
                self.assertIn(rule, {name for name, _ in findings(source, label)}, f"the plant {label} is not named by {rule}")

    def test_a_very_deep_expression_is_judged_without_a_crash(self) -> None:
        for label, source in (
            ("a deep sum of even numbers", "x = " + " + ".join(["2"] * 3000) + "\n"),
            ("a deep concatenation", "x = " + " + ".join(['"a"'] * 3000) + "\n"),
        ):
            with self.subTest(source=label):
                self.assertEqual([], findings(source, label))


class FoldedConstants(unittest.TestCase):
    """The bounded evaluation of an expression of constants, on its own."""

    def folded(self, text: str) -> Any:
        return fold(ast.parse(text, mode="eval").body)

    def test_sums_products_texts_and_bytes_are_evaluated(self) -> None:
        for text, value in (
            ("2 + 3", 5),
            ("2 * 3 + 1", 7),
            ("7 - 2 - 1", 4),
            ("2 ** 10", 1024),
            ("-3 + 5", 2),
            ('"a" + "b"', "ab"),
            ('"ab" * 2', "abab"),
            ('b"a" + b"b"', b"ab"),
            ('b"\\x31\\x32"', b"12"),
            ('b"12".decode()', "12"),
            ('(b"1" + b"2").decode("ascii")', "12"),
            ('f"1{2}"', "12"),
        ):
            with self.subTest(expression=text):
                self.assertEqual(value, self.folded(text))

    def test_anything_else_and_anything_too_large_is_not_evaluated(self) -> None:
        for text in (
            "x + 1",
            "len('a')",
            "True + 1",
            "'a' + 1",
            "b'a' + 'a'",
            "10 ** 400",
            "2 ** 100000",
            "'a' * 10000000",
            "b'\\xff'.decode('ascii')",
            "None",
            "[1] + [2]",
        ):
            with self.subTest(expression=text):
                self.assertIs(MISSING, self.folded(text))


class FailureOutputIsSafe(unittest.TestCase):
    """The output of a failing scan names a file label, a rule and a line, and never the figure, a line of a source or the message of an error (the reviewer's
    probes: a malformed runner, a runner that holds the figure, a scanner that fails with the source in its message and a file that is no text)."""

    LINE = "the_line_that_must_not_be_printed = ("

    def failing_output(self, files: dict[str, bytes], *, scanner: Any = None) -> tuple[str, bool]:
        folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="heldout_scan_")))
        for name, data in files.items():
            (folder / name).write_bytes(data)
        paths = sorted(folder.rglob("*.py"))

        class Probe(unittest.TestCase):
            def runTest(probe) -> None:  # noqa: N802
                check_runners(probe, paths, folder)

        stream = io.StringIO()
        with mock.patch(f"{__name__}.figure_findings", scanner) if scanner else contextlib.nullcontext():
            result = unittest.TextTestRunner(stream=stream, verbosity=0).run(Probe())
        return stream.getvalue(), result.wasSuccessful()

    def assert_safe(self, output: str, *, also: tuple[str, ...] = ()) -> None:
        self.assertFalse(str(FIGURE) in output, "the failure output holds the figure")
        self.assertFalse(f"{FIGURE:,}" in output, "the failure output holds the figure with a separator")
        self.assertFalse(self.LINE in output, "the failure output holds a line of the runner")
        for text in also:
            self.assertFalse(text in output, "the failure output holds text of the source")

    def test_a_malformed_runner_is_named_by_its_file_and_line_and_nothing_else(self) -> None:
        source = f"x = 1\n{self.LINE} {FIGURE}\n"
        output, passed = self.failing_output({"broken.py": source.encode()})
        self.assertFalse(passed)
        self.assert_safe(output)
        self.assertIn("broken.py", output)
        self.assertIn("line 2", output)

    def test_a_runner_that_holds_the_figure_is_named_by_its_rule_and_line(self) -> None:
        output, passed = self.failing_output({"holds.py": f'"""The test set holds {FIGURE:,} patients."""\n'.encode()})
        self.assertFalse(passed)
        self.assert_safe(output)
        self.assertIn("figure_text", output)
        self.assertIn("holds.py", output)

    def test_a_scanner_that_fails_with_the_source_in_its_message_is_named_by_its_class_only(self) -> None:
        def scanner(source: str, figure: int = FIGURE) -> list[tuple[str, int]]:
            raise RuntimeError(f"cannot scan {source!r} for {figure}")

        output, passed = self.failing_output({"fine.py": f"x = '{FIGURE}'\n".encode()}, scanner=scanner)
        self.assertFalse(passed)
        self.assert_safe(output, also=("cannot scan", "x = "))
        self.assertIn("RuntimeError", output)
        self.assertIn("fine.py", output)

    def test_a_file_that_is_no_text_is_named_by_its_class_only(self) -> None:
        output, passed = self.failing_output({"binary.py": b"\xff\xfe" + str(FIGURE).encode()})
        self.assertFalse(passed)
        self.assert_safe(output, also=("codec", "position", "invalid start byte"))
        self.assertIn("UnicodeDecodeError", output)
        self.assertIn("binary.py", output)

    def test_a_source_with_a_null_byte_or_a_deep_nesting_is_named_by_its_class_only(self) -> None:
        output, passed = self.failing_output({"null.py": f"x = {FIGURE}\x00\n".encode(), "deep.py": ("x = " + "(" * 5000 + ")" * 5000 + "\n").encode()})
        self.assertFalse(passed)
        self.assert_safe(output)

    def test_the_scanner_that_fails_gives_a_scan_failure_that_names_no_cause(self) -> None:
        with mock.patch(f"{__name__}.figure_findings", side_effect=RuntimeError(f"the figure {FIGURE}")), self.assertRaises(ScanFailed) as caught:
            findings("x = 1\n", "a plant")
        text = "".join(traceback.format_exception(caught.exception))
        self.assert_safe(text)
        self.assertIn("a plant", text)


class SealedObservationCheck(unittest.TestCase):
    """The check that the fixed sample runner records through ``sealed_observed`` is the one of the sealed runners (``sink_leaks``), and it bites."""

    LEAKS = (
        ("observation_not_sealed", 'record.expect("c", "s", "e", f"{missing}", ok is True)\n'),
        ("observation_not_sealed", 'record.expect("c", "s", "e", str(missing), ok is True)\n'),
        ("expected_not_fixed", 'record.expect("c", "s", f"names case {victim.case}", sealed_observed({"a": True}), ok is True)\n'),
        ("log_not_sealed", 'log.append(f"== {label}: exit {code}\\n{output}")\n'),
        ("command_not_sealed", 'record.commands.append(f".venv/bin/python {arguments}")\n'),
        ("fact_not_sealed", 'record.environment = {"python": platform.python_version()}\n'),
        ("passed_not_boolean", 'record.expect("c", "s", "e", sealed_observed({"a": True}), ok)\n'),
        ("cleanup_not_fixed", 'record.cleanup = f"removed: {gone}"\n'),
    )
    SEALED = (
        'record.expect("c", "s", "e", sealed_observed({"a": rebuilt == tracked}, exit_code=code), code == 0 and rebuilt == tracked)\n'
        'log.append(sealed_observed(words=["candidates", *arguments[:1]], exit_code=code))\n'
        'record.commands.append(sealed_observed(words=["python", *MODULE, *arguments]))\n'
        'record.cleanup = "the folder is removed" if gone else "the folder is not removed"\n'
    )

    def test_each_leak_that_the_old_runner_had_is_named_by_the_sealed_check(self) -> None:
        for rule, source in self.LEAKS:
            with self.subTest(leak=rule):
                self.assertIn(rule, {name for name, _ in scanned("a plant", sink_leaks, source)}, f"the leak {rule} is not named")

    def test_a_record_made_only_through_the_sealed_helper_is_quiet(self) -> None:
        self.assertEqual([], scanned("a control", sink_leaks, self.SEALED))


class SourceSplit(unittest.TestCase):
    """The runner folder holds tracked sources and ignored runners. The scan of the tracked ones never waits for the ignored ones, and an absent set of ignored
    runners is a visible skip."""

    def run_tests(self, names: list[str], *, tracked: list[Path], ignored: list[Path]) -> unittest.TestResult:
        suite = unittest.TestSuite(RunnersHoldNoTestSetCount(name) for name in names)
        with (
            mock.patch(f"{__name__}.tracked_sources", return_value=tracked),
            mock.patch(f"{__name__}.ignored_sources", return_value=ignored),
        ):
            return unittest.TextTestRunner(stream=io.StringIO(), verbosity=0).run(suite)

    def test_the_paths_are_split_by_what_git_tracks(self) -> None:
        paths = [E2E / "run_record.py", E2E / "fixed_sample.py", E2E / "stream_stack" / "read_lake.py"]
        tracked, ignored = split_sources(paths, {"tests/e2e/run_record.py"})
        self.assertEqual([E2E / "run_record.py"], tracked)
        self.assertEqual([E2E / "fixed_sample.py", E2E / "stream_stack" / "read_lake.py"], ignored)

    def test_the_tracked_sources_of_this_checkout_are_found_and_are_not_ignored_runners(self) -> None:
        tracked, ignored = tracked_sources(), ignored_sources()
        self.assertGreater(len(tracked), 0)
        self.assertEqual(set(), set(tracked) & set(ignored))
        self.assertIn(E2E / "run_record.py", tracked)

    def test_no_ignored_runner_is_a_visible_skip_and_the_tracked_part_still_runs(self) -> None:
        clean = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="heldout_tracked_"))) / "tracked.py"
        clean.write_text("x = 1\n")
        result = self.run_tests(
            ["test_no_tracked_source_of_the_runner_folder_holds_the_figure", "test_no_ignored_runner_holds_the_figure"], tracked=[clean], ignored=[]
        )
        self.assertEqual(2, result.testsRun)
        self.assertEqual(1, len(result.skipped))
        self.assertIn("no ignored runner", result.skipped[0][1])
        self.assertTrue(result.wasSuccessful())

    def test_a_tracked_source_that_holds_the_figure_fails_when_no_ignored_runner_is_there(self) -> None:
        holder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="heldout_tracked_"))) / "tracked.py"
        holder.write_text(f'"""holds {FIGURE}"""\n')
        result = self.run_tests(
            ["test_no_tracked_source_of_the_runner_folder_holds_the_figure", "test_no_ignored_runner_holds_the_figure"], tracked=[holder], ignored=[]
        )
        self.assertEqual(1, len(result.failures))
        self.assertEqual(1, len(result.skipped))

    def test_an_ignored_runner_that_holds_the_figure_fails(self) -> None:
        holder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="heldout_ignored_"))) / "runner.py"
        holder.write_text(f'"""holds {FIGURE}"""\n')
        result = self.run_tests(["test_no_ignored_runner_holds_the_figure"], tracked=[], ignored=[holder])
        self.assertEqual(1, len(result.failures))
        self.assertEqual(0, len(result.skipped))


if __name__ == "__main__":
    unittest.main()
