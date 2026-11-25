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
import re
import unittest
from pathlib import Path
from typing import Any

from spark import split_table

ROOT = Path(__file__).resolve().parents[2]
E2E = ROOT / "tests" / "e2e"
FIGURE = split_table.PINNED_COUNTS["test"]
RULE_MODULE = "spark.split_table"
RULE_NAMES = frozenset({"patient_part", "SET_NAMES"})
FLAG_CALLS = frozenset({"all", "any", "bool", "len"})
WRAPPERS = frozenset({"str", "repr", "ascii", "format", "dict", "list", "tuple", "set", "frozenset", "sorted"})
METHODS = frozenset({"format", "join", "items", "values", "copy", "get"})
MISSING = object()
EXPECTATION_POSITIONS = {2: "expected", 3: "observed"}


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
            return self.carries(node.left) or self.carries(node.right)
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
    """The rule and the 1-based line of each place where a runner's source holds the figure or records it, in line order."""
    tree = ast.parse(source)
    pattern = figure_pattern(figure)
    found = [("figure_text", number) for number, line in enumerate(source.splitlines(), start=1) if pattern.search(line)]
    carriers = Carriers(tree, figure)
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and pattern.search(node.value):
            found.append(("figure_text", node.lineno))
        elif isinstance(node, ast.Constant) and isinstance(node.value, int | float) and not isinstance(node.value, bool) and node.value == figure:
            found.append(("figure_number", node.lineno))
        elif isinstance(node, ast.Call):
            found += [("figure_in_expect", item.lineno) for item in expectation_arguments(node) if carriers.carries(item)]
    return sorted(set(found), key=lambda item: (item[1], item[0]))


def runner_sources() -> list[Path]:
    return sorted(E2E.rglob("*.py")) if E2E.is_dir() else []


class RunnersHoldNoTestSetCount(unittest.TestCase):
    def test_the_figure_is_the_rule_modules_test_count(self) -> None:
        self.assertIsInstance(FIGURE, int)
        self.assertGreater(FIGURE, 99)
        self.assertEqual({"training", "validation", "test"}, set(split_table.PINNED_COUNTS))

    @unittest.skipUnless(runner_sources(), "no runner is in this checkout")
    def test_no_runner_source_or_expectation_holds_the_figure(self) -> None:
        for path in runner_sources():
            with self.subTest(runner=str(path.relative_to(ROOT))):
                self.assertEqual([], figure_findings(path.read_text()))


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
    )

    def test_each_plant_is_named_by_its_rule(self) -> None:
        for rule, label, source in self.PLANTS:
            with self.subTest(plant=label):
                self.assertIn(rule, {name for name, _ in figure_findings(source)}, source)

    def test_a_count_made_from_the_split_rule_is_named_where_it_is_recorded(self) -> None:
        for label, source in self.DERIVED_PLANTS:
            with self.subTest(plant=label):
                self.assertIn("figure_in_expect", {name for name, _ in figure_findings(source)}, source)

    def test_each_control_is_quiet(self) -> None:
        for label, source in self.CONTROLS:
            with self.subTest(control=label):
                self.assertEqual([], figure_findings(source), source)

    def test_a_finding_names_the_line_and_never_the_figure(self) -> None:
        found = figure_findings(f'x = 1\n"""holds {FIGURE:,}"""\n')
        self.assertEqual([("figure_text", 2)], found)
        self.assertNotIn(str(FIGURE), repr(found))


if __name__ == "__main__":
    unittest.main()
