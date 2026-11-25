"""The Silver cases runner's independent read compares each table's types with the types of that table.

The runner is untracked, so each test skips when it is not on this machine. Spark is a stand-in: the stand-in program's own ``types_equal`` runs as written,
and the two dumps are read as source.

Failure modes covered here:

- The laboratory dump compared the types of ``silver.lab_results`` with the 75 types of ``silver.cases`` (``spec["types"]``), so a correct table gave
  ``types equal False`` while the runner sent the laboratory types (``lab_types``) that nothing read.
- A runner's list of laboratory types drifted from the job's rule (``LAB_COLUMNS``), so the read would pass a table that the rule refuses, or refuse one that
  it allows.
- A comparison that is not ordered, or that ignores a column, passed a table with its types in another order or with one column fewer.
"""

from __future__ import annotations

import ast
import unittest
from types import SimpleNamespace
from typing import Any

from spark.silver_cases import CASE_COLUMNS, LAB_COLUMNS
from tests.e2e.run_record import Record
from tests.fast.test_runner_gold_sealed import runner


def frame_of(types: list[str]) -> Any:
    """A frame stand-in whose schema holds one field of each given type."""
    fields = [SimpleNamespace(dataType=SimpleNamespace(simpleString=lambda text=text: text)) for text in types]
    return SimpleNamespace(schema=SimpleNamespace(fields=fields))


def function_of(source: str, name: str) -> ast.FunctionDef:
    return next(node for node in ast.walk(ast.parse(source)) if isinstance(node, ast.FunctionDef) and node.name == name)


def types_equal_call(source: str, dump: str) -> ast.Call:
    """The call that gives the ``types_equal`` flag of a dump."""
    flags = next(
        node
        for node in ast.walk(function_of(source, dump))
        if isinstance(node, ast.Dict) and any(ast.unparse(key) == "'types_equal'" for key in node.keys if key)
    )
    value = flags.values[[ast.unparse(key) if key else "" for key in flags.keys].index("'types_equal'")]
    if not isinstance(value, ast.Call):
        raise AssertionError(ast.unparse(value))
    return value


class TypesEqual(unittest.TestCase):
    """The helper of the stand-in program that compares a frame's types with the wanted ones."""

    def helper(self) -> Any:
        source = runner("silver_cases").STAND_IN
        names: dict[str, Any] = {}
        exec(compile(ast.Module(body=[function_of(source, "types_equal")], type_ignores=[]), "<stand-in>", "exec"), names)  # noqa: S102
        return names["types_equal"]

    def test_the_same_types_in_the_same_order_are_equal(self) -> None:
        self.assertIs(True, self.helper()(frame_of(["bigint", "string"]), ["bigint", "string"]))

    def test_another_order_another_type_or_a_missing_column_is_not_equal(self) -> None:
        for label, held in (("order", ["string", "bigint"]), ("type", ["bigint", "int"]), ("fewer", ["bigint"]), ("more", ["bigint", "string", "int"])):
            with self.subTest(label):
                self.assertIs(False, self.helper()(frame_of(held), ["bigint", "string"]))


class DumpsReadTheirOwnTypes(unittest.TestCase):
    """Each dump compares with the key of its own table."""

    def test_the_case_dump_compares_with_the_case_types_and_the_laboratory_dump_with_the_laboratory_types(self) -> None:
        source = runner("silver_cases").STAND_IN
        for dump, key in (("dump_cases", "types"), ("dump_lab", "lab_types")):
            with self.subTest(dump):
                call = types_equal_call(source, dump)
                self.assertEqual("types_equal", ast.unparse(call.func))
                self.assertEqual([f"spec['{key}']"], [ast.unparse(item) for item in call.args[1:]])

    def test_the_spec_gives_the_laboratory_dump_the_laboratory_types_of_the_job_rule_and_the_case_dump_the_case_types(self) -> None:
        module = runner("silver_cases")
        specs: list[dict[str, Any]] = []
        run = module.Run.__new__(module.Run)
        run.record, run.facts = Record(feature="t", slug="t", rerun="none"), {}
        run.known = SimpleNamespace(lab_development_collapsed=1, lab_development_rows=2)
        run.stack = SimpleNamespace(write_spec=specs.append, stand_in=lambda mode, branch, **keywords: (0, {}, "", 3.0))
        run.independent_read({"branch": "b"})
        (spec,) = specs
        self.assertEqual([column.type for column in LAB_COLUMNS], spec["lab_types"])
        self.assertEqual([column.type for column in CASE_COLUMNS], spec["types"])
        self.assertNotEqual(spec["types"], spec["lab_types"])


if __name__ == "__main__":
    unittest.main()
