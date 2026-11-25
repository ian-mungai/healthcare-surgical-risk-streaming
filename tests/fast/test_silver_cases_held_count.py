"""The Silver cases runner never holds the number of test-set patients, in its source, its record or its expectation text.

The runner is untracked, so each test skips when it is not on this machine. Spark is a stand-in (``tests/fast/fake_spark.py``): the stand-in program's own
``expected_facts`` runs as written.

Failure modes covered here:

- The split control wrote the frozen count of each set, the test set's too, into its expectation text, and the runner source that the record attaches repeated
  the number (``FROZEN_COUNTS``). Now the count comes from the split rule (``spark.split_table.PINNED_COUNTS``) in the container, is compared there and
  leaves only as one Boolean flag.
- The comparison used a count that the host sent in the spec, so the number was in the runner and in the host's memory. The spec holds no count of a set.
- A rule count that differs from the table by one patient still gave the flag true.
"""

from __future__ import annotations

import ast
import dataclasses
import unittest
from typing import Any

from scripts.sample.split import patient_part
from spark.silver_cases import cutoff_rows, measure_rows
from spark.split_table import PINNED_COUNTS
from tests.e2e.run_record import ROOT
from tests.fast.fake_spark import Frame, Session
from tests.fast.test_runner_flag_compare import stand_in_namespace
from tests.fast.test_runner_gold_sealed import runner

SMALL = {"training": 6, "validation": 2, "test": 3}
SETS = {"training": "train", "validation": "validation", "test": "test"}


def patients_of(counts: dict[str, int]) -> list[str]:
    """Patient numbers in the published form that the split rule puts in each set, as many of each set as ``counts`` says."""
    found: dict[str, list[str]] = {name: [] for name in counts}
    number = 0
    while any(len(found[name]) < counts[name] for name in counts):
        number += 1
        name = next(key for key, part in SETS.items() if part == patient_part(number))
        if len(found[name]) < counts[name]:
            found[name].append(str(number))
    return [patient for name in counts for patient in found[name]]


def facts_of(counts_in_table: dict[str, int], spec: dict[str, Any]) -> dict[str, Any]:
    """The flags of the stand-in's ``expected_facts`` on a table whose sets hold ``counts_in_table`` patients, with the split rule pinned to ``SMALL``."""
    module = runner("silver_cases")
    patients = patients_of(counts_in_table)
    clinical = [
        {"caseid": str(100 + index), "subjectid": patient, "opstart": "10", "opend": str(10 + 600 * (index + 1) ** 2)} for index, patient in enumerate(patients)
    ]
    lab = [{"caseid": row["caseid"], "dt": "1", "name": "x", "result": "1"} for row in clinical]
    session = Session(tables={"lake.bronze.clinical_cases": Frame(clinical), "lake.bronze.lab_results": Frame(lab)})
    names = stand_in_namespace(
        module.STAND_IN,
        ("expected_facts", "bronze_lab"),
        patient_part=patient_part,
        asdict=dataclasses.asdict,
        measure_rows=measure_rows,
        cutoff_rows=cutoff_rows,
        PINNED_COUNTS=SMALL,
    )
    facts: dict[str, Any] = names["expected_facts"](session, spec)
    return facts


def spec_of(rows: int) -> dict[str, Any]:
    return {"expected_cases": sum(SMALL.values()), "expected_patients": sum(SMALL.values()), "lab_rows": rows}


class CountFromTheRule(unittest.TestCase):
    """The flag compares the table's counts with the split rule's pinned counts inside the program."""

    def test_the_stand_in_takes_its_counts_from_the_split_rule_and_reads_none_from_its_spec(self) -> None:
        source = runner("silver_cases").STAND_IN
        imported = {
            alias.name
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.ImportFrom) and node.module == "spark.split_table"
            for alias in node.names
        }
        self.assertEqual({"PINNED_COUNTS"}, imported)
        keys = {
            node.slice.value
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name) and node.value.id == "spec" and isinstance(node.slice, ast.Constant)
        }
        self.assertNotIn("frozen", keys)

    def test_counts_equal_to_the_rule_give_true_without_a_count_in_the_spec(self) -> None:
        facts = facts_of(SMALL, spec_of(sum(SMALL.values())))
        self.assertIs(True, facts["counts_as_frozen"])

    def test_one_patient_moved_from_one_set_to_another_gives_false(self) -> None:
        for moved in ({"training": 5, "validation": 3, "test": 3}, {"training": 6, "validation": 2, "test": 2}, {"training": 6, "validation": 2, "test": 4}):
            with self.subTest(moved):
                facts = facts_of(moved, spec_of(sum(moved.values())))
                self.assertIs(False, facts["counts_as_frozen"])

    def test_the_facts_hold_no_count_of_a_set(self) -> None:
        facts = facts_of(SMALL, spec_of(sum(SMALL.values())))
        self.assertEqual(
            {"cases_as_expected", "patients_as_expected", "counts_as_frozen", "lab_total_ok", "development_cases", "measure", "cutoffs"}
            | {"lab_development_rows", "lab_development_collapsed"},
            set(facts),
        )


class NoCountInTheRunner(unittest.TestCase):
    """The pinned count of the test set is in no text of the runner: not its source, not a name that it defines, not its stand-in program."""

    def forms(self) -> list[str]:
        count = PINNED_COUNTS["test"]
        return [str(count), f"{count:,}"]

    def test_the_source_of_the_runner_holds_no_form_of_the_test_set_count_and_no_frozen_counts(self) -> None:
        source = (ROOT / "tests" / "e2e" / "silver_cases.py").read_text()
        runner("silver_cases")
        for form in self.forms():
            with self.subTest(form):
                self.assertNotIn(form, source)
        self.assertNotIn("FROZEN_COUNTS", source)

    def test_the_host_part_of_the_runner_never_reads_the_split_rule_counts(self) -> None:
        """Only the program that runs in the container reads ``PINNED_COUNTS`` (it is a string there); no name, import or attribute of the host code does."""
        runner("silver_cases")
        tree = ast.parse((ROOT / "tests" / "e2e" / "silver_cases.py").read_text())
        names = [node.id for node in ast.walk(tree) if isinstance(node, ast.Name)] + [node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)]
        imported = [alias.name for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) for alias in node.names]
        self.assertEqual([], [name for name in [*names, *imported] if "PINNED" in name or "FROZEN" in name])

    def test_no_name_that_the_runner_defines_holds_the_count(self) -> None:
        module = runner("silver_cases")
        wanted = {PINNED_COUNTS["test"], *self.forms()}

        def holds(value: Any) -> bool:
            if isinstance(value, dict):
                return any(holds(item) for pair in value.items() for item in pair)
            if isinstance(value, list | tuple | set | frozenset):
                return any(holds(item) for item in value)
            if isinstance(value, str):
                return any(form in value for form in self.forms())
            return isinstance(value, int | float) and not isinstance(value, bool) and value in wanted

        held = sorted(name for name, value in vars(module).items() if not name.startswith("__") and holds(value))
        self.assertEqual([], held)


if __name__ == "__main__":
    unittest.main()
