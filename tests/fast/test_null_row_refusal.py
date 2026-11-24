"""A null in a row that a job converts is a fixed refusal, never a ``TypeError`` that ends the job with ``internal_error`` and exit 70.

Failure modes covered here:

- A stored ``silver.unscored_share`` row whose ``cutoff_step`` (or any other whole-number column) is null reached ``int(None)`` in ``frames.unscored_facts`` and
  the signal profile ended with exit 70 instead of a refusal.
- A creatinine result of ``silver.lab_results`` with a null ``source_occurrence`` reached ``int(None)`` in the kidney row function, which runs in a Spark Python
  worker, so the job ended with exit 70 and a Spark error text. The driver now refuses it before any plan is made, as the one code ``table_row: failed``.
- The refusal names a value or a count (a count of the rows that are not in form is a fact about the stored table, and the lab table holds held-out patients'
  rows): it is the one fixed line however many rows are not in form.
- The refusal is too wide: a null in a column that may be null (the share of a row with no cutoff, a result that is no measurement, a time that is no
  measurement) is refused, or a row that is in form is refused.
- The label check covers other rows than the ones that the kidney row function converts, or a different set of them (the check and the frame must read the
  same rows: one definition, ``wanted_results``).
"""

from __future__ import annotations

import ast
import contextlib
import io
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

from features.kidney_label import CREATININE
from spark.jobs import silver_labels
from spark.profile import cli, frames
from spark.profile.checks import SignalProfileError, UnscoredFacts
from spark.silver import label_frames
from spark.silver.label_checks import InputFacts, input_problems

CODE = "table_row: failed"
MARKER = 424242
ROOT = Path(__file__).resolve().parents[2]
WHOLE_NUMBER_COLUMNS = (
    "cutoffs",
    "waveform",
    "numeric",
    "either",
    "cases",
    "no_grid",
    "cutoff_step",
    "cutoff_offset",
    "last_margin",
    "waveform_window",
    "waveform_limit",
    "numeric_window",
    "numeric_limit",
)


def unscored_row(**changes: Any) -> dict[str, Any]:
    row: dict[str, Any] = {name: index + 1 for index, name in enumerate(WHOLE_NUMBER_COLUMNS)}
    return {"scope": "development", "unscored_share": 0.5, **row, **changes}


class Rows:
    """A table of a stand-in session: ``collect`` gives the rows."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def collect(self) -> list[dict[str, Any]]:
        return self.rows


class AStoredUnscoredRowWithANull(unittest.TestCase):
    def test_a_row_in_form_is_read(self) -> None:
        facts = frames.unscored_facts(Rows([unscored_row()]))
        self.assertIsInstance(facts, UnscoredFacts)
        self.assertEqual(1, facts.rows)

    def test_a_zero_is_not_a_null(self) -> None:
        facts = frames.unscored_facts(Rows([unscored_row(**dict.fromkeys(WHOLE_NUMBER_COLUMNS, 0))]))
        self.assertEqual([0] * 13, [getattr(facts, name) for name in WHOLE_NUMBER_COLUMNS])

    def test_an_empty_table_and_a_row_with_no_share_are_not_refused(self) -> None:
        self.assertEqual(0, frames.unscored_facts(Rows([])).rows)
        self.assertIsNone(frames.unscored_facts(Rows([unscored_row(unscored_share=None)])).share)

    def test_a_null_in_any_whole_number_column_is_the_fixed_refusal(self) -> None:
        self.assertEqual(13, len(WHOLE_NUMBER_COLUMNS))
        for name in WHOLE_NUMBER_COLUMNS:
            with self.subTest(column=name), self.assertRaises(SignalProfileError) as caught:
                frames.unscored_facts(Rows([unscored_row(**{name: None})]))
            self.assertEqual([CODE], caught.exception.problems)

    def test_the_refusal_holds_no_value_and_no_column_name(self) -> None:
        with self.assertRaises(SignalProfileError) as caught:
            frames.unscored_facts(Rows([unscored_row(cutoff_step=None, cutoff_offset=MARKER)]))
        text = str(caught.exception) + "".join(caught.exception.problems)
        self.assertNotIn(str(MARKER), text)
        self.assertNotIn("cutoff", text)

    def test_two_nulls_and_one_give_the_same_line(self) -> None:
        lines = []
        for changes in ({"cutoff_step": None}, {"cutoff_step": None, "cases": None, "either": None}):
            with self.assertRaises(SignalProfileError) as caught:
                frames.unscored_facts(Rows([unscored_row(**changes)]))
            lines.append(caught.exception.problems)
        self.assertEqual([[CODE], [CODE]], lines)

    def test_the_command_line_prints_the_line_and_exits_1_not_70(self) -> None:
        def execute(_: Any) -> dict[str, Any]:
            frames.unscored_facts(Rows([unscored_row(cutoff_step=None)]))
            return {}

        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            code = cli.run(["verify", "--branch", "b"], execute)
        self.assertEqual((1, f"signal_profile: {CODE}\n"), (code, err.getvalue()))


class Column:
    """An expression over one row: the few operators that ``wanted_results`` uses."""

    def __init__(self, fn: Callable[[dict[str, Any]], Any]) -> None:
        self.fn = fn

    def __eq__(self, other: object) -> Column:  # type: ignore[override]
        return Column(lambda row: self.fn(row) == (other.fn(row) if isinstance(other, Column) else other))

    def __and__(self, other: Column) -> Column:
        return Column(lambda row: bool(self.fn(row)) and bool(other.fn(row)))

    def isNull(self) -> Column:  # noqa: N802
        return Column(lambda row: self.fn(row) is None)

    def isNotNull(self) -> Column:  # noqa: N802
        return Column(lambda row: self.fn(row) is not None)

    __hash__ = None  # type: ignore[assignment]


class Functions:
    @staticmethod
    def col(name: str) -> Column:
        return Column(lambda row: row[name])

    @staticmethod
    def lit(value: Any) -> Column:
        return Column(lambda row: value)


class Lab:
    """A laboratory table of a stand-in session: ``filter``, ``limit`` and ``take``, which ``exists`` uses."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def filter(self, condition: Column) -> Lab:
        return Lab([row for row in self.rows if condition.fn(row)])

    def limit(self, count: int) -> Lab:
        return Lab(self.rows[:count])

    def take(self, count: int) -> list[dict[str, Any]]:
        return self.rows[:count]


def lab_row(**changes: Any) -> dict[str, Any]:
    return {"caseid": 1, "dt": 100, "name": CREATININE, "result": 1.0, "source_occurrence": 1, **changes}


def unusable(*rows: dict[str, Any]) -> int:
    with mock.patch.object(label_frames, "functions", return_value=Functions()):
        return int(label_frames.unusable_results(Lab(list(rows))))


class ALabResultWithANullOccurrence(unittest.TestCase):
    """The creatinine results that the kidney row function converts (a name, a result and a time) with a null ``source_occurrence``: the one flag."""

    def test_a_table_in_form_gives_no_flag(self) -> None:
        self.assertEqual(0, unusable(lab_row(), lab_row(caseid=2, source_occurrence=2)))
        self.assertEqual(0, unusable())

    def test_a_creatinine_result_with_a_null_occurrence_is_the_flag(self) -> None:
        self.assertEqual(1, unusable(lab_row(), lab_row(source_occurrence=None)))

    def test_one_row_and_three_give_the_same_flag(self) -> None:
        self.assertEqual(unusable(lab_row(source_occurrence=None)), unusable(*[lab_row(caseid=n, source_occurrence=None) for n in range(3)]))

    def test_a_row_that_the_kidney_row_function_never_converts_is_not_the_flag(self) -> None:
        for what, change in (
            ("another test", {"name": "hemoglobin"}),
            ("no result", {"result": None}),
            ("no time", {"dt": None}),
        ):
            with self.subTest(row=what):
                self.assertEqual(0, unusable(lab_row(source_occurrence=None, **change)))

    def test_the_kidney_frame_and_the_check_read_the_rows_by_one_definition(self) -> None:
        tree = ast.parse((ROOT / "spark" / "silver" / "label_frames.py").read_text())
        calls = {
            node.name: {item.func.id for item in ast.walk(node) if isinstance(item, ast.Call) and isinstance(item.func, ast.Name)}
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name in {"kidney_frame", "unusable_results"}
        }
        self.assertIn("wanted_results", calls["kidney_frame"])
        self.assertIn("wanted_results", calls["unusable_results"])

    def test_the_input_check_refuses_it_with_the_fixed_code_alone(self) -> None:
        facts = InputFacts(5, 0, 0, (), 3, unusable_results=1)
        self.assertEqual([CODE], input_problems(facts))

    def test_the_input_check_is_not_too_wide(self) -> None:
        self.assertEqual([], input_problems(InputFacts(5, 0, 0, (), 3)))
        self.assertEqual([], input_problems(InputFacts(5, 0, 0, (), 3, unusable_results=0)))

    def test_the_split_and_the_owner_refusals_come_first(self) -> None:
        self.assertEqual(["ambiguous_case_owner: failed"], input_problems(InputFacts(5, 1, 0, (), 3, unusable_results=1)))
        self.assertEqual(["rule_mismatch: failed"], input_problems(InputFacts(5, 0, 0, ("rule_mismatch: failed",), 3, unusable_results=1)))

    def test_the_job_reads_the_laboratory_table_into_the_input_facts(self) -> None:
        cases, lab, split = mock.MagicMock(name="cases"), mock.MagicMock(name="lab"), mock.MagicMock(name="split")
        wanted = InputFacts(5, 0, 0, (), 3, unusable_results=1)
        lake = silver_labels.LabelsLake(mock.MagicMock(), mock.MagicMock(), "b", {"nessie_uri": "u", "warehouse": "w", "region": "r"})
        with (
            mock.patch.object(lake, "cases", return_value=(cases, "c")),
            mock.patch.object(lake, "lab", return_value=(lab, "l")),
            mock.patch.object(lake, "split", return_value=(split, "s")),
            mock.patch.object(lake, "snapshot_id", return_value=1),
            mock.patch.object(lake, "split_problems", return_value=[]),
            mock.patch.object(label_frames, "ownership_flag", return_value=0),
            mock.patch.object(label_frames, "input_facts", return_value=wanted) as read,
        ):
            read_input = lake.read_input("a" * 64)
        read.assert_called_once_with(cases, split, lab)
        self.assertEqual([CODE], input_problems(read_input.facts))

    def test_the_flag_is_read_into_the_input_facts(self) -> None:
        cases, split = mock.MagicMock(), mock.MagicMock()
        with (
            mock.patch.object(label_frames, "functions", return_value=mock.MagicMock()),
            mock.patch.object(label_frames, "exists", return_value=0),
            mock.patch.object(label_frames, "unusable_results", return_value=1) as flag,
        ):
            facts = label_frames.input_facts(cases, split, Lab([]))
        self.assertEqual(1, facts.unusable_results)
        flag.assert_called_once()


if __name__ == "__main__":
    unittest.main()
