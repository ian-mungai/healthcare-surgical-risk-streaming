"""The Spark frames of the label tables, from ``silver.cases``, ``silver.lab_results`` and the patient split, and the facts of their checks.

PySpark is installed in the job image only, so it is imported by name inside the functions and the rest of the sources are checked without it. Spark runs
Python only to call the label rules: ``features.kidney_label`` decides every kidney injury label and ``features.icu_label`` every intensive care label, so the
batch and any later path cannot differ. The two row functions below are thin adapters that flatten a rule's answer into the columns of its table.

The labels are outcomes and the test set is sealed. A label table has a row for every case, but the values read from it are flags (``exists``), never a count:
the facts of a label table hold one number for each check that is only tested for being positive. The column checks (a key null or twice, a null in a column, a
value outside its domain, and the row count, which must equal the number of cases) are Great Expectations checks (``quality_flags``); the exact comparison with
the rebuilt table and the rules that join several columns are Spark. The measure of
``icu_days`` is made from the development patients' values only: a case joins to the patient split by its patient, and only a set of ``DEVELOPMENT_SETS``
counts. The split is used only after its own rule check (the job runs it first).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from functools import reduce
from operator import and_
from typing import Any

from features.icu_label import FRACTIONS_ARE_VALID, INVALID_BELOW, STAY_ABOVE, icu_label
from features.kidney_label import (
    ABSOLUTE_HOURS,
    BASELINE_DAYS,
    CREATININE,
    ELIGIBLE,
    FOLLOWUP_DAYS,
    NEITHER,
    NO_BASELINE,
    NO_FOLLOWUP,
    SECONDS_PER_DAY,
    KidneyLabel,
    Result,
    kidney_label,
)
from features.label_values import NEGATIVE, POSITIVE, UNKNOWN
from spark.bronze.checks import Check, CheckResult, verdict
from spark.bronze.quality import column_checks
from spark.silver.frames import functions, typed, types
from spark.silver.hypotension_checks import DEVELOPMENT_SETS
from spark.silver.hypotension_frames import exists
from spark.silver.label_checks import COLUMN_FLAGS, ICU, KIDNEY, MEASURE, InputFacts, TableFacts, column_plan
from spark.silver.label_measure import IcuMeasureRow, icu_measure_rows
from spark.silver.tables import ICU_DAYS_MEASURE, ICU_LABELS, KIDNEY_INJURY_LABELS

KIDNEY_ROW_COLUMNS = tuple(name for name, _ in KIDNEY_INJURY_LABELS.columns[1:])
ICU_ROW_COLUMNS = tuple(name for name, _ in ICU_LABELS.columns[1:])
SPARK_TYPES = {"bigint": "LongType", "double": "DoubleType", "int": "IntegerType", "boolean": "BooleanType", "string": "StringType"}
LIMIT = 1.7e308
ABSOLUTE_SECONDS = ABSOLUTE_HOURS * 3_600
BASELINE_SECONDS = BASELINE_DAYS * SECONDS_PER_DAY
FOLLOWUP_SECONDS = FOLLOWUP_DAYS * SECONDS_PER_DAY
TOLERANCE = 1e-9
ABSOLUTE_RISE = 0.3
RELATIVE_RATIO = 1.5


def parts(result: Result | None) -> tuple[float | None, int | None, int | None]:
    """The value, the time and the occurrence number of a result, or three nulls."""
    return (None, None, None) if result is None else (result.value, result.time_s, result.occurrence)


def kidney_values(label: KidneyLabel) -> tuple[Any, ...]:
    """The columns of ``kidney_injury_labels`` after the case, in order, from the answer of the rule."""
    return (
        *parts(label.baseline),
        label.baseline_window_start_s,
        label.baseline_window_end_s,
        label.followup_start_s,
        label.absolute_window_end_s,
        label.relative_window_end_s,
        label.early_results,
        label.late_results,
        label.absolute_rise,
        *parts(label.earlier),
        *parts(label.later),
        label.relative_rise,
        *parts(label.relative_result),
        *parts(label.early_highest),
        *parts(label.followup_highest),
        label.latest_time_s,
        label.label,
        label.eligible,
        label.reason,
    )


def kidney_row(anestart: int | None, opend: int | None, results: Sequence[Mapping[str, Any]] | None) -> tuple[Any, ...]:
    """The columns of one kidney injury label from the case's start of anaesthesia, its end of operation and its creatinine results: each with ``dt``,
    ``source_occurrence`` and ``result``. A result with no value is not a measurement."""
    held = [Result(int(row["dt"]), int(row["source_occurrence"]), float(row["result"])) for row in results or [] if row["result"] is not None]
    return kidney_values(kidney_label(anestart, opend, held))


def icu_row(opend: int | None, icu_days: float | None) -> tuple[Any, ...]:
    """The columns of one intensive care label after the case: the end of the operation, the value that was read, the label, its eligibility and why."""
    outcome = icu_label(None if icu_days is None else float(icu_days))
    return opend, outcome.icu_days, outcome.label, outcome.eligible, outcome.reason


def struct_type(columns: tuple[tuple[str, str], ...]) -> Any:
    t = types()
    return t.StructType([t.StructField(name, getattr(t, SPARK_TYPES[kind])()) for name, kind in columns])


def kidney_udf() -> Any:
    return functions().udf(kidney_row, struct_type(KIDNEY_INJURY_LABELS.columns[1:])).asNondeterministic()


def icu_udf() -> Any:
    return functions().udf(icu_row, struct_type(ICU_LABELS.columns[1:])).asNondeterministic()


def kidney_frame(cases: Any, lab: Any) -> Any:
    """``silver.kidney_injury_labels``: one row for each case, from the case's times and every creatinine result of the case that has a value."""
    f = functions()
    wanted = (f.col("name") == f.lit(CREATININE)) & f.col("result").isNotNull() & f.col("dt").isNotNull()
    result = f.struct(f.col("dt"), f.col("source_occurrence"), f.col("result")).alias("_r")
    per_case = lab.filter(wanted).select(f.col("caseid"), result).groupBy("caseid").agg(f.collect_list("_r").alias("_results"))
    joined = cases.select("caseid", "anestart", "opend").join(per_case, "caseid", "left")
    judged = joined.withColumn("_k", kidney_udf()(f.col("anestart"), f.col("opend"), f.col("_results")))
    return typed(judged.select(f.col("caseid").alias("case_id"), f.col("_k.*")), KIDNEY_INJURY_LABELS.columns)


def icu_frame(cases: Any) -> Any:
    """``silver.icu_labels``: one row for each case, from the case's ``icu_days`` as published."""
    f = functions()
    staged = cases.select(f.col("caseid").alias("case_id"), f.col("opend").alias("_opend"), f.col("icu_days").cast("double").alias("_icu_days"))
    judged = staged.withColumn("_i", icu_udf()(f.col("_opend"), f.col("_icu_days")))
    return typed(judged.select("case_id", f.col("_i.*")), ICU_LABELS.columns)


def development_sets(split: Any) -> Any:
    """The patients of the development sets. The patient split must have passed its rule check before this is used."""
    f = functions()
    return split.select("subjectid", f.col("set").alias("_set")).filter(f.col("_set").isin(*DEVELOPMENT_SETS)).select("subjectid")


def development_values(cases: Any, split: Any) -> list[float | None]:
    """The ``icu_days`` of the cases of the development patients only, one for each case. The patient split must have passed its rule check before this is
    used. The held-out patients' values are never selected."""
    f = functions()
    rows = (
        cases.select("subjectid", f.col("icu_days").cast("double").alias("_value"))
        .join(development_sets(split), "subjectid", "inner")
        .select("_value")
        .collect()
    )
    return [None if row["_value"] is None else float(row["_value"]) for row in rows]


def ownership_flag(cases: Any) -> int:
    """One flag, 0 or 1: whether a case row has a case number or a patient that is null, or a case number that another row has. It reads the two key columns
    only, before any join of a case to its patient (``silver.cases`` is typed, so no value is converted here)."""
    f = functions()
    unusable = cases.filter(f.col("caseid").isNull() | f.col("subjectid").isNull())
    repeated = cases.filter(f.col("caseid").isNotNull()).groupBy("caseid").agg(f.count(f.lit(1)).alias("n")).filter(f.col("n") > 1)
    return int(exists(unusable) or exists(repeated))


def measure_rows(cases: Any, split: Any) -> list[IcuMeasureRow]:
    return icu_measure_rows(development_values(cases, split))


def measure_frame(session: Any, rows: Sequence[IcuMeasureRow]) -> Any:
    """``silver.icu_days_measure`` from the measured rows."""
    t = types()
    schema = t.StructType([t.StructField("statistic", t.StringType()), t.StructField("unit", t.StringType()), t.StructField("value", t.DoubleType())])
    return typed(session.createDataFrame([(row.statistic, row.unit, float(row.value)) for row in rows], schema), ICU_DAYS_MEASURE.columns)


def input_facts(cases: Any, split: Any) -> InputFacts:
    """The facts of the input once the split is trusted. ``cases_without_split`` is a flag: whether any case has a patient that the split lacks."""
    without = cases.join(split.select("subjectid"), "subjectid", "left_anti")
    development = cases.filter(functions().col("caseid").isNotNull()).join(development_sets(split), "subjectid", "inner")
    return InputFacts(int(cases.count()), 0, exists(without), (), int(development.count()))


def differing(stored: Any, planned: Any, columns: tuple[tuple[str, str], ...]) -> int:
    """One flag, 0 or 1: whether the two frames hold different rows, in either direction."""
    left, right = typed(stored, columns), typed(planned, columns)
    return int(exists(left.exceptAll(right)) or exists(right.exceptAll(left)))


def quality_flags(frame: Any, table: str, cases: int, run: Callable[[Any, Sequence[Check]], list[CheckResult]] = column_checks) -> dict[str, int]:
    """The column checks of a table (``column_plan``, with the row count of a label table, which must be ``cases``) run in Great Expectations on the stored
    ``frame``: one flag, 0 or 1, for each group of checks, that is 1
    when any check of the group failed. The count that the library found never leaves this function. A check that the runner leaves out counts as failed, and a
    result for a check that the plan does not hold is an error (``verdict``)."""
    plan = column_plan(table, cases)
    checks = [check for flag in COLUMN_FLAGS[table] for check in plan[flag]]
    failed = {failure.name for failure in verdict(checks, run(frame, checks)).failed}
    return {flag: int(any(check.name in failed for check in plan[flag])) for flag in COLUMN_FLAGS[table]}


def implies(present: Any, condition: Any) -> Any:
    """A column that is true when ``present`` is not true, and else when ``condition`` is true (a null condition is false)."""
    f = functions()
    return f.when(present, f.coalesce(condition, f.lit(False))).otherwise(f.lit(True))


def measure_facts(stored: Any, session: Any, cases: Any, split: Any, run: Callable[..., list[CheckResult]] = column_checks) -> TableFacts:
    flags = {
        **quality_flags(stored, MEASURE, 0, run),
        "icu_measure_differs": differing(stored, measure_frame(session, measure_rows(cases, split)), ICU_DAYS_MEASURE.columns),
    }
    return TableFacts(flags)


def icu_facts(stored: Any, cases: Any, run: Callable[..., list[CheckResult]] = column_checks) -> TableFacts:
    f = functions()
    label, reason, eligible, value = f.col("label"), f.col("eligibility_reason"), f.col("eligible"), f.col("icu_days")
    finite = value.between(-LIMIT, LIMIT)
    invalid = ~finite | (value < f.lit(INVALID_BELOW)) | (f.lit(not FRACTIONS_ARE_VALID) & (value != f.floor(value)))
    wanted = f.when(value.isNull(), f.lit(UNKNOWN)).when(invalid, f.lit(UNKNOWN)).when(value > f.lit(STAY_ABOVE), f.lit(POSITIVE)).otherwise(f.lit(NEGATIVE))
    why = f.when(value.isNull(), f.lit("missing")).when(invalid, f.lit("invalid")).otherwise(f.lit("recorded"))
    flags = {
        **quality_flags(stored, ICU, int(cases.count()), run),
        "icu_eligibility": exists(stored.filter(~eligible.eqNullSafe(label != f.lit(UNKNOWN)))),
        "icu_value_rule": exists(stored.filter(~(label.eqNullSafe(wanted) & reason.eqNullSafe(why)))),
        "icu_differs": differing(stored, icu_frame(cases), ICU_LABELS.columns),
    }
    return TableFacts(flags)


def kidney_times_ok() -> Any:
    """A column that is true for a row whose windows and times agree with one another and with the rule's constants."""
    f = functions()
    c = f.col
    checks = [
        implies(c("baseline_window_end_s").isNotNull(), c("baseline_window_start_s") == c("baseline_window_end_s") - f.lit(BASELINE_SECONDS)),
        implies(c("baseline_time_s").isNotNull(), (c("baseline_time_s") >= c("baseline_window_start_s")) & (c("baseline_time_s") < c("baseline_window_end_s"))),
        implies(
            c("followup_start_s").isNotNull(),
            (c("absolute_window_end_s") == c("followup_start_s") + f.lit(ABSOLUTE_SECONDS))
            & (c("relative_window_end_s") == c("followup_start_s") + f.lit(FOLLOWUP_SECONDS)),
        ),
        implies(c("later_time_s").isNotNull(), (c("later_time_s") > c("followup_start_s")) & (c("later_time_s") <= c("absolute_window_end_s"))),
        implies(
            c("earlier_time_s").isNotNull(), (c("earlier_time_s") < c("later_time_s")) & (c("later_time_s") - c("earlier_time_s") <= f.lit(ABSOLUTE_SECONDS))
        ),
        implies(c("relative_time_s").isNotNull(), (c("relative_time_s") > c("followup_start_s")) & (c("relative_time_s") <= c("relative_window_end_s"))),
        implies(c("latest_time_s").isNotNull(), c("latest_time_s") <= c("relative_window_end_s")),
        implies(
            c("early_highest_time_s").isNotNull(),
            (c("early_highest_time_s") > c("followup_start_s")) & (c("early_highest_time_s") <= c("absolute_window_end_s")),
        ),
        implies(
            c("followup_highest_time_s").isNotNull(),
            (c("followup_highest_time_s") > c("followup_start_s")) & (c("followup_highest_time_s") <= c("relative_window_end_s")),
        ),
    ]
    return reduce(and_, checks)


def kidney_rule_bad() -> Any:
    """A column that is true for a row whose label, rises, measurements and reason disagree: a row that has no absolute rise keeps the pair of its largest
    difference, which must be under the limit. The rise tests use a tolerance of a
    billionth: this check reads the stored doubles, and the exact decimal rule is the one the rebuild is compared with (``kidney_differs``)."""
    f = functions()
    c = f.col
    label, reason, absolute, relative = c("label"), c("eligibility_reason"), c("absolute_rise"), c("relative_rise")
    results = c("early_results") + c("late_results")
    has_baseline = c("baseline_time_s").isNotNull()
    reason_ok = (
        f.when(reason == f.lit(ELIGIBLE), has_baseline & (results > 0))
        .when(reason == f.lit(NO_BASELINE), ~has_baseline & (results > 0))
        .when(reason == f.lit(NO_FOLLOWUP), has_baseline & (results == 0))
        .when(reason == f.lit(NEITHER), ~has_baseline & (results == 0))
        .otherwise(f.lit(False))
    )
    rise = absolute.eqNullSafe(f.lit(True)) | relative.eqNullSafe(f.lit(True))
    none = absolute.eqNullSafe(f.lit(False)) & relative.eqNullSafe(f.lit(False))
    nothing_read = absolute.isNull() & relative.isNull() & c("earlier_time_s").isNull() & c("later_time_s").isNull() & c("relative_time_s").isNull()
    label_ok = (
        f.when(label == f.lit(POSITIVE), rise)
        .when(label == f.lit(NEGATIVE), none)
        .when(label == f.lit(UNKNOWN), nothing_read & ~c("eligible").eqNullSafe(f.lit(True)))
        .otherwise(f.lit(False))
    )
    rise_value = c("later_value") - c("earlier_value")
    absolute_ok = implies(absolute.eqNullSafe(f.lit(True)), rise_value >= f.lit(ABSOLUTE_RISE - TOLERANCE)) & implies(
        absolute.eqNullSafe(f.lit(False)) & c("earlier_time_s").isNotNull(), rise_value < f.lit(ABSOLUTE_RISE + TOLERANCE)
    )
    relative_ok = implies(relative.eqNullSafe(f.lit(True)), c("relative_value") >= f.lit(RELATIVE_RATIO) * c("baseline_value") - f.lit(TOLERANCE))
    highest_ok = (
        (c("early_highest_time_s").isNotNull() == (c("early_results") > 0))
        & (c("followup_highest_time_s").isNotNull() == (results > 0))
        & implies(c("early_highest_value").isNotNull(), c("followup_highest_value") >= c("early_highest_value"))
    )
    reaches = c("followup_highest_value") >= f.lit(RELATIVE_RATIO) * c("baseline_value") - f.lit(TOLERANCE)
    stays_under = c("followup_highest_value") < f.lit(RELATIVE_RATIO) * c("baseline_value") + f.lit(TOLERANCE)
    recorded_ok = implies(c("eligible").eqNullSafe(f.lit(True)), f.when(relative.eqNullSafe(f.lit(True)), reaches).otherwise(stays_under))
    return ~f.coalesce(reason_ok & label_ok & absolute_ok & relative_ok & highest_ok & recorded_ok, f.lit(False))


def kidney_facts(stored: Any, cases: Any, lab: Any, run: Callable[..., list[CheckResult]] = column_checks) -> TableFacts:
    f = functions()
    label, reason, eligible = f.col("label"), f.col("eligibility_reason"), f.col("eligible")
    flags = {
        **quality_flags(stored, KIDNEY, int(cases.count()), run),
        "kidney_eligibility": exists(stored.filter(~(eligible.eqNullSafe(label != f.lit(UNKNOWN)) & eligible.eqNullSafe(reason == f.lit(ELIGIBLE))))),
        "kidney_times": exists(stored.filter(~kidney_times_ok())),
        "kidney_rule": exists(stored.filter(kidney_rule_bad())),
        "kidney_differs": differing(stored, kidney_frame(cases, lab), KIDNEY_INJURY_LABELS.columns),
    }
    return TableFacts(flags)
