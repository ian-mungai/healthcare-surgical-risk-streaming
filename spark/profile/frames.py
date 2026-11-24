"""The Spark frames of the signal profile tables, from the Silver signal tables, the patient split and Bronze, and the counts that the checks read.

PySpark is installed in the job image only, so it is imported by name inside the functions and the rest of the sources are checked without it. Spark runs Python
only to call the one count of the unscored cutoffs (``spark.profile.unscored``), once for each case. Everything else is Spark SQL. Spark 4 runs with ANSI mode
on, so a division that could have a zero divisor is ``try_divide`` and a text that may not be a number is read with ``try_cast``.

The development cases are the cases of the patients in the training and validation sets, by a join of ``bronze.clinical_cases`` and ``silver.patient_split``
(``development_cases``). The checks count the development rows by another route: the list of the development case numbers that the job made in Python.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import astuple, fields
from datetime import UTC, datetime
from functools import reduce
from operator import add
from typing import Any

from features.registry import MAX_INVALID_NUMERIC_PERCENT, MAX_INVALID_WAVEFORM_SECONDS, NUMERIC_WINDOW_SECONDS, WAVEFORM_WINDOW_SECONDS
from features.signal_validity import ARTERIAL_WAVEFORM, INVALID_CAUSES
from spark.profile.checks import FLOAT_TOLERANCE, CountsFacts, InputFacts, ProfileFacts, SignalProfileError, UnscoredFacts
from spark.profile.counts import ARTERIAL_TRACK, BY_NAME, FULL_COHORT, NONINVASIVE_TRACK, PRESPECIFIED, SECONDS_PER_HOUR
from spark.profile.grid_rule import RULE_END, GridRule, RuleRead
from spark.profile.tables import CAUSE_COLUMNS, COUNTS, DEVELOPMENT_SCOPE, GRID_RULE, KINDS, NUMERIC_KIND, PROFILE, UNSCORED, WAVEFORM_KIND
from spark.profile.unscored import count_unscored, grid_pair
from spark.silver.frames import count_where, functions, typed, types
from spark.split_table import flag

DEVELOPMENT_SETS = ("training", "validation")
TRACK_KEY = ("kind", "track")
GRID_RULE_FIELDS = tuple(field.name for field in fields(GridRule))


UNSCORED_COUNTS = ("cutoffs", "waveform", "numeric", "either", "no_grid")


def unscored_row(
    valid_seconds: list[int] | None, pairs: Sequence[Sequence[float | None]] | None, step: int, first_offset: int, last_margin: int
) -> tuple[int, int, int, int, int]:
    """The cutoffs of the warning grid of one case, those that fail the waveform limit, the numeric limit and either, and 1 for a case with no grid
    (``count_unscored``). ``pairs`` are the distinct (``opstart``, ``opend``) of the case's clinical rows, as numbers of seconds or nothing (``grid_pair``). The
    step, the first cutoff offset and the last cutoff margin are the values of the saved rule row."""
    opstart, opend = grid_pair(pairs or [])
    counted = count_unscored(valid_seconds or [], opstart, opend, GridRule("", "", "", step, first_offset, last_margin, 0))
    return (counted.cutoffs, counted.waveform, counted.numeric, counted.either, counted.no_grid)


def unscored_udf() -> Any:
    t = types()
    fields = [t.StructField(name, t.LongType()) for name in UNSCORED_COUNTS]
    return functions().udf(unscored_row, t.StructType(fields)).asNondeterministic()


def development_cases(clinical: Any, split: Any) -> Any:
    """One row, ``case_id``, for each case of a patient in the training or validation set."""
    f = functions()
    patients = split.filter(f.col("set").isin(*DEVELOPMENT_SETS)).select("subjectid")
    return clinical.join(f.broadcast(patients), "subjectid").select(f.col("caseid").cast("bigint").alias("case_id")).distinct()


def development_grid(clinical: Any, split: Any, rule: GridRule) -> Any:
    """One row for each case of a patient in the training or validation set: ``case_id`` and ``pairs``, the distinct (origin, end) of the case's clinical
    rows as numbers of seconds. The origin column is the one that the saved rule names and the end column is the rule's end. A time that is missing or
    is not a number is null and stays in its pair: a set of structs keeps a pair with a null, which a minimum and a maximum would not.
    ``unscored_row`` gives a case a grid only when it holds one pair with no null."""
    f = functions()
    patients = split.filter(f.col("set").isin(*DEVELOPMENT_SETS)).select("subjectid")
    held = clinical.join(f.broadcast(patients), "subjectid").select(
        f.col("caseid").cast("bigint").alias("case_id"), *[f.col(name).try_cast("double").alias(name) for name in (rule.grid_origin, RULE_END)]
    )
    return held.groupBy("case_id").agg(f.collect_set(f.struct(*[f.col(name) for name in (rule.grid_origin, RULE_END)])).alias("pairs"))


def kind_profile(frame: Any, kind: str, development: Any) -> Any:
    """The profile rows of one kind of signal: for each track, the development cases that hold it, its rows, its valid rows and each cause."""
    f = functions()
    held = frame.join(f.broadcast(development), "case_id")
    causes = [count_where(f.col("invalid_cause") == f.lit(cause)).alias(name) for cause, name in zip(INVALID_CAUSES, CAUSE_COLUMNS, strict=True)]
    grouped = held.groupBy("track").agg(
        f.countDistinct("case_id").alias("cases"), f.count(f.lit(1)).alias("rows"), count_where(f.col("valid")).alias("valid_rows"), *causes
    )
    return grouped.withColumn("kind", f.lit(kind))


def profile_frame(numeric: Any, waveform: Any, development: Any) -> Any:
    """``silver.signal_profile``: the numeric and the waveform tracks of the development cases."""
    f = functions()
    both = kind_profile(numeric, NUMERIC_KIND, development).unionByName(kind_profile(waveform, WAVEFORM_KIND, development))
    return typed(both.withColumn("valid_share", f.try_divide(f.col("valid_rows").cast("double"), f.col("rows").cast("double"))), PROFILE.columns)


def unscored_frame(waveform: Any, grid: Any, rule: GridRule) -> Any:
    """``silver.unscored_share``: one row, from the arterial waveform seconds of the development cases and their warning grids (``development_grid``), counted
    once for each case, on the saved rule row ``rule``."""
    f = functions()
    seconds = waveform.filter(f.col("track") == f.lit(ARTERIAL_WAVEFORM)).join(f.broadcast(grid.select("case_id")), "case_id")
    per_case = seconds.groupBy("case_id").agg(f.collect_list(f.when(f.col("valid"), f.col("case_second"))).alias("valid_seconds"))
    cases = per_case.join(f.broadcast(grid), "case_id")
    step, first_offset, last_margin = (f.lit(value) for value in (rule.grid_step_seconds, rule.first_cutoff_offset_seconds, rule.last_cutoff_margin_seconds))
    counted = cases.withColumn("_u", unscored_udf()(f.col("valid_seconds"), f.col("pairs"), step, first_offset, last_margin))
    total = counted.agg(
        f.count(f.lit(1)).alias("cases"),
        *[f.coalesce(f.sum(f.col(f"_u.{name}")), f.lit(0)).alias(name) for name in UNSCORED_COUNTS],
    )
    columns = {
        "scope": f.lit(DEVELOPMENT_SCOPE),
        "unscored_share": f.try_divide(f.col("either").cast("double"), f.col("cutoffs").cast("double")),
        "cutoff_step": step,
        "cutoff_offset": first_offset,
        "last_margin": last_margin,
        "waveform_window": f.lit(WAVEFORM_WINDOW_SECONDS),
        "waveform_limit": f.lit(MAX_INVALID_WAVEFORM_SECONDS),
        "numeric_window": f.lit(NUMERIC_WINDOW_SECONDS),
        "numeric_limit": f.lit(MAX_INVALID_NUMERIC_PERCENT),
    }
    return typed(total.withColumns(columns), UNSCORED.columns)


def counts_frame(session: Any, rows: Sequence[Mapping[str, Any]]) -> Any:
    """``silver.source_counts`` from the rows of ``spark.profile.counts.count_rows``."""
    names = [name for name, _ in COUNTS.columns]
    ddl = ", ".join(f"{name} {kind}" for name, kind in COUNTS.columns)
    return session.createDataFrame([tuple(row[name] for name in names) for row in rows], ddl)


def rule_frame(session: Any, rule: GridRule) -> Any:
    """``silver.warning_grid_rule``: the one row of the rule and the time of the write (UTC)."""
    ddl = ", ".join(f"{name} {kind}" for name, kind in GRID_RULE.columns)
    return session.createDataFrame([(*astuple(rule), datetime.now(UTC))], ddl)


def rule_read(frame: Any, snapshot_id: int | None) -> RuleRead:
    """The rule table as the checks read it: row count, first row, snapshot ID and whether every row has a time of write. It is tiny, so it is read whole."""
    held = frame.collect()
    first = held[0] if held else None
    rule = None if first is None else GridRule(*(first[name] for name in GRID_RULE_FIELDS))
    return RuleRead(len(held), rule, snapshot_id, all(row["written_at"] is not None for row in held))


def fixed_counts(clinical: Any, track_list: Any) -> dict[str, float | None]:
    """The reproduced counts of the full cohort: the case counts from ``bronze.clinical_cases`` and the track counts from ``raw_records.track_list``, the
    input that the guide's count is of. Each is a number and is never printed: the job judges it and keeps the flag."""
    f = functions()
    hours = clinical.select((f.col("caseend").try_cast("double") / f.lit(SECONDS_PER_HOUR)).alias("hours")).filter(f.col("hours").isNotNull())
    spread = hours.agg(f.sum("hours").alias("total"), f.avg("hours").alias("mean"), f.expr("percentile(hours, 0.5)").alias("median")).first()
    held = track_list.select("caseid", "tname")

    def cases_with(track: str) -> float:
        return float(held.filter(f.col("tname") == f.lit(track)).select("caseid").distinct().count())

    return {
        "cases": float(clinical.count()),
        "patients": float(clinical.select("subjectid").distinct().count()),
        "tracks": float(track_list.count()),
        "cases_with_arterial_waveform": cases_with(ARTERIAL_TRACK),
        "cases_with_noninvasive_pressure": cases_with(NONINVASIVE_TRACK),
        "recorded_hours": None if spread["total"] is None else float(spread["total"]),
        "mean_case_hours": None if spread["mean"] is None else float(spread["mean"]),
        "median_case_hours": None if spread["median"] is None else float(spread["median"]),
    }


def input_facts(numeric: Any, waveform: Any, case_ids: list[int]) -> InputFacts:
    """The development rows of the Silver signal tables, counted with the list of development case numbers instead of the plan's join."""
    f = functions()
    kept_numeric = numeric.filter(f.col("case_id").isin(case_ids))
    kept_waveform = waveform.filter(f.col("case_id").isin(case_ids))
    tracks = kept_numeric.select("track").distinct().count() + kept_waveform.select("track").distinct().count()
    arterial = kept_waveform.filter(f.col("track") == f.lit(ARTERIAL_WAVEFORM)).select("case_id").distinct().count()
    return InputFacts(kept_numeric.count(), kept_waveform.count(), tracks, arterial)


def profile_facts(frame: Any) -> ProfileFacts:
    f = functions()
    key = f.struct(*[f.col(name) for name in TRACK_KEY])
    added = reduce(add, [f.col("valid_rows"), *[f.col(name) for name in CAUSE_COLUMNS]])
    exact = f.try_divide(f.col("valid_rows").cast("double"), f.col("rows").cast("double"))
    share_ok = f.coalesce(f.abs(f.col("valid_share") - exact) <= f.lit(FLOAT_TOLERANCE), f.lit(False))
    row = frame.agg(
        f.count(f.lit(1)).alias("rows"),
        (f.count(f.lit(1)) - f.countDistinct(key)).alias("duplicate_keys"),
        count_where(added != f.col("rows")).alias("partition"),
        count_where(~share_ok).alias("share"),
        count_where(~f.col("kind").isin(*KINDS)).alias("kind"),
        f.sum(f.when(f.col("kind") == f.lit(NUMERIC_KIND), f.col("rows"))).alias("numeric_rows"),
        f.sum(f.when(f.col("kind") == f.lit(WAVEFORM_KIND), f.col("rows"))).alias("waveform_rows"),
        f.countDistinct(key).alias("tracks"),
    ).first()
    return ProfileFacts(
        int(row["rows"]),
        int(row["duplicate_keys"]),
        int(row["partition"] or 0),
        int(row["share"] or 0),
        int(row["kind"] or 0),
        int(row["numeric_rows"] or 0),
        int(row["waveform_rows"] or 0),
        int(row["tracks"]),
    )


def whole_number(row: Mapping[str, Any], name: str) -> int:
    """A whole-number column of a stored row, or the fixed refusal ``table_row: failed`` for a null (a stored row that is not in form), never a ``TypeError``
    and never the column or a value."""
    value = row[name]
    if value is None:
        raise SignalProfileError(flag("table_row"))
    return int(value)


def unscored_facts(frame: Any) -> UnscoredFacts:
    rows = frame.collect()
    if not rows:
        return UnscoredFacts(0, 0, 0, 0, 0, None, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    one = rows[0]
    share = one["unscored_share"]
    return UnscoredFacts(
        len(rows),
        whole_number(one, "cutoffs"),
        whole_number(one, "waveform"),
        whole_number(one, "numeric"),
        whole_number(one, "either"),
        None if share is None else float(share),
        whole_number(one, "cases"),
        whole_number(one, "no_grid"),
        whole_number(one, "cutoff_step"),
        whole_number(one, "cutoff_offset"),
        whole_number(one, "last_margin"),
        whole_number(one, "waveform_window"),
        whole_number(one, "waveform_limit"),
        whole_number(one, "numeric_window"),
        whole_number(one, "numeric_limit"),
    )


def counts_facts(frame: Any) -> CountsFacts:
    """The facts of ``source_counts``: counts of rows and of flags, never a name that fails. The table is a dozen rows, so it is read whole."""
    held = [row.asDict() for row in frame.collect()]
    names = [row["name"] for row in held]
    expected = {count.name for count in PRESPECIFIED}
    sealed = {count.name for count in PRESPECIFIED if count.scope == FULL_COHORT}
    sealed_with_value = sum(1 for row in held if row["name"] in sealed and row["value"] is not None)
    expected_differs = sum(1 for row in held if row["name"] in BY_NAME and row["expected"] != float(BY_NAME[row["name"]].expected))
    flags = [row["matches"] for row in held]
    return CountsFacts(
        len(held),
        len(set(names) ^ expected),
        sealed_with_value,
        expected_differs,
        sum(1 for flag in flags if flag is None),
        sum(1 for flag in flags if flag is not None),
    )
