"""The Spark frames of the pressure series and the event table, from ``silver.waveform_seconds`` and the clinical table, and the counts that the checks read.

PySpark is installed in the job image only, so it is imported by name inside the functions and the rest of the sources are checked without it. Spark runs
Python only to call the two definitions: ``features.pressure_series`` decides every reading and ``features.hypotension`` decides every event, so the batch and
the stream paths cannot differ. The series is made on a narrow frame (the sum and count of a second's samples, never its samples) with the second before each
one taken by a window; the events are made for one case at a time from the case's series and its operation times.
"""

from __future__ import annotations

from typing import Any

from features.hypotension import EVENT_SECONDS, SECOND_US, SeriesPoint, events_of
from features.pressure_series import SERIES_TRACK, SecondFacts, second_facts, series_second
from features.signal_validity import INVALID_CAUSES, Verdict
from spark.bronze.tables import LINEAGE_COLUMNS
from spark.silver.frames import count_where, functions, typed, types, window
from spark.silver.hypotension_checks import EventFacts, InputFacts, SeriesFacts
from spark.silver.tables import ART_MBP_1S, HYPOTENSION_EVENTS

KEY = ("case_id", "case_second")
EVENT_KEY = ("case_id", "onset_second")
LINEAGE = tuple(name for name, _ in LINEAGE_COLUMNS)
LIMIT = 1.7e308


def facts_row(valid: bool | None, cause: str | None, samples: list[float | None] | None) -> tuple[int, float]:
    """The count and the sum of the samples of a valid second (``second_facts``), and nothing for an invalid one."""
    if valid is None:
        raise ValueError("a second has no verdict")
    facts = second_facts(0, Verdict(valid, cause), samples or [])
    return facts.count, facts.total


def reading_row(
    second: int,
    valid: bool | None,
    cause: str | None,
    count: int,
    total: float,
    before: int | None,
    before_valid: bool | None,
    before_cause: str | None,
    before_count: int | None,
    before_total: float | None,
) -> tuple[float | None, bool, str | None]:
    """The reading of one second from its own facts and those of the row before it (``series_second``): that row may be a gap, which the definition decides."""
    if valid is None:
        raise ValueError("a second has no verdict")
    previous = None if before is None else SecondFacts(before, bool(before_valid), before_cause, int(before_count or 0), float(before_total or 0.0))
    reading = series_second(previous, SecondFacts(second, valid, cause, count, total))
    return reading.value, reading.valid, reading.cause


def events_row(points: list[Any] | None, opstart: float | None, opend: float | None) -> list[tuple[int, int, int, int, int, int, str]]:
    """The events of one case from its series and its operation times (``events_of``). A case with no usable operation times has none."""
    if opstart is None or opend is None or not LIMIT >= opstart >= -LIMIT or not LIMIT >= opend >= -LIMIT or opstart > opend:
        return []
    found = events_of((SeriesPoint(int(p["case_second"]), p["value"], bool(p["valid"])) for p in points or []), opstart, opend)
    return [(e.onset_second, e.onset_us, e.confirmed_us, e.last_low_second, e.end_us, e.duration_seconds, e.end_reason) for e in found]


def facts_udf() -> Any:
    t = types()
    return functions().udf(facts_row, t.StructType([t.StructField("count", t.LongType()), t.StructField("total", t.DoubleType())])).asNondeterministic()


def reading_udf() -> Any:
    t = types()
    fields = [t.StructField("value", t.DoubleType()), t.StructField("valid", t.BooleanType()), t.StructField("cause", t.StringType())]
    return functions().udf(reading_row, t.StructType(fields)).asNondeterministic()


def events_udf() -> Any:
    t = types()
    names = ("onset_second", "onset_us", "confirmed_us", "last_low_second", "end_us", "duration_seconds")
    fields = [t.StructField(name, t.LongType()) for name in names] + [t.StructField("end_reason", t.StringType())]
    return functions().udf(events_row, t.ArrayType(t.StructType(fields))).asNondeterministic()


def arterial(waveform: Any) -> Any:
    """The seconds of the arterial waveform in ``silver.waveform_seconds``, as the series reads them."""
    f = functions()
    return waveform.filter(f.col("track") == f.lit(SERIES_TRACK))


def series_frame(waveform: Any) -> Any:
    """``silver.art_mbp_1s``: one row for each arterial second, its reading from the seconds before it, and the lineage of the second."""
    f = functions()
    staged = arterial(waveform).select(
        *KEY, "valid", "invalid_cause", facts_udf()(f.col("valid"), f.col("invalid_cause"), f.col("samples")).alias("_f"), *LINEAGE
    )
    ordered = window().partitionBy("case_id").orderBy("case_second")
    narrow = staged.select(*KEY, "valid", "invalid_cause", f.col("_f.count").alias("_n"), f.col("_f.total").alias("_total"), *LINEAGE).withColumns(
        {
            "_b_second": f.lag("case_second").over(ordered),
            "_b_valid": f.lag("valid").over(ordered),
            "_b_cause": f.lag("invalid_cause").over(ordered),
            "_b_n": f.lag("_n").over(ordered),
            "_b_total": f.lag("_total").over(ordered),
        }
    )
    arguments = ["case_second", "valid", "invalid_cause", "_n", "_total", "_b_second", "_b_valid", "_b_cause", "_b_n", "_b_total"]
    read = narrow.withColumn("_reading", reading_udf()(*[f.col(name) for name in arguments]))
    shaped = read.withColumn("value", f.col("_reading.value")).withColumn("valid", f.col("_reading.valid")).withColumn("invalid_cause", f.col("_reading.cause"))
    return typed(shaped, ART_MBP_1S.columns)


def operations(clinical: Any) -> Any:
    """The operation start and end of each case from the clinical table, whose published columns are strings. A value that is not a number is null."""
    f = functions()
    return clinical.select(
        f.expr("try_cast(caseid as bigint)").alias("case_id"),
        f.expr("try_cast(opstart as double)").alias("opstart_s"),
        f.expr("try_cast(opend as double)").alias("opend_s"),
    )


def usable(ops: Any) -> Any:
    """The cases whose start and end are finite numbers with the start not after the end. A comparison with NaN is false, so a NaN is never usable."""
    f = functions()
    start, end = f.col("opstart_s"), f.col("opend_s")
    return ops.filter(start.between(-LIMIT, LIMIT) & end.between(-LIMIT, LIMIT) & (start <= end))


def per_case(series: Any) -> Any:
    f = functions()
    points = f.collect_list(f.struct(f.col("case_second"), f.col("value"), f.col("valid"))).alias("_points")
    return series.groupBy("case_id").agg(points, f.min("source_version_id").alias("source_version_id"), f.min("source_sha256").alias("source_sha256"))


def events_frame(series: Any, clinical: Any) -> Any:
    """``silver.hypotension_events``: the events of each case with usable operation times, with the operation times that bounded them."""
    f = functions()
    joined = per_case(series).join(usable(operations(clinical)), "case_id", "inner")
    found = joined.withColumn("_event", f.explode(events_udf()(f.col("_points"), f.col("opstart_s"), f.col("opend_s"))))
    shaped = found.select("case_id", "opstart_s", "opend_s", *LINEAGE, f.col("_event.*"))
    return typed(shaped, HYPOTENSION_EVENTS.columns)


def input_facts(waveform: Any, clinical: Any) -> InputFacts:
    f = functions()
    art = arterial(waveform)
    key = f.struct(*[f.col(name) for name in KEY])
    seconds = art.agg(
        f.count(f.lit(1)).alias("rows"),
        count_where(f.col("case_id").isNull() | f.col("case_second").isNull()).alias("null_keys"),
        f.countDistinct(key).alias("distinct"),
        count_where(f.col("valid").isNull()).alias("no_verdict"),
    ).first()
    cases = operations(clinical).filter(f.col("case_id").isNotNull())
    twice = cases.groupBy("case_id").agg(f.count(f.lit(1)).alias("n")).filter(f.col("n") > 1).count()
    rows, null_keys = int(seconds["rows"]), int(seconds["null_keys"] or 0)
    # A second with no verdict has no cause to name, so it is counted as a second without a key: the input cannot be used.
    return InputFacts(rows, null_keys + int(seconds["no_verdict"] or 0), rows - null_keys - int(seconds["distinct"]), twice)


def series_facts(stored: Any, waveform: Any) -> SeriesFacts:
    f = functions()
    valid, cause, value = f.col("valid"), f.col("invalid_cause"), f.col("value")
    key = f.struct(*[f.col(name) for name in KEY])
    row = stored.agg(
        f.count(f.lit(1)).alias("rows"),
        (f.count(f.lit(1)) - f.countDistinct(key)).alias("duplicate_keys"),
        count_where(valid.isNull()).alias("null"),
        count_where(valid & cause.isNotNull()).alias("valid_with_cause"),
        count_where(~valid & cause.isNull()).alias("invalid_without_cause"),
        count_where(~valid & cause.isNotNull() & ~cause.isin(*INVALID_CAUSES)).alias("unknown"),
        count_where((valid & value.isNull()) | (~valid & value.isNotNull())).alias("value_problem"),
    ).first()
    again = series_frame(waveform)
    columns = tuple((name, kind) for name, kind in ART_MBP_1S.columns)
    left, right = typed(stored, columns), typed(again, columns)
    differs = int(left.exceptAll(right).count()) + int(right.exceptAll(left).count())
    labelled = f.when(valid, f.lit("valid")).otherwise(f.coalesce(cause, f.lit("none")))
    causes = {str(r["cause"]): int(r["n"]) for r in stored.groupBy(labelled.alias("cause")).agg(f.count(f.lit(1)).alias("n")).collect()}
    return SeriesFacts(
        int(row["rows"]),
        int(row["duplicate_keys"]),
        int(row["null"] or 0),
        int(row["valid_with_cause"] or 0),
        int(row["invalid_without_cause"] or 0),
        int(row["unknown"] or 0),
        int(row["value_problem"] or 0),
        differs,
        causes,
    )


def event_facts(stored: Any, series: Any, clinical: Any) -> EventFacts:
    f = functions()
    key = f.struct(*[f.col(name) for name in EVENT_KEY])
    onset, last, duration = f.col("onset_second"), f.col("last_low_second"), f.col("duration_seconds")
    times = (
        (f.col("onset_us") != (onset + 1) * SECOND_US)
        | (f.col("confirmed_us") != (onset + EVENT_SECONDS) * SECOND_US)
        | (f.col("end_us") != (last + 1) * SECOND_US)
        | (duration != last - onset + 1)
    )
    row = stored.agg(
        (f.count(f.lit(1)) - f.countDistinct(key)).alias("duplicate_keys"),
        count_where(duration < EVENT_SECONDS).alias("short"),
        count_where(times).alias("times_differ"),
    ).first()
    columns = tuple((name, kind) for name, kind in HYPOTENSION_EVENTS.columns)
    left, right = typed(stored, columns), typed(events_frame(series, clinical), columns)
    differs = int(left.exceptAll(right).count()) + int(right.exceptAll(left).count())
    without = per_case(series).join(usable(operations(clinical)), "case_id", "left_anti").count()
    return EventFacts(int(row["duplicate_keys"] or 0), int(row["short"] or 0), int(row["times_differ"] or 0), differs, int(without))
