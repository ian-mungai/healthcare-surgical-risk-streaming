"""The Spark frames of the pressure series and the event table, from ``silver.waveform_seconds``, the clinical table and the patient split, and their facts.

PySpark is installed in the job image only, so it is imported by name inside the functions and the rest of the sources are checked without it. Spark runs Python
only to call the two definitions: ``features.pressure_series`` decides every reading and ``features.hypotension`` decides every event, so the batch and the
stream paths cannot differ. The series is made on a narrow frame (the count, sum and error of the sum of a second's samples, never its samples) with the
second before each one taken by a window; the events are made for one case at a time from the case's series and its operation times.

The test set is sealed. The events are made for the cases of the development patients only: a case joins to the patient split by its patient, and only a set of
``DEVELOPMENT_SETS`` counts. The split is used only after its own rule check (the job runs it first). A fact about events is a flag (``exists``), never a count,
and the digest of the event table has no row count.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from features.hypotension import EVENT_SECONDS, SECOND_US, SeriesPoint, events_of
from features.pressure_series import SERIES_TRACK, ExactMean, SecondFacts, second_facts, series_second
from features.signal_validity import INVALID_CAUSES, Verdict
from spark.bronze.quality import row_count_is
from spark.bronze.tables import LINEAGE_COLUMNS
from spark.silver.frames import DIGEST_TYPE, count_where, functions, typed, types, window
from spark.silver.hypotension_checks import DEVELOPMENT_SETS, EventFacts, InputFacts, SeriesFacts
from spark.silver.hypotension_run import Digest
from spark.silver.tables import ART_MBP_1S, HYPOTENSION_EVENTS

KEY = ("case_id", "case_second")
EVENT_KEY = ("case_id", "onset_second")
LINEAGE = tuple(name for name, _ in LINEAGE_COLUMNS)
LIMIT = 1.7e308


def facts_row(valid: bool | None, cause: str | None, samples: Sequence[float | None] | None) -> tuple[int, float, float]:
    """The count, the sum and the error of the sum of a valid second (``second_facts``), and nothing for an invalid one."""
    if valid is None:
        raise ValueError("a second has no verdict")
    facts = second_facts(0, Verdict(valid, cause), samples or [])
    return facts.count, facts.total, facts.residual


def reading_row(
    second: int,
    valid: bool | None,
    cause: str | None,
    count: int,
    total: float,
    residual: float,
    before: int | None,
    before_valid: bool | None,
    before_cause: str | None,
    before_count: int | None,
    before_total: float | None,
    before_residual: float | None,
) -> tuple[bool, str | None, float | None, int | None, float | None, float | None]:
    """The reading of one second from its own facts and those of the row before it (``series_second``): that row may be a gap, which the definition decides.

    It returns the validity, the cause, the value and the exact mean (the count and the two floats of the sum). An invalid reading has no value and no mean."""
    if valid is None:
        raise ValueError("a second has no verdict")
    before_facts = None
    if before is not None:
        before_facts = SecondFacts(before, bool(before_valid), before_cause, int(before_count or 0), float(before_total or 0.0), float(before_residual or 0.0))
    reading = series_second(before_facts, SecondFacts(second, valid, cause, count, total, residual))
    mean = reading.mean
    if mean is None:
        return reading.valid, reading.cause, None, None, None, None
    return reading.valid, reading.cause, mean.value, mean.count, mean.total_high, mean.total_low


def events_row(points: list[Any] | None, opstart: float | None, opend: float | None) -> list[tuple[int, int, int, int, int, int, str]]:
    """The events of one case from its series and its operation times (``events_of``). A case with no usable operation times has none."""
    if opstart is None or opend is None or not LIMIT >= opstart >= -LIMIT or not LIMIT >= opend >= -LIMIT or opstart > opend:
        return []
    series = (SeriesPoint(int(p["case_second"]), mean_of(p), bool(p["valid"])) for p in points or [])
    found = events_of(series, opstart, opend)
    return [(e.onset_second, e.onset_us, e.confirmed_us, e.last_low_second, e.end_us, e.duration_seconds, e.end_reason) for e in found]


def mean_of(point: Any) -> ExactMean | None:
    """The exact mean of a stored reading, or ``None`` for an invalid one."""
    if point["sample_count"] is None:
        return None
    return ExactMean(int(point["sample_count"]), float(point["sum_high"]), float(point["sum_low"]))


def facts_udf() -> Any:
    t = types()
    fields = [t.StructField("count", t.LongType()), t.StructField("total", t.DoubleType()), t.StructField("residual", t.DoubleType())]
    return functions().udf(facts_row, t.StructType(fields)).asNondeterministic()


def reading_udf() -> Any:
    t = types()
    fields = [
        t.StructField("valid", t.BooleanType()),
        t.StructField("cause", t.StringType()),
        t.StructField("value", t.DoubleType()),
        t.StructField("count", t.IntegerType()),
        t.StructField("high", t.DoubleType()),
        t.StructField("low", t.DoubleType()),
    ]
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
    own = facts_udf()(f.col("valid"), f.col("invalid_cause"), f.col("samples")).alias("_f")
    staged = arterial(waveform).select(*KEY, "valid", "invalid_cause", own, *LINEAGE)
    ordered = window().partitionBy("case_id").orderBy("case_second")
    narrow = staged.select(
        *KEY, "valid", "invalid_cause", f.col("_f.count").alias("_n"), f.col("_f.total").alias("_total"), f.col("_f.residual").alias("_residual"), *LINEAGE
    ).withColumns(
        {
            "_b_second": f.lag("case_second").over(ordered),
            "_b_valid": f.lag("valid").over(ordered),
            "_b_cause": f.lag("invalid_cause").over(ordered),
            "_b_n": f.lag("_n").over(ordered),
            "_b_total": f.lag("_total").over(ordered),
            "_b_residual": f.lag("_residual").over(ordered),
        }
    )
    names = ["case_second", "valid", "invalid_cause", "_n", "_total", "_residual", "_b_second", "_b_valid", "_b_cause", "_b_n", "_b_total", "_b_residual"]
    read = narrow.withColumn("_reading", reading_udf()(*[f.col(name) for name in names]))
    shaped = (
        read.withColumn("value", f.col("_reading.value"))
        .withColumn("sample_count", f.col("_reading.count"))
        .withColumn("sum_high", f.col("_reading.high"))
        .withColumn("sum_low", f.col("_reading.low"))
        .withColumn("valid", f.col("_reading.valid"))
        .withColumn("invalid_cause", f.col("_reading.cause"))
    )
    return typed(shaped, ART_MBP_1S.columns)


def operations(clinical: Any) -> Any:
    """The case number, the patient, the operation start and the operation end of each case from the clinical table, whose published columns are strings. A
    number that is not one is null."""
    f = functions()
    return clinical.select(
        f.expr("try_cast(caseid as bigint)").alias("case_id"),
        f.col("subjectid"),
        f.expr("try_cast(opstart as double)").alias("opstart_s"),
        f.expr("try_cast(opend as double)").alias("opend_s"),
    )


def development_cases(clinical: Any, split: Any) -> Any:
    """The cases of the development patients with their operation times. The patient split must have passed its rule check before this is used."""
    f = functions()
    sets = split.select("subjectid", f.col("set").alias("_set")).filter(f.col("_set").isin(*DEVELOPMENT_SETS)).select("subjectid")
    return operations(clinical).join(sets, "subjectid", "inner").filter(f.col("case_id").isNotNull())


def exists(frame: Any) -> int:
    """One flag, 0 or 1, never a count: whether the frame has any row."""
    return int(len(frame.limit(1).take(1)) > 0)


def usable(ops: Any) -> Any:
    """The cases whose start and end are finite numbers with the start not after the end. A comparison with NaN is false, so a NaN is never usable."""
    f = functions()
    start, end = f.col("opstart_s"), f.col("opend_s")
    return ops.filter(start.between(-LIMIT, LIMIT) & end.between(-LIMIT, LIMIT) & (start <= end))


def per_case(series: Any) -> Any:
    f = functions()
    point = f.struct(f.col("case_second"), f.col("valid"), f.col("sample_count"), f.col("sum_high"), f.col("sum_low"))
    return series.groupBy("case_id").agg(
        f.collect_list(point).alias("_points"), f.min("source_version_id").alias("source_version_id"), f.min("source_sha256").alias("source_sha256")
    )


def events_frame(series: Any, clinical: Any, split: Any) -> Any:
    """``silver.hypotension_events``: the events of each development case with usable operation times, with the operation times that bounded them."""
    f = functions()
    joined = per_case(series).join(usable(development_cases(clinical, split)), "case_id", "inner")
    found = joined.withColumn("_event", f.explode(events_udf()(f.col("_points"), f.col("opstart_s"), f.col("opend_s"))))
    shaped = found.select("case_id", "opstart_s", "opend_s", *LINEAGE, f.col("_event.*"))
    return typed(shaped, HYPOTENSION_EVENTS.columns)


def digest_of(frame: Any) -> Digest:
    """The sum of the 64-bit hash of each row, as a decimal so that it cannot pass a long, and no row count. A row is hashed as its JSON text, as
    ``spark.silver.frames.fingerprint`` does."""
    f = functions()
    text = f.to_json(f.struct(*[f.col(name) for name in frame.columns]), {"ignoreNullFields": "false"})
    row = frame.agg(f.sum(f.xxhash64(text).cast(DIGEST_TYPE)).alias("digest")).first()
    return Digest(int(row["digest"] or 0))


def ownership_flag(clinical: Any) -> int:
    """One flag, 0 or 1: whether a clinical row has a case number that is not a number, a patient that is null or a case number that another row has. It is made
    from the case table alone, before any join of a case to its patient."""
    f = functions()
    ops = operations(clinical)
    unusable = ops.filter(f.col("case_id").isNull() | f.col("subjectid").isNull())
    repeated = ops.filter(f.col("case_id").isNotNull()).groupBy("case_id").agg(f.count(f.lit(1)).alias("n")).filter(f.col("n") > 1)
    return int(exists(unusable) or exists(repeated))


def input_facts(waveform: Any, clinical: Any, split: Any) -> InputFacts:
    """The facts of the input once the split is trusted. ``cases_without_split`` is a flag: whether any clinical row has a patient that the split lacks."""
    f = functions()
    art = arterial(waveform)
    key = f.struct(*[f.col(name) for name in KEY])
    seconds = art.agg(
        f.count(f.lit(1)).alias("rows"),
        count_where(f.col("case_id").isNull() | f.col("case_second").isNull()).alias("null_keys"),
        f.countDistinct(key).alias("distinct"),
        count_where(f.col("valid").isNull()).alias("no_verdict"),
    ).first()
    without = operations(clinical).join(split.select("subjectid"), "subjectid", "left_anti")
    rows, null_keys = int(seconds["rows"]), int(seconds["null_keys"] or 0)
    # A second with no verdict has no cause to name, so it is counted as a second without a key: the input cannot be used.
    return InputFacts(rows, null_keys + int(seconds["no_verdict"] or 0), rows - null_keys - int(seconds["distinct"]), 0, exists(without), ())


def series_facts(stored: Any, waveform: Any) -> SeriesFacts:
    f = functions()
    valid, cause, value = f.col("valid"), f.col("invalid_cause"), f.col("value")
    key = f.struct(*[f.col(name) for name in KEY])
    row = stored.agg(
        (f.count(f.lit(1)) - f.countDistinct(key)).alias("duplicate_keys"),
        count_where(valid.isNull()).alias("null"),
        count_where(valid & cause.isNotNull()).alias("valid_with_cause"),
        count_where(~valid & cause.isNull()).alias("invalid_without_cause"),
        count_where(~valid & cause.isNotNull() & ~cause.isin(*INVALID_CAUSES)).alias("unknown"),
        count_where(
            (valid & (value.isNull() | f.col("sample_count").isNull() | f.col("sum_high").isNull() | f.col("sum_low").isNull()))
            | (~valid & (value.isNotNull() | f.col("sample_count").isNotNull() | f.col("sum_high").isNotNull() | f.col("sum_low").isNotNull()))
        ).alias("value_problem"),
    ).first()
    again = series_frame(waveform)
    columns = tuple((name, kind) for name, kind in ART_MBP_1S.columns)
    left, right = typed(stored, columns), typed(again, columns)
    differs = int(left.exceptAll(right).count()) + int(right.exceptAll(left).count())
    return SeriesFacts(
        int(row["duplicate_keys"]),
        int(row["null"] or 0),
        int(row["valid_with_cause"] or 0),
        int(row["invalid_without_cause"] or 0),
        int(row["unknown"] or 0),
        int(row["value_problem"] or 0),
        differs,
        int(not row_count_is(stored, arterial(waveform).count())),
    )


def event_facts(stored: Any, series: Any, clinical: Any, split: Any) -> EventFacts:
    """The facts of the event table, each a flag or a fact that is only tested for being positive."""
    f = functions()
    onset, last, duration = f.col("onset_second"), f.col("last_low_second"), f.col("duration_seconds")
    times = (
        (f.col("onset_us") != (onset + 1) * SECOND_US)
        | (f.col("confirmed_us") != (onset + EVENT_SECONDS) * SECOND_US)
        | (f.col("end_us") != (last + 1) * SECOND_US)
        | (duration != last - onset + 1)
    )
    twice = stored.groupBy(*EVENT_KEY).agg(f.count(f.lit(1)).alias("n")).filter(f.col("n") > 1)
    columns = tuple((name, kind) for name, kind in HYPOTENSION_EVENTS.columns)
    left, right = typed(stored, columns), typed(events_frame(series, clinical, split), columns)
    development = development_cases(clinical, split).select("case_id")
    return EventFacts(
        exists(twice),
        exists(stored.filter(duration < EVENT_SECONDS)),
        exists(stored.filter(times)),
        exists(left.exceptAll(right)) or exists(right.exceptAll(left)),
        exists(stored.join(development, "case_id", "left_anti")),
    )
