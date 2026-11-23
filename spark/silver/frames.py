"""The Spark frames of the Silver signal tables, from Bronze, and the counts that the checks read.

PySpark is installed in the job image only, so it is imported by name inside the functions and the rest of the sources are checked without it. Spark runs
Python only to call the one definition of validity: ``features.signal_validity`` decides every verdict, through the three row functions below, so the batch
and the stream paths cannot differ. Everything else is Spark SQL. Spark 4 runs with ANSI mode on, so an element of an empty array is read with
``try_element_at`` and a sum that could pass a long is taken as a decimal.
"""

from __future__ import annotations

import importlib
import math
from functools import reduce
from operator import or_
from typing import Any

from features.signal_validity import FIRST_OCCURRENCE, INVALID_CAUSES, numeric_verdict, physical_samples, waveform_verdict
from spark.silver.checks import VALID_LABEL, BronzeFacts, NumericFacts, WaveformFacts
from spark.silver.run import Fingerprint
from spark.silver.tables import NUMERIC, WAVEFORM

NUMERIC_KEY = ("case_id", "track", "case_time_us")
WAVEFORM_KEY = ("case_id", "track", "case_second")
TRACK_KEY = ("case_id", "track")
DIGEST_TYPE = "decimal(38,0)"


def functions() -> Any:
    return importlib.import_module("pyspark.sql.functions")


def types() -> Any:
    return importlib.import_module("pyspark.sql.types")


def window() -> Any:
    return importlib.import_module("pyspark.sql.window").Window


def numeric_row(track: str | None, value: float | None) -> tuple[bool, str | None]:
    verdict = numeric_verdict(track or "", value)
    return verdict.valid, verdict.cause


def physical_row(raw: list[float | None] | None, sample_format: int | None, gain: float | None, offset: float | None) -> list[float | None] | None:
    """The physical samples of a raw second. A second with no array, or a track with no format, gain or offset, stays missing."""
    if raw is None or sample_format is None or gain is None or offset is None:
        return None
    return physical_samples(raw, int(sample_format), gain, offset)


def second_row(
    track: str | None, samples: list[float | None] | None, rate: float | None, offset_us: int | None, previous_last: float | None
) -> tuple[bool, str | None]:
    verdict = waveform_verdict(track or "", samples or [], math.nan if rate is None else rate, -1 if offset_us is None else offset_us, previous_last)
    return verdict.valid, verdict.cause


def verdict_type() -> Any:
    t = types()
    return t.StructType([t.StructField("valid", t.BooleanType()), t.StructField("cause", t.StringType())])


def numeric_udf() -> Any:
    return functions().udf(numeric_row, verdict_type()).asNondeterministic()


def physical_udf() -> Any:
    return functions().udf(physical_row, types().ArrayType(types().DoubleType())).asNondeterministic()


def second_udf() -> Any:
    return functions().udf(second_row, verdict_type()).asNondeterministic()


def typed(frame: Any, columns: tuple[tuple[str, str], ...]) -> Any:
    """The frame with exactly the columns of a table, in order, each cast to its declared type."""
    f = functions()
    return frame.select(*[f.col(name).cast(kind).alias(name) for name, kind in columns])


def numeric_frame(bronze: Any) -> Any:
    """``silver.numeric_observations`` from ``bronze.numeric_observations``: the first reading of each time, its dropped duplicates, its verdict."""
    f = functions()
    first = f.col("source_occurrence") == f.lit(FIRST_OCCURRENCE)
    dropped = bronze.filter(~first).groupBy(*NUMERIC_KEY).agg(f.sort_array(f.collect_list("source_occurrence")).alias("duplicate_source_occurrences"))
    kept = bronze.filter(first).join(dropped, list(NUMERIC_KEY), "left")
    ledger = f.coalesce(f.col("duplicate_source_occurrences"), f.array().cast("array<int>"))
    judged = kept.withColumn("duplicate_source_occurrences", ledger).withColumn("_verdict", numeric_udf()(f.col("track"), f.col("value")))
    return typed(judged.withColumn("valid", f.col("_verdict.valid")).withColumn("invalid_cause", f.col("_verdict.cause")), NUMERIC.columns)


def judged(frame: Any) -> Any:
    """The frame with a ``_verdict`` of each second.

    The sample before a second's first is the last sample of the second before it, when that is the second ``s - 1``.

    The last samples are taken on a narrow frame, so the sort that the window needs never holds a second's samples.
    """
    f = functions()
    ordered = window().partitionBy(*TRACK_KEY).orderBy("case_second")
    last = f.try_element_at(f.col("samples"), f.lit(-1))
    narrow = frame.select(*WAVEFORM_KEY, last.alias("_last")).withColumns(
        {"_before_second": f.lag("case_second").over(ordered), "_before_last": f.lag("_last").over(ordered)}
    )
    previous = f.when(f.col("_before_second") == f.col("case_second") - f.lit(1), f.col("_before_last")).alias("_previous_last")
    before = narrow.select(*WAVEFORM_KEY, previous)
    joined = frame.join(before, list(WAVEFORM_KEY), "left")
    verdict = second_udf()(f.col("track"), f.col("samples"), f.col("sample_rate"), f.col("start_offset_us"), f.col("_previous_last"))
    return joined.withColumn("_verdict", verdict)


def waveform_frame(bronze: Any, tracks: Any) -> Any:
    """``silver.waveform_seconds`` from ``bronze.waveform_seconds`` and the rate, format, gain and offset of each track."""
    f = functions()
    wanted = tracks.select(*TRACK_KEY, "sample_rate", "sample_format", "gain", "value_offset")
    raw = bronze.join(wanted, list(TRACK_KEY), "left")
    physical = raw.withColumn("samples", physical_udf()(f.col("samples"), f.col("sample_format"), f.col("gain"), f.col("value_offset")))
    count = f.coalesce(f.size("samples"), f.lit(0))
    decided = judged(physical.withColumn("sample_count", count))
    return typed(decided.withColumn("valid", f.col("_verdict.valid")).withColumn("invalid_cause", f.col("_verdict.cause")), WAVEFORM.columns)


def fingerprint(frame: Any) -> Fingerprint:
    """The number of rows and the sum of the 64-bit hash of each row, as a decimal so that the sum cannot pass a long."""
    f = functions()
    digest = f.xxhash64(*[f.col(name) for name in frame.columns]).cast(DIGEST_TYPE)
    row = frame.agg(f.count(f.lit(1)).alias("rows"), f.sum(digest).alias("digest")).first()
    return Fingerprint(int(row["rows"]), int(row["digest"] or 0))


def count_where(condition: Any) -> Any:
    f = functions()
    return f.sum(f.when(condition, f.lit(1)).otherwise(f.lit(0)))


def causes_of(frame: Any) -> dict[str, int]:
    f = functions()
    label = f.when(f.col("valid"), f.lit(VALID_LABEL)).otherwise(f.coalesce(f.col("invalid_cause"), f.lit("none")))
    return {str(row["cause"]): int(row["n"]) for row in frame.groupBy(label.alias("cause")).agg(f.count(f.lit(1)).alias("n")).collect()}


def verdict_columns(frame: Any) -> tuple[Any, Any, Any, Any, Any]:
    """The counts of a verdict that is not complete, for both tables.

    They are the rows with no verdict, a valid row with a cause, an invalid row with none, a cause that is not one, and a stored verdict that is not the
    recomputed ``_verdict``.
    """
    f = functions()
    valid, cause, again = f.col("valid"), f.col("invalid_cause"), f.col("_verdict")
    same = again["valid"].eqNullSafe(valid) & again["cause"].eqNullSafe(cause)
    return (
        count_where(valid.isNull()),
        count_where(valid & cause.isNotNull()),
        count_where(~valid & cause.isNull()),
        count_where(~valid & cause.isNotNull() & ~cause.isin(*INVALID_CAUSES)),
        count_where(~same),
    )


def numeric_facts(frame: Any) -> NumericFacts:
    f = functions()
    again = frame.withColumn("_verdict", numeric_udf()(f.col("track"), f.col("value")))
    key = f.struct(*[f.col(name) for name in NUMERIC_KEY])
    ledger = f.coalesce(f.size("duplicate_source_occurrences"), f.lit(0)).cast("long")
    row = again.agg(
        f.count(f.lit(1)).alias("rows"),
        (f.count(f.lit(1)) - f.countDistinct(key)).alias("duplicate_keys"),
        f.sum(ledger).alias("ledger_total"),
        count_where(f.col("source_occurrence") != f.lit(FIRST_OCCURRENCE)).alias("not_first"),
        *[
            column.alias(name)
            for column, name in zip(verdict_columns(again), ("null", "valid_with_cause", "invalid_without_cause", "unknown", "differs"), strict=True)
        ],
    ).first()
    return NumericFacts(
        int(row["rows"]),
        int(row["duplicate_keys"]),
        int(row["ledger_total"] or 0),
        int(row["not_first"] or 0),
        int(row["null"] or 0),
        int(row["valid_with_cause"] or 0),
        int(row["invalid_without_cause"] or 0),
        int(row["unknown"] or 0),
        int(row["differs"] or 0),
        causes_of(frame),
    )


def waveform_facts(frame: Any) -> WaveformFacts:
    f = functions()
    again = judged(frame)
    key = f.struct(*[f.col(name) for name in WAVEFORM_KEY])
    stored = f.coalesce(f.size("samples"), f.lit(0))
    row = again.agg(
        f.count(f.lit(1)).alias("rows"),
        (f.count(f.lit(1)) - f.countDistinct(key)).alias("duplicate_keys"),
        count_where(~f.col("sample_count").eqNullSafe(stored)).alias("shape"),
        *[
            column.alias(name)
            for column, name in zip(verdict_columns(again), ("null", "valid_with_cause", "invalid_without_cause", "unknown", "differs"), strict=True)
        ],
    ).first()
    return WaveformFacts(
        int(row["rows"]),
        int(row["duplicate_keys"]),
        int(row["null"] or 0),
        int(row["valid_with_cause"] or 0),
        int(row["invalid_without_cause"] or 0),
        int(row["unknown"] or 0),
        int(row["shape"] or 0),
        int(row["differs"] or 0),
        causes_of(frame),
    )


def null_rows(frame: Any, columns: tuple[str, ...]) -> int:
    """The number of rows of a frame with a null in one of the columns."""
    f = functions()
    return int(frame.filter(reduce(or_, [f.col(name).isNull() for name in columns])).count())


def bronze_facts(numeric: Any, waveform: Any, tracks: Any) -> BronzeFacts:
    """The counts of ``bronze_problems`` from the three Bronze tables."""
    f = functions()

    def present(kind: str) -> Any:
        return tracks.filter(f.col("kind") == f.lit(kind)).select(*TRACK_KEY)

    per_time = numeric.groupBy(*NUMERIC_KEY).agg(
        f.count(f.lit(1)).alias("n"), f.min("source_occurrence").alias("low"), f.max("source_occurrence").alias("high")
    )
    gaps = per_time.filter((f.col("low") != f.lit(FIRST_OCCURRENCE)) | (f.col("high") != f.col("n") - f.lit(1))).count()
    twice = waveform.groupBy(*WAVEFORM_KEY).agg(f.count(f.lit(1)).alias("n")).agg(f.sum(f.col("n") - f.lit(1)).alias("extra")).first()["extra"]
    doubled = tracks.groupBy(*TRACK_KEY).agg(f.count(f.lit(1)).alias("n")).filter(f.col("n") > 1).count()
    return BronzeFacts(
        numeric.count(),
        waveform.count(),
        null_rows(numeric, (*NUMERIC_KEY, "source_occurrence")),
        gaps,
        numeric.join(present("numeric"), list(TRACK_KEY), "left_anti").count(),
        null_rows(waveform, WAVEFORM_KEY),
        int(twice or 0),
        waveform.join(present("waveform"), list(TRACK_KEY), "left_anti").count(),
        doubled,
    )
