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

from features.signal_validity import (
    FIRST_OCCURRENCE,
    FLAT_WINDOW_SECONDS,
    INVALID_CAUSES,
    Extent,
    numeric_verdict,
    physical_samples,
    second_extent,
    waveform_verdict,
)
from spark.bronze.quality import row_count_at_least, row_count_is
from spark.silver.checks import BronzeFacts, NumericFacts, WaveformFacts
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


def extent_row(samples: list[float | None] | None, rate: float | None, offset_us: int | None) -> tuple[int, float | None, float | None]:
    """The complete seconds (0 or 1), least and most of the valid samples of one second (``second_extent``)."""
    extent = second_extent(samples or [], math.nan if rate is None else rate, -1 if offset_us is None else offset_us)
    return extent.seconds, extent.low, extent.high


def second_row(
    track: str | None,
    samples: list[float | None] | None,
    rate: float | None,
    offset_us: int | None,
    previous_last: float | None,
    prior_seconds: int | None,
    prior_low: float | None,
    prior_high: float | None,
) -> tuple[bool, str | None]:
    """The verdict of one second. The prior columns are the extent of the valid samples of the 19 seconds before it: nothing when there are none."""
    prior = Extent(int(prior_seconds or 0), prior_low, prior_high)
    rate = math.nan if rate is None else rate
    verdict = waveform_verdict(track or "", samples or [], rate, -1 if offset_us is None else offset_us, previous_last, prior)
    return verdict.valid, verdict.cause


def verdict_type() -> Any:
    t = types()
    return t.StructType([t.StructField("valid", t.BooleanType()), t.StructField("cause", t.StringType())])


def numeric_udf() -> Any:
    return functions().udf(numeric_row, verdict_type()).asNondeterministic()


def physical_udf() -> Any:
    return functions().udf(physical_row, types().ArrayType(types().DoubleType())).asNondeterministic()


def extent_udf() -> Any:
    t = types()
    fields = [t.StructField("seconds", t.LongType()), t.StructField("low", t.DoubleType()), t.StructField("high", t.DoubleType())]
    return functions().udf(extent_row, t.StructType(fields)).asNondeterministic()


def second_udf() -> Any:
    return functions().udf(second_row, verdict_type()).asNondeterministic()


def typed(frame: Any, columns: tuple[tuple[str, str], ...]) -> Any:
    """The frame with exactly the columns of a table, in order, each cast to its declared type."""
    f = functions()
    return frame.select(*[f.col(name).cast(kind).alias(name) for name, kind in columns])


def numeric_kept(bronze: Any) -> Any:
    """The first reading of each time from ``bronze.numeric_observations``, with the positions of its dropped duplicates (the ledger) and no verdict."""
    f = functions()
    first = f.col("source_occurrence") == f.lit(FIRST_OCCURRENCE)
    dropped = bronze.filter(~first).groupBy(*NUMERIC_KEY).agg(f.sort_array(f.collect_list("source_occurrence")).alias("duplicate_source_occurrences"))
    kept = bronze.filter(first).join(dropped, list(NUMERIC_KEY), "left")
    ledger = f.coalesce(f.col("duplicate_source_occurrences"), f.array().cast("array<int>"))
    return kept.withColumn("duplicate_source_occurrences", ledger)


def numeric_frame(bronze: Any) -> Any:
    """``silver.numeric_observations`` from ``bronze.numeric_observations``: the first reading of each time, its dropped duplicates, its verdict."""
    f = functions()
    judged = numeric_kept(bronze).withColumn("_verdict", numeric_udf()(f.col("track"), f.col("value")))
    return typed(judged.withColumn("valid", f.col("_verdict.valid")).withColumn("invalid_cause", f.col("_verdict.cause")), NUMERIC.columns)


def judged(frame: Any) -> Any:
    """The frame with a ``_verdict`` of each second.

    The sample before a second's first is the last sample of the second before it, when that is the second ``s - 1``. The segment of the flat
    rule is the 20 seconds that end with the second, so the extent of the complete seconds ``s - 19`` to ``s - 1`` is taken over a window of
    second numbers (a second that is absent adds nothing, and a second counts only when it is complete) and the second's own extent is joined to
    it by ``waveform_verdict``. The last samples and the extents are taken on a narrow frame, so the sort that the windows need never holds a
    second's samples.
    """
    f = functions()
    ordered = window().partitionBy(*TRACK_KEY).orderBy("case_second")
    trailing = ordered.rangeBetween(-(FLAT_WINDOW_SECONDS - 1), -1)
    last = f.try_element_at(f.col("samples"), f.lit(-1))
    staged = frame.select(*WAVEFORM_KEY, last.alias("_last"), extent_udf()(f.col("samples"), f.col("sample_rate"), f.col("start_offset_us")).alias("_own"))
    narrow = staged.select(
        *WAVEFORM_KEY, "_last", f.col("_own.seconds").alias("_seconds"), f.col("_own.low").alias("_low"), f.col("_own.high").alias("_high")
    ).withColumns(
        {
            "_before_second": f.lag("case_second").over(ordered),
            "_before_last": f.lag("_last").over(ordered),
            "_prior_seconds": f.coalesce(f.sum("_seconds").over(trailing), f.lit(0)),
            "_prior_low": f.min("_low").over(trailing),
            "_prior_high": f.max("_high").over(trailing),
        }
    )
    previous = f.when(f.col("_before_second") == f.col("case_second") - f.lit(1), f.col("_before_last")).alias("_previous_last")
    before = narrow.select(*WAVEFORM_KEY, previous, "_prior_seconds", "_prior_low", "_prior_high")
    joined = frame.join(before, list(WAVEFORM_KEY), "left")
    arguments = ["track", "samples", "sample_rate", "start_offset_us", "_previous_last", "_prior_seconds", "_prior_low", "_prior_high"]
    return joined.withColumn("_verdict", second_udf()(*[f.col(name) for name in arguments]))


def waveform_physical(bronze: Any, tracks: Any) -> Any:
    """The seconds of ``bronze.waveform_seconds`` with the rate of each track and the physical samples made from the raw ones, and no verdict."""
    f = functions()
    wanted = tracks.select(*TRACK_KEY, "sample_rate", "sample_format", "gain", "value_offset")
    raw = bronze.join(wanted, list(TRACK_KEY), "left")
    physical = raw.withColumn("samples", physical_udf()(f.col("samples"), f.col("sample_format"), f.col("gain"), f.col("value_offset")))
    return physical.withColumn("sample_count", f.coalesce(f.size("samples"), f.lit(0)))


def waveform_frame(bronze: Any, tracks: Any) -> Any:
    """``silver.waveform_seconds`` from ``bronze.waveform_seconds`` and the rate, format, gain and offset of each track."""
    f = functions()
    decided = judged(waveform_physical(bronze, tracks))
    return typed(decided.withColumn("valid", f.col("_verdict.valid")).withColumn("invalid_cause", f.col("_verdict.cause")), WAVEFORM.columns)


def fingerprint(frame: Any) -> Fingerprint:
    """The number of rows and the sum of the 64-bit hash of each row, as a decimal so that the sum cannot pass a long.

    A row is hashed as its JSON text, which keeps the place of every null in an array: Spark's own hash of an array skips its nulls, so ``[null, 80]`` and
    ``[80, null]`` would hash alike. Fields that are null are written, so that a null and an absent field differ too.
    """
    f = functions()
    text = f.to_json(f.struct(*[f.col(name) for name in frame.columns]), {"ignoreNullFields": "false"})
    digest = f.xxhash64(text).cast(DIGEST_TYPE)
    row = frame.agg(f.count(f.lit(1)).alias("rows"), f.sum(digest).alias("digest")).first()
    return Fingerprint(int(row["rows"]), int(row["digest"] or 0))


def count_where(condition: Any) -> Any:
    f = functions()
    return f.sum(f.when(condition, f.lit(1)).otherwise(f.lit(0)))


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


def source_differences(stored: Any, expected: Any, columns: tuple[tuple[str, str], ...]) -> int:
    """The rows of the Bronze-made ``expected`` frame that no stored row equals, and the stored rows that no expected row equals, over the columns that do not
    hold a verdict. A row is equal when each of its columns is, an array element by element with its nulls in place and NaN equal to NaN."""
    names = [name for name, _ in columns if name not in ("valid", "invalid_cause")]
    left, right = typed(stored, tuple((n, k) for n, k in columns if n in names)), typed(expected, tuple((n, k) for n, k in columns if n in names))
    return int(left.exceptAll(right).count()) + int(right.exceptAll(left).count())


def numeric_facts(frame: Any, bronze: Any) -> NumericFacts:
    f = functions()
    again = frame.withColumn("_verdict", numeric_udf()(f.col("track"), f.col("value")))
    key = f.struct(*[f.col(name) for name in NUMERIC_KEY])
    ledger = f.coalesce(f.size("duplicate_source_occurrences"), f.lit(0)).cast("long")
    row = again.agg(
        (f.count(f.lit(1)) - f.countDistinct(key)).alias("duplicate_keys"),
        f.sum(ledger).alias("ledger_total"),
        count_where(f.col("source_occurrence") != f.lit(FIRST_OCCURRENCE)).alias("not_first"),
        *[
            column.alias(name)
            for column, name in zip(verdict_columns(again), ("null", "valid_with_cause", "invalid_without_cause", "unknown", "differs"), strict=True)
        ],
    ).first()
    every_reading = row_count_is(frame, bronze.count() - int(row["ledger_total"] or 0))
    return NumericFacts(
        int(row["duplicate_keys"]),
        int(row["not_first"] or 0),
        int(row["null"] or 0),
        int(row["valid_with_cause"] or 0),
        int(row["invalid_without_cause"] or 0),
        int(row["unknown"] or 0),
        int(row["differs"] or 0),
        source_differences(frame, numeric_kept(bronze), NUMERIC.columns),
        int(not every_reading),
    )


def waveform_facts(frame: Any, bronze: Any, tracks: Any) -> WaveformFacts:
    f = functions()
    again = judged(frame)
    key = f.struct(*[f.col(name) for name in WAVEFORM_KEY])
    stored = f.coalesce(f.size("samples"), f.lit(0))
    row = again.agg(
        (f.count(f.lit(1)) - f.countDistinct(key)).alias("duplicate_keys"),
        count_where(~f.col("sample_count").eqNullSafe(stored)).alias("shape"),
        *[
            column.alias(name)
            for column, name in zip(verdict_columns(again), ("null", "valid_with_cause", "invalid_without_cause", "unknown", "differs"), strict=True)
        ],
    ).first()
    return WaveformFacts(
        int(row["duplicate_keys"]),
        int(row["null"] or 0),
        int(row["valid_with_cause"] or 0),
        int(row["invalid_without_cause"] or 0),
        int(row["unknown"] or 0),
        int(row["shape"] or 0),
        int(row["differs"] or 0),
        source_differences(frame, waveform_physical(bronze, tracks), WAVEFORM.columns),
        int(not row_count_is(frame, bronze.count())),
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

    # The positions at one time are exactly 0 to n - 1 once each: n rows, n distinct positions, the least 0 and the most n - 1.
    per_time = numeric.groupBy(*NUMERIC_KEY).agg(
        f.count(f.lit(1)).alias("n"),
        f.countDistinct("source_occurrence").alias("distinct"),
        f.min("source_occurrence").alias("low"),
        f.max("source_occurrence").alias("high"),
    )
    skewed = (f.col("distinct") != f.col("n")) | (f.col("low") != f.lit(FIRST_OCCURRENCE)) | (f.col("high") != f.col("n") - f.lit(1))
    gaps = per_time.filter(skewed).count()
    twice = waveform.groupBy(*WAVEFORM_KEY).agg(f.count(f.lit(1)).alias("n")).agg(f.sum(f.col("n") - f.lit(1)).alias("extra")).first()["extra"]
    doubled = tracks.groupBy(*TRACK_KEY).agg(f.count(f.lit(1)).alias("n")).filter(f.col("n") > 1).count()
    return BronzeFacts(
        int(not (row_count_at_least(numeric, 1) or row_count_at_least(waveform, 1))),
        null_rows(numeric, (*NUMERIC_KEY, "source_occurrence")),
        gaps,
        numeric.join(present("numeric"), list(TRACK_KEY), "left_anti").count(),
        null_rows(waveform, WAVEFORM_KEY),
        int(twice or 0),
        waveform.join(present("waveform"), list(TRACK_KEY), "left_anti").count(),
        doubled,
    )
