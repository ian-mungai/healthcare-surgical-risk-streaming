"""The measure of ``icu_days`` that Silver records, from development patients only, before the intensive care label is built (analysis decision 8).

The measure holds how many development cases have no value, an invalid value and a recorded value, how many recorded values are a valid zero, above zero and a
fraction of a day, and the least, the greatest and the percentiles (nearest rank) of the recorded values. It is written as the table
``silver.icu_days_measure`` before ``silver.icu_labels``. The module imports no Spark.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

from features.icu_label import is_valid
from spark.silver_cases import PERCENTILES, SilverCasesError, unit

UNITS = ("cases", "days")


@dataclass(frozen=True, slots=True)
class IcuMeasureRow:
    statistic: str
    unit: str
    value: float


def nearest_rank_of(values: Sequence[float], percent: int) -> float:
    """The value of the sorted ``values`` at rank ``ceil(percent * n / 100)`` (counting from 1)."""
    rank = -(-percent * len(values) // 100)
    return sorted(values)[max(rank, 1) - 1]


def icu_measure_rows(values: Sequence[float | None]) -> list[IcuMeasureRow]:
    """The measured values of ``icu_days`` of the development cases, one value (or ``None``) for each case. The order of ``values`` does not matter."""
    if not values:
        raise SilverCasesError(f"no_development_cases: {unit(0, 'case')}")
    recorded = sorted(value for value in values if value is not None and is_valid(value))
    missing = sum(1 for value in values if value is None)
    rows = [
        IcuMeasureRow("cases", "cases", len(values)),
        IcuMeasureRow("missing", "cases", missing),
        IcuMeasureRow("invalid", "cases", len(values) - missing - len(recorded)),
        IcuMeasureRow("recorded", "cases", len(recorded)),
        IcuMeasureRow("zero", "cases", sum(1 for value in recorded if value == 0)),
        IcuMeasureRow("above_zero", "cases", sum(1 for value in recorded if value > 0)),
        IcuMeasureRow("fractional", "cases", sum(1 for value in recorded if value != math.floor(value))),
    ]
    if not recorded:
        return rows
    rows += [IcuMeasureRow("min", "days", recorded[0])]
    rows += [IcuMeasureRow(f"p{percent}", "days", nearest_rank_of(recorded, percent)) for percent in PERCENTILES]
    return [*rows, IcuMeasureRow("max", "days", recorded[-1])]
