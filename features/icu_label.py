"""The label of a stay in intensive care after surgery, from the release's ``icu_days`` (analysis plan, "Stay in intensive care"; analysis decision 8).

For a recorded, valid ``icu_days`` a value above zero is a recorded stay and a valid zero is no recorded stay: a fraction of a day is a stay. A missing value is
unknown, and so is an invalid one: a negative number or a number that is not finite. A missing value is never read as zero. The module uses the standard
library only and imports no Spark.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from features.label_values import NEGATIVE, POSITIVE, UNKNOWN

RECORDED = "recorded"
MISSING = "missing"
INVALID = "invalid"
REASONS = (RECORDED, MISSING, INVALID)


@dataclass(frozen=True, slots=True)
class IcuLabel:
    """The label of one case, its eligibility, why, and the value that was read."""

    label: str
    eligible: bool
    reason: str
    icu_days: float | None


def is_valid(icu_days: float) -> bool:
    return math.isfinite(icu_days) and icu_days >= 0


def icu_label(icu_days: float | None) -> IcuLabel:
    if icu_days is None:
        return IcuLabel(UNKNOWN, False, MISSING, None)
    if not is_valid(icu_days):
        return IcuLabel(UNKNOWN, False, INVALID, icu_days)
    return IcuLabel(POSITIVE if icu_days > 0 else NEGATIVE, True, RECORDED, icu_days)
