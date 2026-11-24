"""The label of a stay in intensive care after surgery, from the release's ``icu_days`` (analysis plan, "Stay in intensive care"; analysis decision 8).

For a recorded, valid ``icu_days`` a value above zero is a recorded stay and a valid zero is no recorded stay: a fraction of a day is a stay. A missing value is
unknown, and so is an invalid one: a negative number or a number that is not finite. A missing value is never read as zero. The module uses the standard
library only and imports no Spark.

**Boundary rules.** Each is a named constant with its status (``BOUNDARY_STATUS``). A zero is a valid value and a value above ``STAY_ABOVE`` is a stay (decided
by the plan, analysis decision 8). A negative value, a value that is not finite and, if the owner decides it, a fraction are invalid: these are the lead's
assumption in the owner's unattended run, for the owner's review.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from features.label_values import NEGATIVE, POSITIVE, UNKNOWN

PLAN = "decided by the plan"
LEAD_ASSUMPTION = "lead assumption in the owner's unattended run, for the owner's review"
# Decided by the plan (analysis decision 8): a value above zero is a stay and zero is no stay.
STAY_ABOVE = 0.0
# Lead assumption (Jev A 1.0): a value below zero or not finite is invalid, and a fraction of a day is a valid value.
INVALID_BELOW = 0.0
FRACTIONS_ARE_VALID = True
BOUNDARY_STATUS = {"STAY_ABOVE": PLAN, "INVALID_BELOW": LEAD_ASSUMPTION, "FRACTIONS_ARE_VALID": LEAD_ASSUMPTION}
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
    return math.isfinite(icu_days) and icu_days >= INVALID_BELOW and (FRACTIONS_ARE_VALID or icu_days == math.floor(icu_days))


def icu_label(icu_days: float | None) -> IcuLabel:
    if icu_days is None:
        return IcuLabel(UNKNOWN, False, MISSING, None)
    if not is_valid(icu_days):
        return IcuLabel(UNKNOWN, False, INVALID, icu_days)
    return IcuLabel(POSITIVE if icu_days > STAY_ABOVE else NEGATIVE, True, RECORDED, icu_days)
