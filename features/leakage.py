"""The leakage check: a row may hold nothing from after its cutoff and no field the plan forbids."""

from __future__ import annotations

from features.model import FeatureRow
from features.registry import is_registered

FORBIDDEN_FIELDS = frozenset({"opdur", "anedur", "caseend", "opend", "aneend", "dis", "los_postop", "los_icu", "death_inhosp"})
FORBIDDEN_PREFIXES = ("intraop_",)


class LeakageError(ValueError):
    """A feature row that breaks the leakage check."""


def row_problems(row: FeatureRow) -> list[str]:
    """Every way the row breaks the check, one text each. An empty list means it passes."""
    problems: list[str] = []
    if row.latest_reading_us is not None and row.latest_reading_us > row.cutoff_us:
        problems.append(f"the latest reading time {row.latest_reading_us} is after the cutoff {row.cutoff_us}")
    if row.latest_clinical_us is not None and row.latest_clinical_us > row.cutoff_us:
        problems.append(f"a clinical availability time {row.latest_clinical_us} is after the cutoff {row.cutoff_us}")
    for name in row.values:
        if name in FORBIDDEN_FIELDS or name.startswith(FORBIDDEN_PREFIXES):
            problems.append(f"the forbidden field {name} is among the features")
        elif not is_registered(name):
            problems.append(f"the unknown field {name} is among the features")
    return problems
