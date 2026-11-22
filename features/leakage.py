"""The leakage check: a row may hold nothing from after its cutoff and no field the plan forbids."""

from __future__ import annotations

from features.model import FeatureRow
from features.registry import is_registered

# The fields that are never features, by the column names of the pinned release: the end times, the stay in intensive care, the death in hospital and every
# intraoperative total. A duration or a length of stay derived from them has no name in the release, so it is not a feature of the registry and is refused as
# unregistered.
FORBIDDEN_FIELDS = frozenset({"caseend", "opend", "aneend", "dis", "icu_days", "death_inhosp"})
FORBIDDEN_PREFIXES = ("intraop_",)


def is_forbidden(name: str) -> bool:
    return name in FORBIDDEN_FIELDS or name.startswith(FORBIDDEN_PREFIXES)


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
        if is_forbidden(name):
            problems.append(f"the forbidden field {name} is among the features")
        elif not is_registered(name):
            problems.append(f"the unknown field {name} is among the features")
    return problems
