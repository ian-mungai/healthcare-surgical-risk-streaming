"""The facts known before surgery, each used only when its availability time is at or before the cutoff."""

from __future__ import annotations

from collections.abc import Mapping

from features.model import ClinicalValue, FeatureValue, GroupResult
from features.registry import CLINICAL_FIELDS, PREOP_PREFIX, SENSITIVITY_FIELDS


def clinical_group(clinical: Mapping[str, ClinicalValue], cutoff_us: int, include_sensitivity: bool) -> GroupResult:
    """The fixed fields (and the sensitivity fields when asked for) always appear. A ``preop_`` field appears when the table holds it.

    A field that is not on the plan's list, and every forbidden field, is never read.
    """
    names = list(CLINICAL_FIELDS) + (list(SENSITIVITY_FIELDS) if include_sensitivity else [])
    names += sorted(name for name in clinical if name.startswith(PREOP_PREFIX) and len(name) > len(PREOP_PREFIX))
    values: dict[str, FeatureValue] = {}
    used: list[int] = []
    for name in names:
        item = clinical.get(name)
        available = item is not None and item.available_us is not None and item.available_us <= cutoff_us
        values[name] = item.value if item is not None and available else None
        if item is not None and available and item.available_us is not None:
            used.append(item.available_us)
    return GroupResult(values, max(used) if used else None)
