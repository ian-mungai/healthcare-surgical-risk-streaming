"""The facts known before surgery, each used only when its availability time is at or before the cutoff."""

from __future__ import annotations

import math
from collections.abc import Mapping

from features.model import ClinicalValue, FeatureInputError, FeatureValue, GroupResult
from features.registry import CLINICAL_FIELDS, PREOP_PREFIX, SENSITIVITY_FIELDS
from features.validate import whole_microseconds


def clinical_value(name: str, value: object) -> FeatureValue:
    """A number as a float and a text as a text. A boolean, a list, and a number that is not finite are refused with the field."""
    if isinstance(value, str) or value is None:
        return value
    if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(value):
        raise FeatureInputError(f"{name}: a clinical value is not a finite number or a text")
    return float(value)


def clinical_group(clinical: Mapping[str, ClinicalValue], cutoff_us: int, include_sensitivity: bool) -> GroupResult:
    """The fixed fields (and the sensitivity fields when asked for) always appear. A ``preop_`` field appears when the table holds it.

    A field that is not on the plan's list, and every forbidden field, is never read. A listed field is checked whether or not it is available.
    """
    whole_microseconds(cutoff_us, "the cutoff")
    names = list(CLINICAL_FIELDS) + (list(SENSITIVITY_FIELDS) if include_sensitivity else [])
    names += sorted(name for name in clinical if name.startswith(PREOP_PREFIX) and len(name) > len(PREOP_PREFIX))
    values: dict[str, FeatureValue] = {}
    used: list[int] = []
    for name in names:
        item = clinical.get(name)
        if item is None:
            values[name] = None
            continue
        value = clinical_value(name, item.value)
        available_us = None if item.available_us is None else whole_microseconds(item.available_us, f"{name}: the availability time")
        if available_us is not None and available_us <= cutoff_us:
            values[name] = value
            used.append(available_us)
        else:
            values[name] = None
    return GroupResult(values, max(used) if used else None)
