"""The facts known before surgery, each used only when its availability time is at or before the cutoff."""

from __future__ import annotations

import math
from collections.abc import Mapping

from features.leakage import is_forbidden
from features.model import ClinicalValue, FeatureInputError, FeatureValue, GroupResult
from features.registry import CLINICAL_FIELDS, PREOP_COLUMNS, SENSITIVITY_FIELDS
from features.validate import number, whole_microseconds


def clinical_value(name: str, value: object) -> FeatureValue:
    """A number as a float and a text as a text. A boolean, a list, and a number that is not finite are refused with the field."""
    if isinstance(value, str) or value is None:
        return value
    result = number(value, f"{name}: a clinical value")
    if not math.isfinite(result):
        raise FeatureInputError(f"{name}: a clinical value is not a finite number or a text")
    return result


def clinical_group(clinical: Mapping[str, ClinicalValue], cutoff_us: int, include_sensitivity: bool) -> GroupResult:
    """Every clinical feature of the registry, in its order: the fixed fields, the pre-operative columns, and the sensitivity fields when asked for.

    The keys come from the registry only. A field that is absent, unavailable at the cutoff or without an availability time is missing. A field of the input
    that is forbidden, or that is neither in the registry nor forbidden, is refused by name and never read. A field of the registry is checked when it is in the
    output, whether or not it is available.
    """
    whole_microseconds(cutoff_us, "the cutoff")
    names = [*CLINICAL_FIELDS, *PREOP_COLUMNS, *SENSITIVITY_FIELDS]
    for name in clinical:
        if is_forbidden(name):
            raise FeatureInputError(f"{name}: a forbidden clinical field")
        if name not in names:
            raise FeatureInputError(f"{name}: a clinical field that is neither in the registry nor forbidden")
    values: dict[str, FeatureValue] = {}
    used: list[int] = []
    for name in (*CLINICAL_FIELDS, *PREOP_COLUMNS, *(SENSITIVITY_FIELDS if include_sensitivity else ())):
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
