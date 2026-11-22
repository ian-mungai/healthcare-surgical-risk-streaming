"""The checks that every input passes before it is used. Each refusal names the track or field. A number is never made from a text, a flag or a float time."""

from __future__ import annotations

import math

from features.model import FeatureInputError


def whole_microseconds(value: object, what: str) -> int:
    """A time as a whole number of microseconds. A float, a boolean and a text are refused, whatever their value."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise FeatureInputError(f"{what} is not a whole number of microseconds")
    return value


def whole_number(value: object, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise FeatureInputError(f"{what} is not a whole number")
    return value


def boolean(value: object, what: str) -> bool:
    if not isinstance(value, bool):
        raise FeatureInputError(f"{what} is not a boolean")
    return value


def number(value: object, what: str) -> float:
    """An integer or a float as a float. A boolean and a text are refused. The value may be not a number: ``finite`` checks that."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise FeatureInputError(f"{what} is not a number")
    return float(value)


def finite(value: object, what: str) -> float:
    result = number(value, what)
    if not math.isfinite(result):
        raise FeatureInputError(f"{what} is not a finite number")
    return result
