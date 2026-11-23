"""The clinical facts of a case, read from one JSON file: ``{"cases": {"<case number>": {"<field>": {"value": ..., "available_us": ...}}}}``.

Each fact has the value and the case time at which it became known, or null when that time is not established, which the feature code reads as
missing. The names of the fields are checked by the feature code against its registry. With no file every clinical feature is missing. Silver is
the later source of these facts.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from features.document import load
from features.model import ClinicalValue, FeatureInputError
from spark.streaming.model import ClinicalLookup


class ClinicalFileError(ValueError):
    """The clinical file cannot be read or does not follow its form."""


def entry(value: object, path: str) -> ClinicalValue:
    if not isinstance(value, dict) or set(value) != {"value", "available_us"}:
        raise ClinicalFileError(f"{path} must be an object with exactly value and available_us")
    available = value["available_us"]
    if available is not None and (isinstance(available, bool) or not isinstance(available, int)):
        raise ClinicalFileError(f"{path}: available_us must be a whole number or null")
    return ClinicalValue(value["value"], available)


def load_clinical(path: str) -> ClinicalLookup:
    """The lookup of a case number to its facts. An empty ``path`` gives a lookup with no facts."""
    if not path:
        return lambda case_id: {}
    try:
        document: Any = load(Path(path).read_text())
    except (OSError, ValueError, FeatureInputError) as error:
        raise ClinicalFileError(f"the clinical file cannot be read: {error}") from None
    if not isinstance(document, dict) or set(document) != {"cases"} or not isinstance(document["cases"], dict):
        raise ClinicalFileError("the clinical file must be an object with the one key cases")
    cases: dict[int, dict[str, ClinicalValue]] = {}
    for name, facts in document["cases"].items():
        if not name.isdigit() or not isinstance(facts, dict):
            raise ClinicalFileError(f"cases.{name} must be a case number with an object of facts")
        cases[int(name)] = {field: entry(fact, f"cases.{name}.{field}") for field, fact in facts.items()}

    def lookup(case_id: int) -> Mapping[str, ClinicalValue]:
        return cases.get(case_id, {})

    return lookup
