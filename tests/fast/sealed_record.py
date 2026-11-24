"""The one way an outcome runner prints or saves what a job did (owner decision D).

A runner records only exit codes, fixed codes, Boolean flags, seconds, and identifiers (snapshot IDs, hashes, reference names) that Nessie confirmed.
This module makes the only text and the only facts that may reach an observation, a saved fact or a printed line:

- ``sealed_observed`` takes Boolean flags (each exactly ``True`` or ``False``), exit codes, seconds, fixed codes, ``ConfirmedId`` values, the class of an
  error and the fixed phrase of a rejected call, and returns a ``Sealed`` text with the matching ``facts``. Any other value raises ``ValueError`` and the
  message names the field, never the value.
- ``ConfirmedId`` is made only by ``confirm_id`` and ``confirm_ids``, which compare an ID that a job reported with the value that Nessie holds. A value that
  differs, a value that Nessie does not hold, a Boolean and text that is no hash or reference name give ``None``.

The syntax check ``sink_leaks`` of ``tests/fast/outcome_accept.py`` refuses, in the six outcome runners, an f-string, ``str()``, ``repr()``, ``format``, a
raw variable or a hand-made ``ConfirmedId`` as an observation or a fact: only the output of ``sealed_observed`` reaches them.
"""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Mapping, Sequence

from spark.bronze_clinical_lab_cli import EXIT_CODES as BRONZE_CODES
from spark.silver.checks import CODES as SIGNALS_CODES
from spark.silver.hypotension_checks import CODES as HYPOTENSION_CODES
from spark.split_table import CODES as SPLIT_CODES
from tests.fast.silver_accept import REASONS, REFUSAL_REASONS, USAGE_REASONS

# The codes of the Silver cases job, which writes them inline and keeps no list.
CASES_CODES = frozenset(
    {
        "source_missing",
        "ambiguous_case_owner",
        "unparsed",
        "lab_unparsed",
        "sealed_lab_form",
        "domain",
        "lab_case_missing",
        "split_join",
        "operation_order",
        "rule_mismatch",
        "changed_cutoff",
        "changed_measure",
        "case_value_differences",
        "case_key_differences",
        "sealed_lab_differs",
        "never_feature_marking",
        "lab_rows",
        "lab_value_differences",
        "pinned_counts",
        "inherited_duplicates",
        "inherited_empty",
        "inherited_incomplete",
        "table_missing",
        "output_schema",
        "bad_length",
        "cutoffs_not_increasing",
        "no_development_cases",
        "lab_copies",
        "key_null",
    }
)
GENERIC_CODES = frozenset({"settings", "usage", "branch_missing", "catalog_error", "internal_error", "checks_failed", "branch_conflict"})
FIXED_CODES = frozenset(SPLIT_CODES) | SIGNALS_CODES | HYPOTENSION_CODES | frozenset(BRONZE_CODES) | CASES_CODES | GENERIC_CODES
FIXED_REASONS = frozenset(REASONS) | frozenset(REFUSAL_REASONS) | frozenset(USAGE_REASONS)
FLAG_NAME = re.compile(r"[A-Za-z][A-Za-z0-9 ,'()/:+_-]{0,150}")
ERROR_CLASS = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,80}")
HASH = re.compile(r"[0-9a-f]{64}")
REFERENCE_NAME = re.compile(r"[A-Za-z0-9]+(?:[_.-][A-Za-z0-9]+)+")
MAX_REFERENCE_NAME = 100
LAST_EXIT = 255
_MINT = object()


class ConfirmedId:
    """A whole snapshot ID, a 64-character lowercase hash or a reference name that equals the value Nessie holds. Only ``confirm_id`` makes one."""

    __slots__ = ("_value",)
    _value: int | str

    def __init__(self, value: int | str, token: object) -> None:
        if token is not _MINT:
            raise TypeError("a confirmed ID is made by confirm_id only")
        object.__setattr__(self, "_value", value)

    @property
    def value(self) -> int | str:
        return self._value

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("a confirmed ID cannot change")

    def __repr__(self) -> str:
        return f"ConfirmedId({self._value!r})"


def identifier(value: object) -> bool:
    """Whether the value has the form of a whole snapshot ID, a lowercase 64-character hash or a reference name."""
    if type(value) is int:
        return True
    return type(value) is str and (HASH.fullmatch(value) is not None or (len(value) <= MAX_REFERENCE_NAME and REFERENCE_NAME.fullmatch(value) is not None))


def confirm_id(reported: object, held: object) -> ConfirmedId | None:
    """The ID that a job reported, as a ``ConfirmedId``, when Nessie holds exactly it: both have the form of an ID, are of one type and are equal. ``held`` is
    what the runner read from Nessie, and is ``None`` when Nessie has no value or the read failed. Any other pair gives ``None``."""
    if held is None or type(reported) is not type(held) or not identifier(reported) or reported != held:
        return None
    return ConfirmedId(reported, _MINT)  # type: ignore[arg-type]


def confirm_reference(name: object, head: object) -> ConfirmedId | None:
    """The name of a branch or a tag that Nessie holds, as a ``ConfirmedId``: ``head`` is the hash that Nessie gave the reference when the runner read it,
    and is ``None`` when there is no such reference or the read failed."""
    if not isinstance(head, str) or HASH.fullmatch(head) is None or not isinstance(name, str) or type(name) is not str:
        return None
    return confirm_id(name, name) if REFERENCE_NAME.fullmatch(name) is not None else None


def confirm_ids(reported: object, names: Sequence[str], lookup: Callable[[str], object]) -> dict[str, ConfirmedId] | None:
    """The map of IDs that a job reported, confirmed one by one, or ``None``. The comparison is driven by the expected ``names``, never by the keys of the
    result: ``lookup`` is asked for each of them, in order, and a map with a key more, a key less or an ID that Nessie does not hold gives ``None``."""
    held = {name: lookup(name) for name in names}
    if not isinstance(reported, Mapping) or set(reported) != set(names):
        return None
    found = {name: confirm_id(reported[name], held[name]) for name in names}
    return None if any(item is None for item in found.values()) else {name: item for name, item in found.items() if item is not None}


class Sealed(str):
    """The text of one observation and the facts that go with it. Only ``sealed_observed`` makes one."""

    facts: dict[str, str]

    def __new__(cls, text: str, facts: dict[str, str], token: object) -> Sealed:
        if token is not _MINT:
            raise TypeError("a sealed text is made by sealed_observed only")
        made = super().__new__(cls, text)
        made.facts = facts
        return made

    def line(self, passed: bool, case: str, step: str) -> str:
        """The printed line of one assertion, as the record prints it."""
        return f"{'PASS' if passed else 'FAIL'} {case} ({step}): {self}\n"


def named(field: str, value: object) -> str:
    if type(value) is not str or FLAG_NAME.fullmatch(value) is None:
        raise ValueError(f"{field} has a name that is not a plain phrase")
    return value


def exit_number(name: str, value: object) -> int:
    if type(value) is not int or not -LAST_EXIT <= value <= LAST_EXIT:
        raise ValueError(f"the exit code {name} is not a whole number of a process, at most {LAST_EXIT} (a negative one is a signal)")
    return value


def sealed_observed(
    flags: Mapping[str, bool] | None = None,
    *,
    exit_code: int | None = None,
    exits: Mapping[str, int] | None = None,
    seconds: float | None = None,
    confirmed: Mapping[str, ConfirmedId] | None = None,
    codes: Sequence[str] | None = None,
    reason: str | None = None,
    error: BaseException | None = None,
) -> Sealed:
    """The observation text and facts of one assertion. Each flag is exactly ``True`` or ``False``, each code one of ``FIXED_CODES``, each ID a
    ``ConfirmedId``, each exit code a whole number of a process, ``seconds`` a finite number of at least 0, ``reason`` one of ``FIXED_REASONS`` and ``error`` an
    exception, of which only the class name is kept. Anything else raises ``ValueError`` that names the field and never the value."""
    parts: list[str] = []
    facts: dict[str, str] = {}
    if error is not None:
        if not isinstance(error, BaseException) or ERROR_CLASS.fullmatch(type(error).__name__) is None:
            raise ValueError("error is not an exception")
        parts.append(f"stopped by {type(error).__name__}")
    if reason is not None:
        if reason not in FIXED_REASONS:
            raise ValueError("reason is not one of the fixed phrases")
        parts.append(f"rejected: {reason}")
    if exit_code is not None:
        parts.append(f"exit {exit_number('exit', exit_code)}")
    for name, code in (exits or {}).items():
        parts.append(f"{named('exits', name)} {exit_number(name, code)}")
    if seconds is not None:
        if type(seconds) not in (int, float) or not math.isfinite(seconds) or seconds < 0:
            raise ValueError("seconds is not a finite number of at least 0")
        parts.append(f"after {seconds:.0f} seconds")
    if flags is not None and not isinstance(flags, Mapping):
        raise ValueError("flags is not a mapping")
    for name, value in (flags or {}).items():
        phrase = named("flags", name)
        if value is not True and value is not False:
            raise ValueError(f"the flag {phrase} is not exactly True or False")
        parts.append(f"{phrase.replace('_', ' ')} {value}")
        facts[phrase] = str(value)
    if codes is not None:
        for text in codes:
            if type(text) is not str or text not in FIXED_CODES:
                raise ValueError("codes holds a code that is not a fixed code")
        parts.append(f"codes [{', '.join(codes)}]")
    for name, item in (confirmed or {}).items():
        phrase = named("confirmed", name)
        if not isinstance(item, ConfirmedId):
            raise ValueError(f"the ID {phrase} was not confirmed by Nessie")
        parts.append(f"{phrase.replace('_', ' ')} {item.value}")
        facts[phrase] = str(item.value)
    return Sealed(", ".join(parts), facts, _MINT)
