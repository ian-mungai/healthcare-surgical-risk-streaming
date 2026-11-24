"""The rules of the Gold dimensions that need no Spark: the six dimensions with their columns and sources, the surrogate key, the unknown member and what
each check refuses.

``gold.dim_case``, ``gold.dim_patient``, ``gold.dim_track``, ``gold.dim_department``, ``gold.dim_surgery_type`` and ``gold.dim_lab_test`` are Kimball
dimensions of type 1: one row for each member, with a surrogate key, the natural key that the source publishes, a few descriptive columns and one unknown
member. A dimension holds no outcome, no never-feature column, no intraoperative or preoperative value and no laboratory result: ``dim_lab_test`` is the
catalog of test names, and ``dim_patient`` holds the patient number only (the split set stays in ``silver.patient_split``).

The surrogate key of a member is the first 15 hex digits of the SHA-256 of ``<dimension>:<natural key as text>``, read as a number (60 bits, always above
zero and below ``KEY_LIMIT``). The key is a function of the member alone, so a rerun, a rebuild from nothing and a night that adds cases all give the same
key to the same member, and a new member never changes another member's key. The unknown member has the key -1, which no hash can give, and a missing
natural key and missing descriptive columns: a fact whose natural key is missing, or that names a member the dimension does not hold, takes the key -1.
A built dimension keeps every member it held: a member that disappears from the source or whose descriptive columns change is refused, not written.

``dim_track`` has one row for each distinct track name (``bronze.tracks.track``, the device, a slash and the track's own name). The publisher's track list
repeats some track names within a case under different track IDs; the track ID is no column of Bronze or of this dimension, and the members are the distinct
names, so a repeated name adds no member and changes neither the natural key nor the grain. If the descriptive columns of one name differ between rows, the
build refuses it as ``ambiguous_members`` instead of choosing a row.

The test set is sealed: nothing here names a case, a patient or a value. Every failure is a code, a dimension and a count.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from spark.bronze.tables import TRACKS
from spark.silver_cases import BIGINT, CASE_COLUMNS, LAB_COLUMNS, STRING, Column, unit

NAMESPACE = "gold"
UNKNOWN_KEY = -1
KEY_HEX_DIGITS = 15
KEY_LIMIT = 16**KEY_HEX_DIGITS
CASES_SOURCE = "silver.cases"
LAB_SOURCE = "silver.lab_results"
TRACKS_SOURCE = "bronze.tracks"


class GoldDimensionsError(ValueError):
    """A rule was broken or a check failed. ``problems`` holds one message for each cause, each a code, a dimension and a count."""

    def __init__(self, *problems: str) -> None:
        super().__init__("; ".join(problems))
        self.problems = list(problems)


@dataclass(frozen=True, slots=True)
class Dimension:
    """One dimension: its table, its surrogate key column, its natural key and descriptive columns, each with the column of the source table it is read from.

    ``refuse_null_key`` is for a dimension whose source row is the member itself (a case, a patient): a row with no natural key there is a broken source.
    For the others, a row with no natural key (no department, no type) has no member and takes the unknown member.
    """

    name: str
    natural: Column
    source: str
    source_natural: str
    attributes: tuple[tuple[Column, str], ...] = ()
    refuse_null_key: bool = False

    @property
    def key(self) -> str:
        return self.name.removeprefix("dim_") + "_key"

    @property
    def table(self) -> str:
        return f"{NAMESPACE}.{self.name}"

    @property
    def columns(self) -> tuple[Column, ...]:
        return (Column(self.key, BIGINT), self.natural, *(column for column, _ in self.attributes))

    @property
    def source_columns(self) -> tuple[tuple[Column, str], ...]:
        """The natural key and each descriptive column, each with the name of the source column it is read from."""
        return ((self.natural, self.source_natural), *self.attributes)


DIMENSIONS = (
    Dimension(
        "dim_case",
        Column("case_id", BIGINT),
        CASES_SOURCE,
        "caseid",
        (
            (Column("emop", BIGINT), "emop"),
            (Column("approach", STRING), "approach"),
            (Column("position", STRING), "position"),
            (Column("ane_type", STRING), "ane_type"),
        ),
        refuse_null_key=True,
    ),
    Dimension("dim_patient", Column("subject_id", STRING), CASES_SOURCE, "subjectid", refuse_null_key=True),
    Dimension(
        "dim_track",
        Column("track", STRING),
        TRACKS_SOURCE,
        "track",
        (
            (Column("device", STRING), "device"),
            (Column("track_name", STRING), "track_name"),
            (Column("kind", STRING), "kind"),
            (Column("unit", STRING), "unit"),
        ),
    ),
    Dimension("dim_department", Column("department_name", STRING), CASES_SOURCE, "department"),
    Dimension("dim_surgery_type", Column("surgery_type_name", STRING), CASES_SOURCE, "optype"),
    Dimension("dim_lab_test", Column("lab_test_name", STRING), LAB_SOURCE, "name"),
)
BY_NAME = {item.name: item for item in DIMENSIONS}
# The columns and types of the three tables that the job reads, as Silver and Bronze write them.
SOURCE_SCHEMAS: dict[str, list[tuple[str, str]]] = {
    CASES_SOURCE: [(column.name, column.type) for column in CASE_COLUMNS],
    LAB_SOURCE: [(column.name, column.type) for column in LAB_COLUMNS],
    TRACKS_SOURCE: list(TRACKS.columns),
}
INPUT_FACTS = ("source_members", "ambiguous_members", "key_null_rows")
OUTPUT_FACTS = (
    "members",
    "unknown_rows",
    "unknown_not_empty",
    "key_duplicates",
    "key_out_of_range",
    "natural_null",
    "natural_duplicates",
    "key_rule_wrong",
    "source_without_member",
    "member_without_source",
    "member_differs",
)


def dimension(name: str) -> Dimension:
    found = BY_NAME.get(name)
    if found is None:
        raise GoldDimensionsError("bad_dimension: a dimension name is not one of the six")
    return found


def surrogate_key(name: str, natural: int | str) -> int:
    """The surrogate key of the member of the dimension ``name`` whose natural key is ``natural``: a whole number or a non-empty text, taken as its text."""
    dimension(name)
    if type(natural) is int:
        text = str(natural)
    elif type(natural) is str and natural:
        text = natural
    else:
        raise GoldDimensionsError("bad_natural_key: a natural key is not a whole number or a non-empty text")
    digest = hashlib.sha256(f"{name}:{text}".encode()).hexdigest()
    return int(digest[:KEY_HEX_DIGITS], 16)


def source_schema_problems(found: Mapping[str, Sequence[tuple[str, str]] | None]) -> list[str]:
    """The source tables that are missing and the ones whose columns or types are not the expected ones, each as a code and a count. ``found`` maps a table to
    its columns, or to ``None`` when the table is not there."""
    missing = sum(1 for table in SOURCE_SCHEMAS if found.get(table) is None)
    wrong = sum(1 for table, fields in SOURCE_SCHEMAS.items() if found.get(table) is not None and list(found[table] or ()) != fields)
    return [f"{code}: {unit(count, 'table')}" for code, count in (("source_missing", missing), ("source_schema", wrong)) if count]


def count_of(facts: Mapping[str, int], name: str) -> int:
    value = facts.get(name)
    if type(value) is not int or value < 0:
        raise GoldDimensionsError("bad_fact: a count is missing or is not a count")
    return value


def facts_of(found: Mapping[str, Mapping[str, int]], item: Dimension) -> Mapping[str, int]:
    facts = found.get(item.name)
    if facts is None:
        raise GoldDimensionsError("bad_fact: a dimension has no facts")
    return facts


def input_problems(found: Mapping[str, Mapping[str, int]]) -> list[str]:
    """What is wrong with the sources, found before anything is written: a source with no member, a member with two sets of descriptive columns and, for a
    case or a patient, a row with no natural key."""
    problems: list[str] = []
    for item in DIMENSIONS:
        facts = facts_of(found, item)
        members, ambiguous, null_rows = (count_of(facts, name) for name in INPUT_FACTS)
        if members == 0:
            problems.append(f"empty_source: {item.name}, {unit(0, 'member')}")
        if ambiguous:
            problems.append(f"ambiguous_members: {item.name}, {unit(ambiguous, 'member')}")
        if item.refuse_null_key and null_rows:
            problems.append(f"key_null: {item.name}, {unit(null_rows, 'row')}")
    return problems


def output_problems(found: Mapping[str, Mapping[str, int]]) -> list[str]:
    """What is wrong with the written tables: the unknown member, the keys, the natural keys, the key rule and the members against the source."""
    problems: list[str] = []
    for item in DIMENSIONS:
        facts = facts_of(found, item)
        counts = {name: count_of(facts, name) for name in OUTPUT_FACTS}
        if counts["members"] == 0:
            problems.append(f"no_members: {item.name}, {unit(0, 'member')}")
        if counts["unknown_rows"] != 1:
            problems.append(f"unknown_member: {item.name}, {unit(counts['unknown_rows'], 'row')}")
        for code, name, noun in (
            ("unknown_member_not_empty", "unknown_not_empty", "row"),
            ("key_duplicate", "key_duplicates", "key"),
            ("key_out_of_range", "key_out_of_range", "row"),
            ("natural_key_null", "natural_null", "row"),
            ("natural_key_duplicate", "natural_duplicates", "key"),
            ("key_rule", "key_rule_wrong", "member"),
            ("source_without_member", "source_without_member", "member"),
            ("member_without_source", "member_without_source", "member"),
            ("member_value_differs", "member_differs", "member"),
        ):
            if counts[name]:
                problems.append(f"{code}: {item.name}, {unit(counts[name], noun)}")
    return problems


def frozen_problems(held: Mapping[str, tuple[int, int]]) -> list[str]:
    """How a rebuild would change the members that a dimension already holds. ``held`` maps each dimension to the count of held members that the rebuild
    would remove and the count whose descriptive columns it would change. A first build holds nothing and has nothing to refuse."""
    problems: list[str] = []
    for item in DIMENSIONS:
        counts = held.get(item.name)
        if counts is None:
            raise GoldDimensionsError("bad_fact: a dimension has no held state")
        removed, changed = counts
        if removed:
            problems.append(f"member_removed: {item.name}, {unit(removed, 'member')}")
        if changed:
            problems.append(f"member_changed: {item.name}, {unit(changed, 'member')}")
    return problems
