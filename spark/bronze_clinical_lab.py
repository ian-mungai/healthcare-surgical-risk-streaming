"""The rules of the Bronze clinical and laboratory tables that need no Spark: which columns pass through, how the source occurrence is numbered
and how a table is digested.

The Spark job ``spark/jobs/bronze_clinical_lab.py`` copies the loaded tables ``raw_records.clinical_data`` and ``raw_records.lab_data`` into
``bronze.clinical_cases`` and ``bronze.lab_results``. Every published column stays a string and no value is changed, trimmed or cast: an
identifier keeps its leading zeros, and a missing value stays in the form that the loaded table holds. Airbyte's own columns
(``_airbyte_*``) are dropped: they hold a random row ID and the load's times, which differ in every load and would stop a rerun from
reconciling. The lineage of a table is the Nessie hash it was read at, which the job reports.

The key of a clinical row is its case. The key of a laboratory row is its case, time, test name and source occurrence. The published table
can hold two rows with the same case, time and test name (357 such groups in the verified file, 327 of them exact copies), so the occurrence
numbers the rows of a group from 1. The numbering ranks the group's rows by their result as text, so it depends on the table's contents and
never on the order in which a load wrote the rows.
"""

from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

SOURCE_NAMESPACE = "raw_records"
BRONZE_NAMESPACE = "bronze"
AIRBYTE_PREFIX = "_airbyte_"
STRING_TYPE = "string"
OCCURRENCE_COLUMN = "source_occurrence"
LAB_COLUMNS = ("caseid", "dt", "name", "result")
GROUP_COLUMNS = LAB_COLUMNS[:3]
CLINICAL_KEY = ("caseid",)
LAB_KEY = (*GROUP_COLUMNS, OCCURRENCE_COLUMN)
TABLES = {"clinical": ("clinical_data", "clinical_cases", CLINICAL_KEY), "lab": ("lab_data", "lab_results", LAB_KEY)}
MISSING = "n"
REPEATED_ROWS_LIMIT = 100_000
HALF_HEX = 16


@dataclass(frozen=True)
class TableContent:
    """What Nessie holds for an Iceberg table at a reference: the location of its current metadata file and its current snapshot."""

    metadata_location: str
    snapshot_id: int


@dataclass(frozen=True)
class KeyStatistics:
    """The rows of a Bronze table, its distinct keys and the null values in its key columns."""

    rows: int
    distinct: int
    nulls: int


class BronzeRulesError(ValueError):
    """A loaded table does not have the form that Bronze accepts. The message names the table's column and never holds a data value."""


def payload_columns(kind: str, found: Sequence[tuple[str, str]]) -> list[str]:
    """The published columns of a loaded table, in order, once Airbyte's own columns are set aside. ``kind`` is ``clinical`` or ``lab``.

    A column that is not a string is refused, because the copy would have to cast it. A laboratory table must hold exactly ``LAB_COLUMNS``.
    A clinical table must hold its key.
    """
    own = [(name, column_type) for name, column_type in found if not name.startswith(AIRBYTE_PREFIX)]
    names = [name for name, _ in own]
    if len(set(names)) != len(names):
        raise BronzeRulesError("a column name is repeated in the loaded table")
    if OCCURRENCE_COLUMN in names:
        raise BronzeRulesError(f"the loaded table has a column named {OCCURRENCE_COLUMN}, which Bronze adds itself")
    wrong = [name for name, column_type in own if column_type != STRING_TYPE]
    if wrong:
        raise BronzeRulesError(f"{len(wrong)} column(s) are not string columns, and a copy would have to cast them: {', '.join(wrong[:3])}")
    if kind == "lab" and names != list(LAB_COLUMNS):
        raise BronzeRulesError(f"the laboratory table must hold exactly the columns {', '.join(LAB_COLUMNS)}")
    if kind == "clinical" and any(name not in names for name in CLINICAL_KEY):
        raise BronzeRulesError(f"the clinical table has no column {', '.join(CLINICAL_KEY)}")
    return names


def assign_occurrences(
    rows: Iterable[tuple[str | None, str | None, str | None, str | None]],
) -> list[tuple[str | None, str | None, str | None, str | None, int]]:
    """Each laboratory row with its source occurrence, in a fixed order. The result does not depend on the order of ``rows``.

    Rows with the same case, time and test name are numbered from 1 by their result as text, a missing result first. Identical rows get different numbers.
    """
    ordered = sorted(rows, key=lambda row: (row[:3], row[3] is not None, row[3] or ""))
    numbered: list[tuple[str | None, str | None, str | None, str | None, int]] = []
    previous: tuple[str | None, ...] | None = None
    occurrence = 0
    for row in ordered:
        occurrence = occurrence + 1 if row[:3] == previous else 1
        previous = row[:3]
        numbered.append((*row, occurrence))
    return numbered


def row_text(values: Iterable[str | None]) -> str:
    """One row as text for a digest. A value is its length, a colon and itself, a missing value is ``n``, and the fields are joined by ``|``.

    The length makes the text unambiguous for any value, and a missing value differs from an empty one.
    """
    return "|".join(MISSING if value is None else f"{len(value)}:{value}" for value in values)


def digest_of(lines: Iterable[str]) -> str:
    """The digest of a table that does not depend on the order of its rows: ``<rows>:<sum of the high halves>:<sum of the low halves>``.

    Each line gives the SHA-256 of its text, read as two unsigned 64-bit numbers. A changed, moved, missing or repeated row changes the count or a sum.
    """
    rows = high = low = 0
    for line in lines:
        hexadecimal = hashlib.sha256(line.encode()).hexdigest()
        rows += 1
        high += int(hexadecimal[:HALF_HEX], 16)
        low += int(hexadecimal[HALF_HEX : 2 * HALF_HEX], 16)
    return f"{rows}:{high}:{low}"


def occurrence_differences(rows: Iterable[tuple[str | None, str | None, str | None, str | None, int]]) -> int:
    """How many rows differ between the occurrence numbers that Spark wrote and the numbers of ``assign_occurrences``, as multisets of rows.

    ``rows`` are the rows of the repeated groups, each with its ``source_occurrence``. The result is 0 only when the two numberings are the same.
    """
    found = Counter(rows)
    plain = [(case, time, name, result) for case, time, name, result, _ in found.elements()]
    expected = Counter(assign_occurrences(plain))
    return sum(((found - expected) + (expected - found)).values())
